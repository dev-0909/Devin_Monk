"""Middleware for MonkDB Data Platform API."""

import time
import structlog
from datetime import datetime, timedelta
from fastapi import Request, Response, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from typing import Dict, Optional
from collections import defaultdict, deque

logger = structlog.get_logger(__name__)


class LoggingMiddleware(BaseHTTPMiddleware):
    """Middleware for structured request/response logging."""
    
    async def dispatch(self, request: Request, call_next):
        """Process request and log details."""
        start_time = time.time()
        
        # Extract request details
        request_id = id(request)
        client_ip = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "unknown")
        
        # Log request
        logger.info(
            "Request started",
            request_id=request_id,
            method=request.method,
            url=str(request.url),
            client_ip=client_ip,
            user_agent=user_agent
        )
        
        try:
            # Process request
            response = await call_next(request)
            
            # Calculate processing time
            process_time = time.time() - start_time
            
            # Log response
            logger.info(
                "Request completed",
                request_id=request_id,
                method=request.method,
                url=str(request.url),
                status_code=response.status_code,
                process_time_ms=round(process_time * 1000, 2),
                client_ip=client_ip
            )
            
            # Add processing time header
            response.headers["X-Process-Time"] = str(process_time)
            
            return response
            
        except Exception as e:
            # Calculate processing time for errors
            process_time = time.time() - start_time
            
            # Log error
            logger.error(
                "Request failed",
                request_id=request_id,
                method=request.method,
                url=str(request.url),
                error=str(e),
                process_time_ms=round(process_time * 1000, 2),
                client_ip=client_ip
            )
            
            raise


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Middleware for rate limiting requests."""
    
    def __init__(self, app, requests_per_minute: int = 100):
        """Initialize rate limiting middleware."""
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        self.requests: Dict[str, deque] = defaultdict(deque)
        self.window_size = timedelta(minutes=1)
    
    def _get_client_id(self, request: Request) -> str:
        """Get client identifier for rate limiting."""
        # Use client IP as identifier
        # In production, you might want to use API keys or user IDs
        return request.client.host if request.client else "unknown"
    
    def _is_rate_limited(self, client_id: str) -> bool:
        """Check if client is rate limited."""
        now = datetime.utcnow()
        window_start = now - self.window_size
        
        # Get client's request history
        client_requests = self.requests[client_id]
        
        # Remove old requests outside the window
        while client_requests and client_requests[0] < window_start:
            client_requests.popleft()
        
        # Check if client has exceeded the limit
        if len(client_requests) >= self.requests_per_minute:
            return True
        
        # Add current request
        client_requests.append(now)
        
        return False
    
    async def dispatch(self, request: Request, call_next):
        """Process request with rate limiting."""
        client_id = self._get_client_id(request)
        
        # Check rate limit
        if self._is_rate_limited(client_id):
            logger.warning(
                "Rate limit exceeded",
                client_id=client_id,
                method=request.method,
                url=str(request.url),
                limit=self.requests_per_minute
            )
            
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "Rate limit exceeded",
                    "limit": self.requests_per_minute,
                    "window": "1 minute",
                    "retry_after": 60
                },
                headers={"Retry-After": "60"}
            )
        
        # Process request
        response = await call_next(request)
        
        # Add rate limit headers
        client_requests = self.requests[client_id]
        remaining = max(0, self.requests_per_minute - len(client_requests))
        
        response.headers["X-RateLimit-Limit"] = str(self.requests_per_minute)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(int((datetime.utcnow() + self.window_size).timestamp()))
        
        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Middleware for adding security headers."""
    
    async def dispatch(self, request: Request, call_next):
        """Add security headers to response."""
        response = await call_next(request)
        
        # Add security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Content-Security-Policy"] = "default-src 'self'"
        
        return response


class CORSMiddleware(BaseHTTPMiddleware):
    """Custom CORS middleware with logging."""
    
    def __init__(self, app, allow_origins: Optional[list] = None, allow_methods: Optional[list] = None, allow_headers: Optional[list] = None):
        """Initialize CORS middleware."""
        super().__init__(app)
        self.allow_origins = allow_origins or ["*"]
        self.allow_methods = allow_methods or ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
        self.allow_headers = allow_headers or ["*"]
    
    async def dispatch(self, request: Request, call_next):
        """Handle CORS for requests."""
        origin = request.headers.get("origin")
        
        # Handle preflight requests
        if request.method == "OPTIONS":
            response = Response()
            
            if origin and (origin in self.allow_origins or "*" in self.allow_origins):
                response.headers["Access-Control-Allow-Origin"] = origin
                response.headers["Access-Control-Allow-Methods"] = ", ".join(self.allow_methods)
                response.headers["Access-Control-Allow-Headers"] = ", ".join(self.allow_headers)
                response.headers["Access-Control-Max-Age"] = "86400"
                
                logger.debug(
                    "CORS preflight handled",
                    origin=origin,
                    method=request.method
                )
            
            return response
        
        # Process actual request
        response = await call_next(request)
        
        # Add CORS headers to response
        if origin and (origin in self.allow_origins or "*" in self.allow_origins):
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Access-Control-Allow-Credentials"] = "true"
        
        return response


class RequestSizeLimitMiddleware(BaseHTTPMiddleware):
    """Middleware for limiting request body size."""
    
    def __init__(self, app, max_size_bytes: int = 10 * 1024 * 1024):  # 10MB default
        """Initialize request size limit middleware."""
        super().__init__(app)
        self.max_size_bytes = max_size_bytes
    
    async def dispatch(self, request: Request, call_next):
        """Check request size before processing."""
        content_length_str = request.headers.get("content-length")
        
        if content_length_str:
            content_length = int(content_length_str)
            
            if content_length > self.max_size_bytes:
                logger.warning(
                    "Request size limit exceeded",
                    content_length=content_length,
                    max_size=self.max_size_bytes,
                    client_ip=request.client.host if request.client else "unknown"
                )
                
                raise HTTPException(
                    status_code=413,
                    detail={
                        "error": "Request entity too large",
                        "max_size_bytes": self.max_size_bytes,
                        "received_size_bytes": content_length
                    }
                )
        
        return await call_next(request)