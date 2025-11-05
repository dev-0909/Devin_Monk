"""Main FastAPI application for MonkDB Data Platform."""

import structlog
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from typing import AsyncGenerator

from api.config import get_settings
from api.routes import health, data, monitoring
from api.middleware import LoggingMiddleware, RateLimitMiddleware
from api.services.database import DatabaseService
from api.services.kafka import KafkaService

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

# Metrics
REQUEST_COUNT = Counter('http_requests_total', 'Total HTTP requests', ['method', 'endpoint', 'status'])
REQUEST_DURATION = Histogram('http_request_duration_seconds', 'HTTP request duration')


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager."""
    logger.info("Starting MonkDB Data Platform API")
    
    # Initialize services
    settings = get_settings()
    
    # Initialize database connection
    db_service = DatabaseService(settings.database.monkdb)
    await db_service.connect()
    app.state.db_service = db_service
    
    # Initialize Kafka service
    kafka_service = KafkaService(settings.kafka)
    await kafka_service.connect()
    app.state.kafka_service = kafka_service
    
    logger.info("API startup complete")
    
    yield
    
    # Cleanup
    logger.info("Shutting down MonkDB Data Platform API")
    await db_service.disconnect()
    await kafka_service.disconnect()
    logger.info("API shutdown complete")


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    settings = get_settings()
    
    app = FastAPI(
        title="MonkDB Data Platform API",
        description="Production-ready MonkDB + PySpark + Kafka + Airflow data platform",
        version="1.0.0",
        docs_url="/docs" if settings.api.debug else None,
        redoc_url="/redoc" if settings.api.debug else None,
        lifespan=lifespan
    )
    
    # Add middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.api.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=["*"]  # Configure appropriately for production
    )
    
    app.add_middleware(LoggingMiddleware)
    
    if settings.api.rate_limiting.enabled:
        app.add_middleware(
            RateLimitMiddleware,
            requests_per_minute=settings.api.rate_limiting.requests_per_minute
        )
    
    # Add request metrics middleware
    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next):
        with REQUEST_DURATION.time():
            response = await call_next(request)
            REQUEST_COUNT.labels(
                method=request.method,
                endpoint=request.url.path,
                status=response.status_code
            ).inc()
            return response
    
    # Include routers
    app.include_router(health.router, prefix="/health", tags=["health"])
    app.include_router(data.router, prefix="/api/v1/data", tags=["data"])
    app.include_router(monitoring.router, prefix="/monitoring", tags=["monitoring"])
    
    # Metrics endpoint
    @app.get("/metrics")
    async def metrics():
        """Prometheus metrics endpoint."""
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
    
    # Global exception handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error("Unhandled exception", exc_info=exc, path=request.url.path)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"}
        )
    
    return app


# Create app instance
app = create_app()


if __name__ == "__main__":
    import uvicorn
    
    settings = get_settings()
    uvicorn.run(
        "api.main:app",
        host=settings.api.host,
        port=settings.api.port,
        reload=settings.api.reload,
        workers=settings.api.workers if not settings.api.reload else 1,
        log_config=None  # Use structlog configuration
    )