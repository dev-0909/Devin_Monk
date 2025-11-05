"""Health check routes for MonkDB Data Platform API."""

import structlog
from datetime import datetime
from fastapi import APIRouter, Request, HTTPException

from api.models.monitoring import HealthCheckModel, HealthStatus, ServiceType
from api.models.base import ResponseModel

logger = structlog.get_logger(__name__)
router = APIRouter()


@router.get("/", response_model=ResponseModel)
async def health_check(request: Request) -> ResponseModel:
    """Basic health check endpoint."""
    try:
        # Basic health check
        health_data = {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0",
            "environment": "development"  # This should come from config
        }
        
        logger.info("Health check requested", client_ip=request.client.host)
        
        return ResponseModel.success_response(
            data=health_data,
            message="Service is healthy"
        )
    
    except Exception as e:
        logger.error("Health check failed", error=str(e))
        raise HTTPException(status_code=503, detail="Service unavailable")


@router.get("/detailed", response_model=ResponseModel)
async def detailed_health_check(request: Request) -> ResponseModel:
    """Detailed health check with service dependencies."""
    try:
        health_checks = []
        overall_status = HealthStatus.HEALTHY
        
        # Check API service
        api_check = HealthCheckModel(
            service=ServiceType.API,
            status=HealthStatus.HEALTHY,
            response_time_ms=1.0,
            details={"uptime": "running"}
        )
        health_checks.append(api_check)
        
        # Check database connectivity
        try:
            db_service = request.app.state.db_service
            await db_service.ping()
            db_check = HealthCheckModel(
                service=ServiceType.DATABASE,
                status=HealthStatus.HEALTHY,
                response_time_ms=5.0,
                details={"connection": "active"}
            )
        except Exception as e:
            db_check = HealthCheckModel(
                service=ServiceType.DATABASE,
                status=HealthStatus.UNHEALTHY,
                error_message=str(e)
            )
            overall_status = HealthStatus.UNHEALTHY
        
        health_checks.append(db_check)
        
        # Check Kafka connectivity
        try:
            kafka_service = request.app.state.kafka_service
            await kafka_service.ping()
            kafka_check = HealthCheckModel(
                service=ServiceType.KAFKA,
                status=HealthStatus.HEALTHY,
                response_time_ms=10.0,
                details={"brokers": "connected"}
            )
        except Exception as e:
            kafka_check = HealthCheckModel(
                service=ServiceType.KAFKA,
                status=HealthStatus.UNHEALTHY,
                error_message=str(e)
            )
            if overall_status == HealthStatus.HEALTHY:
                overall_status = HealthStatus.DEGRADED
        
        health_checks.append(kafka_check)
        
        response_data = {
            "overall_status": overall_status.value,
            "timestamp": datetime.utcnow().isoformat(),
            "checks": [check.dict() for check in health_checks]
        }
        
        logger.info(
            "Detailed health check completed",
            overall_status=overall_status.value,
            client_ip=request.client.host
        )
        
        # Return appropriate HTTP status
        if overall_status == HealthStatus.UNHEALTHY:
            raise HTTPException(
                status_code=503,
                detail=ResponseModel.error_response(
                    message="Service unhealthy",
                    errors={"health_checks": response_data}
                ).dict()
            )
        
        return ResponseModel.success_response(
            data=response_data,
            message=f"Service status: {overall_status.value}"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Detailed health check failed", error=str(e))
        raise HTTPException(status_code=500, detail="Health check failed")


@router.get("/ready", response_model=ResponseModel)
async def readiness_check(request: Request) -> ResponseModel:
    """Readiness check for Kubernetes."""
    try:
        # Check if all critical services are ready
        db_service = request.app.state.db_service
        kafka_service = request.app.state.kafka_service
        
        # Perform quick connectivity checks
        await db_service.ping()
        await kafka_service.ping()
        
        logger.info("Readiness check passed", client_ip=request.client.host)
        
        return ResponseModel.success_response(
            data={"ready": True, "timestamp": datetime.utcnow().isoformat()},
            message="Service is ready"
        )
    
    except Exception as e:
        logger.error("Readiness check failed", error=str(e))
        raise HTTPException(status_code=503, detail="Service not ready")


@router.get("/live", response_model=ResponseModel)
async def liveness_check() -> ResponseModel:
    """Liveness check for Kubernetes."""
    # Simple liveness check - just return that the service is alive
    return ResponseModel.success_response(
        data={"alive": True, "timestamp": datetime.utcnow().isoformat()},
        message="Service is alive"
    )