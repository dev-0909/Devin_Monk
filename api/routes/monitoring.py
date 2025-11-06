"""Monitoring routes for MonkDB Data Platform API."""

import structlog
from datetime import datetime, timedelta
from fastapi import APIRouter, Request, HTTPException, Query, Depends
from typing import Optional, Dict, Any

from api.models.monitoring import MetricsModel, SystemMetricsModel, ApplicationMetricsModel, AlertModel
from api.models.base import ResponseModel, PaginationModel
from api.services.database import DatabaseService

logger = structlog.get_logger(__name__)
router = APIRouter()


def get_db_service(request: Request) -> DatabaseService:
    """Get database service from request state."""
    return request.app.state.db_service


@router.get("/metrics", response_model=ResponseModel)
async def get_metrics(
    metric_name: Optional[str] = Query(None),
    hours: int = Query(1, ge=1, le=168),  # Max 1 week
    db_service: DatabaseService = Depends(get_db_service)
) -> ResponseModel:
    """Get metrics data."""
    try:
        # Build query filter
        query_filter: dict = {
            "created_at": {"$gte": datetime.utcnow() - timedelta(hours=hours)}
        }
        if metric_name:
            query_filter["name"] = metric_name
        
        # Get metrics
        results = await db_service.find_data(
            "monitoring",
            query_filter,
            sort=[("created_at", -1)],
            limit=1000
        )
        
        logger.info(
            "Retrieved metrics",
            metric_name=metric_name,
            hours=hours,
            count=len(results)
        )
        
        return ResponseModel.success_response(
            data=results,
            message="Metrics retrieved successfully"
        )
    
    except Exception as e:
        logger.error("Failed to get metrics", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to retrieve metrics")


@router.post("/metrics", response_model=ResponseModel)
async def store_metric(
    metric: MetricsModel,
    db_service: DatabaseService = Depends(get_db_service)
) -> ResponseModel:
    """Store a metric data point."""
    try:
        result = await db_service.insert_data("monitoring", metric.dict())
        
        logger.info(
            "Metric stored",
            name=metric.name,
            type=metric.type,
            value=metric.value
        )
        
        return ResponseModel.success_response(
            data={"id": str(result.inserted_id)},
            message="Metric stored successfully"
        )
    
    except Exception as e:
        logger.error("Failed to store metric", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to store metric")


@router.get("/system", response_model=ResponseModel)
async def get_system_metrics() -> ResponseModel:
    """Get current system metrics."""
    try:
        # In a real implementation, this would collect actual system metrics
        # For now, return mock data
        system_metrics = SystemMetricsModel(
            cpu_usage_percent=45.2,
            memory_usage_percent=67.8,
            disk_usage_percent=23.1,
            network_io_bytes={"rx": 1024000, "tx": 512000},
            disk_io_bytes={"read": 2048000, "write": 1024000},
            load_average=[1.2, 1.5, 1.8]
        )
        
        logger.info("Retrieved system metrics")
        
        return ResponseModel.success_response(
            data=system_metrics.dict(),
            message="System metrics retrieved successfully"
        )
    
    except Exception as e:
        logger.error("Failed to get system metrics", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to retrieve system metrics")


@router.get("/application", response_model=ResponseModel)
async def get_application_metrics(
    db_service: DatabaseService = Depends(get_db_service)
) -> ResponseModel:
    """Get current application metrics."""
    try:
        # In a real implementation, this would aggregate actual metrics from database
        # query_filter = {
        #     "created_at": {"$gte": datetime.utcnow() - timedelta(minutes=5)}
        # }
        
        # For now, return mock data
        app_metrics = ApplicationMetricsModel(
            active_connections=25,
            requests_per_second=12.5,
            average_response_time_ms=150.0,
            error_rate_percent=0.5,
            queue_size=10,
            processed_messages=1500,
            failed_messages=5
        )
        
        logger.info("Retrieved application metrics")
        
        return ResponseModel.success_response(
            data=app_metrics.dict(),
            message="Application metrics retrieved successfully"
        )
    
    except Exception as e:
        logger.error("Failed to get application metrics", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to retrieve application metrics")


@router.get("/alerts", response_model=ResponseModel)
async def list_alerts(
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    severity: Optional[str] = Query(None),
    active_only: bool = Query(True),
    db_service: DatabaseService = Depends(get_db_service)
) -> ResponseModel:
    """List alerts with pagination."""
    try:
        # Build query filter
        query_filter: Dict[str, Any] = {}
        if severity:
            query_filter["severity"] = severity
        if active_only:
            query_filter["resolved_at"] = {"$exists": False}
        
        # Get paginated results
        skip = (page - 1) * size
        results = await db_service.find_data(
            "alerts",
            query_filter,
            skip=skip,
            limit=size,
            sort=[("created_at", -1)]
        )
        
        # Get total count
        total = await db_service.count_data("alerts", query_filter)
        
        # Create pagination info
        pagination = PaginationModel.create(page, size, total)
        
        logger.info(
            "Listed alerts",
            page=page,
            size=size,
            total=total,
            severity=severity,
            active_only=active_only
        )
        
        return ResponseModel.success_response(
            data=results,
            message="Alerts retrieved successfully",
            pagination=pagination
        )
    
    except Exception as e:
        logger.error("Failed to list alerts", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to retrieve alerts")


@router.post("/alerts", response_model=ResponseModel)
async def create_alert(
    alert: AlertModel,
    db_service: DatabaseService = Depends(get_db_service)
) -> ResponseModel:
    """Create a new alert."""
    try:
        result = await db_service.insert_data("alerts", alert.dict())
        
        logger.warning(
            "Alert created",
            alert_id=str(result.inserted_id),
            name=alert.name,
            severity=alert.severity,
            service=alert.service
        )
        
        return ResponseModel.success_response(
            data={"id": str(result.inserted_id)},
            message="Alert created successfully"
        )
    
    except Exception as e:
        logger.error("Failed to create alert", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to create alert")


@router.patch("/alerts/{alert_id}/acknowledge", response_model=ResponseModel)
async def acknowledge_alert(
    alert_id: str,
    user: str = Query(..., description="User acknowledging the alert"),
    db_service: DatabaseService = Depends(get_db_service)
) -> ResponseModel:
    """Acknowledge an alert."""
    try:
        # Update alert with acknowledgment
        update_data = {
            "acknowledged_at": datetime.utcnow(),
            "acknowledged_by": user,
            "updated_at": datetime.utcnow()
        }
        
        result = await db_service.update_data(
            "alerts",
            {"_id": alert_id},
            {"$set": update_data}
        )
        
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Alert not found")
        
        logger.info(
            "Alert acknowledged",
            alert_id=alert_id,
            acknowledged_by=user
        )
        
        return ResponseModel.success_response(
            message="Alert acknowledged successfully"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to acknowledge alert", alert_id=alert_id, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to acknowledge alert")


@router.patch("/alerts/{alert_id}/resolve", response_model=ResponseModel)
async def resolve_alert(
    alert_id: str,
    db_service: DatabaseService = Depends(get_db_service)
) -> ResponseModel:
    """Resolve an alert."""
    try:
        # Update alert with resolution
        update_data = {
            "resolved_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        
        result = await db_service.update_data(
            "alerts",
            {"_id": alert_id},
            {"$set": update_data}
        )
        
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Alert not found")
        
        logger.info("Alert resolved", alert_id=alert_id)
        
        return ResponseModel.success_response(
            message="Alert resolved successfully"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to resolve alert", alert_id=alert_id, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to resolve alert")