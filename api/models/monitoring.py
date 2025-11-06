"""Monitoring models for MonkDB Data Platform."""

from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List
from pydantic import Field, validator

from .base import BaseModel, TimestampedModel


class HealthStatus(str, Enum):
    """Health check status."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"


class ServiceType(str, Enum):
    """Service types for monitoring."""
    API = "api"
    DATABASE = "database"
    KAFKA = "kafka"
    SPARK = "spark"
    AIRFLOW = "airflow"


class HealthCheckModel(BaseModel):
    """Health check model."""
    
    service: ServiceType = Field(..., description="Service being checked")
    status: HealthStatus = Field(..., description="Health status")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Check timestamp")
    response_time_ms: Optional[float] = Field(None, ge=0, description="Response time in milliseconds")
    details: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional health details")
    error_message: Optional[str] = Field(None, description="Error message if unhealthy")
    
    @validator('response_time_ms')
    def validate_response_time(cls, v):
        """Validate response time is non-negative."""
        if v is not None and v < 0:
            raise ValueError("Response time must be non-negative")
        return v


class MetricType(str, Enum):
    """Metric types."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


class MetricsModel(TimestampedModel):
    """Metrics model."""
    
    name: str = Field(..., description="Metric name")
    type: MetricType = Field(..., description="Metric type")
    value: float = Field(..., description="Metric value")
    labels: Dict[str, str] = Field(default_factory=dict, description="Metric labels")
    help_text: Optional[str] = Field(None, description="Metric description")
    unit: Optional[str] = Field(None, description="Metric unit")
    
    @validator('name')
    def validate_metric_name(cls, v):
        """Validate metric name format."""
        if not v or not isinstance(v, str):
            raise ValueError("Metric name must be a non-empty string")
        # Basic validation for Prometheus metric naming
        if not v.replace('_', '').replace(':', '').isalnum():
            raise ValueError("Metric name must contain only alphanumeric characters, underscores, and colons")
        return v


class SystemMetricsModel(TimestampedModel):
    """System metrics model."""
    
    cpu_usage_percent: float = Field(..., ge=0, le=100, description="CPU usage percentage")
    memory_usage_percent: float = Field(..., ge=0, le=100, description="Memory usage percentage")
    disk_usage_percent: float = Field(..., ge=0, le=100, description="Disk usage percentage")
    network_io_bytes: Dict[str, int] = Field(default_factory=dict, description="Network I/O bytes")
    disk_io_bytes: Dict[str, int] = Field(default_factory=dict, description="Disk I/O bytes")
    load_average: List[float] = Field(default_factory=list, description="System load average")
    
    @validator('cpu_usage_percent', 'memory_usage_percent', 'disk_usage_percent')
    def validate_percentage(cls, v):
        """Validate percentage values."""
        if not 0 <= v <= 100:
            raise ValueError("Percentage must be between 0 and 100")
        return v


class ApplicationMetricsModel(TimestampedModel):
    """Application-specific metrics model."""
    
    active_connections: int = Field(..., ge=0, description="Number of active connections")
    requests_per_second: float = Field(..., ge=0, description="Requests per second")
    average_response_time_ms: float = Field(..., ge=0, description="Average response time in milliseconds")
    error_rate_percent: float = Field(..., ge=0, le=100, description="Error rate percentage")
    queue_size: int = Field(..., ge=0, description="Queue size")
    processed_messages: int = Field(..., ge=0, description="Number of processed messages")
    failed_messages: int = Field(..., ge=0, description="Number of failed messages")
    
    @validator('error_rate_percent')
    def validate_error_rate(cls, v):
        """Validate error rate percentage."""
        if not 0 <= v <= 100:
            raise ValueError("Error rate must be between 0 and 100")
        return v


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertModel(TimestampedModel):
    """Alert model."""
    
    name: str = Field(..., description="Alert name")
    severity: AlertSeverity = Field(..., description="Alert severity")
    message: str = Field(..., description="Alert message")
    service: ServiceType = Field(..., description="Affected service")
    metric_name: Optional[str] = Field(None, description="Related metric name")
    threshold_value: Optional[float] = Field(None, description="Threshold value that triggered the alert")
    current_value: Optional[float] = Field(None, description="Current metric value")
    resolved_at: Optional[datetime] = Field(None, description="Alert resolution timestamp")
    acknowledged_at: Optional[datetime] = Field(None, description="Alert acknowledgment timestamp")
    acknowledged_by: Optional[str] = Field(None, description="User who acknowledged the alert")
    
    def is_active(self) -> bool:
        """Check if the alert is still active."""
        return self.resolved_at is None
    
    def acknowledge(self, user: str):
        """Acknowledge the alert."""
        self.acknowledged_at = datetime.utcnow()
        self.acknowledged_by = user
        self.update_timestamp()
    
    def resolve(self):
        """Resolve the alert."""
        self.resolved_at = datetime.utcnow()
        self.update_timestamp()