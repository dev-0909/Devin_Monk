"""Data models for MonkDB Data Platform."""

from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List
from pydantic import Field, validator

from .base import TimestampedModel


class DataSourceType(str, Enum):
    """Data source types."""
    KAFKA = "kafka"
    MONKDB = "monkdb"
    API = "api"
    FILE = "file"
    STREAM = "stream"


class ProcessingStatus(str, Enum):
    """Data processing status."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class DataIngestionModel(TimestampedModel):
    """Data ingestion model."""
    
    source_type: DataSourceType = Field(..., description="Type of data source")
    source_id: str = Field(..., description="Unique identifier for the data source")
    topic: Optional[str] = Field(None, description="Kafka topic name")
    partition: Optional[int] = Field(None, description="Kafka partition")
    offset: Optional[int] = Field(None, description="Kafka offset")
    data: Dict[str, Any] = Field(..., description="Raw data payload")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional metadata")
    size_bytes: Optional[int] = Field(None, description="Data size in bytes")
    checksum: Optional[str] = Field(None, description="Data checksum for integrity")
    
    @validator('data')
    def validate_data_not_empty(cls, v):
        """Validate that data is not empty."""
        if not v:
            raise ValueError("Data payload cannot be empty")
        return v
    
    @validator('size_bytes')
    def validate_size_bytes(cls, v):
        """Validate size_bytes is positive."""
        if v is not None and v < 0:
            raise ValueError("Size bytes must be non-negative")
        return v


class DataProcessingModel(TimestampedModel):
    """Data processing model."""
    
    job_id: str = Field(..., description="Unique job identifier")
    job_name: str = Field(..., description="Human-readable job name")
    input_data_id: str = Field(..., description="Reference to input data")
    status: ProcessingStatus = Field(default=ProcessingStatus.PENDING, description="Processing status")
    spark_application_id: Optional[str] = Field(None, description="Spark application ID")
    processing_config: Dict[str, Any] = Field(default_factory=dict, description="Processing configuration")
    progress_percentage: float = Field(default=0.0, ge=0.0, le=100.0, description="Processing progress")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    retry_count: int = Field(default=0, ge=0, description="Number of retry attempts")
    max_retries: int = Field(default=3, ge=0, description="Maximum retry attempts")
    started_at: Optional[datetime] = Field(None, description="Processing start time")
    completed_at: Optional[datetime] = Field(None, description="Processing completion time")
    duration_seconds: Optional[float] = Field(None, ge=0, description="Processing duration")
    
    @validator('progress_percentage')
    def validate_progress(cls, v):
        """Validate progress percentage."""
        if not 0.0 <= v <= 100.0:
            raise ValueError("Progress percentage must be between 0 and 100")
        return v
    
    def mark_started(self):
        """Mark the job as started."""
        self.status = ProcessingStatus.PROCESSING
        self.started_at = datetime.utcnow()
        self.update_timestamp()
    
    def mark_completed(self):
        """Mark the job as completed."""
        self.status = ProcessingStatus.COMPLETED
        self.completed_at = datetime.utcnow()
        self.progress_percentage = 100.0
        if self.started_at:
            self.duration_seconds = (self.completed_at - self.started_at).total_seconds()
        self.update_timestamp()
    
    def mark_failed(self, error_message: str):
        """Mark the job as failed."""
        self.status = ProcessingStatus.FAILED
        self.error_message = error_message
        self.completed_at = datetime.utcnow()
        if self.started_at:
            self.duration_seconds = (self.completed_at - self.started_at).total_seconds()
        self.update_timestamp()
    
    def can_retry(self) -> bool:
        """Check if the job can be retried."""
        return self.retry_count < self.max_retries and self.status == ProcessingStatus.FAILED


class DataOutputModel(TimestampedModel):
    """Data output model."""
    
    job_id: str = Field(..., description="Reference to processing job")
    output_type: str = Field(..., description="Type of output data")
    destination: str = Field(..., description="Output destination")
    data: Dict[str, Any] = Field(..., description="Processed data")
    schema_version: str = Field(default="1.0", description="Data schema version")
    quality_score: Optional[float] = Field(None, ge=0.0, le=1.0, description="Data quality score")
    validation_errors: List[str] = Field(default_factory=list, description="Validation errors")
    size_bytes: Optional[int] = Field(None, description="Output data size in bytes")
    compression_ratio: Optional[float] = Field(None, ge=0.0, description="Compression ratio")
    
    @validator('quality_score')
    def validate_quality_score(cls, v):
        """Validate quality score."""
        if v is not None and not 0.0 <= v <= 1.0:
            raise ValueError("Quality score must be between 0.0 and 1.0")
        return v
    
    @validator('data')
    def validate_output_data(cls, v):
        """Validate output data is not empty."""
        if not v:
            raise ValueError("Output data cannot be empty")
        return v
    
    def is_valid(self) -> bool:
        """Check if the output data is valid."""
        return len(self.validation_errors) == 0 and (
            self.quality_score is None or self.quality_score >= 0.8
        )