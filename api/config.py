"""Configuration management for MonkDB Data Platform API."""

import os
import yaml
from functools import lru_cache
from pathlib import Path
from typing import List, Optional
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings


class MonkDBConfig(BaseModel):
    """MonkDB database configuration."""
    host: str = "localhost"
    port: int = 27017
    database: str = "monkdb"
    username: str = "admin"
    password: str = "password"
    auth_source: str = "admin"
    connection_pool_size: int = 10
    max_idle_time_ms: int = 30000
    server_selection_timeout_ms: int = 5000
    
    @property
    def uri(self) -> str:
        """Generate MongoDB connection URI."""
        return f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}?authSource={self.auth_source}"


class PostgreSQLConfig(BaseModel):
    """PostgreSQL database configuration."""
    host: str = "localhost"
    port: int = 5432
    database: str = "airflow"
    username: str = "airflow"
    password: str = "airflow"
    pool_size: int = 5
    max_overflow: int = 10
    
    @property
    def uri(self) -> str:
        """Generate PostgreSQL connection URI."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class DatabaseConfig(BaseModel):
    """Database configuration container."""
    monkdb: MonkDBConfig = Field(default_factory=MonkDBConfig)
    postgres: PostgreSQLConfig = Field(default_factory=PostgreSQLConfig)


class KafkaTopicsConfig(BaseModel):
    """Kafka topics configuration."""
    data_ingestion: str = "data.ingestion"
    data_processing: str = "data.processing"
    data_output: str = "data.output"
    monitoring: str = "platform.monitoring"


class KafkaConfig(BaseModel):
    """Kafka configuration."""
    bootstrap_servers: str = "localhost:9092"
    client_id: str = "monkdb-platform"
    consumer_group_id: str = "monkdb-consumers"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 1000
    session_timeout_ms: int = 30000
    max_poll_records: int = 500
    topics: KafkaTopicsConfig = Field(default_factory=KafkaTopicsConfig)
    
    @validator('bootstrap_servers')
    def validate_bootstrap_servers(cls, v):
        """Validate bootstrap servers format."""
        if not v or not isinstance(v, str):
            raise ValueError("Bootstrap servers must be a non-empty string")
        return v


class RateLimitingConfig(BaseModel):
    """Rate limiting configuration."""
    enabled: bool = True
    requests_per_minute: int = 100


class APIConfig(BaseModel):
    """API configuration."""
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    reload: bool = False
    workers: int = 1
    secret_key: str = "your-secret-key-here"
    cors_origins: List[str] = Field(default_factory=lambda: ["http://localhost:3000"])
    rate_limiting: RateLimitingConfig = Field(default_factory=RateLimitingConfig)
    
    @validator('secret_key')
    def validate_secret_key(cls, v):
        """Validate secret key strength."""
        if len(v) < 32:
            raise ValueError("Secret key must be at least 32 characters long")
        return v


class MonitoringConfig(BaseModel):
    """Monitoring configuration."""
    metrics_enabled: bool = True
    metrics_port: int = 9090
    metrics_path: str = "/metrics"
    health_check_enabled: bool = True
    health_check_path: str = "/health"
    health_check_timeout: int = 30
    tracing_enabled: bool = False
    jaeger_endpoint: Optional[str] = None


class SecurityConfig(BaseModel):
    """Security configuration."""
    jwt_secret: str = "your-jwt-secret-here"
    jwt_algorithm: str = "HS256"
    jwt_expiration_hours: int = 24
    rbac_enabled: bool = True
    default_role: str = "viewer"
    
    @validator('jwt_secret')
    def validate_jwt_secret(cls, v):
        """Validate JWT secret strength."""
        if len(v) < 32:
            raise ValueError("JWT secret must be at least 32 characters long")
        return v


class DataProcessingConfig(BaseModel):
    """Data processing configuration."""
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay_seconds: int = 5
    timeout_seconds: int = 300
    checkpointing_enabled: bool = True
    checkpointing_interval_seconds: int = 60
    checkpointing_location: str = "/tmp/checkpoints"


class AppConfig(BaseModel):
    """Application configuration."""
    name: str = "monkdb-data-platform"
    version: str = "1.0.0"
    description: str = "Production-ready MonkDB + PySpark + Kafka + Airflow data platform"
    debug: bool = False


class Settings(BaseSettings):
    """Main application settings."""
    app: AppConfig = Field(default_factory=AppConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    data_processing: DataProcessingConfig = Field(default_factory=DataProcessingConfig)
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"
        case_sensitive = False


def load_yaml_config(config_path: str) -> dict:
    """Load configuration from YAML file."""
    path = Path(config_path)
    if not path.exists():
        return {}
    
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {}


def merge_configs(base_config: dict, override_config: dict) -> dict:
    """Recursively merge configuration dictionaries."""
    result = base_config.copy()
    
    for key, value in override_config.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_configs(result[key], value)
        else:
            result[key] = value
    
    return result


@lru_cache()
def get_settings() -> Settings:
    """Get application settings with caching."""
    # Load base configuration
    base_config = load_yaml_config("config/app.yaml")
    
    # Load environment-specific configuration
    environment = os.getenv("ENVIRONMENT", "development")
    env_config = load_yaml_config(f"config/{environment}.yaml")
    
    # Merge configurations
    merged_config = merge_configs(base_config, env_config)
    
    # Create settings instance
    return Settings(**merged_config)