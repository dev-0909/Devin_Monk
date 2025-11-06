"""Base models for MonkDB Data Platform."""

from datetime import datetime
from typing import Optional, Any, Dict
from pydantic import BaseModel as PydanticBaseModel, Field, ConfigDict
from bson import ObjectId


class PyObjectId(ObjectId):
    """Custom ObjectId type for Pydantic."""
    
    @classmethod
    def __get_validators__(cls):
        yield cls.validate
    
    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId")
        return ObjectId(v)
    
    @classmethod
    def __get_pydantic_json_schema__(cls, field_schema):
        field_schema.update(type="string")


class BaseModel(PydanticBaseModel):
    """Base model with common configuration."""
    
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={ObjectId: str},
        validate_assignment=True,
        use_enum_values=True,
        extra="forbid"
    )
    
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    
    class Config:
        json_encoders = {
            ObjectId: str,
            datetime: lambda v: v.isoformat()
        }


class TimestampedModel(BaseModel):
    """Base model with timestamp fields."""
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None
    
    def update_timestamp(self):
        """Update the updated_at timestamp."""
        self.updated_at = datetime.utcnow()


class PaginationModel(PydanticBaseModel):
    """Pagination model for API responses."""
    
    page: int = Field(ge=1, default=1, description="Page number")
    size: int = Field(ge=1, le=100, default=20, description="Page size")
    total: int = Field(ge=0, description="Total number of items")
    pages: int = Field(ge=0, description="Total number of pages")
    
    @classmethod
    def create(cls, page: int, size: int, total: int) -> "PaginationModel":
        """Create pagination model from parameters."""
        pages = (total + size - 1) // size if total > 0 else 0
        return cls(page=page, size=size, total=total, pages=pages)


class ResponseModel(PydanticBaseModel):
    """Generic API response model."""
    
    success: bool = True
    message: Optional[str] = None
    data: Optional[Any] = None
    errors: Optional[Dict[str, Any]] = None
    pagination: Optional[PaginationModel] = None
    
    @classmethod
    def success_response(
        cls,
        data: Any = None,
        message: str = "Success",
        pagination: Optional[PaginationModel] = None
    ) -> "ResponseModel":
        """Create a success response."""
        return cls(
            success=True,
            message=message,
            data=data,
            pagination=pagination
        )
    
    @classmethod
    def error_response(
        cls,
        message: str = "Error",
        errors: Optional[Dict[str, Any]] = None
    ) -> "ResponseModel":
        """Create an error response."""
        return cls(
            success=False,
            message=message,
            errors=errors
        )