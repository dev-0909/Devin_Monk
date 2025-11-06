"""Data management routes for MonkDB Data Platform API."""

import structlog
from fastapi import APIRouter, Request, HTTPException, Query, Depends
from typing import Optional

from api.models.data import DataIngestionModel, DataProcessingModel, DataOutputModel
from api.models.base import ResponseModel, PaginationModel
from api.services.database import DatabaseService

logger = structlog.get_logger(__name__)
router = APIRouter()


def get_db_service(request: Request) -> DatabaseService:
    """Get database service from request state."""
    return request.app.state.db_service


@router.post("/ingest", response_model=ResponseModel)
async def ingest_data(
    data: DataIngestionModel,
    request: Request,
    db_service: DatabaseService = Depends(get_db_service)
) -> ResponseModel:
    """Ingest data into the platform."""
    try:
        # Store ingested data
        result = await db_service.insert_data("data_ingestion", data.dict())
        
        logger.info(
            "Data ingested successfully",
            data_id=str(result.inserted_id),
            source_type=data.source_type,
            size_bytes=data.size_bytes
        )
        
        return ResponseModel.success_response(
            data={"id": str(result.inserted_id)},
            message="Data ingested successfully"
        )
    
    except Exception as e:
        logger.error("Data ingestion failed", error=str(e))
        raise HTTPException(status_code=500, detail="Data ingestion failed")


@router.get("/ingestion", response_model=ResponseModel)
async def list_ingested_data(
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    source_type: Optional[str] = Query(None),
    db_service: DatabaseService = Depends(get_db_service)
) -> ResponseModel:
    """List ingested data with pagination."""
    try:
        # Build query filter
        query_filter = {}
        if source_type:
            query_filter["source_type"] = source_type
        
        # Get paginated results
        skip = (page - 1) * size
        results = await db_service.find_data(
            "data_ingestion",
            query_filter,
            skip=skip,
            limit=size,
            sort=[("created_at", -1)]
        )
        
        # Get total count
        total = await db_service.count_data("data_ingestion", query_filter)
        
        # Create pagination info
        pagination = PaginationModel.create(page, size, total)
        
        logger.info(
            "Listed ingested data",
            page=page,
            size=size,
            total=total,
            source_type=source_type
        )
        
        return ResponseModel.success_response(
            data=results,
            message="Ingested data retrieved successfully",
            pagination=pagination
        )
    
    except Exception as e:
        logger.error("Failed to list ingested data", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to retrieve data")


@router.post("/process", response_model=ResponseModel)
async def create_processing_job(
    job: DataProcessingModel,
    db_service: DatabaseService = Depends(get_db_service)
) -> ResponseModel:
    """Create a new data processing job."""
    try:
        # Store processing job
        result = await db_service.insert_data("data_processing", job.dict())
        
        logger.info(
            "Processing job created",
            job_id=job.job_id,
            job_name=job.job_name,
            input_data_id=job.input_data_id
        )
        
        return ResponseModel.success_response(
            data={"id": str(result.inserted_id), "job_id": job.job_id},
            message="Processing job created successfully"
        )
    
    except Exception as e:
        logger.error("Failed to create processing job", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to create processing job")


@router.get("/processing", response_model=ResponseModel)
async def list_processing_jobs(
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    status: Optional[str] = Query(None),
    db_service: DatabaseService = Depends(get_db_service)
) -> ResponseModel:
    """List processing jobs with pagination."""
    try:
        # Build query filter
        query_filter = {}
        if status:
            query_filter["status"] = status
        
        # Get paginated results
        skip = (page - 1) * size
        results = await db_service.find_data(
            "data_processing",
            query_filter,
            skip=skip,
            limit=size,
            sort=[("created_at", -1)]
        )
        
        # Get total count
        total = await db_service.count_data("data_processing", query_filter)
        
        # Create pagination info
        pagination = PaginationModel.create(page, size, total)
        
        logger.info(
            "Listed processing jobs",
            page=page,
            size=size,
            total=total,
            status=status
        )
        
        return ResponseModel.success_response(
            data=results,
            message="Processing jobs retrieved successfully",
            pagination=pagination
        )
    
    except Exception as e:
        logger.error("Failed to list processing jobs", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to retrieve processing jobs")


@router.get("/processing/{job_id}", response_model=ResponseModel)
async def get_processing_job(
    job_id: str,
    db_service: DatabaseService = Depends(get_db_service)
) -> ResponseModel:
    """Get a specific processing job by ID."""
    try:
        result = await db_service.find_one_data("data_processing", {"job_id": job_id})
        
        if not result:
            raise HTTPException(status_code=404, detail="Processing job not found")
        
        logger.info("Retrieved processing job", job_id=job_id)
        
        return ResponseModel.success_response(
            data=result,
            message="Processing job retrieved successfully"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get processing job", job_id=job_id, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to retrieve processing job")


@router.post("/output", response_model=ResponseModel)
async def store_output_data(
    output: DataOutputModel,
    db_service: DatabaseService = Depends(get_db_service)
) -> ResponseModel:
    """Store processed output data."""
    try:
        # Store output data
        result = await db_service.insert_data("data_output", output.dict())
        
        logger.info(
            "Output data stored",
            job_id=output.job_id,
            output_type=output.output_type,
            size_bytes=output.size_bytes,
            quality_score=output.quality_score
        )
        
        return ResponseModel.success_response(
            data={"id": str(result.inserted_id)},
            message="Output data stored successfully"
        )
    
    except Exception as e:
        logger.error("Failed to store output data", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to store output data")


@router.get("/output", response_model=ResponseModel)
async def list_output_data(
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    job_id: Optional[str] = Query(None),
    output_type: Optional[str] = Query(None),
    db_service: DatabaseService = Depends(get_db_service)
) -> ResponseModel:
    """List output data with pagination."""
    try:
        # Build query filter
        query_filter = {}
        if job_id:
            query_filter["job_id"] = job_id
        if output_type:
            query_filter["output_type"] = output_type
        
        # Get paginated results
        skip = (page - 1) * size
        results = await db_service.find_data(
            "data_output",
            query_filter,
            skip=skip,
            limit=size,
            sort=[("created_at", -1)]
        )
        
        # Get total count
        total = await db_service.count_data("data_output", query_filter)
        
        # Create pagination info
        pagination = PaginationModel.create(page, size, total)
        
        logger.info(
            "Listed output data",
            page=page,
            size=size,
            total=total,
            job_id=job_id,
            output_type=output_type
        )
        
        return ResponseModel.success_response(
            data=results,
            message="Output data retrieved successfully",
            pagination=pagination
        )
    
    except Exception as e:
        logger.error("Failed to list output data", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to retrieve output data")