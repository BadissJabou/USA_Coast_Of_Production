#!/usr/bin/env python3
"""
FastAPI REST API for Agricultural Data

This module provides a REST API interface for accessing agricultural crop budget data.

Author: Agricultural Data Team
Date: 2024
"""

from fastapi import FastAPI, HTTPException, Query, Path, Depends
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import pandas as pd
import logging
from datetime import datetime
import yaml
from pathlib import Path

# Import our modules
from scripts.database_manager import AgriculturalDatabase
from scripts.data_validator import DataValidator, ValidationResult
from scripts.base_scraper import DataSchema


# Pydantic models for API
class DataRecord(BaseModel):
    """Model for a single data record."""
    item: str = Field(..., description="Cost or revenue item name")
    value: float = Field(..., description="Numerical value")
    unit: str = Field(..., description="Measurement unit")
    commodity: str = Field(..., description="Crop type")
    location: str = Field(..., description="State or region")
    year: str = Field(..., description="Crop year")
    source: str = Field(..., description="Data source")
    soil_type: Optional[str] = Field(None, description="Soil quality")
    rotation: Optional[str] = Field(None, description="Crop rotation type")
    tillage: Optional[str] = Field(None, description="Tillage practice")
    category: Optional[str] = Field(None, description="Cost category")
    notes: Optional[str] = Field(None, description="Additional notes")


class DataResponse(BaseModel):
    """Model for data response."""
    records: List[DataRecord]
    total_count: int
    page: int
    page_size: int
    filters: Dict[str, Any]


class ValidationRequest(BaseModel):
    """Model for validation request."""
    data: List[DataRecord]


class ValidationResponse(BaseModel):
    """Model for validation response."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    quality_score: float
    cleaned_data: Optional[List[DataRecord]] = None


class SummaryResponse(BaseModel):
    """Model for summary response."""
    total_records: int
    unique_sources: List[str]
    unique_commodities: List[str]
    unique_locations: List[str]
    date_range: Dict[str, str]
    database_size: int


class HealthResponse(BaseModel):
    """Model for health check response."""
    status: str
    timestamp: datetime
    version: str
    database_connected: bool


# Initialize FastAPI app
app = FastAPI(
    title="Agricultural Data API",
    description="REST API for agricultural crop budget data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables
db_manager: Optional[AgriculturalDatabase] = None
validator: Optional[DataValidator] = None
logger = logging.getLogger(__name__)


def get_db_manager() -> AgriculturalDatabase:
    """Dependency to get database manager."""
    global db_manager
    if db_manager is None:
        db_manager = AgriculturalDatabase()
    return db_manager


def get_validator() -> DataValidator:
    """Dependency to get data validator."""
    global validator
    if validator is None:
        validator = DataValidator()
    return validator


# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    global db_manager, validator
    try:
        db_manager = AgriculturalDatabase()
        validator = DataValidator()
        logger.info("API services initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    logger.info("API shutdown")


# Health check endpoint
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    try:
        db_connected = db_manager is not None
        return HealthResponse(
            status="healthy" if db_connected else "unhealthy",
            timestamp=datetime.now(),
            version="1.0.0",
            database_connected=db_connected
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail="Health check failed")


# Data endpoints
@app.get("/data", response_model=DataResponse)
async def get_data(
    commodity: Optional[str] = Query(None, description="Filter by commodity"),
    location: Optional[str] = Query(None, description="Filter by location"),
    year: Optional[str] = Query(None, description="Filter by year"),
    source: Optional[str] = Query(None, description="Filter by source"),
    table: str = Query("processed_data", description="Table to query"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=1000, description="Page size"),
    db: AgriculturalDatabase = Depends(get_db_manager)
):
    """Get agricultural data with optional filters."""
    try:
        # Calculate offset
        offset = (page - 1) * page_size
        
        # Query data
        df = db.query_data(
            table=table,
            commodity=commodity,
            location=location,
            year=year,
            source=source,
            limit=page_size
        )
        
        # Convert to records
        records = []
        for _, row in df.iterrows():
            record = DataRecord(
                item=row.get('item', ''),
                value=row.get('value', 0.0),
                unit=row.get('unit', ''),
                commodity=row.get('commodity', ''),
                location=row.get('location', ''),
                year=row.get('year', ''),
                source=row.get('source', ''),
                soil_type=row.get('soil_type'),
                rotation=row.get('rotation'),
                tillage=row.get('tillage'),
                category=row.get('category'),
                notes=row.get('notes')
            )
            records.append(record)
        
        # Get total count for pagination
        total_df = db.query_data(
            table=table,
            commodity=commodity,
            location=location,
            year=year,
            source=source
        )
        total_count = len(total_df)
        
        return DataResponse(
            records=records,
            total_count=total_count,
            page=page,
            page_size=page_size,
            filters={
                "commodity": commodity,
                "location": location,
                "year": year,
                "source": source,
                "table": table
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to get data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/{record_id}")
async def get_record_by_id(
    record_id: int = Path(description="Record ID"),
    table: str = Query("processed_data", description="Table to query"),
    db: AgriculturalDatabase = Depends(get_db_manager)
):
    """Get a specific record by ID."""
    try:
        df = db.query_data(table=table, limit=1)
        if df.empty or record_id not in df['id'].values:
            raise HTTPException(status_code=404, detail="Record not found")
        
        record = df[df['id'] == record_id].iloc[0]
        return DataRecord(
            item=record.get('item', ''),
            value=record.get('value', 0.0),
            unit=record.get('unit', ''),
            commodity=record.get('commodity', ''),
            location=record.get('location', ''),
            year=record.get('year', ''),
            source=record.get('source', ''),
            soil_type=record.get('soil_type'),
            rotation=record.get('rotation'),
            tillage=record.get('tillage'),
            category=record.get('category'),
            notes=record.get('notes')
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get record {record_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/data/validate", response_model=ValidationResponse)
async def validate_data(
    request: ValidationRequest,
    validator: DataValidator = Depends(get_validator)
):
    """Validate agricultural data."""
    try:
        # Convert to DataFrame
        data_dict = [record.dict() for record in request.data]
        df = pd.DataFrame(data_dict)
        
        # Rename columns to match expected format
        column_mapping = {
            'item': 'Item',
            'value': 'Value',
            'unit': 'Unit',
            'commodity': 'Commodity',
            'location': 'Location',
            'year': 'Year',
            'source': 'Source',
            'soil_type': 'Soil_Type',
            'rotation': 'Rotation',
            'tillage': 'Tillage',
            'category': 'Category',
            'notes': 'Notes'
        }
        df = df.rename(columns=column_mapping)
        
        # Validate data
        result = validator.validate_data(df)
        
        # Convert cleaned data back to records
        cleaned_records = None
        if result.cleaned_data is not None:
            cleaned_records = []
            for _, row in result.cleaned_data.iterrows():
                record = DataRecord(
                    item=row.get('Item', ''),
                    value=row.get('Value', 0.0),
                    unit=row.get('Unit', ''),
                    commodity=row.get('Commodity', ''),
                    location=row.get('Location', ''),
                    year=row.get('Year', ''),
                    source=row.get('Source', ''),
                    soil_type=row.get('Soil_Type'),
                    rotation=row.get('Rotation'),
                    tillage=row.get('Tillage'),
                    category=row.get('Category'),
                    notes=row.get('Notes')
                )
                cleaned_records.append(record)
        
        return ValidationResponse(
            is_valid=result.is_valid,
            errors=result.errors,
            warnings=result.warnings,
            quality_score=validator.get_data_quality_score(df),
            cleaned_data=cleaned_records
        )
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/summary", response_model=SummaryResponse)
async def get_summary(db: AgriculturalDatabase = Depends(get_db_manager)):
    """Get database summary statistics."""
    try:
        summary = db.get_data_summary()
        
        return SummaryResponse(
            total_records=summary.get('total_raw_records', 0),
            unique_sources=summary.get('unique_sources', []),
            unique_commodities=summary.get('unique_commodities', []),
            unique_locations=summary.get('unique_locations', []),
            date_range=summary.get('date_range', {}),
            database_size=summary.get('database_size', 0)
        )
        
    except Exception as e:
        logger.error(f"Failed to get summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/export")
async def export_data(
    format: str = Query("excel", description="Export format (excel, csv, json)"),
    table: str = Query("processed_data", description="Table to export"),
    commodity: Optional[str] = Query(None, description="Filter by commodity"),
    location: Optional[str] = Query(None, description="Filter by location"),
    year: Optional[str] = Query(None, description="Filter by year"),
    source: Optional[str] = Query(None, description="Filter by source"),
    db: AgriculturalDatabase = Depends(get_db_manager)
):
    """Export data to file."""
    try:
        # Query filtered data
        df = db.query_data(
            table=table,
            commodity=commodity,
            location=location,
            year=year,
            source=source
        )
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found for export")
        
        # Create temporary file
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{format}") as tmp_file:
            if format.lower() == "excel":
                df.to_excel(tmp_file.name, index=False)
                media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif format.lower() == "csv":
                df.to_csv(tmp_file.name, index=False)
                media_type = "text/csv"
            elif format.lower() == "json":
                df.to_json(tmp_file.name, orient='records', indent=2)
                media_type = "application/json"
            else:
                raise HTTPException(status_code=400, detail="Unsupported format")
            
            # Return file
            return FileResponse(
                path=tmp_file.name,
                media_type=media_type,
                filename=f"agricultural_data.{format}",
                background=lambda: os.unlink(tmp_file.name)  # Cleanup after response
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/commodities")
async def get_commodities(db: AgriculturalDatabase = Depends(get_db_manager)):
    """Get list of available commodities."""
    try:
        summary = db.get_data_summary()
        return {"commodities": summary.get('unique_commodities', [])}
    except Exception as e:
        logger.error(f"Failed to get commodities: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/locations")
async def get_locations(db: AgriculturalDatabase = Depends(get_db_manager)):
    """Get list of available locations."""
    try:
        summary = db.get_data_summary()
        return {"locations": summary.get('unique_locations', [])}
    except Exception as e:
        logger.error(f"Failed to get locations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sources")
async def get_sources(db: AgriculturalDatabase = Depends(get_db_manager)):
    """Get list of available data sources."""
    try:
        summary = db.get_data_summary()
        return {"sources": summary.get('unique_sources', [])}
    except Exception as e:
        logger.error(f"Failed to get sources: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/years")
async def get_years(db: AgriculturalDatabase = Depends(get_db_manager)):
    """Get list of available years."""
    try:
        df = db.query_data(table="processed_data")
        if df.empty:
            return {"years": []}
        
        years = sorted(df['year'].unique().tolist())
        return {"years": years}
    except Exception as e:
        logger.error(f"Failed to get years: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Admin endpoints
@app.post("/admin/cleanup")
async def cleanup_old_data(
    days_to_keep: int = Query(30, ge=1, description="Days to keep data"),
    db: AgriculturalDatabase = Depends(get_db_manager)
):
    """Clean up old data."""
    try:
        deleted_count = db.cleanup_old_data(days_to_keep)
        return {"message": f"Cleaned up {deleted_count} old records", "deleted_count": deleted_count}
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/stats")
async def get_admin_stats(db: AgriculturalDatabase = Depends(get_db_manager)):
    """Get detailed admin statistics."""
    try:
        summary = db.get_data_summary()
        
        # Get additional stats
        raw_df = db.query_data(table="raw_data")
        processed_df = db.query_data(table="processed_data")
        
        stats = {
            "database_summary": summary,
            "raw_data_stats": {
                "total_records": len(raw_df),
                "latest_scrape": raw_df['scraped_at'].max() if not raw_df.empty else None,
                "earliest_scrape": raw_df['scraped_at'].min() if not raw_df.empty else None
            },
            "processed_data_stats": {
                "total_records": len(processed_df),
                "latest_processing": processed_df['processed_at'].max() if not processed_df.empty else None,
                "earliest_processing": processed_df['processed_at'].min() if not processed_df.empty else None
            }
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get admin stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
