import os
import uuid
from io import BytesIO
from typing import List, Dict, Any, Optional
from datetime import timedelta

import pandas as pd
from pydantic import BaseModel
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend 
from fastapi_cache.decorator import cache

from database import get_db, FileMetadata, create_tables
from minio_service import minio_service
from config import settings

class FileMetadataResponse(BaseModel):
    id: int
    filename: str
    format: str

    class Config:
        from_attributes = True

class MergedPreviewResponse(BaseModel):
    cache_key: str
    preview: List[Dict[str, Any]]
    message: str

def get_file_dataframe(minio_data: BytesIO, file_format: str) -> pd.DataFrame:
    """Reads file bytes into a pandas DataFrame."""
    try:
        if file_format.lower() == 'csv':
            return pd.read_csv(minio_data)
        elif file_format.lower() in ('xlsx', 'xls'):
            return pd.read_excel(minio_data)
        else:
            raise ValueError("Unsupported file format.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading file into DataFrame: {e}")

app = FastAPI(title="API Data Service (Native Setup)")

@app.on_event("startup")
async def startup():
    try:
        create_tables() 
    except Exception as e:
        print(f"FATAL: Database connection/table creation error: {e}")

    try:
        FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")
        print("FastAPI Cache initialized with In-Memory Backend.")
    except Exception as e:
        print(f"FATAL: Cache initialization error: {e}")

@app.post("/files/upload", status_code=201, response_model=FileMetadataResponse, tags=["File Operations"])
async def upload_file(
    file: UploadFile = File(...), 
    db: Session = Depends(get_db)
):
    """ Uploads a CSV or Excel file, stores metadata in PostgreSQL, and the file in MinIO. """
    
    ext = os.path.splitext(file.filename)[1].lower()
    file_format = ext.strip('.')
    if file_format not in ["csv", "xlsx"]:
        raise HTTPException(status_code=400, detail="Invalid file format.")

    filename = file.filename
    file_content = await file.read()
    
    try:
        minio_service.upload_file(filename, file_content, file.content_type)
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=f"MinIO Service Unavailable: {e}")
    except Exception as e:
         raise HTTPException(status_code=500, detail=f"Failed to upload file to MinIO: {e}")

    try:
        db_file = FileMetadata(filename=filename, format=file_format)
        db.add(db_file)
        db.commit()
        db.refresh(db_file)
        return db_file
    except Exception:
        raise HTTPException(status_code=500, detail="Database error: Failed to store file metadata.")

@app.get("/files", response_model=List[FileMetadataResponse], tags=["File Operations"])
def view_stored_files(db: Session = Depends(get_db)):
    """Retrieves a list of all file metadata stored in the database."""
    files = db.query(FileMetadata).all()
    return files

@app.get("/files/merge", response_model=MergedPreviewResponse, tags=["Data Operations"])
async def merge_files_temporarily(
    file_id_1: int = Query(..., description="ID of the first file to merge"),
    file_id_2: int = Query(..., description="ID of the second file to merge"),
    common_column: str = Query(..., description="The column name to merge on"),
    db: Session = Depends(get_db)
):
    """ Fetches two files, merges them, caches the result in memory, and returns a preview and the cache key. """
    
    meta1 = db.query(FileMetadata).filter(FileMetadata.id == file_id_1).first()
    meta2 = db.query(FileMetadata).filter(FileMetadata.id == file_id_2).first()
    if not meta1 or not meta2:
        raise HTTPException(status_code=404, detail="One or both file IDs not found.")

    try:
        data1 = minio_service.fetch_file(meta1.filename)
        data2 = minio_service.fetch_file(meta2.filename)
        df1 = get_file_dataframe(data1, meta1.format)
        df2 = get_file_dataframe(data2, meta2.format)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching/reading files: {e}")
    
    if common_column not in df1.columns or common_column not in df2.columns:
        raise HTTPException(status_code=400, detail=f"Common column '{common_column}' not found.")

    merged_df = pd.merge(df1, df2, on=common_column, how='inner')

    merged_json = merged_df.to_json(orient="records")
    cache_key = str(uuid.uuid4())
    
    cache_backend = FastAPICache.get_backend()
    await cache_backend.set(cache_key, merged_json, expire=int(timedelta(hours=1).total_seconds())) 

    preview_data = merged_df.head().to_dict(orient="records")
    
    return MergedPreviewResponse(
        cache_key=cache_key, 
        preview=preview_data,
        message=f"Merged dataset cached in memory with key: {cache_key}"
    )

@app.post("/files/save_merged", status_code=201, response_model=FileMetadataResponse, tags=["Data Operations"])
async def save_merged_dataset(
    cache_key: str = Query(..., description="Temporary cache key from /files/merge"),
    new_filename: str = Query(..., description="Desired name for the new merged file (e.g., final_data.csv)"),
    db: Session = Depends(get_db)
):
    """ Retrieves the merged dataset from cache, saves it permanently to MinIO, and updates PostgreSQL metadata. """
    
    cache_backend = FastAPICache.get_backend()
    merged_json = await cache_backend.get(cache_key)
    
    if not merged_json:
        raise HTTPException(status_code=404, detail="Merged dataset not found or has expired in cache.")

    merged_df = pd.read_json(merged_json, orient="records")

    ext = os.path.splitext(new_filename)[1].lower()
    file_format = ext.strip('.')
    
    output_buffer = BytesIO()
    content_type = ""
    
    if file_format == 'csv':
        merged_df.to_csv(output_buffer, index=False)
        content_type = "text/csv"
    elif file_format in ('xlsx', 'xls'):
        merged_df.to_excel(output_buffer, index=False)
        content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        raise HTTPException(status_code=400, detail="New file format must be CSV or XLSX.")

    output_buffer.seek(0)
    
    try:
        minio_service.upload_file(new_filename, output_buffer.read(), content_type)
    except Exception as e:
         raise HTTPException(status_code=500, detail=f"Failed to upload merged file to MinIO: {e}")

    db_file = FileMetadata(filename=new_filename, format=file_format)
    db.add(db_file)
    db.commit()
    db.refresh(db_file)
        
    await cache_backend.delete(cache_key)

    return db_file