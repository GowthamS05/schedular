import os
import uuid
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any
from contextlib import asynccontextmanager
import io

import pandas as pd
import httpx
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response
from pymongo import MongoClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
class Config:
    MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
    DATABASE_NAME = os.getenv("DATABASE_NAME", "excel_processor")
    MAX_PROCESSING_TIME = 120  # 2 minutes in seconds

config = Config()

# MongoDB setup
client = MongoClient(config.MONGODB_URL)
db = client[config.DATABASE_NAME]
jobs_collection = db.jobs

# APScheduler setup
scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    scheduler.start()
    logger.info("Scheduler started successfully")
    
    yield
    
    # Shutdown
    scheduler.shutdown()
    logger.info("Scheduler stopped")

# FastAPI app
app = FastAPI(
    title="Excel Processing Service",
    description="Upload Excel files and process job IDs asynchronously",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Models
class JobStatus:
    PENDING = "pending"
    IN_PROGRESS = "in_progress" 
    COMPLETED = "completed"
    FAILED = "failed"

class JobRecord:
    def __init__(self, unique_id: str, job_ids: List[int], status: str = JobStatus.PENDING):
        self.unique_id = unique_id
        self.job_ids = job_ids
        self.status = status
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
        self.processed_excel_binary = None  # Binary data for processed Excel
        self.error_message = None
        self.results = []

    def to_dict(self):
        return {
            "unique_id": self.unique_id,
            "job_ids": self.job_ids,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "processed_excel_binary": self.processed_excel_binary,
            "error_message": self.error_message,
            "results": self.results
        }

# Utility functions
def extract_job_ids_from_excel(file_content: bytes) -> List[int]:
    """Extract job_id values from Excel file content"""
    try:
        # Read Excel from bytes
        df = pd.read_excel(io.BytesIO(file_content))
        
        if 'job_id' not in df.columns:
            raise ValueError("Excel file must contain a 'job_id' column")
        
        # Extract job_ids and convert to integers, filtering out invalid values
        job_ids = []
        for value in df['job_id'].dropna():
            try:
                job_id = int(value)
                if job_id > 0:  # Ensure positive integers
                    job_ids.append(job_id)
            except (ValueError, TypeError):
                logger.warning(f"Invalid job_id value: {value}")
                continue
        
        if not job_ids:
            raise ValueError("No valid job_ids found in the Excel file")
            
        return job_ids
        
    except Exception as e:
        logger.error(f"Error extracting job_ids from Excel: {str(e)}")
        raise

async def fetch_todo_data(job_id: int) -> Dict[str, Any]:
    """Fetch data from JSONPlaceholder API for a specific job_id"""
    url = f"https://jsonplaceholder.typicode.com/todos/{job_id}"
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url)
            
            if response.status_code == 200:
                return {
                    "job_id": job_id,
                    "status": "success",
                    "data": response.json()
                }
            elif response.status_code == 404:
                return {
                    "job_id": job_id,
                    "status": "not_found",
                    "data": {"error": "Todo not found"}
                }
            else:
                return {
                    "job_id": job_id,
                    "status": "error",
                    "data": {"error": f"HTTP {response.status_code}"}
                }
                
    except httpx.TimeoutException:
        return {
            "job_id": job_id,
            "status": "timeout",
            "data": {"error": "Request timeout"}
        }
    except Exception as e:
        return {
            "job_id": job_id,
            "status": "error",
            "data": {"error": str(e)}
        }

def create_processed_excel(results: List[Dict]) -> bytes:
    """Create processed Excel file with results and return as bytes"""
    try:
        # Prepare data for Excel
        excel_data = []
        
        for result in results:
            job_id = result["job_id"]
            status = result["status"]
            data = result["data"]
            
            if status == "success":
                # Extract relevant fields from the API response
                todo_data = data
                excel_data.append({
                    "job_id": job_id,
                    "status": status,
                    "todo_id": todo_data.get("id"),
                    "user_id": todo_data.get("userId"),
                    "title": todo_data.get("title"),
                    "completed": todo_data.get("completed"),
                    "api_response": str(data)
                })
            else:
                # Handle errors
                excel_data.append({
                    "job_id": job_id,
                    "status": status,
                    "todo_id": None,
                    "user_id": None,
                    "title": None,
                    "completed": None,
                    "api_response": str(data)
                })
        
        # Create DataFrame and save to Excel in memory
        df = pd.DataFrame(excel_data)
        output = io.BytesIO()
        df.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)
        
        logger.info("Processed Excel file created in memory")
        return output.getvalue()
        
    except Exception as e:
        logger.error(f"Error creating processed Excel file: {str(e)}")
        raise

async def process_job_ids(unique_id: str):
    """Background task to process job IDs"""
    logger.info(f"Starting to process job IDs for {unique_id}")
    
    try:
        # Update status to in_progress
        jobs_collection.update_one(
            {"unique_id": unique_id},
            {
                "$set": {
                    "status": JobStatus.IN_PROGRESS,
                    "updated_at": datetime.now()
                }
            }
        )
        
        # Fetch job record
        job_record = jobs_collection.find_one({"unique_id": unique_id})
        if not job_record:
            raise Exception("Job record not found")
        
        job_ids = job_record["job_ids"]
        logger.info(f"Processing {len(job_ids)} job IDs for {unique_id}")
        
        # Process job IDs concurrently with timeout
        tasks = [fetch_todo_data(job_id) for job_id in job_ids]
        
        try:
            # Use asyncio.wait_for to enforce overall timeout
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=config.MAX_PROCESSING_TIME
            )
            
            # Handle any exceptions in results
            processed_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    processed_results.append({
                        "job_id": job_ids[i],
                        "status": "error",
                        "data": {"error": str(result)}
                    })
                else:
                    processed_results.append(result)
                    
        except asyncio.TimeoutError:
            logger.error(f"Processing timed out for job {unique_id}")
            jobs_collection.update_one(
                {"unique_id": unique_id},
                {
                    "$set": {
                        "status": JobStatus.FAILED,
                        "error_message": "Processing timed out",
                        "updated_at": datetime.now()
                    }
                }
            )
            return
        
        # Create processed Excel file in memory
        processed_excel_bytes = create_processed_excel(processed_results)
        
        # Update job record with completion and store binary data
        jobs_collection.update_one(
            {"unique_id": unique_id},
            {
                "$set": {
                    "status": JobStatus.COMPLETED,
                    "processed_excel_binary": processed_excel_bytes,
                    "results": processed_results,
                    "updated_at": datetime.now()
                }
            }
        )
        
        logger.info(f"Job {unique_id} completed successfully")
        
    except Exception as e:
        logger.error(f"Error processing job {unique_id}: {str(e)}")
        jobs_collection.update_one(
            {"unique_id": unique_id},
            {
                "$set": {
                    "status": JobStatus.FAILED,
                    "error_message": str(e),
                    "updated_at": datetime.now()
                }
            }
        )

# API Endpoints

@app.post("/upload", 
          summary="Upload Excel file for processing",
          description="Upload an Excel file containing job_id column for async processing")
async def upload_excel_file(file: UploadFile = File(...)):
    """
    Step 1: Upload Excel file and extract job IDs
    """
    # Validate file type
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(
            status_code=400, 
            detail="File must be an Excel file (.xlsx or .xls)"
        )
    
    # Generate unique ID
    unique_id = str(uuid.uuid4())
    
    try:
        # Read file content directly
        content = await file.read()
        
        # Extract job IDs from Excel content
        job_ids = extract_job_ids_from_excel(content)
        
        # Store job record in MongoDB (without storing original file)
        job_record = JobRecord(unique_id=unique_id, job_ids=job_ids)
        jobs_collection.insert_one(job_record.to_dict())
        
        # Schedule background processing with 2-minute deadline
        run_time = datetime.now() + timedelta(seconds=5)  # Start after 5 seconds
        scheduler.add_job(
            process_job_ids,
            trigger=DateTrigger(run_date=run_time),
            args=[unique_id],
            id=f"process_{unique_id}",
            max_instances=1
        )
        
        logger.info(f"Scheduled job {unique_id} to run at {run_time}")
        
        logger.info(f"Job {unique_id} created with {len(job_ids)} job IDs")
        
        return {
            "unique_id": unique_id,
            "message": "File uploaded successfully",
            "job_ids_count": len(job_ids),
            "estimated_completion": "within 2 minutes"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error processing upload: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/status/{unique_id}",
         summary="Check job processing status", 
         description="Query the status of a processing job by unique ID")
async def get_job_status(unique_id: str):
    """
    Step 4: Get job status and download link
    """
    # Fetch job record from MongoDB
    job_record = jobs_collection.find_one({"unique_id": unique_id})
    
    if not job_record:
        raise HTTPException(
            status_code=404, 
            detail="Job ID not found"
        )
    
    status = job_record["status"]
    
    if status == JobStatus.PENDING or status == JobStatus.IN_PROGRESS:
        return {"status": "in_progress"}
    elif status == JobStatus.FAILED:
        return {
            "status": "failed",
            "error_message": job_record.get("error_message", "Unknown error")
        }
    elif status == JobStatus.COMPLETED:
        return {
            "status": "completed",
            "download_link": f"/download/{unique_id}",
            "processed_at": job_record["updated_at"],
            "results_count": len(job_record.get("results", []))
        }
    else:
        return {"status": "unknown"}

@app.get("/download/{unique_id}",
         summary="Download processed Excel file",
         description="Download the processed Excel file for a completed job")
async def download_processed_file(unique_id: str):
    """
    Step 5: Download processed Excel file
    """
    # Fetch job record
    job_record = jobs_collection.find_one({"unique_id": unique_id})
    
    if not job_record:
        raise HTTPException(status_code=404, detail="Job ID not found")
    
    if job_record["status"] != JobStatus.COMPLETED:
        raise HTTPException(
            status_code=400, 
            detail="Job is not completed yet"
        )
    
    processed_excel_binary = job_record.get("processed_excel_binary")
    if not processed_excel_binary:
        raise HTTPException(
            status_code=404, 
            detail="Processed file not found"
        )
    
    filename = f"processed_results_{unique_id}.xlsx"
    
    return Response(
        content=processed_excel_binary,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@app.get("/jobs",
         summary="List all jobs",
         description="Get a list of all processing jobs (for debugging)")
async def list_jobs():
    """
    Debug endpoint to list all jobs
    """
    jobs = list(jobs_collection.find({}, {"_id": 0}))
    return {"jobs": jobs}

@app.delete("/jobs/{unique_id}",
           summary="Delete a job",
           description="Delete a job record and its associated files")
async def delete_job(unique_id: str):
    """
    Clean up endpoint to delete jobs
    """
    job_record = jobs_collection.find_one({"unique_id": unique_id})
    
    if not job_record:
        raise HTTPException(status_code=404, detail="Job ID not found")
    
    # No need to delete files since we're not storing them separately
    
    # Delete from MongoDB
    jobs_collection.delete_one({"unique_id": unique_id})
    
    # Cancel scheduled job if it exists
    try:
        scheduler.remove_job(f"process_{unique_id}")
    except Exception:
        pass  # Job might not exist or already completed
    
    return {"message": "Job deleted successfully"}

@app.get("/scheduler/status",
         summary="Scheduler status",
         description="Check the current status of the scheduler")
async def get_scheduler_status():
    """Debug endpoint to check scheduler status"""
    jobs = scheduler.get_jobs()
    return {
        "scheduler_state": str(scheduler.state),
        "is_running": scheduler.running,
        "total_jobs": len(jobs),
        "jobs": [
            {
                "id": job.id,
                "next_run_time": str(job.next_run_time) if job.next_run_time else None,
                "trigger": str(job.trigger)
            }
            for job in jobs
        ]
    }

@app.get("/",
         summary="Health check",
         description="Basic health check endpoint")
async def root():
    return {
        "message": "Excel Processing Service is running",
        "version": "1.0.0",
        "scheduler_running": scheduler.running,
        "endpoints": {
            "upload": "/upload",
            "status": "/status/{unique_id}",
            "download": "/download/{unique_id}",
            "scheduler_status": "/scheduler/status",
            "docs": "/docs"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )