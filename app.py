# app.py
"""
Main FastAPI application for triggering jobs via Google Cloud Tasks.
"""
import logging
import json
from datetime import datetime, timezone

import requests
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import vertexai
from google.cloud import tasks_v2
from google.protobuf import json_format
from filelock import FileLock, Timeout
from google.cloud.run_v2.services import jobs
from google.cloud.run_v2.types import job as job_types

from src.config import Config, load_mapping_config_from_gcs, save_mapping_config_to_gcs
from src.pipeline import JobPipeline
from src.file_processor import parse_raw_xml_stream_from_response

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

templates = Jinja2Templates(directory="templates")

# Lifespan manager to handle resources
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pipeline = None
    logger.info("Service startup: Initializing resources...")
    try:
        Config.validate()
        vertexai.init(project=Config.GOOGLE_PROJECT_ID, location=Config.GOOGLE_LOCATIONS[0])
        app.state.pipeline = await JobPipeline.create()
        logger.info("Resources initialized successfully.")
    except Exception as e:
        logger.critical(f"Initialization failed: {e}", exc_info=True)

    yield

    logger.info("Service shutdown: Cleaning up resources...")
    if app.state.pipeline and app.state.pipeline.db_service:
        await app.state.pipeline.db_service.close_pool()
        logger.info("Database connection pool closed.")


# Initialize FastAPI app
app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health():
    """Health check endpoint that also verifies database connectivity."""
    pipeline = app.state.pipeline
    if not pipeline or not pipeline.db_service:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Pipeline is not initialized or database service is unavailable."
        )

    try:
        # A simple query to ensure the DB connection is alive
        await pipeline.db_service.get_feed_by_id(1)
        return {
            "status": "healthy",
            "service": "Job Data Ingestion Trigger",
            "database_connection": "ok"
        }
    except Exception as e:
        logger.error(f"Health check failed during database query: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Health check failed: database connection error."
        )
@app.post("/trigger-feed/{feed_id}", status_code=status.HTTP_202_ACCEPTED)
async def trigger_feed_by_id(feed_id: int):
    """
    Receives a request and queues a long-running task in Google Cloud Tasks.
    """
    pipeline = app.state.pipeline

    if not pipeline or not pipeline.db_service:
        raise HTTPException(status_code=503, detail="Pipeline service is not available.")

    try:
        feed_config = await pipeline.db_service.get_feed_by_id(feed_id)
        if not feed_config:
            raise HTTPException(status_code=404, detail=f"Feed with ID {feed_id} not found.")
    except Exception as e:
        logger.error(f"Error fetching feed config for {feed_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not retrieve feed configuration.")

    try:
        client = tasks_v2.CloudTasksAsyncClient()
        project = Config.GOOGLE_PROJECT_ID
        location = "us-central1"
        feed_size_label = feed_config.get('feed_size_label')
        if feed_size_label == 'LARGE':
            try:
                logger.info(f"Routing LARGE feed {feed_id} to a Cloud Run Job.")
                client = jobs.JobsAsyncClient()

                # The name of the Job we created in Step 2
                job_name = f"projects/{project}/locations/{location}/jobs/large-feed-processor"

                # This is how we pass the dynamic --feed-id argument to this specific execution
                overrides = job_types.RunJobRequest.Overrides(
                    container_overrides=[
                        job_types.RunJobRequest.Overrides.ContainerOverride(
                            args=["process_feed.py", f"--feed-id={feed_id}"]
                        )
                    ]
                )

                request = job_types.RunJobRequest(name=job_name, overrides=overrides)

                # Start the job execution
                job_execution = await client.run_job(request=request)
                logger.info(f"Successfully started Cloud Run Job execution for feed {feed_id}.")

                return {"status": "cloud_run_job_started", "feed_id": feed_id}

            except Exception as e:
                logger.error(f"Failed to start Cloud Run Job for feed_id {feed_id}: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail="Failed to start the job processing task.")
        else:
            queue = "job-pipeline-queue"
            worker_url = "https://worker-service-o7jm5zleya-uc.a.run.app/run-task" # Your worker URL
            logger.info(f"Routing feed {feed_id} to regular worker (label: {feed_size_label}).")


        queue_path = client.queue_path(project, location, queue)

        # Construct the task payload
        payload = {"feed_id": feed_id}
        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": worker_url,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(payload).encode("utf-8"),
            }
        }


        response = await client.create_task(parent=queue_path, task=task)
        logger.info(f"Created and queued task: {response.name}")

        return {
            "status": "task_queued",
            "feed_id": feed_id,
            "task_name": response.name,
            "message": "Feed processing has been queued. The worker will process it shortly."
        }
    except Exception as e:
        logger.error(f"Failed to queue task for feed_id {feed_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to queue the processing task.")

@app.get("/execution-status/{feed_id}")
async def get_execution_status(feed_id: int):
    """
    Retrieves the most recent execution report for a specific feed ID.
    """
    # This endpoint remains the same
    logger.info(f"Request received for last execution status of feed ID: {feed_id}")
    pipeline = app.state.pipeline
    if not pipeline or not pipeline.db_service:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Pipeline service is not available.")
    try:
        report = await pipeline.db_service.get_latest_execution_report(feed_id)
        if not report:
            return {"status": "NOT_RUN", "feed_id": feed_id, "message": "This feed exists but has not been processed yet."}
        return report
    except Exception as e:
        logger.error(f"Error fetching execution status for feed {feed_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal error occurred.")




@app.post("/update-mapping")
async def update_mapping(request: Request):
    """
    Reads mapping_config.json, updates it with new mappings, and writes it back,
    requiring an API key for authorization.
    """
    try:
        data = await request.json()
        new_mappings = data.get("new_mappings", {})
        provided_key = data.get("api_key")

        if not Config.MAPPING_API_KEY or provided_key != Config.MAPPING_API_KEY:
            raise HTTPException(status_code=401, detail="Invalid or missing API key.")

        if not new_mappings:
            raise HTTPException(status_code=400, detail="No new mappings provided.")

        config_data = await load_mapping_config_from_gcs()
        config_data['mappings'].update(new_mappings)

        await save_mapping_config_to_gcs(config_data)

        return JSONResponse({"status": "success", "message": "Mappings updated successfully in GCS."})
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Failed to update mapping file: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/inspect-feed", response_class=HTMLResponse)
async def inspect_feed(request: Request, feed_id: int = None):
    if not feed_id:
        return templates.TemplateResponse("inspect.html", {"request": request, "results": None})

    pipeline = app.state.pipeline
    if not pipeline or not pipeline.db_service:
        return HTMLResponse("<h1>Error: Pipeline service not available</h1>", status_code=503)
    try:
        feed_config = await pipeline.db_service.get_feed_by_id(feed_id)
        if not feed_config or not feed_config.get('source_url'):
            return HTMLResponse(f"<h1>Error: Feed ID {feed_id} not found or has no source URL</h1>", status_code=404)
        source_url = feed_config.get('source_url')

        job_sample = []
        limit = 10
        http_response = requests.get(source_url, stream=True, timeout=120)
        http_response.raise_for_status()

        for job_chunk in parse_raw_xml_stream_from_response(http_response, chunk_size=limit):
            job_sample.extend(job_chunk)
            if len(job_sample) >= limit:
                break

        if not job_sample:
            return templates.TemplateResponse("inspect.html", {"request": request, "feed_id": feed_id,
                                                               "results": {"total_jobs": "N/A"}})

        job_preview = job_sample[:5]
        for job in job_preview:
            if 'description' in job:
                job['description'] = job['description'][:100] + '...' if len(job['description']) > 100 else job[
                    'description']

        table_headers = sorted(list(set(key for job in job_sample for key in job.keys())))

        mapping_config = await load_mapping_config_from_gcs()

        results = {
            "job_preview": job_preview,
            "table_headers": table_headers,
            "current_mappings": mapping_config.get('mappings', {}),
            "target_schema_keys": mapping_config.get('target_mapping_options', [])
        }

        return templates.TemplateResponse("inspect.html", {"request": request, "feed_id": feed_id, "results": results})
    except Exception as e:
        logger.error(f"Failed to inspect feed {feed_id}: {e}", exc_info=True)
        return HTMLResponse(f"<h1>An error occurred while inspecting feed: {e}</h1>", status_code=500)


@app.post("/approve-job/{job_hash}", status_code=status.HTTP_200_OK)
async def approve_manual_review_job(job_hash: str):
    """
    Approves a job from the manual review queue, moving it to open_jobs and Xano.
    """
    pipeline = app.state.pipeline
    if not pipeline or not pipeline.db_service:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Pipeline service is not available.")

    try:
        # 1. Fetch the job from the manual review table
        job_to_approve = await pipeline.db_service.get_manual_review_job_by_hash(job_hash)
        if not job_to_approve:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job hash not found in manual review queue.")

        # 2. Prepare the job for the open_jobs table
        job_to_approve.pop('id', None)
        job_to_approve['status'] = 'ACTIVE'
        job_to_approve['updated_at'] = datetime.now(timezone.utc)

        # 3. Upsert the job into the open_jobs table
        await pipeline.db_service.insert_jobs_bulk([job_to_approve])
        logger.info(f"Job {job_hash} successfully upserted into open_jobs.")

        # 4. Add the job to Xano
        await pipeline.xano_service.sync_jobs_to_xano_bulk(jobs_to_add=[job_to_approve], hashes_to_delete=[])
        logger.info(f"Job {job_hash} successfully synced to Xano.")

        # 5. Remove the job from the manual review table
        await pipeline.db_service.delete_manual_review_job_by_hash(job_hash)

        return {"status": "success", "message": "Job approved and moved to open_jobs and Xano."}

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Failed to approve job {job_hash}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
