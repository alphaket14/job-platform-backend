# worker.py
"""
Cloud Run Worker Service for processing long-running pipeline jobs via Cloud Tasks.
"""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, status
import vertexai
from src.config import Config
from src.pipeline import JobPipeline
import google.auth
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Lifespan manager to handle resources
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Worker startup: Initializing pipeline...")
    try:
        try:
            creds, project = google.auth.default()
            if hasattr(creds, 'service_account_email'):
                logger.info(f"Cloud Run is using service account: {creds.service_account_email}")
            else:
                logger.warning("Could not determine service account email from credentials.")
        except Exception as auth_error:
            logger.error(f"Google Auth Error: {auth_error}")
        Config.validate()
        vertexai.init(project=Config.GOOGLE_PROJECT_ID, location=Config.GOOGLE_LOCATIONS[0])
        app.state.pipeline = await JobPipeline.create()
        logger.info("Pipeline initialized successfully.")
    except Exception as e:
        logger.critical(f"Worker pipeline initialization failed: {e}", exc_info=True)
        app.state.pipeline = None

    yield  # Worker is running

    logger.info("Worker shutdown: Cleaning up resources...")
    if app.state.pipeline and app.state.pipeline.db_service:
        await app.state.pipeline.db_service.close_pool()
        logger.info("Database connection pool closed.")


# Initialize the FastAPI app for the worker
app = FastAPI(lifespan=lifespan)


@app.post("/run-task", status_code=status.HTTP_200_OK)
async def run_task(request: Request):
    """
    Receives a task from Cloud Tasks and executes the job feed pipeline.
    """
    if not app.state.pipeline:
        logger.error("Pipeline not initialized. Cannot process task.")
        raise HTTPException(status_code=503, detail="Pipeline service unavailable.")

    try:
        body = await request.json()
        feed_id = body.get('feed_id')
        if feed_id is None:
            raise HTTPException(status_code=400, detail="Missing 'feed_id' in request body.")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body.")

    logger.info(f"Worker received task for feed_id: {feed_id}")

    try:
        feed_config = await app.state.pipeline.db_service.get_feed_by_id(feed_id)
        if not feed_config:
            logger.error(f"Feed with ID {feed_id} not found in database.")
            # Return 200 OK so Cloud Tasks doesn't retry a permanently failing task
            return {"status": "error", "detail": f"Feed ID {feed_id} not found."}

        # Directly await the pipeline. The full 60-minute timeout applies here.
        await app.state.pipeline.process_feed_batched(
            input_path=feed_config.get('source_url'),
            feed_id=feed_id,
            feed_name=feed_config.get('name')
        )
        logger.info(f"Worker successfully completed task for feed_id: {feed_id}")
        return {"status": "success", "feed_id": feed_id}
    except Exception as e:
        logger.error(f"An unexpected error occurred during pipeline execution for feed_id {feed_id}: {e}",
                     exc_info=True)
        # Raise an exception to signal failure to Cloud Tasks for a potential retry.
        raise HTTPException(status_code=500, detail=f"Pipeline failed for feed_id {feed_id}.")