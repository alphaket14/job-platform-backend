"""
Configuration management for the Job Data Ingestion Pipeline.
"""
import asyncio
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Application configuration."""
    
    # PostgreSQL Configuration (replacing Supabase)
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'job_platform')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    
    # Transaction Pooler Connection String (for PgBouncer, RDS Proxy, etc.)
    POSTGRES_CONNECTION_STRING = os.getenv('POSTGRES_CONNECTION_STRING')
    
    # Legacy Supabase Configuration (for backward compatibility)
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_KEY = os.getenv('SUPABASE_KEY')
    
    # Google AI Configuration
    GOOGLE_PROJECT_ID = os.getenv('GOOGLE_PROJECT_ID')

    GOOGLE_LOCATIONS = os.getenv('GOOGLE_LOCATIONS', 'us-central1,us-east1,us-east4,us-east5,us-south1,us-west1,us-west4').split(',')
    GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
    GOOGLE_APPLICATION_CREDENTIALS=os.getenv('GOOGLE_APPLICATION_CREDENTIALS','')
    GCS_CACHE_BUCKET = os.getenv('GCS_CACHE_BUCKET')
    BIGQUERY_PROJECT_ID = os.getenv('BIGQUERY_PROJECT_ID', GOOGLE_PROJECT_ID)
    BIGQUERY_DATASET_ID = os.getenv('BIGQUERY_DATASET_ID', 'job_pipeline_dataset')
    BIGQUERY_TABLE_ID = os.getenv('BIGQUERY_TABLE_ID', 'master_jobs_table')
    
    # Xano Configuration
    XANO_API_URL = os.getenv('XANO_API_URL')
    XANO_API_KEY = os.getenv('XANO_API_KEY')
    XANO_BULK_BATCH_SIZE = int(os.getenv('XANO_BULK_BATCH_SIZE', '250'))
    MAX_CONCURRENT_XANO_CALLS = int(os.getenv('MAX_CONCURRENT_XANO_CALLS', '20'))
    MAPPING_API_KEY = os.getenv('MAPPING_API_KEY')

    # Application Configuration
    AUTO_APPROVE_THRESHOLD = float(os.getenv('AUTO_APPROVE_THRESHOLD', '0.86'))  # 0-1 scale
    MANUAL_REVIEW_THRESHOLD = float(os.getenv('MANUAL_REVIEW_THRESHOLD', '0.50'))  # 0-1 scale
    PORT = int(os.getenv('PORT', '8080'))
    GEMINI_MODEL = os.getenv('GEMINI_MODEL', 'gemini-2.5-flash')
    GEMINI_API_TIMEOUT = int(os.getenv('GEMINI_API_TIMEOUT', '90'))

    MAX_CONCURRENT_BATCHES = int(os.getenv('MAX_CONCURRENT_BATCHES', '10'))
    MAX_CONCURRENT_AI_CALLS = int(os.getenv('MAX_CONCURRENT_AI_CALLS', '800'))

    UPSTASH_SEARCH_URL = os.getenv('UPSTASH_SEARCH_URL')
    UPSTASH_SEARCH_TOKEN = os.getenv('UPSTASH_SEARCH_TOKEN')

    # Performance Configuration
    MULTI_LOCATION_ACCURACY_MODE = os.getenv('MULTI_LOCATION_ACCURACY_MODE', 'balanced')  # 'fast', 'balanced', 'accurate'
    JOB_HASH_BATCH_SIZE = int(os.getenv('JOB_HASH_BATCH_SIZE', '1000'))  # Batch size for job hash generation
    AI_ENRICHMENT_BATCH_SIZE = int(os.getenv('AI_ENRICHMENT_BATCH_SIZE', '10'))  # Jobs per AI call (increased from 10)
    PIPELINE_BATCH_SIZE = int(os.getenv('PIPELINE_BATCH_SIZE', '80'))  # Jobs per pipeline batch
    
    # Notification Configuration
    NOTIFICATIONS_ENABLED = os.getenv('NOTIFICATIONS_ENABLED', 'false').lower() == 'true'
    NOTIFICATION_TYPE = os.getenv('NOTIFICATION_TYPE', 'none').lower()  # 'slack', 'email', 'webhook', 'both', 'all', 'none'
    
    # Slack Configuration
    SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
    
    # Custom Webhook Configuration
    CUSTOM_WEBHOOK_URL = os.getenv('CUSTOM_WEBHOOK_URL')
    
    # Email Configuration
    EMAIL_RECIPIENTS = os.getenv('EMAIL_RECIPIENTS')  # Comma-separated list
    SMTP_SERVER = os.getenv('SMTP_SERVER')
    SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
    SMTP_USERNAME = os.getenv('SMTP_USERNAME')
    SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')
    SMTP_FROM_EMAIL = os.getenv('SMTP_FROM_EMAIL')
    
    # Validation
    @classmethod
    def validate(cls):
        """Validate that all required environment variables are set."""
        # Check if using connection string or individual parameters
        if cls.POSTGRES_CONNECTION_STRING:
            # Using connection string - only need password if not in connection string
            required_vars = ['XANO_API_URL', 'XANO_API_KEY','GOOGLE_PROJECT_ID', ]
        else:
            # Using individual parameters
            required_vars = ['POSTGRES_PASSWORD', 'XANO_API_URL', 'XANO_API_KEY','GOOGLE_PROJECT_ID', ]
        
        missing_vars = []
        for var in required_vars:
            if not getattr(cls, var):
                missing_vars.append(var)
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        return True

import json
from google.cloud import storage
from google.api_core.exceptions import NotFound

logger = logging.getLogger(__name__)

# This can be a global client
storage_client = None

async def get_storage_client():
    """Initializes and returns a GCS client."""
    global storage_client
    if storage_client is None:
        storage_client = storage.Client()
    return storage_client

async def load_mapping_config_from_gcs() -> dict:
    """Loads the mapping configuration from the GCS bucket."""
    try:
        client = await get_storage_client()
        bucket = client.bucket(Config.GCS_CACHE_BUCKET)
        blob = bucket.blob("mapping_config.json")
        json_data = await asyncio.to_thread(blob.download_as_text)
        return json.loads(json_data)
    except NotFound:
        logger.error("mapping_config.json not found in GCS bucket!")
        return {"mappings": {}, "target_mapping_options": []}
    except Exception as e:
        logger.error(f"Failed to load mapping_config.json from GCS: {e}", exc_info=True)
        return {"mappings": {}, "target_mapping_options": []}

async def save_mapping_config_to_gcs(config_data: dict):
    """Saves the provided configuration data to mapping_config.json in GCS."""
    try:
        client = await get_storage_client()
        bucket = client.bucket(Config.GCS_CACHE_BUCKET)
        blob = bucket.blob("mapping_config.json")
        json_data = json.dumps(config_data, indent=2)
        await asyncio.to_thread(blob.upload_from_string, json_data, content_type='application/json')
        logger.info("Successfully saved mapping_config.json to GCS.")
    except Exception as e:
        logger.error(f"Failed to save mapping_config.json to GCS: {e}", exc_info=True)
        raise