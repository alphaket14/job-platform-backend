#!/usr/bin/env python3
"""
Standalone script to delete specific jobs by their hash and eid from all data stores,
sourcing the list of jobs from a central API endpoint.
"""
import asyncio
import logging
import httpx
from src.config import Config
from src.database import DatabaseService
from src.xano_service import XanoService
from src.upstash_service import UpstashService
from src.bigquery_service import BigQueryService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# The API endpoint containing the list of jobs to delete
DELETION_LIST_URL = "https://api.collabwork.com/api:-rXln6tl/duplicate_hashes"


async def main():
    """Main asynchronous entry point for the purge script."""
    db_service = None
    xano_service = None
    exit_code = 0

    try:
        Config.validate()

        # 1. Fetch the list of jobs to delete
        logger.info(f"Fetching deletion list from {DELETION_LIST_URL}...")
        async with httpx.AsyncClient() as client:
            response = await client.get(DELETION_LIST_URL, timeout=60.0)
            response.raise_for_status()
            jobs_to_delete = response.json()

        if not jobs_to_delete:
            logger.info("Deletion list is empty. Nothing to do.")
            return 0

        # 2. Collect all unique hashes and eids
        all_hashes_to_delete = list({item['hash'] for item in jobs_to_delete if 'hash' in item})
        all_eids_to_delete = list({eid for item in jobs_to_delete if 'eids' in item for eid in item['eids']})

        logger.info(
            f"Found {len(all_hashes_to_delete)} unique hashes and {len(all_eids_to_delete)} unique eids to delete.")

        # 3. Initialize all services
        db_service = await DatabaseService.create()
        xano_service = XanoService()
        upstash_service = UpstashService()
        bigquery_service = BigQueryService()

        # 4. Create and run deletion tasks concurrently
        logger.info("--- Starting Deletion Process Across All Systems ---")

        deletion_tasks = [
            upstash_service.delete_jobs_by_eid(all_eids_to_delete),
            xano_service.sync_jobs_to_xano_bulk(jobs_to_add=[], hashes_to_delete=all_hashes_to_delete),
            bigquery_service.delete_jobs_by_hashes(all_hashes_to_delete),
            db_service.delete_open_jobs_by_hashes(all_hashes_to_delete)
        ]

        results = await asyncio.gather(*deletion_tasks, return_exceptions=True)

        # 5. Report results
        service_names = ["Upstash", "Xano", "BigQuery", "Supabase/Postgres"]
        has_errors = False
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Deletion FAILED for {service_names[i]}: {result}")
                has_errors = True
            else:
                logger.info(f"Deletion SUCCEEDED for {service_names[i]}.")

        if has_errors:
            exit_code = 1

    except httpx.HTTPStatusError as e:
        logger.error(
            f"Failed to fetch the deletion list. Status: {e.response.status_code}, Response: {e.response.text}")
        exit_code = 1
    except Exception as e:
        logger.error(f"A critical error occurred: {e}", exc_info=True)
        exit_code = 1
    finally:
        if db_service:
            await db_service.close_pool()
        if xano_service:
            await xano_service.close_async_client()

    if exit_code == 0:
        logger.info("--- Purge Script Completed Successfully ---")
    else:
        logger.error("--- Purge Script Completed with Errors ---")

    return exit_code


if __name__ == "__main__":
    import sys

    exit_code = asyncio.run(main())
    sys.exit(exit_code)