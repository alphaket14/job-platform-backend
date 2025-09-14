import asyncio
import logging
from src.config import Config
from src.database import DatabaseService
from src.xano_service import XanoService
from src.upstash_service import UpstashService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def repopulate_single_feed(feed_id: int, xano_service: XanoService, upstash_service: UpstashService):
    """
    Fetches jobs from Xano and upserts them to Upstash for one feed.
    """
    logger.info(f"--- Starting upsert process for Feed ID: {feed_id} ---")
    try:
        # 1. Get the authoritative list of jobs from Xano for this feed
        jobs_from_xano = await xano_service.get_all_jobs_for_feed(feed_id)
        if not jobs_from_xano:
            logger.warning(f"No jobs found in Xano for feed {feed_id}. Nothing to upsert.")
            return feed_id, "Success (No Jobs)"

        # 2. Perform upserts. The parallel logic is inside the upsert_jobs function.
        await upstash_service.upsert_jobs(jobs_from_xano)

        logger.info(f"--- Successfully completed upsert for {len(jobs_from_xano)} jobs for Feed ID: {feed_id} ---")
        return feed_id, "Success"
    except Exception as e:
        logger.error(f"--- Upsert FAILED for Feed ID: {feed_id}: {e} ---", exc_info=True)
        return feed_id, "Failed"


async def main():
    """Main asynchronous entry point for the repopulation script."""
    db_service = None
    xano_service = None
    exit_code = 0
    try:
        Config.validate()
        db_service = await DatabaseService.create()
        xano_service = XanoService()
        upstash_service = UpstashService()

        logger.info("--- Starting Upstash Repopulation from Xano ---")

        # Get all active feed IDs from the primary database
        logger.info("Fetching list of active feeds from PostgreSQL database...")
        feeds_query = "SELECT id FROM feeds WHERE is_active = TRUE"
        feed_records = await db_service._pool.fetch(feeds_query)
        active_feed_ids = [record['id'] for record in feed_records]

        if not active_feed_ids:
            logger.warning("No active feeds to process. Exiting.")
            return 0

        logger.info(f"Found {len(active_feed_ids)} active feeds to process: {active_feed_ids}")

        # Create and run a concurrent task for each feed
        tasks = [repopulate_single_feed(feed_id, xano_service, upstash_service) for feed_id in active_feed_ids]
        results = await asyncio.gather(*tasks)

        # Report the outcome
        success_count = sum(1 for _, status in results if "Success" in status)
        failed_count = len(results) - success_count
        logger.info("--- Repopulation Complete ---")
        logger.info(f"Summary: {success_count} feeds processed successfully, {failed_count} feeds failed.")

        if failed_count > 0:
            exit_code = 1

    except Exception as e:
        logger.error(f"A critical error occurred: {e}", exc_info=True)
        exit_code = 1
    finally:
        if db_service:
            await db_service.close_pool()
        if xano_service:
            await xano_service.close_async_client()

    return exit_code


if __name__ == "__main__":
    import sys

    exit_code = asyncio.run(main())
    sys.exit(exit_code)