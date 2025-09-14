import asyncio
import json
import logging
import uuid

import asyncpg
import aiohttp

# Import the configuration from your project's config file
from src.config import Config

# --- Configuration ---
# The name of your master table in PostgreSQL where job_hash and bigquery_id exist
DB_TABLE_NAME = "open_jobs"  # <-- IMPORTANT: Verify this is your correct table name!

# Set up basic logging to see the script's progress
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


async def fetch_data_from_postgres() -> list:
    """
    Connects to PostgreSQL and uses a direct 'fetch' to get all records,
    bypassing the cursor logic to avoid issues with PgBouncer.
    """
    logging.info("Connecting to PostgreSQL to fetch job data (using direct fetch method)...")

    try:
        # We still include statement_cache_size=0 as it's best practice for PgBouncer
        pool = await asyncpg.create_pool(
            dsn=Config.POSTGRES_CONNECTION_STRING,
            min_size=1,
            max_size=5,
            statement_cache_size=0
        )

        all_records = []
        async with pool.acquire() as conn:
            # This is the new, direct query. No cursor, no transactions needed for a simple SELECT.
            sql_query = f"SELECT job_hash, bigquery_id FROM {DB_TABLE_NAME} WHERE bigquery_id IS NOT NULL"
            logging.info("Executing direct fetch...")

            records = await conn.fetch(sql_query)

            if records:
                # Convert the list of Record objects to a list of dicts, handling UUIDs
                all_records = [
                    {key: str(value) if isinstance(value, uuid.UUID) else value for key, value in record.items()}
                    for record in records
                ]

        await pool.close()
        logging.info(f"✅ Successfully fetched {len(all_records)} records from PostgreSQL.")
        return all_records

    except Exception as e:
        logging.error(f"❌ Database connection or query failed: {e}", exc_info=True)
        return []


async def send_batch_to_xano(session: aiohttp.ClientSession, batch: list, batch_num: int, semaphore: asyncio.Semaphore):
    """Sends a single batch of data to the Xano API, respecting the concurrency limit."""
    async with semaphore:  # Wait for an open slot before making the API call
        logging.info(f"  -> Sending Batch {batch_num} ({len(batch)} records)...")

        # IMPORTANT: Structure the payload exactly as the endpoint expects
        payload = {
            "update_list": batch
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {Config.XANO_API_KEY}"
        }

        try:
            api_url = f"{Config.XANO_API_URL}/job_posting/edit_bigquery_id"
            async with session.post(api_url, headers=headers, json=payload) as response:
                if response.status == 200:
                    logging.info(f"  -> Batch {batch_num}... ✅ Success")
                else:
                    error_text = await response.text()
                    logging.error(
                        f"  -> Batch {batch_num}... ❌ FAILED (Status: {response.status}) - Response: {error_text[:200]}")
        except Exception as e:
            logging.error(f"  -> Batch {batch_num}... ❌ FAILED (Connection Error: {e})")


async def update_data_in_xano(records: list):
    """
    Splits records into batches and sends them to Xano concurrently.
    """
    if not records:
        logging.warning("No records to send to Xano.")
        return

    # Split the total list of records into smaller batches
    batches = [records[i:i + Config.XANO_BULK_BATCH_SIZE] for i in range(0, len(records), Config.XANO_BULK_BATCH_SIZE)]
    total_batches = len(batches)
    logging.info(f"Preparing to send {len(records)} records to Xano in {total_batches} batches.")

    # A semaphore limits how many API calls can be running at the same time
    semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_XANO_CALLS)

    async with aiohttp.ClientSession() as session:
        # Create a list of tasks to run concurrently
        tasks = [send_batch_to_xano(session, batch, i + 1, semaphore) for i, batch in enumerate(batches)]
        # Wait for all the batch-sending tasks to complete
        await asyncio.gather(*tasks)


async def main():
    """Main function to orchestrate the sync process."""
    try:
        Config.validate()
        logging.info("Configuration validated successfully.")
    except ValueError as e:
        logging.error(f"Configuration error: {e}")
        return

    # 1. Fetch data from PostgreSQL
    records_to_sync = await fetch_data_from_postgres()

    # 2. If data was fetched, send it to Xano
    if records_to_sync:
        await update_data_in_xano(records_to_sync)

    logging.info("🚀 Sync process finished.")


if __name__ == "__main__":
    asyncio.run(main())