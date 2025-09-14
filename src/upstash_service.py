# src/upstash_service.py

import asyncio
import logging
import re
from typing import Dict, Any, List

from upstash_search import Search
from .config import Config

logger = logging.getLogger(__name__)


def strip_html_and_truncate(text: str, length: int = 400) -> str:
    """Removes HTML tags and truncates text."""
    if not text:
        return ""
    # Remove HTML tags
    clean_text = re.sub(r'<[^>]+>', '', text)
    # Normalize whitespace and truncate
    normalized_text = re.sub(r'\s+', ' ', clean_text).strip()
    return normalized_text[:length]


class UpstashService:
    """Service for synchronizing job data with the standalone Upstash Search service."""

    def __init__(self):
        """Initializes the Search client."""
        self.search = None
        if Config.UPSTASH_SEARCH_URL and Config.UPSTASH_SEARCH_TOKEN:
            try:
                self.search = Search(url=Config.UPSTASH_SEARCH_URL, token=Config.UPSTASH_SEARCH_TOKEN)
                logger.info("Upstash Search client initialized successfully.")
            except Exception as e:
                logger.error(f"Failed to initialize Upstash Search client: {e}")
        else:
            logger.warning("Upstash Search URL or Token not configured. UpstashService is disabled.")

    def _transform_job_for_upstash(self, job: Dict[str, Any]) -> Dict:
        """Transforms a Xano job object into the document format required by Upstash Search."""
        locations = job.get('location', [])
        location_str = ", ".join(
            " ".join(filter(None, [loc.get('city'), loc.get('state'), loc.get('country')]))
            for loc in locations
        )

        searchable_text = (job.get('searchable_text', '') or '')[:2500]

        return {
            "id": job.get('eid'),
            "content": {
                "searchableText": searchable_text,
                "title": job.get('title', ''),
                "company": job.get('company', ''),
                "description": strip_html_and_truncate(job.get('ai_description', '')),
                "location": location_str[:800],
                "employment_type": job.get('employment_type', ''),
                "is_remote": job.get('is_remote', False),
                "sector": job.get('sector', ''),
                "industry": job.get('industry', ''),
            },
            "metadata": {
                "logo_url": job.get('company_logo_url', ''),
                "application_url": job.get('application_url', ''),
                "posted_at": job.get('posted_at', 0),
                "eid": job.get('eid', ''),
                "feed_id": job.get('feed_id', 0)
            }
        }

    async def delete_jobs_by_eid(self, eids_to_delete: List[str]):
        """Deletes a list of jobs from the Upstash Search index in parallel batches."""
        if not self.search or not eids_to_delete:
            return

        index = self.search.index("idx:jobs")

        batch_size = 100
        semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent requests

        async def delete_worker(batch: List[str]):
            """Acquires the semaphore and runs the delete for one batch."""
            async with semaphore:
                try:
                    # The .delete() call is synchronous, so we run it in a separate thread
                    await asyncio.to_thread(index.delete, ids=batch)
                    logger.info(f"Successfully deleted batch of {len(batch)} jobs.")
                except Exception as e:
                    logger.error(f"An error occurred during Upstash deletion batch: {e}", exc_info=True)

        logger.info(
            f"Deleting {len(eids_to_delete)} jobs from Upstash in parallel batches...")

        # Create a list of tasks for the worker to execute
        delete_tasks = [
            delete_worker(eids_to_delete[i:i + batch_size])
            for i in range(0, len(eids_to_delete), batch_size)
        ]

        # Run all tasks concurrently
        await asyncio.gather(*delete_tasks)

        logger.info("Finished all deletion tasks for Upstash.")

    async def upsert_jobs(self, jobs_from_xano: List[Dict[str, Any]]):
        """Upserts a list of jobs into the Upstash Search index in parallel batches."""
        if not self.search or not jobs_from_xano:
            return

        index = self.search.index("idx:jobs")

        documents_to_upsert = [
            self._transform_job_for_upstash(job)
            for job in jobs_from_xano if job.get('eid')
        ]

        if not documents_to_upsert:
            logger.info("No documents to upsert to Upstash.")
            return

        batch_size = 100
        semaphore = asyncio.Semaphore(10)

        async def upsert_worker(batch: List[Dict]):
            async with semaphore:
                try:
                    await asyncio.to_thread(index.upsert, documents=batch)
                    logger.info(f"Successfully upserted batch of {len(batch)} jobs.")
                except Exception as e:
                    logger.error(f"An error occurred during Upstash upsert batch: {e}", exc_info=True)

        logger.info(
            f"Upserting {len(documents_to_upsert)} jobs to Upstash with a concurrency of 10 batches...")

        upsert_tasks = [
            upsert_worker(documents_to_upsert[i:i + batch_size])
            for i in range(0, len(documents_to_upsert), batch_size)
        ]

        await asyncio.gather(*upsert_tasks)

        logger.info("Finished all upsert tasks for Upstash.")