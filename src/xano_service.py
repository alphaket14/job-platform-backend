"""
Xano service for job data synchronization.
"""
import json
import logging
import asyncio
import uuid
from dateutil import parser

import httpx
import os
from typing import Dict, Any, List, Set
from datetime import datetime
from .config import Config

logger = logging.getLogger(__name__)


def to_millis(dt):
    return int(dt.timestamp() * 1000)


def parse_posted_at(posted_at):
    if posted_at is None:
        return 0

    try:
        if isinstance(posted_at, (int, float)):
            ts = float(posted_at)
            if ts > 1e12:
                return int(ts)
            else:
                return int(ts * 1000)

        if isinstance(posted_at, str):
            try:
                dt = datetime.fromisoformat(posted_at.replace('Z', '+00:00'))
            except ValueError:
                try:
                    dt = parser.parse(posted_at, ignoretz=True)
                except Exception:
                    return 0
            return to_millis(dt)
    except Exception:
        return 0

    return 0


class XanoService:
    """Service for Xano database operations."""

    def __init__(self):
        """Initialize the Xano service."""
        self.api_url = Config.XANO_API_URL
        self.api_key = Config.XANO_API_KEY
        self.headers = {
            'Content-Type': 'application/json'
        }
        # Only add Authorization header if API key is provided
        if self.api_key:
            self.headers['Authorization'] = f'Bearer {self.api_key.strip()}'

        # Create async client for parallel operations
        self.async_client = None

    async def _get_async_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client with connection pooling."""
        if self.async_client is None:
            self.async_client = httpx.AsyncClient(
                timeout=300.0,
                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
                headers=self.headers
            )
        return self.async_client

    async def close_async_client(self):
        """Close the async HTTP client."""
        if self.async_client:
            await self.async_client.aclose()
            self.async_client = None

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - ensures cleanup."""
        await self.close_async_client()

    def transform_job_for_xano(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform job data from internal pipeline format to Xano schema format.
        
        Args:
            job_data: Job data in internal pipeline format
            
        Returns:
            Dict[str, Any]: Job data transformed to Xano schema format
        """
        xano_job = {}

        def to_float(value, default=0.0):
            if value is None:
                return default
            try:
                return float(value)
            except (ValueError, TypeError):
                return default

        # Direct field mappings
        xano_job['bigquery_id'] = job_data.get('bigquery_id')
        xano_job['external_job_id'] = job_data.get('external_job_id', '')
        xano_job['eid'] = uuid.uuid4().hex
        xano_job['job_hash'] = job_data.get('job_hash', '')
        xano_job['application_url'] = job_data.get('application_url', '')
        xano_job['title'] = job_data.get('title', '')
        xano_job['description'] = job_data.get('description', '')
        xano_job['ai_title'] = job_data.get('ai_title', '')
        xano_job['ai_description'] = job_data.get('ai_description', '')
        xano_job['is_multi_location'] = job_data.get('is_multi_location', False)
        xano_job['is_remote'] = job_data.get('is_remote', False)
        xano_job['is_international'] = job_data.get('is_international', False)
        xano_job['salary_min'] = to_float(job_data.get('salary_min'))
        xano_job['salary_max'] = to_float(job_data.get('salary_max'))
        xano_job['salary_period'] = job_data.get('salary_period', '')
        xano_job['sector_id'] = job_data.get('sector_id', 0)
        xano_job['industry_group_id'] = job_data.get('industry_group_id', 0)
        xano_job['industry_id'] = job_data.get('industry_id', 0)
        xano_job['cpc'] = to_float(job_data.get('cpc'))
        xano_job['cpa'] = to_float(job_data.get('cpa'))
        xano_job['location_string'] = job_data.get('location_string', '')
        xano_job['_geo'] = job_data.get('_geo')
        # Ensure feed_id and execution_id are properly formatted as integers
        feed_id = job_data.get('feed_id', 0)
        xano_job['feed_id'] = int(feed_id) if feed_id is not None else 0

        xano_job['ai_enrichment_status'] = job_data.get('ai_enrichment_status', '')
        xano_job['ai_enrichment_error'] = job_data.get('ai_enrichment_error', '')

        execution_id = job_data.get('execution_id', 0)
        xano_job['execution_id'] = int(execution_id) if execution_id is not None else 0

        partner_id = job_data.get('partner_id', 0)
        xano_job['partner_id'] = int(partner_id) if partner_id is not None else 0

        # Status field - should be "ACTIVE" for auto-approved jobs
        xano_job['status'] = job_data.get('status', 'ACTIVE')

        # Field name transformations
        xano_job['company'] = job_data.get('company_name', '')
        xano_job['location'] = job_data.get('locations', [])
        xano_job['posted_at'] = parse_posted_at(job_data.get('posted_at'))
        # Convert ai_confidence_score from float to integer
        xano_job['ai_confidence_score'] = int(to_float(job_data.get('ai_confidence_score')))

        # Transform employment_type
        employment_type = job_data.get('employment_type', '')
        if employment_type == 'FULL_TIME':
            xano_job['employment_type'] = 'Full-time'
        else:
            xano_job['employment_type'] = 'Part-time'

        # AI enrichment fields
        xano_job['ai_job_tasks'] = job_data.get('ai_job_tasks', [])
        xano_job['ai_skills'] = job_data.get('ai_skills', [])
        xano_job['ai_search_terms'] = job_data.get('ai_search_terms', [])
        xano_job['ai_top_tags'] = job_data.get('ai_top_tags', [])

        xano_job['job_function_id'] = job_data.get('job_function_id', 0)

        xano_job['sector'] = job_data.get('sector', '')
        xano_job['industry_group'] = job_data.get('industry_group', '')
        xano_job['industry'] = job_data.get('industry', '')
        xano_job['searchable_text'] = job_data.get('searchable_text', '')

        xano_job['company_logo_url'] = job_data.get('company_logo_url', '')

        return xano_job

    async def get_xano_hash_to_eid_map(self, feed_id: int) -> Dict[str, str]:
        """
        Fetches all existing jobs from Xano for a given feed_id and
        returns a dictionary mapping job_hash to eid.
        """
        get_endpoint = f"{self.api_url}/job_posting/supabase/feeds"
        params = {'feed_id': feed_id}
        try:
            client = await self._get_async_client()
            logger.info(f"Fetching hash-to-eid map from Xano for feed_id {feed_id}...")
            response = await client.get(get_endpoint, params=params)

            if response.status_code == 200:
                jobs = response.json()
                # Create a dictionary of {job_hash: eid}
                hash_eid_map = {
                    job['job_hash']: job['eid']
                    for job in jobs if 'job_hash' in job and 'eid' in job
                }
                logger.info(f"Found {len(hash_eid_map)} jobs in Xano for feed_id {feed_id}.")
                return hash_eid_map
            else:
                logger.error(
                    f"Failed to fetch jobs from Xano. Status: {response.status_code}, Response: {response.text}")
                return {}
        except Exception as e:
            logger.error(f"Exception while fetching hash-to-eid map from Xano: {e}", exc_info=True)
            return {}

    async def _send_bulk_request(self, endpoint: str, payload: Dict, action: str, batch_num: int, total_batches: int,
                                 count: int) -> Dict:
        method = "DELETE" if action == "delete" else "POST"
        max_retries = 3
        delay = 5  # Initial delay in seconds

        for attempt in range(max_retries):
            try:
                client = await self._get_async_client()
                logger.info(
                    f"Sending batch {batch_num}/{total_batches} to Xano to {action} {count} jobs (Attempt {attempt + 1}/{max_retries})...")
                response = await client.request(method, endpoint, json=payload)

                if response.status_code == 200:
                    logger.info(f"Successfully {action}d batch {batch_num}/{total_batches} in Xano.")
                    return {'success': True, 'count': count}

                # Fail fast on client errors (4xx), no retry needed.
                if 400 <= response.status_code < 500:
                    logger.error(
                        f"Client error on batch {batch_num}/{total_batches}. Status: {response.status_code}, Response: {response.text}")
                    return {'success': False, 'count': count, 'error': f"HTTP {response.status_code}"}

                # Retry on server errors (5xx)
                logger.warning(
                    f"Server error on batch {batch_num}/{total_batches}. Status: {response.status_code}. Retrying...")

            except httpx.ReadTimeout:
                logger.warning(f"ReadTimeout on batch {batch_num}/{total_batches}. Retrying...")
            except Exception as e:
                logger.error(f"Unexpected exception on batch {batch_num}/{total_batches}: {e}", exc_info=True)
                # For unexpected errors, stop retrying this batch.
                return {'success': False, 'count': count, 'error': str(e)}

            # Wait before the next retry
            if attempt < max_retries - 1:
                await asyncio.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                logger.error(f"Batch {batch_num}/{total_batches} failed after {max_retries} attempts.")
                return {'success': False, 'count': count, 'error': f"Failed after {max_retries} retries."}

        return {'success': False, 'count': count, 'error': 'Max retries exceeded'}  # Should not be reached

    async def sync_jobs_to_xano_bulk(self, jobs_to_add: List[Dict[str, Any]], hashes_to_delete: List[str]) -> Dict[
        str, Any]:
        """
        Orchestrates the full synchronization process: deleting old jobs and adding new ones in batches.
        """
        batch_size = Config.XANO_BULK_BATCH_SIZE
        semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_XANO_CALLS)

        # --- Deletion Phase ---
        if hashes_to_delete:
            logger.info(f"Starting deletion phase for {len(hashes_to_delete)} jobs from Xano.")
            delete_endpoint = f"{self.api_url}/job_posting/supabase/bulk_remove"
            total_delete_batches = (len(hashes_to_delete) + batch_size - 1) // batch_size

            async def delete_worker(batch, batch_num):
                async with semaphore:
                    payload = {"job_hashes": batch}
                    return await self._send_bulk_request(delete_endpoint, payload, "delete", batch_num,
                                                         total_delete_batches, len(batch))

            delete_tasks = [delete_worker(hashes_to_delete[i:i + batch_size], (i // batch_size) + 1) for i in
                            range(0, len(hashes_to_delete), batch_size)]
            await asyncio.gather(*delete_tasks)

        # --- Addition Phase ---
        if jobs_to_add:
            logger.info(f"Starting addition phase for {len(jobs_to_add)} jobs to Xano.")
            add_endpoint = f"{self.api_url}/job_posting/supabase_bulk"
            transformed_jobs = [self.transform_job_for_xano(job) for job in jobs_to_add]
            total_add_batches = (len(transformed_jobs) + batch_size - 1) // batch_size

            async def add_worker(batch, batch_num):
                async with semaphore:
                    payload = {"job_postings": batch}
                    return await self._send_bulk_request(add_endpoint, payload, "add", batch_num, total_add_batches,
                                                         len(batch))

            add_tasks = [add_worker(transformed_jobs[i:i + batch_size], (i // batch_size) + 1) for i in
                         range(0, len(transformed_jobs), batch_size)]
            await asyncio.gather(*add_tasks)

        logger.info("Xano synchronization process complete.")
        return {'success': True}


    async def get_all_jobs_for_feed(self, feed_id: int) -> List[Dict[str, Any]]:
        """Fetches all job records from Xano for a given feed_id."""
        get_endpoint = f"{self.api_url}/job_posting/supabase/feeds"
        params = {'feed_id': feed_id}
        try:
            client = await self._get_async_client()
            logger.info(f"Fetching all jobs from Xano for feed_id {feed_id}...")
            response = await client.get(get_endpoint, params=params)
            if response.status_code == 200:
                jobs = response.json()
                logger.info(f"Found {len(jobs)} jobs in Xano for feed_id {feed_id}.")
                return jobs
            else:
                logger.error(
                    f"Failed to fetch jobs from Xano. Status: {response.status_code}, Response: {response.text}")
                return []
        except Exception as e:
            logger.error(f"Exception while fetching all jobs from Xano: {e}", exc_info=True)
            return []

    async def get_jobs_by_hash(self, job_hash: str) -> List[Dict[str, Any]]:
        """
        Fetches all job records from Xano for a given job_hash.
        """
        get_jobs_endpoint = f"{self.api_url}/job_posting/supabase/by_hash"
        params = {"hash": job_hash}

        try:
            client = await self._get_async_client()
            response = await client.get(get_jobs_endpoint, params=params)

            if response.status_code == 200:
                results = response.json()
                # If the API returns a list, return it. Otherwise, return an empty list.
                if results and isinstance(results, list):
                    return results
                else:
                    logger.warning(f"No jobs found in Xano for hash '{job_hash}'.")
                    return []
            else:
                logger.error(
                    f"Failed to fetch jobs by hash '{job_hash}'. Status: {response.status_code}, Response: {response.text}")
                return []
        except Exception as e:
            logger.error(f"Exception while fetching jobs by hash '{job_hash}': {e}", exc_info=True)
            return []

