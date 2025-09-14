# src/database.py

"""
Asynchronous database operations for PostgreSQL using asyncpg.
"""
import logging
import json
from email.utils import parsedate_to_datetime
from typing import Dict, Any, List, Set, Optional
import asyncpg
from .config import Config
from datetime import datetime, timezone
from dateutil import parser

logger = logging.getLogger(__name__)

def _parse_datetime_string(dt_string: Optional[str]) -> Optional[datetime]:
    """
    Safely parses various string formats into a naive UTC datetime object.
    Handles ISO 8601, RFC 2822, and many other formats via dateutil.
    """
    if not dt_string or not isinstance(dt_string, str):
        return None

    try:
        dt_obj = parser.parse(dt_string, ignoretz=True)
        return dt_obj.astimezone(timezone.utc).replace(tzinfo=None) if dt_obj.tzinfo else dt_obj
    except Exception:
        return None

def _make_naive_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Ensures a datetime object is naive and in UTC."""
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt

class DatabaseService:
    """Service for asynchronous database operations with PostgreSQL using asyncpg."""
    _pool: asyncpg.Pool = None

    @classmethod
    async def create(cls):
        """Asynchronously creates and initializes the DatabaseService."""
        self = cls()
        if not cls._pool:
            try:
                pool_options = {
                    "min_size": 5, "max_size": 20, "statement_cache_size": 0
                }
                if Config.POSTGRES_CONNECTION_STRING:
                    logger.info("Connecting to PostgreSQL using connection string.")
                    cls._pool = await asyncpg.create_pool(dsn=Config.POSTGRES_CONNECTION_STRING, **pool_options)
                else:
                    logger.info("Connecting to PostgreSQL using connection parameters.")
                    cls._pool = await asyncpg.create_pool(
                        host=Config.POSTGRES_HOST, port=Config.POSTGRES_PORT,
                        database=Config.POSTGRES_DB, user=Config.POSTGRES_USER,
                        password=Config.POSTGRES_PASSWORD, **pool_options
                    )
                logger.info("asyncpg connection pool initialized successfully.")
            except Exception as e:
                logger.error(f"Failed to initialize asyncpg connection pool: {e}", exc_info=True)
                raise
        return self

    async def close_pool(self):
        """Closes the connection pool."""
        if self._pool:
            await self._pool.close()
            self.__class__._pool = None
            logger.info("asyncpg connection pool closed.")

    async def get_active_job_hashes_for_feed(self, feed_id: int) -> Dict[str, int]:
        """Gets active job hashes and their IDs for a specific feed."""
        query = "SELECT job_hash, id FROM open_jobs WHERE status = 'ACTIVE' AND feed_id = $1 AND job_hash IS NOT NULL"
        try:
            records = await self._pool.fetch(query, feed_id)
            return {row['job_hash']: row['id'] for row in records}
        except Exception as e:
            logger.error(f"Error fetching active job hashes for feed {feed_id}: {e}", exc_info=True)
            return {}

    async def get_existing_hashes_from_all_tables(self, hashes: List[str]) -> Set[str]:
        """Checks for hash existence across open, manual_review, and rejected tables."""
        if not hashes:
            return set()

        query = """
            (SELECT job_hash FROM open_jobs WHERE job_hash = ANY($1::text[]))
            UNION
            (SELECT job_hash FROM manual_review_jobs WHERE job_hash = ANY($1::text[]))
            UNION
            (SELECT job_hash FROM rejected_jobs WHERE job_hash = ANY($1::text[]))
        """
        try:
            records = await self._pool.fetch(query, hashes)
            return {row['job_hash'] for row in records}
        except Exception as e:
            logger.error(f"Error checking existing hashes across tables: {e}", exc_info=True)
            return set()

    # --- START: NEW FUNCTION ---
    async def get_jobs_by_hashes(self, job_hashes: List[str]) -> List[Dict[str, Any]]:
        """Gets full job records from open_jobs for a list of job hashes."""
        if not job_hashes:
            return []
        
        query = "SELECT * FROM open_jobs WHERE job_hash = ANY($1::text[])"
        try:
            records = await self._pool.fetch(query, job_hashes)
            # Convert asyncpg.Record objects to standard Python dictionaries
            return [dict(row) for row in records]
        except Exception as e:
            logger.error(f"Error fetching jobs by hashes: {e}", exc_info=True)
            return []
    # --- END: NEW FUNCTION ---

    async def get_open_job_hashes_for_feed(self, feed_id: int) -> Set[str]:
        """Gets the set of all job_hashes from the open_jobs table for a specific feed."""
        query = "SELECT job_hash FROM open_jobs WHERE feed_id = $1 AND job_hash IS NOT NULL"
        try:
            records = await self._pool.fetch(query, feed_id)
            return {row['job_hash'] for row in records}
        except Exception as e:
            logger.error(f"Error fetching open job hashes for feed {feed_id}: {e}", exc_info=True)
            return set()



    async def _insert_jobs_bulk_generic(self, table_name: str, jobs_data: List[Dict[str, Any]], execution_id: int = None, mode: str = 'upsert') -> Dict[str, int]:
        """
        Generic function to bulk insert/update jobs into a table.
        """
        feed_id = jobs_data[0].get('feed_id') if jobs_data else None
        if mode == 'replace' and not feed_id and jobs_data:
            logger.error(f"Cannot perform 'replace' on '{table_name}' without a feed_id.")
            return {'successful': 0, 'failed': len(jobs_data)}
        if not jobs_data and mode != 'replace':
            return {'successful': 0, 'failed': 0}

        processed_jobs = []
        for job in jobs_data:
            processed_job = job.copy()
            for field in {'locations', 'ai_job_tasks', 'ai_search_terms', 'ai_top_tags', 'ai_skills', '_geo'}:
                if field in processed_job and isinstance(processed_job[field], (dict, list)):
                    processed_job[field] = json.dumps(processed_job[field])
            for field in {'posted_at', 'created_at', 'updated_at', 'expires_at'}:
                if field in processed_job and (val := processed_job[field]):
                    processed_job[field] = _parse_datetime_string(val) if isinstance(val, str) else _make_naive_utc(val)
            if execution_id:
                processed_job['execution_id'] = execution_id
            processed_job.pop('id', None)
            processed_job.pop('debug_id', None)
            processed_job.pop('job_functions', None)
            processed_job.pop('city', None)
            processed_job.pop('state', None)
            processed_job.pop('country', None)
            processed_jobs.append(processed_job)

        columns = list(processed_jobs[0].keys()) if processed_jobs else []
        placeholders = ', '.join(f'${i+1}' for i in range(len(columns)))
        data_to_insert = [tuple(job.get(col) for col in columns) for job in processed_jobs]

        if mode == 'upsert':
            update_cols = [col for col in columns if col not in ['job_hash', 'created_at', 'id']]
            update_clause = ', '.join(f"{col} = EXCLUDED.{col}" for col in update_cols)
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT (job_hash) DO UPDATE SET {update_clause}"
            try:
                await self._pool.executemany(query, data_to_insert)
                logger.info(f"Bulk upserted {len(processed_jobs)} jobs into '{table_name}'.")
                return {'successful': len(processed_jobs), 'failed': 0}
            except Exception as e:
                logger.error(f"Error during bulk upsert into '{table_name}': {e}", exc_info=True)
                return {'successful': 0, 'failed': len(jobs_data)}
        elif mode == 'replace':
            delete_query = f"DELETE FROM {table_name} WHERE feed_id = $1"
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    try:
                        status = await conn.execute(delete_query, feed_id)
                        deleted_count = int(status.split(' ')[-1])
                        if processed_jobs:
                            await conn.executemany(insert_query, data_to_insert)
                        logger.info(f"Replaced jobs in '{table_name}' for feed_id {feed_id}. Deleted {deleted_count}, Inserted {len(processed_jobs)}.")
                        return {'successful': len(processed_jobs), 'failed': 0}
                    except Exception as e:
                        logger.error(f"Error during replace transaction for '{table_name}' on feed_id {feed_id}: {e}", exc_info=True)
                        return {'successful': 0, 'failed': len(jobs_data)}
        return {'successful': 0, 'failed': 0}

    async def insert_jobs_bulk(self, jobs_data: List[Dict[str, Any]], execution_id: int = None) -> Dict[str, int]:
        return await self._insert_jobs_bulk_generic('open_jobs', jobs_data, execution_id, mode='upsert')

    async def insert_manual_review_jobs_bulk(self, jobs_data: List[Dict[str, Any]], execution_id: int = None) -> Dict[str, int]:
        return await self._insert_jobs_bulk_generic('manual_review_jobs', jobs_data, execution_id, mode='upsert')

    async def insert_rejected_jobs_bulk(self, jobs_data: List[Dict[str, Any]], execution_id: int = None) -> Dict[str, int]:
        return await self._insert_jobs_bulk_generic('rejected_jobs', jobs_data, execution_id, mode='upsert')

    async def archive_jobs_by_hashes(self, job_hashes: List[str]) -> int:
        if not job_hashes: return 0
        archive_cols = [
            "id", "created_at", "updated_at", "external_job_id", "job_hash", "title", "company_name",
            "description", "locations", "is_multi_location", "is_remote", "is_international",
            "application_url", "posted_at", "employment_type", "status", "ai_confidence_score",
            "ai_enrichment_status", "ai_enrichment_error", "ai_title", "ai_description", "ai_job_tasks",
            "ai_search_terms", "ai_top_tags", "ai_skills", "sector_id", "industry_group_id",
            "industry_id", "job_function_id", "sector", "industry_group", "industry", "ih_reasoning",
            "jf_reasoning", "confidence_reasoning", "salary_min", "salary_max", "salary_period",
            "currency", "cpc", "cpa", "feed_id", "execution_id", "job_source",
            "location_string", "_geo"
        ]
        col_list = ", ".join(archive_cols)
        update_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in archive_cols if col not in ['job_hash', 'created_at', 'id'])
        copy_query = f"INSERT INTO archive_jobs ({col_list}) SELECT {col_list} FROM open_jobs WHERE job_hash = ANY($1::text[]) ON CONFLICT (job_hash) DO UPDATE SET {update_clause}"
        delete_query = "DELETE FROM open_jobs WHERE job_hash = ANY($1::text[])"
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                try:
                    copy_status = await conn.execute(copy_query, job_hashes)
                    copied_count = int(copy_status.split(' ')[-1])
                    delete_status = await conn.execute(delete_query, job_hashes)
                    deleted_count = int(delete_status.split(' ')[-1])
                    if copied_count == deleted_count:
                        logger.info(f"Successfully archived {copied_count} jobs.")
                        return copied_count
                    raise Exception(f"Archive count mismatch. Copied: {copied_count}, Deleted: {deleted_count}")
                except Exception as e:
                    logger.error(f"Error during job archival transaction: {e}", exc_info=True)
                    return 0

    async def create_initial_execution_report(self, feed_id: int, feed_name: str) -> int:
        query = "INSERT INTO execution_reports (feed_id, feed_name, start_time, run_status) VALUES ($1, $2, $3, 'RUNNING') RETURNING id"
        try:
            now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
            return await self._pool.fetchval(query, feed_id, feed_name, now_naive)
        except Exception as e:
            logger.error(f"Error creating initial execution report: {e}", exc_info=True)
            return 0

    async def update_execution_report(self, execution_id: int, report_data: Dict[str, Any]) -> bool:
        report_data.pop('execution_report_id', None)
        for field in ['start_time', 'end_time', 'created_at']:
            if field in report_data and (val := report_data[field]):
                report_data[field] = _parse_datetime_string(val) if isinstance(val, str) else _make_naive_utc(val)
        if 'error_summary' in report_data and isinstance(report_data['error_summary'], list):
            report_data['error_summary'] = json.dumps(report_data['error_summary'])
        cols = list(report_data.keys())
        set_clauses = ', '.join(f"{col} = ${i+1}" for i, col in enumerate(cols))
        values = list(report_data.values()) + [execution_id]
        query = f"UPDATE execution_reports SET {set_clauses} WHERE id = ${len(values)}"
        try:
            await self._pool.execute(query, *values)
            logger.info(f"Updated execution report {execution_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating execution report {execution_id}: {e}", exc_info=True)
            return False

    async def get_feed_by_id(self, feed_id: int) -> Optional[Dict[str, Any]]:
        try:
            record = await self._pool.fetchrow("SELECT * FROM feeds WHERE id = $1", feed_id)
            return dict(record) if record else None
        except Exception as e:
            logger.error(f"Error retrieving feed {feed_id}: {e}", exc_info=True)
            return None

    async def get_feed_aggregates(self, feed_id: int) -> Dict[str, Any]:
        query = "SELECT AVG(duration_seconds) as avg_processing_time, AVG(feed_file_size_bytes) as avg_feed_size FROM execution_reports WHERE feed_id = $1 AND duration_seconds IS NOT NULL AND feed_file_size_bytes IS NOT NULL;"
        try:
            record = await self._pool.fetchrow(query, feed_id)
            return dict(record) if record else {"avg_processing_time": 0.0, "avg_feed_size": 0.0}
        except Exception as e:
            logger.error(f"Error calculating feed aggregates for feed {feed_id}: {e}", exc_info=True)
            return {"avg_processing_time": 0.0, "avg_feed_size": 0.0}

    async def update_feed_statistics(self, feed_id: int, stats_data: Dict[str, Any]) -> bool:
        columns = list(stats_data.keys())
        set_clauses = [f"{col} = ${i+1}" for i, col in enumerate(columns)]
        values = list(stats_data.values()) + [feed_id]
        query = f"UPDATE feeds SET {', '.join(set_clauses)} WHERE id = ${len(values)}"
        try:
            await self._pool.execute(query, *values)
            logger.info(f"Updated statistics for feed {feed_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating statistics for feed {feed_id}: {e}", exc_info=True)
            return False

    async def get_latest_execution_report(self, feed_id: int) -> Optional[Dict[str, Any]]:
        query = "SELECT * FROM execution_reports WHERE feed_id = $1 ORDER BY start_time DESC LIMIT 1;"
        try:
            record = await self._pool.fetchrow(query, feed_id)
            if record:
                report = dict(record)
                if 'error_summary' in report and isinstance(report['error_summary'], str):
                    try: report['error_summary'] = json.loads(report['error_summary'])
                    except json.JSONDecodeError: pass
                return report
            return None
        except Exception as e:
            logger.error(f"Error retrieving latest execution report for feed {feed_id}: {e}", exc_info=True)
            return None

    async def get_manual_review_job_by_hash(self, job_hash: str) -> Optional[Dict[str, Any]]:
        """Fetches a single job from the manual_review_jobs table by its hash."""
        query = "SELECT * FROM manual_review_jobs WHERE job_hash = $1"
        try:
            record = await self._pool.fetchrow(query, job_hash)
            return dict(record) if record else None
        except Exception as e:
            logger.error(f"Error fetching manual review job with hash {job_hash}: {e}", exc_info=True)
            return None

    async def delete_manual_review_job_by_hash(self, job_hash: str) -> int:
        """Deletes a job from the manual_review_jobs table by its hash."""
        query = "DELETE FROM manual_review_jobs WHERE job_hash = $1"
        try:
            status = await self._pool.execute(query, job_hash)
            deleted_count = int(status.split(' ')[-1])
            logger.info(f"Deleted {deleted_count} job(s) with hash {job_hash} from manual_review_jobs.")
            return deleted_count
        except Exception as e:
            logger.error(f"Error deleting manual review job with hash {job_hash}: {e}", exc_info=True)
            return 0

    async def delete_open_jobs_by_hashes(self, job_hashes: List[str]) -> int:
        """Deletes jobs directly from the open_jobs table by their hash."""
        if not job_hashes:
            return 0

        query = "DELETE FROM open_jobs WHERE job_hash = ANY($1::text[])"
        try:
            status = await self._pool.execute(query, job_hashes)
            deleted_count = int(status.split(' ')[-1])
            logger.info(f"Hard deleted {deleted_count} job(s) from open_jobs.")
            return deleted_count
        except Exception as e:
            logger.error(f"Error deleting jobs from open_jobs by hash: {e}", exc_info=True)
            return 0