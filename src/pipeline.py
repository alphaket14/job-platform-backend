# src/pipeline.py

import asyncio
import logging
import uuid
import json
from typing import Dict, Any, List, Set
from datetime import datetime, timezone

import requests
from .config import Config, load_mapping_config_from_gcs
from .database import DatabaseService
from .file_processor import parse_xml_stream_from_response, parse_xml_stream
from .job_hasher import get_canonical_job_hash
from .location import aggregate_multi_location_jobs_efficient
from .notification_service import NotificationService
from .review_queue import ReviewQueue
from .run_statistics import RunStatistics
from .schema import transform_job_data
from .ai_service import AIService
from .xano_service import XanoService
from .upstash_service import UpstashService
from .bigquery_service import BigQueryService
from .geolocation_service import generate_location_string, get_geo_coordinates, save_geo_cache

logger = logging.getLogger(__name__)


# --- START: DEBUGGING HELPER ---
def _log_hash_sample(data: list, stage_name: str):
    """A helper function to log a sample of hashes at different stages."""
    if not data:
        logger.info(f"HASH LOG ({stage_name}): No jobs in list.")
        return

    # Check if the list contains job dictionaries or just hash strings
    if isinstance(data[0], dict):
        hashes = {job.get('job_hash', 'N/A') for job in data}
    else:  # Assumes a list of strings
        hashes = set(data)

    sample = list(hashes)[:5]
    logger.info(f"HASH LOG ({stage_name}): Found {len(hashes)} unique hashes. Sample: {sample}")


# --- END: DEBUGGING HELPER ---

PARTNER_MAPPING = {
    'appcast': 6, 'buyer': 7, 'veritone': 8, 'joveo': 9, 'csquared': 10,
    'irecruitics': 11, 'zip recruiter': 12, 'sample': 13, 'recruitics': 14, 'job target': 15
}


def _map_and_finalize_job_data(job: Dict[str, Any], feed_id: int, feed_name: str, partner_id: int) -> Dict[str, Any]:
    if 'reasoning_industry' in job: job['ih_reasoning'] = job.pop('reasoning_industry')
    if 'reasoning_confidence' in job: job['confidence_reasoning'] = job.pop('reasoning_confidence')
    if 'reasoning' in job: job['jf_reasoning'] = job.pop('reasoning')
    if 'confidence_score' in job: job['ai_confidence_score'] = job.pop('confidence_score')
    job['feed_id'], job['partner_id'], job['job_source'] = feed_id, partner_id, feed_name
    now_utc = datetime.now(timezone.utc)
    job['created_at'], job['updated_at'] = job.get('created_at', now_utc), now_utc
    return job


def _filter_and_normalize_jobs(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    final_schema_keys = {
        'bigquery_id', 'external_job_id', 'job_hash', 'title', 'company_name', 'description',
        'locations', 'is_multi_location', 'is_remote', 'is_international',
        'application_url', 'posted_at', 'employment_type', 'status', 'ai_confidence_score',
        'ai_enrichment_status', 'ai_enrichment_error', 'ai_title', 'ai_description', 'ai_job_tasks',
        'ai_search_terms', 'ai_top_tags', 'ai_skills', 'sector_id', 'industry_group_id',
        'industry_id', 'job_function_id', 'sector', 'industry_group', 'industry', 'ih_reasoning',
        'jf_reasoning', 'confidence_reasoning', 'salary_min', 'salary_max', 'salary_period',
        'currency', 'cpc', 'cpa', 'feed_id', 'execution_id', 'job_source', 'created_at', 'updated_at',
        'partner_id', 'location_string', '_geo','searchable_text'
    }
    normalized_jobs = []
    for job_data in jobs:
        filtered_job = {key: job_data.get(key) for key in final_schema_keys if key in job_data}
        for key in final_schema_keys:
            if key not in filtered_job:
                if key in ['ai_job_tasks', 'ai_search_terms', 'ai_top_tags', 'ai_skills', 'locations']:
                    filtered_job[key] = []
                elif key in ['sector_id', 'industry_group_id', 'industry_id', 'job_function_id', 'partner_id',
                             'execution_id', '_geo']:
                    filtered_job[key] = None
                elif key in ['cpc', 'cpa', 'ai_confidence_score', 'salary_min', 'salary_max']:
                    filtered_job[key] = 0.0
                elif key in ['is_multi_location', 'is_remote', 'is_international']:
                    filtered_job[key] = False
                else:
                    filtered_job[key] = None
        if not filtered_job.get('job_hash'):
            filtered_job['job_hash'] = get_canonical_job_hash(job_data)
        normalized_jobs.append(filtered_job)
    return normalized_jobs


class JobPipeline:
    def __init__(self):
        self.db_service: DatabaseService = None
        self.ai_service: AIService = None
        self.xano_service = XanoService()
        self.upstash_service = UpstashService()
        self.bigquery_service: BigQueryService = None
        self.review_queue = ReviewQueue()
        self.notification_service = NotificationService()
        self.auto_approve_threshold = Config.AUTO_APPROVE_THRESHOLD
        self.manual_review_threshold = Config.MANUAL_REVIEW_THRESHOLD

    @classmethod
    async def create(cls):
        self = cls();
        self.db_service = await DatabaseService.create()
        self.bigquery_service = BigQueryService()
        return self

    async def process_feed_batched(self, input_path: str, feed_id: int = 0, feed_name: str = "sample_feed") -> Dict[
        str, Any]:
        statistics = RunStatistics(feed_id=feed_id, feed_name=feed_name)
        self.ai_service = AIService(statistics=statistics)
        mapping_config = await load_mapping_config_from_gcs()
        feed_config = await self.db_service.get_feed_by_id(feed_id)
        if not feed_config: logger.error(f"Feed with ID {feed_id} not found.")
        should_bypass_filters = feed_config.get('bypass_filters', False)
        if should_bypass_filters: logger.info(f"Bypass filters is ENABLED for feed_id: {feed_id}.")
        execution_id = await self.db_service.create_initial_execution_report(feed_id, feed_name)
        results = {"success": False, "execution_id": execution_id, "errors": []}
        all_ai_failed_jobs = []

        try:
            logger.info("--- Stage 1: Parsing & Pre-filtering ---")
            partially_transformed_jobs = []
            if input_path.startswith(('http://', 'https://')):
                response = requests.get(input_path, stream=True, timeout=120)
                response.raise_for_status()
                for job_chunk, bytes_read in parse_xml_stream_from_response(response, chunk_size=100, track_bytes=True,
                                                                            mapping_config=mapping_config):
                    partially_transformed_jobs.extend(job_chunk)
                    statistics.set_feed_file_size(bytes_read)
            else:
                for job_chunk in parse_xml_stream(input_path, chunk_size=100, mapping_config=mapping_config):
                    partially_transformed_jobs.extend(job_chunk)

            transformed_jobs_list = [transform_job_data(job, mapping_config) for job in partially_transformed_jobs if
                                     job]
            statistics.set_total_in_feed(len(transformed_jobs_list))

            transformed_jobs_active = [job for job in transformed_jobs_list if job.get('status') != 'CLOSED']
            for job in transformed_jobs_active: job['job_hash'] = get_canonical_job_hash(job)
            statistics.increment_skipped_closed(len(transformed_jobs_list) - len(transformed_jobs_active))

            _log_hash_sample(transformed_jobs_active, "Initial Hashes from Feed")

            jobs_to_process = transformed_jobs_active
            logger.info(
                f"Processing all {len(jobs_to_process)} active jobs from the feed. Caching will be used for existing jobs.")



            logger.info(f"--- Stage 1 Complete: Found {len(jobs_to_process)} new jobs to process. ---")

            logger.info("--- Stages 2-5: Full Processing of New Jobs ---")
            if should_bypass_filters:
                logger.info("Bypassing first multi-location aggregation.")
                aggregated_jobs_1 = jobs_to_process
                duplicates_found_1 = 0
            else:
                aggregated_jobs_1, duplicates_found_1 = await asyncio.to_thread(aggregate_multi_location_jobs_efficient,
                                                                                jobs_to_process,
                                                                                accuracy_mode=Config.MULTI_LOCATION_ACCURACY_MODE)
            statistics.skipped_duplicate_initial_aggregation = duplicates_found_1
            _log_hash_sample(aggregated_jobs_1, "Hashes After 1st Aggregation")

            all_enriched_jobs = await self.ai_service.get_initial_enrichment_with_gemini(aggregated_jobs_1)
            jobs_that_succeeded_ai = [job for job in all_enriched_jobs if job.get('ai_enrichment_status') != 'failed']
            initial_failed_jobs = [job for job in all_enriched_jobs if job.get('ai_enrichment_status') == 'failed']
            all_ai_failed_jobs.extend(initial_failed_jobs)
            logger.info(f"Initial AI Enrichment complete. Succeeded: {len(jobs_that_succeeded_ai)}, Failed: {len(initial_failed_jobs)}")

            if should_bypass_filters:
                logger.info("Bypassing second multi-location aggregation.")
                final_aggregated_jobs = jobs_that_succeeded_ai
                duplicates_found_2 = 0
            else:
                final_aggregated_jobs, duplicates_found_2 = await asyncio.to_thread(
                    aggregate_multi_location_jobs_efficient,
                    jobs_that_succeeded_ai,
                    use_ai_title=True,
                    accuracy_mode=Config.MULTI_LOCATION_ACCURACY_MODE)
            statistics.skipped_duplicate_ai_aggregation = duplicates_found_2
            _log_hash_sample(final_aggregated_jobs, "Hashes After 2nd Aggregation")
            logger.info(f"--- Stages 2-5 Complete: {len(final_aggregated_jobs)} jobs remain after all processing. ---")

            logger.info("--- Stages 6-8: Final Classification and Routing ---")
            job_functions_map = await self.ai_service.get_job_functions_batch_async(final_aggregated_jobs)
            fully_processed_jobs = await self.ai_service.classify_job_functions_with_gemini(final_aggregated_jobs)

            jobs_succeeded_classification = [job for job in fully_processed_jobs if
                                             job.get('ai_enrichment_status') != 'failed']
            classification_failed_jobs = [job for job in fully_processed_jobs if
                                          job.get('ai_enrichment_status') == 'failed']
            all_ai_failed_jobs.extend(classification_failed_jobs)

            logger.info(f"AI Job Function Classification complete. Succeeded: {len(jobs_succeeded_classification)}, Failed: {len(classification_failed_jobs)}")


            feed_company_name = feed_config.get('company_name', '').lower().strip()
            partner_id_for_feed = PARTNER_MAPPING.get(feed_company_name)
            finalized_jobs = [_map_and_finalize_job_data(job, feed_id, feed_name, partner_id_for_feed) for job in
                              jobs_succeeded_classification]
            job_function_lookup = self.ai_service.build_job_function_lookup(job_functions_map)
            all_populated_jobs = self.ai_service._populate_industry_strings(finalized_jobs, job_function_lookup)
            logger.info(f"Generating location_string and _geo fields for {len(all_populated_jobs)} jobs...")
            for job in all_populated_jobs:
                locations = job.get('locations', [])
                job['location_string'] = generate_location_string(locations)
                job['_geo'] = get_geo_coordinates(locations)

            unique_jobs_by_hash = {job['job_hash']: job for job in all_populated_jobs}
            all_populated_jobs = list(unique_jobs_by_hash.values())
            logger.info(f"Enforced uniqueness. Final job count for processing: {len(all_populated_jobs)}")


            approved_jobs, manual_review_jobs, rejected_jobs = [], [], []
            for job in all_populated_jobs:
                confidence = job.get('ai_confidence_score', 0) / 100.0
                if should_bypass_filters:
                    job['status'] = 'ACTIVE'; approved_jobs.append(job)
                elif confidence < self.manual_review_threshold:
                    job['status'] = 'REJECTED'; rejected_jobs.append(job)
                elif confidence < self.auto_approve_threshold:
                    job['status'] = 'MANUAL_REVIEW'; manual_review_jobs.append(job)
                else:
                    job['status'] = 'ACTIVE'; approved_jobs.append(job)
            logger.info(f"Final routing complete. Approved: {len(approved_jobs)}, Manual Review: {len(manual_review_jobs)}, Rejected: {len(rejected_jobs)}")
            if approved_jobs:
                logger.info(f"Generating searchable text for {len(approved_jobs)} approved jobs...")
                approved_jobs = await self.ai_service.generate_searchable_text_async(approved_jobs)

            statistics.increment_records_failed(len(all_ai_failed_jobs))
            statistics.increment_jobs_auto_approved(len(approved_jobs))
            statistics.increment_jobs_manual_review(len(manual_review_jobs))
            statistics.increment_jobs_rejected(len(rejected_jobs))
            logger.info(f"--- Stages 6-8 Complete ---")
            final_succeeded_hashes = {job['job_hash'] for job in all_populated_jobs}
            db_hashes = set((await self.db_service.get_active_job_hashes_for_feed(feed_id)).keys())
            hashes_to_archive = db_hashes - final_succeeded_hashes

            # Check if there is any work to do (new jobs to save or old jobs to archive)
            if not all_populated_jobs and not hashes_to_archive:
                logger.warning("No new jobs to process and no jobs to archive.")
                results["success"] = True
                # The rest of the finalization will happen in the `finally` block
                return results
            # --- Final Saving Stage ---
            logger.info("--- Final Saving Stage ---")
            jobs_to_upsert = approved_jobs + manual_review_jobs + rejected_jobs

            archived_jobs_for_bq = []
            if hashes_to_archive:
                logger.info(f"--- HASH COMPARISON FOR ARCHIVING ---")
                _log_hash_sample(list(db_hashes), "DATABASE (Current)")
                _log_hash_sample(list(final_succeeded_hashes ), "FINAL PROCESSED FEED")
                logger.info(f"Archiving {len(hashes_to_archive)} jobs from PostgreSQL...")
                archived_count = await self.db_service.archive_jobs_by_hashes(list(hashes_to_archive))
                statistics.increment_jobs_closed(archived_count)
                logger.info(f"Fetching full data for {len(hashes_to_archive)} archived jobs for BigQuery...")
                full_archived_jobs = await self.db_service.get_jobs_by_hashes(list(hashes_to_archive))
                for job in full_archived_jobs: job['status'] = 'ARCHIVED'
                archived_jobs_for_bq = full_archived_jobs

            all_jobs_for_bq = jobs_to_upsert + archived_jobs_for_bq
            if all_jobs_for_bq:
                _log_hash_sample(all_jobs_for_bq, "Final Hashes for Upsert")
                logger.info(f"Upserting {len(all_jobs_for_bq)} records to BigQuery...")
                bq_id_map = await self.bigquery_service.get_existing_job_ids([j['job_hash'] for j in all_jobs_for_bq])
                for job in all_jobs_for_bq:
                    job['execution_id'] = execution_id
                    if job['job_hash'] in bq_id_map:
                        job['bigquery_id'] = bq_id_map[job['job_hash']]
                    else:
                        job['bigquery_id'] = str(uuid.uuid4())
                await self.bigquery_service.upsert_jobs_bulk(all_jobs_for_bq)

            logger.info("Saving new jobs to downstream systems (PostgreSQL & Xano)...")
            db_approved = _filter_and_normalize_jobs(approved_jobs)
            db_manual = _filter_and_normalize_jobs(manual_review_jobs)
            db_rejected = _filter_and_normalize_jobs(rejected_jobs)
            db_tasks = []
            if db_approved: db_tasks.append(self.db_service.insert_jobs_bulk(db_approved, execution_id))
            if db_manual: db_tasks.append(self.db_service.insert_manual_review_jobs_bulk(db_manual, execution_id))
            if db_rejected: db_tasks.append(self.db_service.insert_rejected_jobs_bulk(db_rejected, execution_id))
            if db_tasks: await asyncio.gather(*db_tasks)
            if db_approved:
                logger.info("Syncing approved jobs with Xano...")

                local_hashes = await self.db_service.get_open_job_hashes_for_feed(feed_id)
                xano_hash_eid_map = await self.xano_service.get_xano_hash_to_eid_map(feed_id)
                xano_hashes = set(xano_hash_eid_map.keys())
                hashes_to_delete_from_xano = list(xano_hashes - local_hashes)
                jobs_to_add_to_xano = [job for job in db_approved if job['job_hash'] in (local_hashes - xano_hashes)]

                if hashes_to_delete_from_xano:
                    eids_to_delete_from_upstash = [
                        xano_hash_eid_map[job_hash] for job_hash in hashes_to_delete_from_xano if
                        job_hash in xano_hash_eid_map
                    ]

                    if eids_to_delete_from_upstash:
                        await self.upstash_service.delete_jobs_by_eid(eids_to_delete_from_upstash)
                statistics.xano_jobs_added, statistics.xano_jobs_deleted = len(jobs_to_add_to_xano), len(
                    hashes_to_delete_from_xano)
                await self.xano_service.sync_jobs_to_xano_bulk(jobs_to_add_to_xano, hashes_to_delete_from_xano)

                # 5. Get the final, authoritative list of all jobs from Xano
                logger.info("Fetching final job list from Xano to upsert to Upstash...")
                final_jobs_from_xano = await self.xano_service.get_all_jobs_for_feed(feed_id)

                # 6. Upsert the final list into Upstash
                if final_jobs_from_xano:
                    await self.upstash_service.upsert_jobs(final_jobs_from_xano)

            results["success"] = True
        except Exception as e:
            error_msg = f"Pipeline processing failed: {e}"
            logger.error(error_msg, exc_info=True)
            results["errors"].append(error_msg)
            statistics.add_error(error_msg)
        finally:
            save_geo_cache()
            statistics.finalize_run("SUCCESS" if results.get("success") else "FAILURE")
            final_report = statistics.get_summary_dict()
            await self.db_service.update_execution_report(execution_id, final_report)
            try:
                current_feed = await self.db_service.get_feed_by_id(feed_id)
                aggregates = await self.db_service.get_feed_aggregates(feed_id)
                feed_update_data = {
                    "last_checked_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc),
                    "last_run_status": final_report['run_status'],
                    "unique_jobs_processed": final_report['processed_successfully'],
                    "cw_auto_approved_jobs": final_report['jobs_auto_approved'],
                    "avg_processing_time": aggregates.get('avg_processing_time'),
                    "avg_feed_size": aggregates.get('avg_feed_size'),
                }
                if final_report['run_status'] == 'SUCCESS':
                    feed_update_data['last_succeeded_at'] = datetime.now(timezone.utc)
                    feed_update_data['consecutive_failure_count'] = 0
                else:
                    current_failures = current_feed.get('consecutive_failure_count', 0) if current_feed else 0
                    current_errors = current_feed.get('error_count', 0) if current_feed else 0
                    feed_update_data['consecutive_failure_count'] = (current_failures or 0) + 1
                    feed_update_data['error_count'] = (current_errors or 0) + 1
                    if final_report.get('error_summary'): feed_update_data['last_error_message'] = str(
                        final_report['error_summary'][0])
                await self.db_service.update_feed_statistics(feed_id, feed_update_data)
            except Exception as e:
                logger.error(f"Failed to update feeds table for feed_id {feed_id}: {e}", exc_info=True)

            self.notification_service.send_notification(final_report)
            await self.xano_service.close_async_client()
            logger.info("Pipeline run finished.")
        return results