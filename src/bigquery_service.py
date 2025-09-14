# src/bigquery_service.py

import logging
import asyncio
import json
import uuid
from typing import Dict, Any, List
from google.cloud import bigquery
from .config import Config
from datetime import datetime, timezone, timedelta
from dateutil import parser

logger = logging.getLogger(__name__)


class BigQueryService:
    """Service for all BigQuery operations."""

    def __init__(self):
        """
        Initializes the BigQuery client and sets up the table reference.
        """
        try:
            self.client = bigquery.Client(project=Config.BIGQUERY_PROJECT_ID)
            self.dataset_id = Config.BIGQUERY_DATASET_ID
            self.table_id = f"{Config.BIGQUERY_PROJECT_ID}.{self.dataset_id}.{Config.BIGQUERY_TABLE_ID}"
            logger.info(f"BigQueryService initialized. Target table: {self.table_id}")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}", exc_info=True)
            raise

    async def get_existing_job_ids(self, job_hashes: List[str]) -> Dict[str, str]:
        """
        Queries BigQuery to find which job hashes from the provided list
        already exist and returns a dictionary mapping {job_hash: bigquery_id}.
        """
        if not job_hashes:
            return {}

        logger.info(f"Querying BigQuery for {len(job_hashes)} job hashes to check for existing IDs.")

        query = f"""
            SELECT job_hash, bigquery_id
            FROM `{self.table_id}`
            WHERE job_hash IN UNNEST(@hashes)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("hashes", "STRING", job_hashes),
            ]
        )

        def run_query():
            try:
                query_job = self.client.query(query, job_config=job_config)
                results = query_job.result()
                id_map = {row.job_hash: row.bigquery_id for row in results}
                logger.info(f"Found {len(id_map)} existing jobs in BigQuery.")
                return id_map
            except Exception as e:
                logger.error(f"BigQuery query failed in get_existing_job_ids: {e}", exc_info=True)
                return {}

        return await asyncio.to_thread(run_query)

    async def upsert_jobs_bulk(self, jobs: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Uses a temporary table to stage data and then performs a MERGE
        operation for a robust and scalable bulk upsert.
        """
        if not jobs:
            return {'successful': 0, 'failed': 0}

        table_schema = self.client.get_table(self.table_id).schema
        timestamp_columns = {field.name for field in table_schema if field.field_type == 'TIMESTAMP'}

        rows_to_load = []
        for job in jobs:
            sanitized_job = job.copy()
            for key, value in sanitized_job.items():
                if key == '_geo' and isinstance(value, dict) and 'lng' in value and 'lat' in value:
                    sanitized_job[key] = f"POINT({value['lng']} {value['lat']})"
                elif key in timestamp_columns:
                    if value is None:
                        sanitized_job[key] = None
                        continue
                    try:
                        dt_obj = parser.parse(str(value))

                        if dt_obj.tzinfo is None:
                            aware_dt = dt_obj.replace(tzinfo=timezone.utc)
                        else:
                            aware_dt = dt_obj.astimezone(timezone.utc)

                        sanitized_job[key] = aware_dt.isoformat()
                    except (parser.ParserError, TypeError, ValueError):
                        sanitized_job[key] = None
                elif isinstance(value, (list, dict)):
                    sanitized_job[key] = json.dumps(value)

            rows_to_load.append(sanitized_job)

        temp_table_id = f"temp_upsert_data_{uuid.uuid4().hex}"
        temp_table_ref = f"{self.dataset_id}.{temp_table_id}"

        job_config = bigquery.LoadJobConfig(
            schema=table_schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ignore_unknown_values=True,
        )

        try:
            logger.info(f"Loading {len(rows_to_load)} records into temporary table: {temp_table_ref}")
            load_job = self.client.load_table_from_json(
                rows_to_load,
                temp_table_ref,
                job_config=job_config,
            )
            load_job.result()
            logger.info("Successfully loaded data into temporary table.")

            expires = datetime.now(timezone.utc) + timedelta(hours=1)
            temp_table = self.client.get_table(temp_table_ref)
            temp_table.expires = expires
            self.client.update_table(temp_table, ["expires"])
            logger.info(f"Temporary table will expire at {expires.isoformat()}")

        except Exception as e:
            logger.error(f"Failed to load data into temporary table: {e}", exc_info=True)
            return {'successful': 0, 'failed': len(jobs)}

        update_columns = [field.name for field in table_schema if
                          field.name not in ['bigquery_id', 'job_hash', 'created_at']]

        query = f"""
            MERGE `{self.table_id}` AS T
            USING `{temp_table_ref}` AS S
            ON T.job_hash = S.job_hash
            WHEN MATCHED THEN
                UPDATE SET {', '.join([f'T.{col} = S.{col}' for col in update_columns])}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ({', '.join([field.name for field in table_schema])})
                VALUES ({', '.join([f'S.{field.name}' for field in table_schema])})
        """

        try:
            logger.info(f"Starting BigQuery MERGE operation from temporary table.")
            merge_job = self.client.query(query)
            merge_job.result()
            logger.info(f"BigQuery MERGE operation successful. "
                        f"Rows affected: {merge_job.num_dml_affected_rows}")
            return {'successful': len(rows_to_load), 'failed': 0}
        except Exception as e:
            logger.error(f"BigQuery MERGE operation failed: {e}", exc_info=True)
            return {'successful': 0, 'failed': len(rows_to_load)}

    async def delete_jobs_by_hashes(self, job_hashes: List[str]) -> bool:
        """
        Deletes jobs from the master BigQuery table based on a list of job hashes.
        """
        if not job_hashes:
            logger.info("No job hashes provided for BigQuery deletion.")
            return True

        logger.info(f"Deleting {len(job_hashes)} records from BigQuery table: {self.table_id}")

        query = f"""
            DELETE FROM `{self.table_id}`
            WHERE job_hash IN UNNEST(@hashes)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("hashes", "STRING", job_hashes),
            ]
        )

        def run_delete_query():
            try:
                query_job = self.client.query(query, job_config=job_config)
                query_job.result()  # Wait for the job to complete
                logger.info(f"BigQuery DELETE operation successful. "
                            f"Rows affected: {query_job.num_dml_affected_rows}")
                return True
            except Exception as e:
                logger.error(f"BigQuery DELETE operation failed: {e}", exc_info=True)
                return False

        return await asyncio.to_thread(run_delete_query)