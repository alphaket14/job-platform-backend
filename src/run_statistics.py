"""
Run statistics collection for pipeline execution reporting.
"""
import time
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class RunStatistics:
    """Statistics collector for pipeline execution runs."""

    feed_id: int = 0
    feed_name: str = "unknown"
    run_status: str = "RUNNING"
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0

    total_in_feed: int = 0
    processed_successfully: int = 0
    skipped_closed: int = 0
    skipped_duplicate: int = 0
    records_failed: int = 0
    jobs_closed: int = 0
    jobs_auto_approved: int = 0
    jobs_manual_review: int = 0
    jobs_rejected: int = 0

    skipped_duplicate_initial_aggregation: int = 0
    skipped_duplicate_ai_aggregation: int = 0
    xano_jobs_added: int = 0
    xano_jobs_deleted: int = 0

    api_calls_gemini: int = 0
    api_calls_custom: int = 0
    cache_hits: int = 0

    error_summary: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    gemini_input_tokens: int = 0
    gemini_output_tokens: int = 0
    gemini_total_tokens: int = 0

    feed_file_size_bytes: int = 0

    def __post_init__(self):
        """Initialize the statistics object."""
        self.start_time = datetime.now(timezone.utc)
        logger.info(f"Run statistics initialized for feed_id={self.feed_id}, feed_name='{self.feed_name}'")

    def increment_processed_successfully(self, count: int = 1):
        self.processed_successfully += count

    def increment_skipped_closed(self, count: int = 1):
        self.skipped_closed += count

    def increment_skipped_duplicate(self, count: int = 1):
        self.skipped_duplicate += count

    def increment_records_failed(self, count: int = 1):
        self.records_failed += count

    def increment_jobs_closed(self, count: int = 1):
        self.jobs_closed += count

    def increment_jobs_auto_approved(self, count: int = 1):
        self.jobs_auto_approved += count

    def increment_jobs_manual_review(self, count: int = 1):
        self.jobs_manual_review += count

    def increment_jobs_rejected(self, count: int = 1):
        self.jobs_rejected += count

    def increment_api_call(self, api_type: str, count: int = 1):
        if "gemini" in api_type:
            self.api_calls_gemini += count
        else:
            self.api_calls_custom += count

    def increment_cache_hit(self, cache_type: str, count: int = 1):
        self.cache_hits += count

    def add_error(self, error_message: str):
        self.errors.append(error_message)
        if len(self.errors) > 10:
            self.errors = self.errors[-10:]
        logger.error(f"Added error to statistics: {error_message}")

    def set_total_in_feed(self, count: int):
        self.total_in_feed = count

    def set_feed_file_size(self, size_bytes: int):
        self.feed_file_size_bytes = size_bytes

    def add_gemini_tokens(self, input_tokens: int, output_tokens: int):
        self.gemini_input_tokens += input_tokens
        self.gemini_output_tokens += output_tokens
        self.gemini_total_tokens += (input_tokens + output_tokens)

    def finalize_run(self, status: str = "SUCCESS"):
        """Finalize the run statistics."""
        self.end_time = datetime.now(timezone.utc)
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()
        self.run_status = status
        self.processed_successfully = self.jobs_auto_approved + self.jobs_manual_review + self.jobs_rejected
        if self.errors:
            self.error_summary = self.errors

        logger.info(f"Run finalized with status: {self.run_status}, duration: {self.duration_seconds:.2f}s")

    def get_summary_dict(self) -> Dict[str, Any]:
        """Get a dictionary representation of the statistics for database storage."""
        return {
            "feed_id": self.feed_id,
            "feed_name": self.feed_name,
            "run_status": self.run_status,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "total_in_feed": self.total_in_feed,
            "processed_successfully": self.processed_successfully,
            "skipped_closed": self.skipped_closed,
            "skipped_duplicate": self.skipped_duplicate,
            "records_failed": self.records_failed,
            "jobs_closed": self.jobs_closed,
            "jobs_auto_approved": self.jobs_auto_approved,
            "jobs_manual_review": self.jobs_manual_review,
            "jobs_rejected": self.jobs_rejected,
            "api_calls_gemini": self.api_calls_gemini,
            "api_calls_custom": self.api_calls_custom,
            "cache_hits": self.cache_hits,
            "error_summary": self.error_summary,
            "gemini_input_tokens": self.gemini_input_tokens,
            "gemini_output_tokens": self.gemini_output_tokens,
            "gemini_total_tokens": self.gemini_total_tokens,
            "feed_file_size_bytes": self.feed_file_size_bytes,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "skipped_duplicate_initial_aggregation": self.skipped_duplicate_initial_aggregation,
            "skipped_duplicate_ai_aggregation": self.skipped_duplicate_ai_aggregation,
            "xano_jobs_added": self.xano_jobs_added,
            "xano_jobs_deleted": self.xano_jobs_deleted,
        }
