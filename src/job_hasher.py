"""
Job hashing utilities for deduplication.
"""
import json
import base64
import hashlib
import logging
import re
from typing import Dict, Any

logger = logging.getLogger(__name__)


def clean_description_for_hashing(description: str) -> str:
    """
    Cleans a job description to create a more stable basis for hashing by
    removing common sources of minor, inconsequential changes.
    """
    if not description:
        return ""

    # Remove HTML tags
    cleaned = re.sub(r'<[^>]+>', ' ', description)

    # Remove URLs
    cleaned = re.sub(r'https?://\S+', ' ', cleaned)

    # Remove non-alphanumeric characters (keeps letters and numbers)
    cleaned = re.sub(r'[^a-zA-Z0-9\s]', ' ', cleaned)

    # Normalize whitespace (replace multiple spaces/newlines with a single space)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    return cleaned.lower()

def get_canonical_job_hash(job: Dict[str, Any]) -> str:
    title = (job.get("title") or "").strip()
    company = (job.get("company_name") or "").strip()
    raw_employment_type = (job.get("employment_type") or "").upper()
    if "FULL" in raw_employment_type:
        employment_type = "FULL_TIME"
    else:
        employment_type = "PART_TIME"
    raw_description = job.get("description") or ""
    cleaned_description = clean_description_for_hashing(raw_description)

    canonical_string = f"{title}|{company}|{employment_type}|{cleaned_description}"

    hasher = hashlib.sha256(canonical_string.encode('utf-8'))
    hash_text= base64.b64encode(hasher.digest()).decode('utf-8')
    # if hash_text== "/EYIOL/SKyVIpfeCUZluG+Y8yheSUngWXexclM1kPI0=":
    #     print(f"String for targeted hash : {canonical_string}")

    # print(f"canonical_string for hashing: {canonical_string[:500]}")
    # external_job_id = job.get("external_job_id","")
    # if external_job_id == "599757218":
    #
    #     print(f"DEBUG: External Job ID:{external_job_id} Hash value: {hash_text}")
    #     print(f"String: {canonical_string}")
    return hash_text