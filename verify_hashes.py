#!/usr/bin/env python3
"""
Standalone script to verify that the canonical hashing function is stable.
It fetches job data from Xano by its hash and re-calculates the hash for each returned record.
"""
import argparse
import asyncio
import logging
from src.config import Config
from src.xano_service import XanoService
from src.job_hasher import get_canonical_job_hash

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def verify_hash(job_hash: str, xano_service: XanoService) -> int:
    """Fetches all jobs for a hash, re-hashes them, and returns the number of mismatches."""
    jobs = await xano_service.get_jobs_by_hash(job_hash)
    if not jobs:
        print(f"\nCould not retrieve any jobs for hash: {job_hash}")
        return 1  # Count this as a failure/mismatch

    mismatches = 0
    print("-" * 80)
    logger.info(f"Verifying {len(jobs)} job(s) found for hash: {job_hash}")

    for job in jobs:
        original_hash = job.get('job_hash')

        job_for_hashing = job.copy()
        if 'company' in job_for_hashing:
            job_for_hashing['company_name'] = job_for_hashing.pop('company')

        title = job_for_hashing.get('title', '')
        company = job_for_hashing.get('company_name', '')
        employment_type = job_for_hashing.get('employment_type', '')
        description = job_for_hashing.get('description', '')

        print(f"\n--> Verifying Job (eid: {job.get('eid')}, title: {title})")
        print(f"  - Title          : {title}")
        print(f"  - Company        : {company}")
        print(f"  - Employment Type: {employment_type}")
        print(f"  - Description    : {description[:200]}...")

        new_hash = get_canonical_job_hash(job_for_hashing)

        print(f"  Original Hash: {original_hash}")
        print(f"  New Hash     : {new_hash}")

        if original_hash == new_hash:
            print("  Result       : ✅ MATCH")
        else:
            print("  Result       : ❌ MISMATCH")
            mismatches += 1

    return mismatches


async def main():
    """Main asynchronous entry point for the hash verification script."""
    parser = argparse.ArgumentParser(description='Verify job hashes against Xano data.')
    parser.add_argument(
        'hashes',
        metavar='HASH',
        type=str,
        nargs='+',
        help='One or more job_hash values to verify.'
    )
    args = parser.parse_args()

    xano_service = None
    exit_code = 0
    try:
        Config.validate()
        xano_service = XanoService()

        logger.info(f"--- Starting Hash Verification for {len(args.hashes)} hash(es) ---")

        tasks = [verify_hash(job_hash, xano_service) for job_hash in args.hashes]
        results = await asyncio.gather(*tasks)

        total_mismatches = sum(results)

        print("-" * 80)
        logger.info(f"Verification complete. Found a total of {total_mismatches} mismatch(es).")
        if total_mismatches > 0:
            exit_code = 1

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        exit_code = 1
    finally:
        if xano_service:
            await xano_service.close_async_client()

    return exit_code


if __name__ == "__main__":
    import sys

    exit_code = asyncio.run(main())
    sys.exit(exit_code)