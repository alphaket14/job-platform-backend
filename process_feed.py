#!/usr/bin/env python3
"""
Command-line interface for processing individual feeds.
"""
import argparse
import asyncio
import logging
import os
import sys

import vertexai

from src.config import Config
from src.pipeline import JobPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Main asynchronous entry point for the command-line interface."""
    parser = argparse.ArgumentParser(description='Process job feeds from the database')
    parser.add_argument(
        '--feed-id', '-f',
        type=int,
        required=True,
        help='ID of the feed to process'
    )
    args = parser.parse_args()

    pipeline = None
    exit_code = 0
    try:
        Config.validate()
        logger.info(f"Initializing Vertex AI for project: {Config.GOOGLE_PROJECT_ID}")
        try:
            # Initialize with the first location in the list as the default
            vertexai.init(project=Config.GOOGLE_PROJECT_ID, location=Config.GOOGLE_LOCATIONS[0])
            logger.info(f"Vertex AI initialized successfully for default location: {Config.GOOGLE_LOCATIONS[0]}")
        except Exception as e:
            logger.error(f"Failed to initialize Vertex AI: {e}", exc_info=True)
            exit_code = 1

        pipeline = await JobPipeline.create()

        feed_config = await pipeline.db_service.get_feed_by_id(args.feed_id)
        if not feed_config:
            logger.error(f"Feed with ID {args.feed_id} not found.")
            exit_code = 1

        source_url = feed_config.get('source_url')
        if not source_url:
            logger.error(f"No source URL for feed {args.feed_id}")
            exit_code = 1

        logger.info(f"Starting pipeline for feed ID: {args.feed_id}")

        # The process_feed_batched method is already async, so we just await it
        results = await pipeline.process_feed_batched(
            input_path=source_url,
            feed_id=args.feed_id,
            feed_name=feed_config.get('name')
        )

        if results.get("success"):
            logger.info(f"Feed {args.feed_id} processed successfully.")
            exit_code = 0
        else:
            logger.error(f"Feed {args.feed_id} processing failed. Errors: {results.get('errors')}")
            exit_code = 1

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        exit_code = 1
    finally:
        # Ensure the database pool is closed
        if pipeline and pipeline.db_service:
            await pipeline.db_service.close_pool()
    return exit_code


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
