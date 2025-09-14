"""
Manual review queue management.
"""
import json
import os
import logging
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class ReviewQueue:
    """Service for managing manual review queue."""
    
    def __init__(self, queue_file: str = "manual_review_queue.json"):
        """Initialize the review queue."""
        self.queue_file = queue_file
    
    def load_review_queue(self) -> List[Dict[str, Any]]:
        """Load the existing review queue from the JSON file."""
        if os.path.exists(self.queue_file):
            try:
                with open(self.queue_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning(f"Could not parse {self.queue_file}, starting with empty queue")
                return []
        return []
    
    def save_review_queue(self, queue_data: List[Dict[str, Any]]) -> None:
        """Save the updated review queue to the JSON file."""
        try:
            with open(self.queue_file, 'w') as f:
                json.dump(queue_data, f, indent=2)
            logger.info(f"Saved review queue to {self.queue_file}")
        except Exception as e:
            logger.error(f"Error saving review queue: {e}")
    
    def send_for_manual_review(self, job_data: Dict[str, Any]) -> bool:
        """
        Add a job to the manual review queue.
        
        Args:
            job_data: The job data to be reviewed.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        title = job_data.get('ai_title', job_data.get('title', 'N/A'))
        logger.info(f"Sending job for manual review: '{title}'")
        
        try:
            # Load the current queue
            review_queue = self.load_review_queue()
            
            # Add metadata for the review process
            review_item = {
                "review_status": "pending",
                "added_to_queue_at": datetime.utcnow().isoformat() + 'Z',
                "job_data": job_data
            }
            
            # Add the new item to the queue
            review_queue.append(review_item)
            
            # Save the updated queue
            self.save_review_queue(review_queue)
            
            logger.info(f"Job successfully added to '{self.queue_file}'. Total jobs in queue: {len(review_queue)}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding job to review queue: {e}")
            return False
    
    def get_queue_status(self) -> Dict[str, Any]:
        """Get the current status of the review queue."""
        try:
            queue = self.load_review_queue()
            return {
                "total_items": len(queue),
                "pending_items": len([item for item in queue if item.get("review_status") == "pending"]),
                "queue_file": self.queue_file
            }
        except Exception as e:
            logger.error(f"Error getting queue status: {e}")
            return {"total_items": 0, "pending_items": 0, "error": str(e)}