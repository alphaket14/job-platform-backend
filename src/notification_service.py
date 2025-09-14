"""
Notification service for execution reports.
"""
import os
import smtplib
import logging
import requests
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, Optional
from datetime import datetime
from .config import Config

logger = logging.getLogger(__name__)

def _datetime_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


class NotificationService:
    """Service for sending notifications about execution reports."""

    def __init__(self):
        """Initialize the notification service."""
        self.enabled = Config.NOTIFICATIONS_ENABLED
        self.notification_type = Config.NOTIFICATION_TYPE.lower()
        self.slack_webhook_url = Config.SLACK_WEBHOOK_URL
        self.custom_webhook_url = Config.CUSTOM_WEBHOOK_URL
        self.email_recipients = Config.EMAIL_RECIPIENTS
        self.smtp_server = Config.SMTP_SERVER
        self.smtp_port = Config.SMTP_PORT
        self.smtp_username = Config.SMTP_USERNAME
        self.smtp_password = Config.SMTP_PASSWORD
        self.smtp_from_email = Config.SMTP_FROM_EMAIL

    def _format_slack_message(self, report_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format execution report data for Slack message."""
        status = report_data.get('run_status', 'UNKNOWN')
        feed_name = report_data.get('feed_name', 'Unknown Feed')
        duration = report_data.get('duration_seconds', 0)
        feed_id = report_data.get('feed_id', 'N/A')

        color_map = {'SUCCESS': 'good', 'FAILURE': 'danger', 'RUNNING': 'warning'}
        color = color_map.get(status, '#439FE0')

        status_emoji = {'SUCCESS': '✅', 'FAILURE': '❌', 'RUNNING': '🔄'}.get(status, '❓')

        title = f"{status_emoji} Feed Execution Report: {feed_name} (ID: {feed_id})"
        if status == 'FAILURE':
            title = f"🚨 [FAILURE] Feed Processing FAILED for {feed_name} (ID: {feed_id})"

        ts = int(datetime.now().timestamp())

        # Helper for formatting numbers
        def nf(num):
            return f"{num:,}" if isinstance(num, (int, float)) else str(num)

        fields = [
            {"title": "Status", "value": status, "short": True},
            {"title": "Duration", "value": f"{duration:.2f}s", "short": True},
            {"title": "Total in Feed", "value": nf(report_data.get('total_in_feed', 0)), "short": True},
            {"title": "Processed Successfully", "value": nf(report_data.get('processed_successfully', 0)), "short": True},
            {"title": "Auto-Approved", "value": nf(report_data.get('jobs_auto_approved', 0)), "short": True},
            {"title": "Manual Review", "value": nf(report_data.get('jobs_manual_review', 0)), "short": True},
            {"title": "Rejected", "value": nf(report_data.get('jobs_rejected', 0)), "short": True},
            {"title": "Failed", "value": nf(report_data.get('records_failed', 0)), "short": True},
            {"title": "Skipped (Duplicate)", "value": nf(report_data.get('skipped_duplicate', 0)), "short": True},
            {"title": "Skipped (Closed in Feed)", "value": nf(report_data.get('skipped_closed', 0)), "short": True},
            {"title": "Jobs Archived", "value": nf(report_data.get('jobs_closed', 0)), "short": True},
            {"title": "Feed Size (MB)", "value": f"{report_data.get('feed_file_size_bytes', 0) / (1024*1024):.2f}", "short": True},
            {"title": "Gemini API Calls", "value": nf(report_data.get('api_calls_gemini', 0)), "short": True},
            {"title": "Total Tokens", "value": nf(report_data.get('gemini_total_tokens', 0)), "short": True},
            {"title": "Cache Hits", "value": nf(report_data.get('cache_hits', 0)), "short": True},
            {"title": "Custom API Calls", "value": nf(report_data.get('api_calls_custom', 0)), "short": True},
        ]

        error_summary = report_data.get('error_summary')
        if error_summary and error_summary != "[]":
            error_str = '; '.join(map(str, error_summary))
            fields.append({"title": "Error Summary", "value": f"```{error_str[:1000]}```", "short": False})

        return {
            "attachments": [{"color": color, "title": title, "fields": fields, "footer": "Job Processing Pipeline", "ts": ts}]
        }

    def _format_email_message(self, report_data: Dict[str, Any]) -> str:
        """Format execution report data for email message."""
        status = report_data.get('run_status', 'UNKNOWN')
        feed_name = report_data.get('feed_name', 'Unknown Feed')

        # Helper for formatting numbers
        def nf(num):
            return f"{num:,}" if isinstance(num, (int, float)) else str(num)

        body = f"""
        <h2>Job Feed Execution Report</h2>
        <p><strong>Feed:</strong> {feed_name} (ID: {report_data.get('feed_id')})</p>
        <p><strong>Status:</strong> {status}</p>
        <p><strong>Time:</strong> {report_data.get('start_time')} to {report_data.get('end_time')}</p>
        <p><strong>Duration:</strong> {report_data.get('duration_seconds', 0):.2f} seconds</p>
        <hr>
        <h3>Processing Summary</h3>
        <ul>
            <li><strong>Total Jobs in Feed:</strong> {nf(report_data.get('total_in_feed', 0))}</li>
            <li><strong>Processed Successfully:</strong> {nf(report_data.get('processed_successfully', 0))}</li>
            <li><strong>Jobs Auto-Approved:</strong> {nf(report_data.get('jobs_auto_approved', 0))}</li>
            <li><strong>Jobs for Manual Review:</strong> {nf(report_data.get('jobs_manual_review', 0))}</li>
            <li><strong>Jobs Rejected (Low Confidence):</strong> {nf(report_data.get('jobs_rejected', 0))}</li>
            <li><strong>Jobs Failed (AI/Processing Errors):</strong> {nf(report_data.get('records_failed', 0))}</li>
        </ul>
        <h3>Feed Management</h3>
        <ul>
            <li><strong>Skipped (Already Active Duplicate):</strong> {nf(report_data.get('skipped_duplicate', 0))}</li>
            <li><strong>Skipped (Closed in Source Feed):</strong> {nf(report_data.get('skipped_closed', 0))}</li>
            <li><strong>Jobs Archived (No Longer in Feed):</strong> {nf(report_data.get('jobs_closed', 0))}</li>
        </ul>
        <hr>
        <h3>Performance Metrics</h3>
        <ul>
            <li><strong>Feed Size:</strong> {report_data.get('feed_file_size_bytes', 0) / (1024*1024):.2f} MB</li>
            <li><strong>Gemini API Calls:</strong> {nf(report_data.get('api_calls_gemini', 0))}</li>
            <li><strong>Gemini Total Tokens:</strong> {nf(report_data.get('gemini_total_tokens', 0))} (Input: {nf(report_data.get('gemini_input_tokens', 0))}, Output: {nf(report_data.get('gemini_output_tokens', 0))})</li>
            <li><strong>Custom API Calls:</strong> {nf(report_data.get('api_calls_custom', 0))}</li>
            <li><strong>Cache Hits:</strong> {nf(report_data.get('cache_hits', 0))}</li>
        </ul>
        """

        error_summary = report_data.get('error_summary',"[]")
        if error_summary and error_summary != "[]":
            errors = "<br>".join(map(str, error_summary))
            body += f"""
            <hr>
            <h3>Error Summary</h3>
            <p><font color="red">{errors}</font></p>
            """

        return f"<html><body>{body}</body></html>"

    def send_slack_notification(self, report_data: Dict[str, Any]) -> bool:
        """Send notification to Slack."""
        try:
            if not self.slack_webhook_url:
                logger.warning("Slack webhook URL not configured")
                return False

            message = self._format_slack_message(report_data)
            response = requests.post(self.slack_webhook_url, json=message, timeout=30)

            if response.status_code == 200:
                logger.info("Slack notification sent successfully")
                return True
            else:
                logger.error(f"Failed to send Slack notification. Status: {response.status_code}, Response: {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error sending Slack notification: {e}")
            return False

    def send_email_notification(self, report_data: Dict[str, Any]) -> bool:
        """Send notification via email."""
        try:
            if not all([self.smtp_server, self.smtp_username, self.smtp_password, self.email_recipients]):
                logger.warning("Email configuration incomplete")
                return False

            msg = MIMEMultipart('alternative')
            status = report_data.get('run_status', 'UNKNOWN')
            feed_name = report_data.get('feed_name', 'Unknown Feed')

            subject_prefix = "[ALERT]" if status == 'FAILURE' else "[INFO]"
            msg['Subject'] = f"{subject_prefix} Job Feed Execution Report - {feed_name}"
            msg['From'] = self.smtp_from_email
            msg['To'] = self.email_recipients

            html_body = self._format_email_message(report_data)
            msg.attach(MIMEText(html_body, 'html'))

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_username, self.smtp_password)
                server.send_message(msg)

            logger.info("Email notification sent successfully")
            return True

        except Exception as e:
            logger.error(f"Error sending email notification: {e}")
            return False

    def send_webhook_notification(self, report_data: Dict[str, Any]) -> bool:
        """Send notification to custom webhook URL."""
        try:
            if not self.custom_webhook_url:
                logger.warning("Custom webhook URL not configured")
                return False

            json_payload = json.dumps(report_data, default=_datetime_serializer)

            response = requests.post(
                self.custom_webhook_url,
                data=json_payload,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )

            if response.status_code in [200, 201, 202]:
                logger.info("Webhook notification sent successfully")
                return True
            else:
                logger.error(f"Failed to send webhook notification. Status: {response.status_code}, Response: {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error sending webhook notification: {e}")
            return False

    def send_notification(self, report_data: Dict[str, Any]) -> bool:
        """Send notification based on configured type."""
        if not self.enabled:
            logger.info("Notifications are disabled")
            return True

        if self.notification_type in ['slack', 'all', 'slack_web']:
            self.send_slack_notification(report_data)
        if self.notification_type in ['email', 'all']:
            self.send_email_notification(report_data)
        if self.notification_type in ['webhook', 'all', 'slack_web']:
            self.send_webhook_notification(report_data)

        return True
