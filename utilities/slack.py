"""
Slack utilities for Gaming BI System.

This module provides Slack notification functionality using Incoming Webhooks
for sending alerts and monitoring notifications.
"""

import os
import requests
from typing import Optional
from pathlib import Path
import re

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    # Load .env from project root (2 levels up from utilities/)
    env_path = Path(__file__).resolve().parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    # python-dotenv not installed, skip .env loading
    pass


def _format_for_slack(content: str) -> str:
    """
    Convert markdown table to Slack-friendly format.
    
    Args:
        content (str): Original markdown content
        
    Returns:
        str: Slack-optimized content
    """
    lines = content.split('\n')
    result_lines = []
    
    for line in lines:
        if '|' in line and not line.strip().startswith('|') and not re.match(r'^[\-\+\|]+$', line.strip()):
            # This is a data row, format it nicely
            parts = [part.strip() for part in line.split('|') if part.strip()]
            if len(parts) >= 6:  # Ensure we have enough columns
                # Format as key-value pairs
                formatted_line = f"• **{parts[0]}**: {parts[1]} | **Hours**: {parts[4]} | **Threshold**: {parts[6] if len(parts) > 6 else 'N/A'}"
                result_lines.append(formatted_line)
            else:
                result_lines.append(line)
        elif re.match(r'^[\-\+\|]+$', line.strip()):
            # Skip separator lines
            continue
        else:
            result_lines.append(line)
    
    return '\n'.join(result_lines)


def send_slack_webhook(
    webhook_url: str,
    text: str,
    title: Optional[str] = None,
    color: str = "danger",
    fields: Optional[list] = None
) -> bool:
    """
    Send a message to Slack using Incoming Webhook.
    
    Args:
        webhook_url (str): Slack Incoming Webhook URL
        text (str): Main message text
        title (Optional[str]): Message title (optional)
        color (str): Color for the message border ("good", "warning", "danger")
        fields (Optional[list]): List of field dictionaries with "title" and "value" keys
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not webhook_url:
        print("[WARNING] No Slack webhook URL provided")
        return False
    
    # Build attachment structure
    attachment = {
        "color": color,
        "text": text
    }
    
    if title:
        attachment["title"] = title
    
    if fields:
        attachment["fields"] = fields
    
    # Slack hard limit is ~40k characters; keep a safe margin
    max_len = 38000
    if len(attachment["text"]) > max_len:
        attachment["text"] = attachment["text"][:max_len] + "\n… [truncated]"

    payload = {"attachments": [attachment]}

    # Simple retry with backoff for 429/5xx
    backoff_secs = [1, 2, 4]
    for attempt, delay in enumerate([0] + backoff_secs):
        try:
            if delay:
                import time
                time.sleep(delay)
            response = requests.post(webhook_url, json=payload, timeout=10)
            if response.status_code in (429, 500, 502, 503, 504):
                if attempt < len(backoff_secs):
                    continue
            response.raise_for_status()
            return True
        except Exception as e:
            if attempt == len(backoff_secs):
                print(f"[WARNING] Failed to send Slack notification: {e}")
                return False
            continue


def send_alert_notification(
    alert_type: str,
    count: int,
    details: str,
    alert_file_path: Optional[Path] = None
) -> bool:
    """
    Send a standardized alert notification to Slack with full file content.
    
    Args:
        alert_type (str): Type of alert (e.g., "ETL Process", "KPI", "Table Freshness")
        count (int): Number of alerts found
        details (str): Additional details about the alerts
        alert_file_path (Optional[Path]): Path to the detailed alert file
        
    Returns:
        bool: True if successful, False otherwise
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    
    if not webhook_url:
        print("[WARNING] SLACK_WEBHOOK_URL environment variable not set")
        return False
    
    # Read the full alert file content if available
    if alert_file_path and alert_file_path.exists():
        try:
            with open(alert_file_path, 'r', encoding='utf-8') as f:
                file_content = f.read()
        except Exception as e:
            print(f"[WARNING] Could not read alert file: {e}")
            file_content = f"Could not read file: {alert_file_path.name}"
    else:
        file_content = f"No detailed report available"
    
    # Convert to compact format for better Slack readability
    if os.getenv("SLACK_SUMMARY_ONLY", "false").lower() == "true":
        # Only send a short summary to Slack
        title = f"🚨 {alert_type} Monitoring Alert"
        text = (
            f"Hi BI Developer - you have {count} NEW ALERT{'S' if count > 1 else ''}!\n"
            f"Details were written to: {alert_file_path if alert_file_path else 'N/A'}"
        )
        return send_slack_webhook(
            webhook_url=webhook_url,
            text=text,
            title=title,
            color="danger"
        )

    if "Alert Details" in file_content and "|" in file_content:
        file_content = _format_for_slack(file_content)
    
    # Build message with full content
    title = f"🚨 {alert_type} Monitoring Alert"
    text = f"Hi BI Developer - you have {count} NEW ALERT{'S' if count > 1 else ''}!\n\n{file_content}"
    
    return send_slack_webhook(
        webhook_url=webhook_url,
        text=text,
        title=title,
        color="danger"
    )


def send_success_notification(
    monitoring_type: str,
    message: str = "All checks completed successfully"
) -> bool:
    """
    Send a success notification to Slack (optional, for when no alerts are found).
    
    Args:
        monitoring_type (str): Type of monitoring (e.g., "ETL Process", "KPI", "Table Freshness")
        message (str): Success message
        
    Returns:
        bool: True if successful, False otherwise
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    
    if not webhook_url:
        return False
    
    # Only send success notifications if explicitly enabled
    if not os.getenv("SLACK_SEND_SUCCESS", "false").lower() == "true":
        return True
    
    title = f"✅ {monitoring_type} Monitoring - All Good"
    text = message
    
    return send_slack_webhook(
        webhook_url=webhook_url,
        text=text,
        title=title,
        color="good"
    )
