"""
BigQuery utilities for Gaming BI System.
This module provides centralized BigQuery client management and logging functionality. 
"""

from google.cloud import bigquery
from google.auth import default as google_auth_default
import os
import warnings
from typing import Optional
import time


def get_bq_client(project_id: str, dry_run: bool = False) -> Optional[bigquery.Client]:
    """
    Return a BigQuery client for the given project ID.
    
    Args:
        project_id (str): Google Cloud project ID
        dry_run (bool): If True, return None for dry-run mode
        
    Returns:
        Optional[bigquery.Client]: BigQuery client or None if dry_run
    """
    if dry_run:
        return None
    
    try:
        # Reduce noisy SDK warnings unless verbose
        if not os.getenv("BI_VERBOSE"):
            warnings.filterwarnings(
                "ignore",
                message="Your application has authenticated using end user credentials from Google Cloud SDK",
                category=UserWarning,
                module="google.auth._default",
            )
            warnings.filterwarnings(
                "ignore",
                message="BigQuery Storage module not found",
                category=UserWarning,
                module="google.cloud.bigquery.table",
            )

        # Use ADC with explicit quota project to avoid SDK warning and charge the right project
        credentials, _ = google_auth_default(quota_project_id=project_id)
        return bigquery.Client(project=project_id, credentials=credentials)
    except Exception as e:
        if os.getenv("BI_VERBOSE"):
            print(f"[WARNING] Could not create BigQuery client: {e}")
        return None

# logging helpers moved to utilities.daily_logs


def execute_query(
    client: Optional[bigquery.Client], 
    query: str, 
    dry_run: bool = False
) -> Optional[bigquery.QueryJob]:
    """
    Execute a BigQuery query safely.
    
    Args:
        client (Optional[bigquery.Client]): BigQuery client
        query (str): SQL query to execute
        dry_run (bool): If True, don't execute query
        
    Returns:
        Optional[bigquery.QueryJob]: Query job result or None
    """
    if dry_run:
        print(f"[DRY-RUN] Would execute query: {query[:100]}...")
        return None
    
    if client is None:
        print("[WARNING] No BigQuery client available")
        return None
    
    try:
        return client.query(query)
    except Exception as e:
        print(f"[ERROR] Query execution failed: {e}")
        return None


def run_query_and_df(
    client: Optional[bigquery.Client],
    query: str,
    dry_run: bool = False,
):
    """
    Execute a query and return a pandas DataFrame with simple retries.
    Retries on 429/5xx with backoff (1s, 2s, 4s).

    Args:
        client: BigQuery client or None if dry-run
        query: SQL query to execute
        dry_run: Do not execute when True

    Returns:
        DataFrame on success, or None when dry-run or failure
    """
    if dry_run:
        print(f"[DRY-RUN] Would execute query: {query[:100]}...")
        return None
    if client is None:
        print("[WARNING] No BigQuery client available")
        return None

    backoff_secs = [0, 1, 2, 4]
    for delay in backoff_secs:
        try:
            if delay:
                time.sleep(delay)
            job = client.query(query)
            return job.to_dataframe()
        except Exception as e:
            msg = str(e)
            # Basic retry for rate limits and transient errors
            if any(code in msg for code in ["429", "500", "502", "503", "504"]) and delay != backoff_secs[-1]:
                continue
            print(f"[ERROR] Query to DataFrame failed: {e}")
            return None
