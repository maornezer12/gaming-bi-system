"""
CLI utilities for Gaming BI System.
This module provides standardized command-line interface functions.
"""

import argparse


def create_standard_cli() -> argparse.ArgumentParser:
    """Create a standardized CLI parser for scripts.

    Flags:
        project_id: Google Cloud project ID (positional)
        --job_name: Logical job name (default: log)
        --job_action: One of init|daily|delete (default: daily)
        --dry-run: If set, do not execute queries
        --days-back: Integer days back for date params (default: 0)

    Returns:
        Configured argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("project_id", help="Google Cloud project ID")
    parser.add_argument("--job_name", default="log", help="Job name")
    parser.add_argument("--job_action", default="daily", choices=["init", "daily", "delete"], help="Job action")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true", help="Run in dry-run mode")
    parser.add_argument("--days-back", type=int, default=0, help="Number of days back to process")
    return parser
