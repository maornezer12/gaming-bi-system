"""
Formatting utilities for Gaming BI System.
This module provides data formatting, template functions, and date utilities
used across all ETL and monitoring scripts.
"""

from datetime import datetime, date, timedelta
from typing import Dict, Any
import pandas as pd


def format_query_template(query_template: str, task_conf: dict, project_id: str, job_name: str, job_action: str, y_m_d: str, run_time: datetime) -> str:
    """
    Format a SQL query template with standard parameters.
    
    Args:
        query_template (str): SQL template string
        task_conf (dict): Task-specific configuration
        project_id (str): Google Cloud project ID
        job_name (str): Job name
        job_action (str): Job action
        y_m_d (str): Date string in YYYY-MM-DD format
        run_time (datetime): Current run time
        
    Returns:
        str: Formatted SQL query
    """
    return query_template.format(
        **task_conf,
        date=y_m_d,
        run_time=run_time,
        project=project_id,
        job_name=job_name,
        job_action=job_action
    )


def df_to_string_table(df: pd.DataFrame) -> str:
    """
    Convert DataFrame to formatted string table for display.
    
    Args:
        df (pd.DataFrame): DataFrame to format
        
    Returns:
        str: Formatted table string
    """
    if df.empty:
        return "No data to display"
    
    # Find the maximum width of each column, but cap at reasonable limits
    column_widths = {}
    for column in df.columns:
        max_content_width = df[column].astype(str).apply(len).max()
        column_width = min(max(max_content_width, len(column)), 30)  # Cap at 30 chars
        column_widths[column] = column_width
    
    # Create the header row
    header_row = ' | '.join([column.center(column_widths[column]) for column in df.columns])
    table = header_row + "\n"
    
    # Create the separator row
    separator_row = '-+-'.join(['-' * column_widths[column] for column in df.columns])
    table += separator_row + "\n"
    
    # Format and add each data row
    for _, row in df.iterrows():
        row_str = ' | '.join([str(row[column])[:column_widths[column]].ljust(column_widths[column]) for column in df.columns])
        table += row_str + "\n"
    
    return table


def df_to_compact_table(df: pd.DataFrame) -> str:
    """
    Convert DataFrame to compact, Slack-friendly table format.
    
    Args:
        df (pd.DataFrame): DataFrame to format
        
    Returns:
        str: Compact table string optimized for Slack
    """
    if df.empty:
        return "No data to display"
    
    # For Slack, use a more compact format
    lines = []
    
    for _, row in df.iterrows():
        # Create a compact row format
        row_items = []
        for column in df.columns:
            value = str(row[column])
            # Truncate very long values
            if len(value) > 25:
                value = value[:22] + "..."
            row_items.append(f"**{column}**: {value}")
        
        lines.append(" • " + " | ".join(row_items))
    
    return "\n".join(lines)


def get_date_params(days_back: int = 0) -> tuple[date, datetime, str]:
    """
    Get standardized date parameters.
    
    Args:
        days_back (int): Number of days back to process
        
    Returns:
        tuple[date, datetime, str]: (date_today, run_time, y_m_d)
    """
    date_today = date.today()
    run_time = datetime.now()
    y_m_d = (date_today + timedelta(days=-days_back)).strftime("%Y-%m-%d")
    
    return date_today, run_time, y_m_d
