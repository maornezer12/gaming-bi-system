"""
Path utilities for Gaming BI System.

This module provides comprehensive path management and directory structure functions
used across all ETL and monitoring scripts. Handles dynamic path generation based
on job names, task names, and project structure.
"""

import os
import re
from pathlib import Path
from typing import Tuple, Optional
from utilities.io import ensure_dir


def ensure_dirs(*paths: Path) -> None:
    """
    Ensure all provided directories exist.
    """
    for p in paths:
        ensure_dir(p)

def get_project_root(file_path: str) -> Path:
    """
    Get the project root directory from any file path.
    Automatically detects the correct level based on file location.
    
    Args:
        file_path (str): Path to any file in the project
        
    Returns:
        Path: Project root directory
    """
    current_path = Path(file_path).resolve()
    
    # For files in pipelines/ directory
    if 'pipelines' in current_path.parts:
        return current_path.parents[1]  # Go up 2 levels from pipelines/
    
    # For files in monitoring/ directory  
    if 'monitoring' in current_path.parts:
        return current_path.parents[2]  # Go up 3 levels from monitoring/
    
    # For files in utilities/ directory
    if 'utilities' in current_path.parts:
        return current_path.parents[1]  # Go up 2 levels from utilities/
    
    # Default fallback
    return current_path.parents[1]


def get_job_temp_paths(job_name: str, temp_root: Path) -> Tuple[Path, Path, Path]:
    """
    Get temp paths for a specific job, creating the mirror structure.
    
    Args:
        job_name (str): Name of the job (e.g., 'fact', 'daily_user_panel')
        temp_root (Path): Root temp directory
        
    Returns:
        Tuple[Path, Path, Path]: (logs_path, error_path, alerts_path)
    """
    
    job_temp = temp_root / "pipelines" / job_name
    logs_path = job_temp / "logs"
    error_path = job_temp / "errors"
    alerts_path = job_temp / "alerts"
    
    # Create directories only when needed
    ensure_dirs(logs_path, error_path, alerts_path)
    
    return logs_path, error_path, alerts_path


def get_task_paths(job_name: str, task_name: str, project_root: Path) -> Tuple[Path, Path, Path]:
    """
    Get paths for a specific task within a job.
    
    Args:
        job_name (str): Name of the job (e.g., 'fact', 'daily_user_panel')
        task_name (str): Name of the task (e.g., 'load_fact', 'clear_table')
        project_root (Path): Project root directory
        
    Returns:
        Tuple[Path, Path, Path]: (sql_path, config_path, temp_path)
    """
    # SQL template path
    sql_path = project_root / f"pipelines/{job_name}/{task_name}.sql"
    
    # Handle special case for clear_table
    if task_name == "clear_table" and not sql_path.exists():
        sql_path = project_root / "pipelines" / "clear_table.sql"
    
    # Config path
    config_path = project_root / f"pipelines/{job_name}/{job_name}_config.json"
    
    # Temp path for this specific task
    temp_root = project_root / "temp"
    temp_path = temp_root / f"pipelines/{job_name}/{task_name}"
    
    return sql_path, config_path, temp_path


def get_monitoring_paths(project_root: Path) -> Tuple[Path, Path, Path, Path, Path]:
    """
    Get paths for monitoring modules.
    
    Args:
        project_root (Path): Project root directory
        
    Returns:
        Tuple[Path, Path, Path, Path, Path]: config_path, sql_template_path, logs_path, error_path, alerts_path
    """
    config_path = project_root / "monitoring" / "logs_monitoring" / "logs_config.json"
    sql_template_path = project_root / "monitoring" / "logs_monitoring" / "logs_query.sql"
    
    # Create temp directory structure
    temp_root = project_root / "temp" / "monitoring" / "logs_monitoring"
    logs_path = temp_root / "logs"
    error_path = temp_root / "errors"
    alerts_path = temp_root / "alerts"
    
    return config_path, sql_template_path, logs_path, error_path, alerts_path


def get_kpi_monitoring_paths(project_root: Path) -> Tuple[Path, Path, Path, Path, Path]:
    """
    Get paths for KPI monitoring module.
    
    Args:
        project_root (Path): Project root directory
        
    Returns:
        Tuple[Path, Path, Path, Path, Path]: config_path, queries_path, logs_path, error_path, alerts_path
    """
    config_path = project_root / "monitoring" / "kpis_monitoring" / "kpis_config.json"
    queries_path = project_root / "monitoring" / "kpis_monitoring" / "queries"
    
    # Create temp directory structure
    temp_root = project_root / "temp" / "monitoring" / "kpis_monitoring"
    logs_path = temp_root / "logs"
    error_path = temp_root / "errors"
    alerts_path = temp_root / "alerts"
    
    return config_path, queries_path, logs_path, error_path, alerts_path


def get_table_monitoring_paths(project_root: Path) -> Tuple[Path, Path, Path, Path, Path]:
    # Config path
    config_path = project_root / "monitoring" / "table_monitoring" / "tables_config.json"
    
    # SQL template path
    sql_template_path = project_root / "monitoring" / "table_monitoring" / "table_freshness_alert.sql"
    
    # Temp paths
    temp_root = project_root / "temp"
    monitoring_temp = temp_root / "monitoring" / "table_monitoring"
    logs_path = monitoring_temp / "logs"
    error_path = monitoring_temp / "errors"
    alerts_path = monitoring_temp / "alerts"
    
    # Create directories
    ensure_dirs(logs_path, error_path, alerts_path)
    
    return config_path, sql_template_path, logs_path, error_path, alerts_path


def get_standard_paths(file_path: str) -> dict:
    """
    Get all standard paths for a given file location.
    
    Args:
        file_path (str): Path to the calling file
        
    Returns:
        dict: Dictionary containing all standard paths
    """
    project_root = get_project_root(file_path)
    temp_root = project_root / "temp"
    
    return {
        'project_root': project_root,
        'temp_root': temp_root,
        'pipelines_root': project_root / "pipelines",
        'monitoring_root': project_root / "monitoring",
        'utilities_root': project_root / "utilities"
    }