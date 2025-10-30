"""
Project-wide constants for the Gaming BI System.

Groups:
- Paths: repo roots and temp directories
- Formatting: display/date formats
- BigQuery: canonical table names
- Defaults: default thresholds and flags
"""
from pathlib import Path

# Root directory paths
ROOT_DIR = Path(__file__).resolve().parents[1]
TEMP_DIR = ROOT_DIR / "temp"

# Directory names
LOGS_DIR = "logs"
ERRORS_DIR = "errors"
ALERTS_DIR = "alerts"

# Temp directory structure
TEMP_MONITORING = TEMP_DIR / "monitoring"
TEMP_PIPELINES = TEMP_DIR / "pipelines"

# Titles and formatting
TITLE_MONITORING = "[Logs Monitoring]"
TITLE_PIPELINE = "[ETL Pipeline]"
DATE_FMT = "%Y-%m-%d"
DATETIME_FMT = "%Y-%m-%d %H:%M:%S"

# BigQuery table names
LOGS_TABLE = "logs.daily_logs"

# Default values
DEFAULT_THRESHOLD_HOURS = 24
DEFAULT_DAYS_BACK = 0
