# System Documentation - Gaming BI System

Technical documentation for the Gaming BI System architecture, components, and development.

## System Architecture

### Core Components

- **ETL Runner** (`pipelines/etl_runner.py`): The main orchestrator that executes data processing jobs
- **Pipeline Configurations**: JSON files that define how each data processing job should run
- **SQL Templates**: Reusable SQL queries for data transformation
- **Monitoring System** (`monitoring/logs_monitoring/logs_monitoring.py`): Tracks job execution and alerts on failures
- **Utilities**: Centralized helper functions for IO operations, BigQuery client, and constants
- **Scheduler**: Automated execution of daily data processing jobs

### Data Flow

```
Raw Game Data (BigQuery)
    ↓
FACT Table (Raw Events)
    ↓
Daily User Panel (Daily Aggregations)
    ↓
User Panel (Lifetime User Profiles)
    ↓
Curated Marts (dim_user, fct_sessions, fct_purchases)
```

## Data Processing Pipelines

### 1. FACT Pipeline
- **Purpose**: Stores raw gaming events (sessions, matches, purchases)
- **Source**: `project_game.playpltx_fact`
- **Destination**: `fp_gaming_raw_data.fact`
- **Frequency**: Daily incremental loads

### 2. Daily User Panel Pipeline
- **Purpose**: Creates daily aggregated metrics per user
- **Source**: `fp_gaming_raw_data.fact`
- **Destination**: `fp_gaming_panels.daily_user_panel`
- **Metrics**: Sessions, matches, revenue, coins gained per day

### 3. User Panel Pipeline
- **Purpose**: Maintains a single lifetime record per user with cumulative KPIs
- **Source**: `fp_gaming_panels.daily_user_panel`
- **Destination**: `fp_gaming_panels.user_panel`
- **Features**:
  - Lifetime sessions, revenue, coins, last activity date
  - First/last install date logic and rolling aggregates
  - Idempotent daily upserts based on the latest `daily_user_panel`

### 4. Curated Pipelines
- **dim_user** (`pipelines/dim_user/`)
  - One row per user
  - Keys/attributes: user_id, install_date, install_country, install_device
  - Built from `fp_gaming_panels.user_panel` and raw metadata when needed
  - Target: `fp_gaming_curated.dim_user`

- **fct_sessions** (`pipelines/fct_sessions/`)
  - One row per session
  - Fields: session_id, user_id, session_start, session_length_sec, device, country
  - Source: `fp_gaming_raw_data.fact` filtered to session events
  - Target: `fp_gaming_curated.fct_sessions`

- **fct_purchases** (`pipelines/fct_purchases/`)
  - One row per purchase transaction
  - Fields: transaction_id, user_id, product_id, price, currency, event_time
  - Source: `fp_gaming_raw_data.fact` filtered to purchase events
  - Target: `fp_gaming_curated.fct_purchases`

## Project Structure

```
gaming-bi-system/
├── pipelines/                    # ETL pipeline configurations and runner
│   ├── etl_runner.py            # Main ETL orchestrator
│   ├── action_config.json       # Task execution order
│   ├── clear_table.sql          # Generic table clearing query
│   ├── fact/                    # FACT pipeline
│   │   ├── fact_config.json
│   │   ├── init_fact.sql
│   │   └── load_fact.sql
│   ├── daily_user_panel/        # Daily user panel pipeline
│   │   ├── daily_user_panel_config.json
│   │   ├── init_daily_user_panel.sql
│   │   └── load_daily_user_panel.sql
│   └── user_panel/              # User panel pipeline
│       ├── user_panel_config.json
│       ├── init_user_panel.sql
│       └── load_user_panel.sql
├── monitoring/                   # System monitoring
│   ├── logs_monitoring/
│   │   ├── logs_config.json
│   │   ├── logs_monitoring.py
│   │   └── logs_query.sql
│   ├── kpis_monitoring/          # KPI monitoring system
│   │   ├── kpis_config.json
│   │   ├── kpis_monitoring.py
│   │   └── queries/
│   │       ├── dau_alerts.sql
│   │       ├── installs_alerts.sql
│   │       └── last_activity_alerts.sql
│   └── table_monitoring/         # Table freshness monitoring
│       ├── tables_config.json
│       ├── table_monitoring.py
│       └── table_freshness_alert.sql
├── utilities/                    # Centralized helper functions
│   ├── __init__.py
│   ├── constants.py             # Global constants and paths
│   ├── io.py                    # File operations and data formatting
│   ├── bq.py                    # BigQuery client and logging
│   ├── cli.py                   # CLI functions
│   ├── dates.py                 # Date utilities
│   ├── formatting.py            # Data formatting and templates
│   └── paths.py                 # Smart path management with auto-detection
├── temp/                        # Temporary files and logs
│   ├── pipelines/               # ETL job temp files
│   │   ├── fact/
│   │   │   ├── logs/
│   │   │   ├── errors/
│   │   │   └── alerts/
│   │   ├── daily_user_panel/
│   │   └── user_panel/
│   └── monitoring/              # Monitoring temp files
│       └── logs_monitoring/
│           ├── logs/
│           ├── errors/
│           └── alerts/
├── scheduler/                    # Automated job scheduling
│   ├── crontab.sh
│   ├── execute_fact_etl.sh
│   ├── execute_full_bi_etl.sh
│   ├── execute_panel_etl.sh
│   └── execute_user_panel_etl.sh
└── requirements.txt              # Python dependencies
```

## Configuration

### Pipeline Configuration

Each pipeline is configured via JSON files in the `pipelines/` directory:

- **`*_config.json`**: Defines table sources, destinations, and parameters
- **`action_config.json`**: Defines the execution order for different job actions (init, daily, delete)

### Monitoring Configuration

The monitoring system tracks job execution in `logs.daily_logs` table and can alert when jobs haven't run within expected timeframes. KPIs are computed from curated sources without pre-aggregated tables (DAU, installs, last activity, ARPDAU, D1 retention).

### Table Monitoring System

The table monitoring system ensures BigQuery tables are fresh and updated within defined thresholds:

- **Purpose**: Monitor table freshness by checking last modification timestamps
- **Method**: Query BigQuery metadata using INFORMATION_SCHEMA.TABLES
- **Alerting**: Raise alerts when tables exceed freshness thresholds (in hours)
- **Configuration**: `monitoring/table_monitoring/tables_config.json`
- **SQL Template**: Single parameterized SQL template for all tables
- **Output**: Formatted freshness reports saved to temp directory and BigQuery logs

## Utilities

The system uses centralized utilities for consistency:

- **`utilities/constants.py`**: Global constants, paths, and configuration values
- **`utilities/io.py`**: File operations, JSON handling, and data formatting
- **`utilities/bq.py`**: BigQuery client management and logging functions
- **`utilities/cli.py`**: Standardized command-line interface functions
- **`utilities/dates.py`**: Date and time utility functions
- **`utilities/formatting.py`**: Data formatting and SQL template functions
- **`utilities/paths.py`**: Smart path management with auto-detection

### Using Path Utilities

```python
from utilities.paths import get_standard_paths, get_job_temp_paths, get_task_paths, get_monitoring_paths

# Get all standard paths (auto-detects project root)
paths = get_standard_paths(__file__)
project_root = paths['project_root']
temp_root = paths['temp_root']

# Get job-specific temp paths: {job_name}/logs|errors|alerts
logs_path, error_path, alerts_path = get_job_temp_paths(job_name, temp_root)

# Get task-specific paths: {job_name}/{task_name}
sql_path, config_path, task_temp_path = get_task_paths(job_name, task_name, project_root)

# Get monitoring paths
config_path, sql_path, logs_path, error_path, alerts_path = get_monitoring_paths(project_root)

# Get KPI monitoring paths
config_path, queries_path, logs_path, error_path, alerts_path = get_kpi_monitoring_paths(project_root)

# Get table monitoring paths
config_path, sql_template_path, logs_path, error_path, alerts_path = get_table_monitoring_paths(project_root)
```

**Key Features:**
- **Auto-detection**: Automatically finds project root from any file location
- **Generic patterns**: Uses `{job_name}/{task_name}` structure
- **Mirror structure**: Creates temp directories that mirror main structure
- **No hardcoded paths**: All paths are dynamically generated

### Using CLI Utilities

```python
from utilities.cli import create_standard_cli

# Create standardized CLI parser
parser = create_standard_cli("My ETL Script")
flags = parser.parse_args()

# Available arguments:
# - project_id (required)
# - --job_name (default: "log")
# - --job_action (choices: init, daily, delete)
# - --dry-run
# - --days-back (default: 0)
```

### Using Date Utilities

```python
from utilities.dates import get_date_params

# Get standardized date parameters
date_today, run_time, y_m_d = get_date_params(days_back=0)
```

### Using Formatting Utilities

```python
from utilities.formatting import format_query_template

# Format SQL template with standard parameters
query = format_query_template(
    query_template, 
    task_conf, 
    project_id, 
    job_name, 
    job_action, 
    y_m_d, 
    run_time
)
```

### Using BigQuery Utilities

```python
from utilities.bq import get_bq_client, insert_log

# Get BigQuery client
client = get_bq_client("your-project-id", dry_run=False)

# Log a step
insert_log("project-id", "job-name", "daily", "step-name", "message", client)
```

### Using IO Utilities

```python
from utilities.io import read_json, write_file, ensure_dir, header, df_to_string_table
from pathlib import Path

# Read configuration
config = read_json(Path("config.json"))

# Write file with auto directory creation
write_file(Path("temp/logs/output.txt"), "content")

# Ensure directory exists
ensure_dir(Path("temp/logs"))

# Print formatted header
header("Processing ETL Job")

# Format DataFrame for display
table_str = df_to_string_table(df)
```


## Monitoring and Logging

The system maintains detailed logs of all ETL operations in BigQuery with multi-step granularity:

- **Table**: `{project_id}.logs.daily_logs`
- **Tracks**: Job execution times, success/failure status, step-by-step progress
- **Steps**: `start`, `init_config`, `load_query`, `render_query`, `write_outputs`, `execute_query`, `query_completed`, `end`
- **Retention**: Configurable based on business needs
- **Temp Files**: All logs, errors, and alerts are written to organized `temp/` directory structure
- **Smart Paths**: Uses dynamic path generation with `{job_name}/{task_name}` patterns
- **Auto-detection**: Automatically finds project root from any file location

## Automation

### Cron Jobs
The system supports automated daily execution via cron:

```bash
# Daily at 10:00 AM - Full BI pipeline
0 10 * * * bash ~/workspace/ppltx-tutorial/jobs/gaming_bi_system/execute_full_bi_etl.sh
```

### Manual Execution
Individual pipelines can be executed manually using the provided shell scripts in the `scheduler/` directory.

## Development

### Adding New Pipelines

1. Create a new directory under `pipelines/`
2. Add configuration JSON file
3. Create SQL template files (init, load, clear)
4. Update `action_config.json` if needed
5. Test with dry-run mode

### Debugging

- Use `--dry-run` flag to test queries without execution
- Check logs in organized `temp/` directory structure:
  - `temp/pipelines/{job_name}/logs/` - SQL queries and outputs
  - `temp/pipelines/{job_name}/errors/` - Error messages
  - `temp/pipelines/{job_name}/alerts/` - Alert notifications
  - `temp/monitoring/logs_monitoring/logs/` - Monitoring queries
  - `temp/monitoring/logs_monitoring/errors/` - Monitoring errors
- Review BigQuery logs in `logs.daily_logs` table for detailed step-by-step execution
- **Smart Paths**: All paths are dynamically generated using `utilities/paths.py`
- **Auto-detection**: Project root is automatically detected from any file location
- **Generic Patterns**: Uses `{job_name}/{task_name}` structure for scalability

