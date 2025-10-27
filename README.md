# Gaming BI System
A comprehensive BI system designed to process and analyze gaming data using Google BigQuery. This system extracts, transforms, and loads (ETL) gaming event data into structured data warehouses for business analytics and reporting.

## 🎮 What This System Does 

Imagine you're running a mobile game company and you want to understand how players behave, how much money they spend, and how engaged they are with your game. This system does exactly that by:

1. **Collecting Game Data**: Every time a player does something in your game (starts a session, plays a match, makes a purchase), this data gets collected
2. **Organizing the Data**: The system takes all this raw data and organizes it into meaningful tables that business analysts can easily understand
3. **Creating User Profiles**: It builds comprehensive profiles for each player showing their lifetime activity, spending, and engagement
4. **Daily Updates**: Every day, it automatically updates these profiles with the latest player activity
5. **Monitoring**: It keeps track of whether everything is working properly and alerts you if something goes wrong



## 🏗️ System Architecture

### Core Components

- **ETL Runner** (`etl_runner.py`): The main orchestrator that executes data processing jobs
- **Pipeline Configurations**: JSON files that define how each data processing job should run
- **SQL Templates**: Reusable SQL queries for data transformation
- **Monitoring System**: Tracks job execution and alerts on failures
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
```

## 📊 Data Processing Pipelines

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
- **Purpose**: Maintains lifetime user profiles with cumulative metrics
- **Source**: `fp_gaming_panels.daily_user_panel`
- **Destination**: `fp_gaming_panels.user_panel`
- **Features**: User lifetime stats, install dates, total activity

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Google Cloud SDK installed and authenticated
- Access to BigQuery project: `ppltx-m--tutorial-dev` or `my-bi-project-ppltx`

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd gaming-bi-system
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up Google Cloud authentication:
```bash
gcloud auth application-default login
```

### Running ETL Jobs

#### Initialize Tables (One-time setup)
```bash
# Initialize FACT table
python etl_runner.py ppltx-m--tutorial-dev --job-name fact --job-action init

# Initialize Daily User Panel
python etl_runner.py ppltx-m--tutorial-dev --job-name daily_user_panel --job-action init

# Initialize User Panel
python etl_runner.py ppltx-m--tutorial-dev --job-name user_panel --job-action init
```

#### Daily Data Processing
```bash
# Process daily data for all pipelines
python etl_runner.py ppltx-m--tutorial-dev --job-name fact --job-action daily
python etl_runner.py ppltx-m--tutorial-dev --job-name daily_user_panel --job-action daily
python etl_runner.py ppltx-m--tutorial-dev --job-name user_panel --job-action daily
```

#### Dry Run (Test without executing)
```bash
python etl_runner.py ppltx-m--tutorial-dev --job-name fact --job-action daily --dry-run
```

## 📁 Project Structure

```
gaming-bi-system/
├── etl_runner.py                 # Main ETL orchestrator
├── requirements.txt              # Python dependencies
├── pipelines/                    # ETL pipeline configurations
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
│   └── log_monitoring/
│       ├── logs_config.json
│       ├── logs_monitoring.py
│       └── logs_query.sql
├── scheduler/                    # Automated job scheduling
│   ├── crontab.sh
│   ├── execute_fact_etl.sh
│   ├── execute_full_bi_etl.sh
│   ├── execute_panel_etl.sh
│   └── execute_user_panel_etl.sh
└── utilities/                    # Helper functions
    ├── __init__.py
    ├── my_etl_files.py
    └── df_to_string_table.py
```

## 🔧 Configuration

### Pipeline Configuration

Each pipeline is configured via JSON files in the `pipelines/` directory:

- **`*_config.json`**: Defines table sources, destinations, and parameters
- **`action_config.json`**: Defines the execution order for different job actions (init, daily, delete)

### Monitoring Configuration

The monitoring system tracks job execution in `logs.daily_logs` table and can alert when jobs haven't run within expected timeframes.

## ⚠️ Known Issues

### Monitoring System
- **Bug**: The log monitoring system (`monitoring/log_monitoring/logs_monitoring.py`) has several issues:
  - Incorrect variable references (`etl_configuration` vs `log_config`)
  - Path resolution problems
  - Incomplete error handling
  - **Status**: Not properly registered in the main ETL runner

### Scheduler Files
- **Outdated**: Several scheduler files in `scheduler/` directory contain outdated paths and configurations
- **Status**: Will be updated in future releases

## 📈 Monitoring and Logging

The system maintains detailed logs of all ETL operations in BigQuery:

- **Table**: `{project_id}.logs.daily_logs`
- **Tracks**: Job execution times, success/failure status, step-by-step progress
- **Retention**: Configurable based on business needs

## 🔄 Automation

### Cron Jobs
The system supports automated daily execution via cron:

```bash
# Daily at 10:00 AM - Full BI pipeline
0 10 * * * bash ~/workspace/ppltx-tutorial/jobs/gaming_bi_system/execute_full_bi_etl.sh
```

### Manual Execution
Individual pipelines can be executed manually using the provided shell scripts in the `scheduler/` directory.

## 🛠️ Development

### Adding New Pipelines

1. Create a new directory under `pipelines/`
2. Add configuration JSON file
3. Create SQL template files (init, load, clear)
4. Update `action_config.json` if needed
5. Test with dry-run mode

### Debugging

- Use `--dry-run` flag to test queries without execution
- Check logs in `temp/pipelines/logs/` directory
- Review error messages in `temp/pipelines/errors/` directory

