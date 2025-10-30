# Scheduler

Automated job scheduling scripts for the Gaming BI System.

## Overview

This directory contains shell scripts for automated execution of the complete Gaming BI pipeline including ETL processes and monitoring systems.

## Scripts

### 🚀 Orchestrators

- [execute_core_etl.sh](execute_core_etl.sh) – fact, daily_user_panel, user_panel
- [execute_curated_etl.sh](execute_curated_etl.sh) – dim_user, fct_sessions, fct_purchases
- [execute_monitoring.sh](execute_monitoring.sh) – logs, table, kpis monitoring
- [execute_all.sh](execute_all.sh) – core + curated + monitoring (with delays)

### ⏰ [crontab.sh](crontab.sh)
Cron job configuration examples for automated scheduling.

## Usage

### Manual Execution
```bash
# Full pipeline (ETL + Monitoring)
./scheduler/execute_all.sh

# Core ETL only (fact → daily_user_panel → user_panel)
./scheduler/execute_core_etl.sh

# Curated ETL only (dim_user → fct_sessions → fct_purchases)
./scheduler/execute_curated_etl.sh

# Monitoring only (logs, KPIs, tables)
./scheduler/execute_monitoring.sh
```

### Automated Scheduling
1. Copy examples from `crontab.sh`
2. Set variables:
   - `PROJECT_ID=ppltx-m--tutorial-dev`
   - `PATH_TO=/absolute/path/to/gaming-bi-system`
3. Add to your crontab: `crontab -e`
4. Suggested spacing between jobs is 5–10 minutes; orchestrator examples already include sleeps.

## Configuration

The script uses relative paths and automatically navigates to the project root. Update the `PROJECT_ID` variable to match your environment.

## Output

The script provides:
- **Console output**: Progress indicators and completion status
- **BigQuery logs**: Detailed execution logs in `logs.daily_logs` table
- **Local files**: Reports and alerts in `temp/` directory structure
- **Visual indicators**: Emojis and clear status messages

## Pipeline Flow

```
ETL Pipelines (core):
FACT → Daily User Panel → User Panel

ETL Pipelines (curated):
dim_user → fct_sessions → fct_purchases

Monitoring Systems:
Logs Monitoring → KPI Monitoring → Table Monitoring
```
