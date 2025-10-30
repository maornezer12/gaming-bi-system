# Monitoring System

This directory contains comprehensive monitoring modules for the Gaming BI System.

## Overview

The monitoring system ensures data quality, system health, and operational excellence through automated checks and alerts.

## Modules

### 📊 [Logs Monitoring](logs_monitoring/)
Monitors ETL job execution and completion status.

### 📈 [KPI Monitoring](kpis_monitoring/)
Tracks key performance indicators and detects significant changes. Uses curated sources (`fp_gaming_curated`) and computes metrics (DAU, installs, ARPDAU, D1 retention, last activity) on the fly.

### 🗃️ [Table Monitoring](table_monitoring/)
Ensures BigQuery tables are fresh and updated within defined thresholds. Includes core and curated tables.

## Usage

All monitoring modules follow the same CLI pattern:

```bash
python monitoring/<module>/<module>.py <project_id> --job_name <name> --job_action daily [--dry-run]
```

## Output

- **BigQuery Logs**: All operations logged to `logs.daily_logs` table
- **Local Reports**: Detailed reports saved to `temp/monitoring/<module>/`
- **Alerts**: Critical issues highlighted with visual notifications

## Configuration

Each module has its own configuration file:
- `logs_monitoring/logs_config.json`
- `kpis_monitoring/kpis_config.json`
- `table_monitoring/tables_config.json`
