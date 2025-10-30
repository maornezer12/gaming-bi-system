# Table Monitoring

Ensures BigQuery tables (core + curated) are fresh and updated within defined thresholds to maintain data quality.

## Purpose

Monitors table freshness by checking last modification timestamps and alerts when tables become stale.

## What it does

1. **Reads table configurations** from `tables_config.json`
2. **Queries BigQuery metadata** using INFORMATION_SCHEMA.TABLES
3. **Calculates hours since last update** for each table
4. **Compares against thresholds** configured per table
5. **Raises alerts** for tables exceeding freshness limits
6. **Generates freshness reports** with detailed status

## Configuration

See `tables_config.json` for the complete list: `daily_user_panel`, `user_panel`, `fact`, and curated `dim_user`, `fct_sessions`, `fct_purchases`.

## Usage

```bash
python monitoring/table_monitoring/table_monitoring.py ppltx-m--tutorial-dev --job_name tables --job_action daily
```

## Output

- **Console**: Table freshness status and alerts
- **BigQuery**: Detailed execution logs
- **Files**: SQL queries and freshness reports in `temp/monitoring/table_monitoring/`
