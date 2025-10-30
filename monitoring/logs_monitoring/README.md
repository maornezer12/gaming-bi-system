# Logs Monitoring

Monitors ETL job execution and completion status to ensure all critical processes run successfully.

## Purpose

Checks that ETL jobs (core and curated) have completed within expected timeframes and raises alerts when jobs are overdue.

## What it does

1. **Reads job configurations** from `logs_config.json`
2. **Queries BigQuery logs** to find last successful job completions
3. **Compares timestamps** against configured thresholds
4. **Raises alerts** for jobs that haven't run within expected time
5. **Generates reports** with job status and alert details

## Configuration

See `logs_config.json` for the complete list, including `dim_user`, `fct_sessions`, and `fct_purchases`.

## Usage

```bash
python monitoring/logs_monitoring/logs_monitoring.py ppltx-m--tutorial-dev --job_name log --job_action daily
```