# KPI Monitoring

Monitors key performance indicators and detects significant changes that may indicate data quality issues.

## Purpose

Tracks KPIs like DAU, Installs, Last Activity, ARPDAU, and D1 retention to identify unusual patterns or data anomalies.

## What it does

1. **Reads KPI configurations** from `kpis_config.json`
2. **Loads SQL templates** for each KPI from `queries/` directory
3. **Compares current values** with previous day using LAG functions
4. **Calculates percentage changes** and compares against thresholds
5. **Raises alerts** when changes exceed configured limits
6. **Generates summary reports** with KPI status and alerts

## Configuration

Key fields per KPI (uses curated sources `fp_gaming_curated`):
- dau → `fct_sessions`
- installs → `dim_user`
- last_activity → `fct_sessions`
- arpdau → `fct_sessions` + `fct_purchases`
- retention_d1 → `dim_user` + `fct_sessions`

## Usage

```bash
python monitoring/kpis_monitoring/kpis_monitoring.py ppltx-m--tutorial-dev --job_name kpis --job_action daily
```

## Output

- **Console**: KPI status and alerts
- **BigQuery**: Detailed execution logs
- **Files**: SQL queries and alert messages in `temp/monitoring/kpis_monitoring/`
