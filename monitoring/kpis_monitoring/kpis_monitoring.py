#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
KPI Monitoring System for Gaming BI
===================================

This script monitors KPI metrics such as DAU, Installs, and Last Activity,
compares them against previous days, and raises alerts when significant 
deviations are detected.

Usage:
    python monitoring/kpis_monitoring/kpis_monitoring.py <project_id> [--job_name <name>] [--job_action <action>] [--dry-run]

Examples:
    python monitoring/kpis_monitoring/kpis_monitoring.py ppltx-m--tutorial-dev --job_name kpis --job_action daily
    python monitoring/kpis_monitoring/kpis_monitoring.py ppltx-m--tutorial-dev --job_name kpis --job_action daily --dry-run
"""

import sys
from pathlib import Path
import pandas as pd

# Ensure project root is on sys.path BEFORE importing utilities
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import from existing utilities
from utilities.io import header, read_file, write_file, read_json
from utilities.bq import get_bq_client
from utilities.daily_logs import insert_log, next_step_id
from utilities.cli import create_standard_cli
from utilities.formatting import get_date_params, df_to_string_table
from utilities.paths import get_standard_paths, get_kpi_monitoring_paths
from utilities.slack import send_alert_notification, send_success_notification
from utilities.monitoring_utils import compose_alert_markdown, write_and_notify, require_keys

# --- setup paths ---
paths = get_standard_paths(__file__)
project_root = paths['project_root']
config_path, queries_path, logs_path, error_path, alerts_path = get_kpi_monitoring_paths(project_root)

# --- CLI ---
parser = create_standard_cli()
flags = parser.parse_args()

project_id = flags.project_id
job_name = flags.job_name
job_action = flags.job_action
days_back = flags.days_back
dry_run = flags.dry_run

# Get BigQuery client
client = get_bq_client(project_id, dry_run)

# Get standardized date parameters
date_today, run_time, y_m_d = get_date_params(days_back)

# Get KPI configuration
insert_log(project_id, job_name, job_action, "init_config", "Loading KPI configuration", client, dry_run, step_id=next_step_id())
kpis_config = read_json(config_path)

if not kpis_config or "tables" not in kpis_config:
    header(f"Could not load KPI configuration at: {config_path}")
    sys.exit(1)

insert_log(project_id, job_name, job_action, "validate_config", "Configuration validation completed", client, dry_run, step_id=next_step_id())

check_flag_column = 'raise_flag'

df_all = pd.DataFrame()   # Initialize empty DataFrame

# Iterate all the KPI groups in the config
for kpi_group_name, kpi_config in kpis_config["tables"].items():
    header(kpi_group_name)
    
    for kpi_name, kpi_conf in kpi_config["kpis"].items():
        if not kpi_conf.get("isEnable", True):
            continue
            
        insert_log(project_id, job_name, job_action, "load_query", f"Loading SQL template for KPI: {kpi_name}", client, dry_run, step_id=next_step_id())
        
        # Load SQL template
        sql_template_path = project_root / f"monitoring/kpis_monitoring/queries/{kpi_name}_alerts.sql"
        query_sql = read_file(sql_template_path)
        
        insert_log(project_id, job_name, job_action, "render_query", f"Rendering SQL query for KPI: {kpi_name}", client, dry_run, step_id=next_step_id())
        
        # Query parameters
        query_params_base = {
            "date": y_m_d,
            "run_time": run_time,
            "project": project_id,
            "job_action": job_action,
            "kpi_name": kpi_name,
        }
        
        # Validate and enrich query params
        require_keys(kpi_conf, ["thresh_in_percent", "d1"], f"kpis_config.tables[{kpi_group_name}].kpis[{kpi_name}]")
        # Enriched query params
        query_params = dict(query_params_base)
        query_params.update(kpi_conf)
        
        query = query_sql.format(**query_params)
        
        insert_log(project_id, job_name, job_action, "write_outputs", f"Writing SQL to temp folder for KPI: {kpi_name}", client, dry_run, step_id=next_step_id())
        # Write query to log
        write_file(logs_path / f"kpi_{kpi_name}.sql", query)
        
        if not dry_run:
            try:
                insert_log(project_id, job_name, job_action, "execute_query", f"Executing BigQuery query for KPI: {kpi_name}", client, dry_run, step_id=next_step_id())
                job_id = client.query(query)
                query_df = job_id.to_dataframe()
                
                insert_log(project_id, job_name, job_action, "aggregate_results", f"Merging DataFrame for KPI: {kpi_name}", client, dry_run, step_id=next_step_id())
                # Union the query results – skip empty frames to avoid pandas FutureWarning
                if query_df is not None and not query_df.empty:
                    if df_all.empty:
                        df_all = query_df
                    else:
                        df_all = pd.concat([df_all, query_df], ignore_index=True)
                    
            except Exception as error:
                error_message = f"The error is {error}"
                header(f"Hi BI Developer we have a problem in {kpi_name} query\nOpen file {str(error_path)}/{kpi_name}_error.md")
                print(error_message)
                write_file(error_path / f"{kpi_name}_error.md", error_message)

# Final check – only if df_all has data
if not df_all.empty and (df_all[check_flag_column]).any():
    
    # Create detailed alert report with table
    alert_df = df_all[df_all[check_flag_column]]
    alert_content = compose_alert_markdown(
        title=f"KPI Monitoring Alert - {y_m_d}",
        summary="*There is a significant change in the KPIs*",
        df=alert_df,
        run_time=run_time,
    )

    alert_file = write_and_notify(
        alerts_path=alerts_path,
        filename_stem=f"{job_name}_monitoring_alert_{y_m_d}",
        content=alert_content,
        alert_type="KPI",
        count=len(alert_df),
        details="Significant changes detected in KPIs",
    )
    
    # Show brief message to user
    header("🚨 KPI MONITORING ALERTS 🚨")
    print(f"Hi BI Developer - you have NEW ALERTS!")
    print(f"Check the detailed report at: {alert_file}")
    print(f"Found {len(alert_df)} KPIs with significant changes.")
    print("Slack notification sent to #logs_monitoring_alerts")

# Send success notification if no alerts
if df_all.empty or not (df_all[check_flag_column]).any():
    send_success_notification(
        monitoring_type="KPI",
        message="All KPIs within normal ranges"
    )

insert_log(project_id, job_name, job_action, "end", "KPI monitoring completed", client, dry_run, step_id=next_step_id())
