#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
python monitoring/logs_monitoring/logs_monitoring.py ppltx-m--tutorial-dev --job_name log --job_action daily --dry-run
"""
from pathlib import Path
import sys
import pandas as pd

# Ensure project root is on sys.path BEFORE importing utilities
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utilities.io import header, read_file, write_file, read_json
from utilities.bq import get_bq_client
from utilities.daily_logs import insert_log, next_step_id
from utilities.cli import create_standard_cli
from utilities.formatting import get_date_params, df_to_string_table
from utilities.paths import get_standard_paths, get_monitoring_paths
from utilities.slack import send_alert_notification, send_success_notification
from utilities.monitoring_utils import compose_alert_markdown, write_and_notify, require_keys

# --- setup paths ---
paths = get_standard_paths(__file__)

project_root = paths['project_root']
config_path, sql_path, logs_path, error_path, alerts_path = get_monitoring_paths(project_root)

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

# get etl configuration
insert_log(project_id, job_name, job_action, "init_config", "Loading monitoring configuration", client, dry_run, step_id=next_step_id())
logs_config = read_json(config_path)
if not logs_config or "tables" not in logs_config:
    header(f"Could not load tables name to check at: {config_path}")
    sys.exit(1)

query_sql = read_file(sql_path)
insert_log(project_id, job_name, job_action, "load_query_template", "SQL template loaded successfully", client, dry_run, step_id=next_step_id())

check_flag_column = 'raise_flag'

df_all = pd.DataFrame()   #  Initialize empty DataFrame

# Iterate all the validation groups in the config
for monitoring_name, monitoring_config in logs_config["tables"].items():
    header(monitoring_name)

    query_params_base = {
         "date": y_m_d,
         "run_time": run_time,
         "project": project_id,
         "job_action": job_action
    }

    insert_log(project_id, job_name, job_action, "render_query", f"Rendering SQL query for monitoring: {monitoring_name}", client, dry_run, step_id=next_step_id())
    # Validate and merge query params with monitoring config
    require_keys(monitoring_config, ["step_name", "thresh_in_hours"], f"logs_config.tables[{monitoring_name}]")
    query_params = dict(query_params_base)
    query_params.update(monitoring_config)

    query = query_sql.format(**query_params)
    
    # Write query to temp/logs folder
    write_file(logs_path / f"log_{monitoring_name}.sql", query)
    if not flags.dry_run:
        try:
            insert_log(project_id, job_name, job_action, "execute_query", f"Executing BigQuery query for monitoring: {monitoring_name}", client, dry_run, step_id=next_step_id())
            job_id = client.query(query)
            query_df = job_id.to_dataframe()

            insert_log(project_id, job_name, job_action, "aggregate_results", f"Merging DataFrame for monitoring: {monitoring_name}", client, dry_run, step_id=next_step_id())
            # Union the query results
            if df_all.empty:
                df_all = query_df
            else:
                df_all = pd.concat([df_all, query_df], ignore_index=True)
        except Exception as error:
            error_message = f"The error is {error}"
            header(f"Hi BI Developer we have a problem\nOpen file {str(error_path)}/{monitoring_name}_error.md")
            print(error_message)
            write_file(error_path / f"{monitoring_name}_error.md", error_message)


#  Final check – only if df_all has data
if not df_all.empty and (df_all[check_flag_column]).any():
    
    # Get the threshold from config for the message
    threshold_hours = logs_config["tables"][list(logs_config["tables"].keys())[0]]["thresh_in_hours"]
    
    # Create detailed alert report with table
    alert_df = df_all[df_all[check_flag_column]]
    alert_content = compose_alert_markdown(
        title=f"ETL Process Monitoring Alert - {y_m_d}",
        summary=f"*These processes hadn't run in more than {threshold_hours} hours*",
        df=alert_df,
        run_time=run_time,
    )

    alert_file = write_and_notify(
        alerts_path=alerts_path,
        filename_stem=f"{job_name}_monitoring_alert_{y_m_d}",
        content=alert_content,
        alert_type="ETL Process",
        count=len(alert_df),
        details=f"Processes haven't completed within {threshold_hours} hours",
    )
    
    # Show brief message to user
    header("🚨 ETL PROCESS MONITORING ALERTS 🚨")
    print(f"Hi BI Developer - you have NEW ALERTS!")
    print(f"Check the detailed report at: {alert_file}")
    print(f"Found {len(alert_df)} processes that haven't completed within {threshold_hours} hours.")
    print("Slack notification sent to #logs_monitoring_alerts")

# Send success notification if no alerts
if df_all.empty or not (df_all[check_flag_column]).any():
    send_success_notification(
        monitoring_type="ETL Process",
        message="All ETL processes completed within expected timeframe"
    )

insert_log(project_id, job_name, job_action, "end", "Logs monitoring completed", client, dry_run, step_id=next_step_id())
