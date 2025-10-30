#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
python monitoring/table_monitoring/table_monitoring.py ppltx-m--tutorial-dev --job_name tables --job_action daily --dry-run
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
from utilities.paths import get_standard_paths, get_table_monitoring_paths
from utilities.slack import send_alert_notification, send_success_notification
from utilities.monitoring_utils import compose_alert_markdown, write_and_notify, require_keys

# --- setup paths ---
paths = get_standard_paths(__file__)
project_root = paths['project_root']
config_path, sql_template_path, logs_path, error_path, alerts_path = get_table_monitoring_paths(project_root)

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

# Get table configuration
insert_log(project_id, job_name, job_action, "init_config", "Loading table configuration", client, dry_run, step_id=next_step_id())
tables_config = read_json(config_path)

if not tables_config or "tables" not in tables_config:
    header(f"Could not load table configuration at: {config_path}")
    sys.exit(1)

insert_log(project_id, job_name, job_action, "validate_config", "Configuration validation completed", client, dry_run, step_id=next_step_id())

# Load SQL template
insert_log(project_id, job_name, job_action, "load_query_template", "Loading SQL template", client, dry_run, step_id=next_step_id())
sql_template = read_file(sql_template_path)

# Dictionary for results
results_list = []
check_flag_column = 'raise_flag'

# Iterate all the tables in the config
for table_id, table_conf in tables_config["tables"].items():
    if not table_conf.get("enabled", True):
        continue
        
    header(f"Checking table: {table_id}")
    
    insert_log(project_id, job_name, job_action, "render_query", f"Rendering SQL query for table: {table_id}", client, dry_run, step_id=next_step_id())
    
    # Validate required config keys
    require_keys(table_conf, ["dataset", "table", "description", "thresh_in_hours"], f"tables_config.tables[{table_id}]")
    # Query parameters
    query_params = {
        "project_id": project_id,
        "dataset": table_conf["dataset"],
        "table": table_conf["table"],
        "description": table_conf["description"],
        "thresh_in_hours": table_conf["thresh_in_hours"],
        "run_time": run_time.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    query = sql_template.format(**query_params)
    
    insert_log(project_id, job_name, job_action, "write_outputs", f"Writing SQL to temp folder for table: {table_id}", client, dry_run, step_id=next_step_id())
    # Write query to log
    write_file(logs_path / f"table_{table_conf['table']}.sql", query)
    
    if not dry_run:
        try:
            insert_log(project_id, job_name, job_action, "execute_query", f"Executing BigQuery query for table: {table_id}", client, dry_run, step_id=next_step_id())
            job_id = client.query(query)
            query_df = job_id.to_dataframe()
            
            # Add threshold comparison
            query_df['thresh_in_hours'] = table_conf["thresh_in_hours"]
            query_df['raise_flag'] = query_df['hours_diff'] > table_conf["thresh_in_hours"]
            
            insert_log(project_id, job_name, job_action, "aggregate_results", f"Processing results for table: {table_id}", client, dry_run, step_id=next_step_id())
            results_list.append(query_df)
            
        except Exception as error:
            error_message = f"The error is {error}"
            header(f"Hi BI Developer we have a problem with table {table_id}\nOpen file {str(error_path)}/{table_conf['table']}_error.md")
            print(error_message)
            write_file(error_path / f"{table_conf['table']}_error.md", error_message)

# Combine all results
if results_list:
    df_all = pd.concat(results_list, ignore_index=True)
    
    # Final check – only if df_all has data
    if not df_all.empty and (df_all[check_flag_column]).any():
        
        # Create detailed alert report with table
        alert_df = df_all[df_all[check_flag_column]]
        alert_content = compose_alert_markdown(
            title=f"Table Freshness Alert - {y_m_d}",
            summary="*These tables are not fresh (exceeded freshness threshold)*",
            df=alert_df,
            run_time=run_time,
        )

        alert_file = write_and_notify(
            alerts_path=alerts_path,
            filename_stem=f"{job_name}_monitoring_alert_{y_m_d}",
            content=alert_content,
            alert_type="Table Freshness",
            count=len(alert_df),
            details="Tables exceeded freshness threshold",
        )
        
        # Show brief message to user
        header("🚨 TABLE FRESHNESS ALERTS 🚨")
        print(f"Hi BI Developer - you have NEW ALERTS!")
        print(f"Check the detailed report at: {alert_file}")
        print(f"Found {len(alert_df)} tables that are not fresh.")
        print("Slack notification sent to #logs_monitoring_alerts")
    
    # Write summary report
    insert_log(project_id, job_name, job_action, "write_summary", "Writing summary report", client, dry_run, step_id=next_step_id())
    summary_content = f"""# Table Freshness Report - {y_m_d}

## Summary
- **Total Tables Checked**: {len(df_all)}
- **Tables with Alerts**: {len(df_all[df_all[check_flag_column]]) if not df_all.empty else 0}
- **Check Time**: {run_time.strftime('%Y-%m-%d %H:%M:%S')}

## Results

{df_to_string_table(df_all) if not df_all.empty else 'No results'}

## Alerts
"""
    
    if not df_all.empty and (df_all[check_flag_column]).any():
        summary_content += f"\n{df_to_string_table(df_all[df_all[check_flag_column]])}"
    else:
        summary_content += "\n✅ All tables are fresh!"
    
    write_file(logs_path / f"table_monitoring_{y_m_d}.md", summary_content)

# Send success notification if no alerts
if not results_list or df_all.empty or not (df_all[check_flag_column]).any():
    send_success_notification(
        monitoring_type="Table Freshness",
        message="All tables are fresh and up to date"
    )

insert_log(project_id, job_name, job_action, "end", "Table monitoring completed", client, dry_run, step_id=next_step_id())
