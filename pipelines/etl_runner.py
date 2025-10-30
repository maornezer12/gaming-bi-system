#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Run Commands

--- fact ---

python pipelines/etl_runner.py ppltx-m--tutorial-dev --job_name fact --job_action init --dry-run
python pipelines/etl_runner.py ppltx-m--tutorial-dev --job_name fact --job_action daily --dry-run

--- daily_user_panel ---

python pipelines/etl_runner.py ppltx-m--tutorial-dev --job_name daily_user_panel --job_action init --dry-run
python pipelines/etl_runner.py ppltx-m--tutorial-dev --job_name daily_user_panel --job_action daily --dry-run

--- user_panel ---

python pipelines/etl_runner.py ppltx-m--tutorial-dev --job_name user_panel --job_action init --dry-run
python pipelines/etl_runner.py ppltx-m--tutorial-dev --job_name user_panel --job_action daily --dry-run

--- curated layer (new) ---

python pipelines/etl_runner.py ppltx-m--tutorial-dev --job_name dim_user --job_action init  --dry-run
python pipelines/etl_runner.py ppltx-m--tutorial-dev --job_name dim_user --job_action daily --dry-run

python pipelines/etl_runner.py ppltx-m--tutorial-dev --job_name fct_sessions --job_action init  --dry-run
python pipelines/etl_runner.py ppltx-m--tutorial-dev --job_name fct_sessions --job_action daily --dry-run

python pipelines/etl_runner.py ppltx-m--tutorial-dev --job_name fct_purchases --job_action init  --dry-run
python pipelines/etl_runner.py ppltx-m--tutorial-dev --job_name fct_purchases --job_action daily --dry-run

"""
import sys
from pathlib import Path

# Ensure project root is on sys.path BEFORE importing utilities
project_root_boot = Path(__file__).resolve().parent.parent 
if str(project_root_boot) not in sys.path:
    sys.path.insert(0, str(project_root_boot))

from utilities.io import header, read_file, write_file, read_json
from utilities.bq import get_bq_client
from utilities.daily_logs import insert_log, next_step_id
from utilities.cli import create_standard_cli
from utilities.formatting import get_date_params, format_query_template
from utilities.paths import get_standard_paths, get_job_temp_paths, get_task_paths

# --- setup paths ---
paths = get_standard_paths(__file__)

project_root = paths['project_root']
temp_root = paths['temp_root']
pipelines_root = paths['pipelines_root']
monitoring_root = paths['monitoring_root']
utilities_root = paths['utilities_root']

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

# Get date parameters
date_today, run_time, y_m_d = get_date_params(days_back)

# Get job-specific temp paths
logs_path, error_path, alerts_path = get_job_temp_paths(job_name, temp_root)


insert_log(project_id, job_name, job_action, "init_config", "Loading configuration files", client, dry_run, step_id=next_step_id())

tasks_config = read_json(pipelines_root/ f"{job_name}/{job_name}_config.json")

action_config = read_json(pipelines_root / "action_config.json")


selected_tasks = action_config.get(job_action, [])
selected_tasks = [task.replace("{job_name}", job_name) for task in selected_tasks]

if not selected_tasks:
    header(f"No tasks found for action: {job_action} in action_config.json")
    sys.exit(0)

etl_group = next(iter(tasks_config.values()))
tasks = etl_group["tasks"]

for task_name in selected_tasks:
    if task_name not in tasks:
        print(f"Task {task_name} not defined in config, skipping.")
        continue

    task_conf = tasks[task_name]
    if not task_conf.get("isEnable", True):
        continue
    
    # load and render query
    insert_log(project_id, job_name, job_action, "load_query", f"Loading SQL template for task: {task_name}", client, dry_run, step_id=next_step_id())
    sql_path, __ , _ = get_task_paths(job_name, task_name, project_root)
    query_template = read_file(sql_path)
    insert_log(project_id, job_name, job_action, "render_query", f"Rendering SQL template for task: {task_name}", client, dry_run, step_id=next_step_id())    
    query = format_query_template(query_template, task_conf, project_id, job_name, job_action, y_m_d, run_time)

    
    # Write query to temp/logs folder 
    write_file(logs_path / f"{task_name}.sql", query)

    if flags.dry_run:
        header(f"[DRY-RUN] Would execute: {task_name}")
        continue

    try:
        insert_log(project_id, job_name, job_action, "execute_query", f"Executing BigQuery query for task: {task_name}", client, dry_run, step_id=next_step_id())
        header(f"Running task: {task_name}")
        if client:
            client.query(query).result()
        else:
            print(f"[WARNING] No BigQuery client available")
    except Exception as e:
        sql_out_path = logs_path / f"{task_name}.sql"
        msg = (
            f"Error in task '{task_name}': {e}\n"
            f"Rendered SQL: {sql_out_path}"
        )
        header(f"Hi BI Developer we have a problem\nOpen file {str(error_path)}/{task_name}_error.md")
        print(msg)
        write_file(error_path / f"{task_name}_error.md", msg)
# Log end
insert_log(project_id, job_name, job_action, "end", "ETL pipeline completed successfully", client, dry_run, step_id=next_step_id())
