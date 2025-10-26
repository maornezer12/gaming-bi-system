#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generic ETL Runner
------------------
Executes any ETL process (FACT, PANEL, USER PANEL, etc.)
based on configuration files.

Each ETL process is defined in:
  - config/<etl_name>_config.json  → defines tasks and parameters
  - config/action_config.json      → defines task order per action (init/daily/delete)

The runner dynamically replaces {etl_name} in SQL templates
and executes queries stored in /queries.


Run Commands


--- fact_etl ---

python etl_runner.py ppltx-m--tutorial-dev --etl-name fact --etl-action init --dry-run
python etl_runner.py ppltx-m--tutorial-dev --etl-name fact --etl-action delete --dry-run
python etl_runner.py ppltx-m--tutorial-dev --etl-name fact --etl-action daily --dry-run

--- daily_user_panel_etl ---

python etl_runner.py ppltx-m--tutorial-dev --etl-name daily_user_panel --etl-action init --dry-run
python etl_runner.py ppltx-m--tutorial-dev --etl-name daily_user_panel --etl-action delete --dry-run
python etl_runner.py ppltx-m--tutorial-dev --etl-name daily_user_panel --etl-action daily --dry-run

--- user_panel_etl ---

python etl_runner.py ppltx-m--tutorial-dev --etl-name user_panel --etl-action init --dry-run
python etl_runner.py ppltx-m--tutorial-dev --etl-name user_panel --etl-action delete --dry-run
python etl_runner.py ppltx-m--tutorial-dev --etl-name user_panel --etl-action daily --dry-run

"""

from pathlib import Path
import os, sys, uuid, platform, argparse
from datetime import datetime, timedelta, date
from winreg import error

from google.cloud import bigquery
import pandas as pd
from utilities.my_etl_files import (readJsonFile, ensureDirectory, writeFile, readFile, header)

# --- setup paths ---
home = Path("C:/") if os.name == 'nt' else Path(os.path.expanduser("~"))    # C:\
repo_root = Path(__file__).resolve().parent                                 # C:\ gaming-bi-system
logs_path = repo_root / "temp" / "logs"                                     # C:\gaming-bi-system\temp\logs
error_path = repo_root / "temp" / "errors"                               # C:\gaming-bi-system\temp\errors
# C:\gaming-bi-system\temp\logs
ensureDirectory(logs_path)

# --- CLI ---
parser = argparse.ArgumentParser()
parser.add_argument("project_id", choices=["ppltx-m--tutorial-dev", "my-bi-project-ppltx"], default="ppltx-m--tutorial-dev")
parser.add_argument("--etl-action", choices=["init", "daily", "delete"], required=True)
parser.add_argument("--etl-name", required=True)
parser.add_argument("--dry-run", action="store_true")
parser.add_argument("--days-back", type=int, default=0)
flags = parser.parse_args()

project_id = flags.project_id
etl_name = flags.etl_name
etl_action = flags.etl_action
days_back = flags.days_back

client = bigquery.Client(project=project_id)

date_today = date.today()
run_time = datetime.now()
y_m_d = (date_today - timedelta(days=days_back)).strftime("%Y-%m-%d")

log_table = f"{project_id}.logs.daily_logs"
log_dict = {
    'ts': datetime.now(),
    'dt': datetime.now().strftime("%Y-%m-%d"),
    'uid': str(uuid.uuid4())[:8],
    'username': platform.node(),
    'job_name': etl_name,
    'job_type': etl_action,
    'file_name': os.path.basename(__file__),
    'step_id': 0
}

def set_log(step):
    log_dict['step_id'] += 1
    log_dict['step_name'] = step
    log_dict['ts'] = datetime.now()
    df = pd.DataFrame(log_dict, index=[0])
    client.load_table_from_dataframe(df, log_table).result()

if not flags.dry_run:
    set_log("start")


tasks_config = readJsonFile(repo_root / f"pipelines/{etl_name}/{etl_name}_config.json")
action_config = readJsonFile(repo_root / "pipelines" / "action_config.json")

selected_tasks = action_config.get(etl_action, [])
selected_tasks = [task.replace("{etl_name}", etl_name) for task in selected_tasks]

if not action_config:
    header(f"Could not load action_config.json at: {repo_root / "pipelines" / "action_config.json"}")
    sys.exit(1)

selected_tasks = action_config.get(etl_action, [])
selected_tasks = [task.replace("{etl_name}", etl_name) for task in selected_tasks]

if not selected_tasks:
    header(f"No tasks found for action: {etl_action} in action_config.json")
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

    query_path = repo_root / f"pipelines/{etl_name}/{task_name}.sql"
    if not query_path.exists() and task_name == "clear_table":
        query_path = repo_root / "pipelines" / "clear_table.sql"

    query_template = readFile(query_path)
    query = query_template.format(
        **task_conf,
        date=y_m_d,
        run_time=run_time,
        project=project_id,
        etl_name=etl_name
    )

    log_file = logs_path / f"{task_name}.sql"
    writeFile(log_file, query)

    if flags.dry_run:
        header(f"[Dry Run] Would execute {task_name}")
        continue

    try:
        header(f"Running task: {task_name}")
        client.query(query).result()
    except Exception as e:
        msg = f"Error in {task_name}: {e}"
        header(f"Hi BI Developer we have a problem\nOpen file {str(error_path)}/{task_name}_error.md")
        print(msg)
        writeFile(error_path / f"{task_name}_error.md", msg)

if not flags.dry_run:
    set_log("end")