#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""

python monitoring/logs_monitoring/logs_monitoring.py ppltx-m--tutorial-dev --job_name log --job_action daily --dry-run

"""
from pathlib import Path
import os
import re
import sys
from datetime import datetime, timedelta, date
from google.cloud import bigquery
import argparse
import uuid
import platform
import pandas as pd
from utilities.my_etl_files import (readJsonFile, ensureDirectory, writeFile, readFile, header)


# --- setup paths ---
home = Path("C:/") if os.name == 'nt' else Path(os.path.expanduser("~"))    
repo_root = Path(__file__).resolve().parent                                
# repo_root = Path(__file__).resolve().parent[2]                                 
logs_path = repo_root / "temp" / "monitoring" / "logs"
error_path = repo_root / "temp" / "monitoring" / "errors"
CONFIG_PATH = repo_root / "monitoring" / "logs_monitoring" / "logs_config.json"
SQL_PATH =  repo_root / "monitoring" / "logs_monitoring" / "logs_query.sql"

ensureDirectory(logs_path)
ensureDirectory(error_path)


# --- CLI ---
parser = argparse.ArgumentParser()
parser.add_argument("project_id", choices=["ppltx-m--tutorial-dev", "my-bi-project-ppltx"], default="ppltx-m--tutorial-dev")
parser.add_argument("--job_action", choices=["init", "daily", "delete"], required=True)
parser.add_argument("--job_name", required=True)
parser.add_argument("--dry-run", action="store_true")
parser.add_argument("--days-back", type=int, default=0)
flags = parser.parse_args()

project_id = flags.project_id
job_name = flags.job_name
job_action = flags.job_action
days_back = flags.days_back

client = bigquery.Client(project=project_id)


# Get dates
date_today = date.today()
run_time = datetime.now()
y_m_d = (date_today + timedelta(days=-days_back)).strftime("%Y-%m-%d")

log_table = f"{project_id}.logs.daily_logs"
log_dict = {
    'ts': datetime.now(),
    'dt': datetime.now().strftime("%Y-%m-%d"),
    'uid': str(uuid.uuid4())[:8],
    'username': platform.node(),
    'job_name': job_name,
    'job_action': job_action,
    'file_name': os.path.basename(__file__),
    'step_id': 0,
}

def set_log(step):
    log_dict['step_id'] += 1
    log_dict['step_name'] = step
    log_dict['ts'] = datetime.now()
    df = pd.DataFrame(log_dict, index=[0])
    client.load_table_from_dataframe(df, log_table).result()

if not flags.dry_run:
    set_log("start")

# get etl configuration
logs_config = readJsonFile(CONFIG_PATH)
# etl_configuration = readJsonFile(home / repo_name / repo_tail / f"config/{job_name}_config.json")
if not logs_config or "tables" not in logs_config:
    header(f"Could not load tables name to check at: {CONFIG_PATH}")
    sys.exit(1)



# dictionary for queries
query_dict = {}
alert_columns ='raise_flag'

df_all = pd.DataFrame()   #  Initialize empty DataFrame

# Iterate all the validation groups in the conf
for alert_group_name, alerts in logs_config["tables"].items():
    header(alert_group_name)
    query_sql = readFile(SQL_PATH)
    # query_sql = readFile(home / repo_name / repo_tail / f"queries/{job_name}_alert.sql")
    conf = {alert_group_name: alerts}

    query_params_base = {
         "date": y_m_d,
         "run_time": run_time,
         "project": project_id,
         "job_action": job_action
    }

    for alert_name, alert_conf in conf.items():
        print(alert_name)

        # enriched query params
        query_params = dict(query_params_base)
        query_params.update(alert_conf)

        query = query_sql.format(**query_params)

        # write a query to log (use pre-defined logs_path)
        writeFile(logs_path / f"log_{alert_name}.sql", query)
        if not flags.dry_run:
            try:
                job_id = client.query(query)
                query_df = job_id.to_dataframe()
                query_dict[alert_name] = {}

                # union the query results
                if df_all.empty:
                    df_all = query_df
                else:
                    df_all = pd.concat([df_all, query_df], ignore_index=True)
            except Exception as s:
                msg_error = f"The error is {s}"
                header(f"Hi BI Developer we have a problem\nOpen file {str(error_path)}/error.md")
                print(msg_error)
                writeFile(error_path / "error.md", msg_error)


#  Final check – only if df_all has data
if not df_all.empty and (df_all[alert_columns]).any():

    print(df_all[df_all[alert_columns]])
    error_msg = "[Logs Alert]"
    df_alert = df_all[df_all[alert_columns]]
    df_alert = df_alert.loc[:, df_alert.columns != 'message']
    msg = f"{error_msg}\n\n*These processes hadn't run in more than N hour*\n"

    writeFile(logs_path / f"{job_name}_monitoring_msg.md", msg)
    print(msg)

if not flags.dry_run:
    set_log("end")
