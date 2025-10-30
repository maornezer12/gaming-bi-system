#!/bin/bash
set -euo pipefail
PROJECT_ID=${1:-ppltx-m--tutorial-dev}
ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
PY=python

$PY "$ROOT_DIR/pipelines/etl_runner.py" "$PROJECT_ID" --job_name dim_user      --job_action daily
sleep 300
$PY "$ROOT_DIR/pipelines/etl_runner.py" "$PROJECT_ID" --job_name fct_sessions   --job_action daily
sleep 300
$PY "$ROOT_DIR/pipelines/etl_runner.py" "$PROJECT_ID" --job_name fct_purchases  --job_action daily
