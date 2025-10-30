#!/bin/bash
set -euo pipefail
PROJECT_ID=${1:-ppltx-m--tutorial-dev}
ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
PY=python

$PY "$ROOT_DIR/pipelines/etl_runner.py" "$PROJECT_ID" --job_name fact              --job_action daily
sleep 300
$PY "$ROOT_DIR/pipelines/etl_runner.py" "$PROJECT_ID" --job_name daily_user_panel --job_action daily
sleep 300
$PY "$ROOT_DIR/pipelines/etl_runner.py" "$PROJECT_ID" --job_name user_panel       --job_action daily
