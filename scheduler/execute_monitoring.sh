#!/bin/bash
set -euo pipefail
PROJECT_ID=${1:-ppltx-m--tutorial-dev}
ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
PY=python

$PY "$ROOT_DIR/monitoring/logs_monitoring/logs_monitoring.py"  "$PROJECT_ID" --job_name log   --job_action daily
sleep 300
$PY "$ROOT_DIR/monitoring/table_monitoring/table_monitoring.py" "$PROJECT_ID" --job_name tables --job_action daily
sleep 300
$PY "$ROOT_DIR/monitoring/kpis_monitoring/kpis_monitoring.py"   "$PROJECT_ID" --job_name kpis  --job_action daily
