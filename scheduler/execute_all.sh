#!/bin/bash
set -euo pipefail
PROJECT_ID=${1:-ppltx-m--tutorial-dev}
ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

bash "$ROOT_DIR/scheduler/execute_core_etl.sh" "$PROJECT_ID"
sleep 3000 # 50 minutes
bash "$ROOT_DIR/scheduler/execute_curated_etl.sh" "$PROJECT_ID"
sleep 3000 # 50 minutes
bash "$ROOT_DIR/scheduler/execute_monitoring.sh" "$PROJECT_ID"
