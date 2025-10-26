#!/usr/bin/env bash
# ╔════════════════════════════════════════════════════════╗
# ║  USER PANEL ETL Scheduler - Daily and Init modes       ║
# ╚════════════════════════════════════════════════════════╝

cd ~/workspace/ppltx-tutorial/ || exit 1

PROJECT_ID="ppltx-m--tutorial-dev"
ETL_NAME="user_panel"
PYTHON_PATH="python3 jobs/gaming_bi_system/etl_runner.py"

echo " Starting USER PANEL ETL at $(date)"

# 1. Initialize (only needed once)
# $PYTHON_PATH "$PROJECT_ID" --etl-name "$ETL_NAME" --etl-action init

# 2. Daily load
$PYTHON_PATH "$PROJECT_ID" --etl-name "$ETL_NAME" --etl-action daily

echo " USER PANEL ETL finished at $(date)"
