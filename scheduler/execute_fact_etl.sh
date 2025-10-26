#!/usr/bin/env bash
# ╔════════════════════════════════════════════════════════╗
# ║  FACT ETL Scheduler - Daily and Init modes             ║
# ╚════════════════════════════════════════════════════════╝

# Navigate to project root
cd ~/workspace/ppltx-tutorial/

# --- CONFIG ---
PROJECT_ID="ppltx-m--tutorial-dev"
ETL_NAME="fact"
PYTHON_PATH="python3 jobs/gaming_bi_system/etl_runner.py"

# --- RUN ---
# 1. Initialize once (if needed)
# $PYTHON_PATH "$PROJECT_ID" --etl-name "$ETL_NAME" --etl-action init

# 2. Daily ETL (default mode)
$PYTHON_PATH "$PROJECT_ID" --etl-name "$ETL_NAME" --etl-action daily
