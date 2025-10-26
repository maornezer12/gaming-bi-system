#!/usr/bin/env bash
# ╔════════════════════════════════════════════════════════╗
# ║  FULL GAMING BI PIPELINE - FACT → PANEL sequence       ║
# ╚════════════════════════════════════════════════════════╝

cd ~/workspace/ppltx-tutorial/ || exit 1

PROJECT_ID="ppltx-m--tutorial-dev"
PYTHON_PATH="python3 jobs/gaming_bi_system/etl_runner.py"

echo " Starting full BI ETL process at $(date)"

# --- 1. FACT ETL ---
echo " Running FACT ETL..."
$PYTHON_PATH "$PROJECT_ID" --etl-name fact --etl-action daily
echo " FACT ETL completed."

# --- 2. PANEL ETL ---
echo " Running PANEL ETL..."
$PYTHON_PATH "$PROJECT_ID" --etl-name panel --etl-action daily
echo " PANEL ETL completed."

# --- 3. USER PANEL ETL ---
echo " Running USER PANEL ETL..."
$PYTHON_PATH "$PROJECT_ID" --etl-name user_panel --etl-action daily
echo " USER PANEL ETL completed."

echo " All ETL processes finished at $(date)"
