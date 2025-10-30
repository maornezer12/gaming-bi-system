# ╔═══════════════════════════════════════════╗
# ║  Gaming BI System - Complete Pipeline    ║
# ║  ETL + Monitoring (All-in-One)           ║
# ╚═══════════════════════════════════════════╝

# Run complete pipeline: ETL + Monitoring
# ┌───────────── minute (0 - 59)
# │ ┌───────────── hour (0 - 23)
# │ │ ┌───────────── day of the month (1 - 31)
# │ │ │ ┌───────────── month (1 - 12)
# │ │ │ │ ┌───────────── day of the week (0 - 7) (Sunday=0 or 7)
# │ │ │ │ │
# │ │ │ │ │
0 8 * * * bash /path/to/gaming-bi-system/scheduler/execute_full_bi_etl.sh >> /path/to/gaming-bi-system/temp/scheduler/full_pipeline.log 2>&1

# Example: Individual daily jobs (run starting 07:00 server time)
# Replace /path/to with your absolute path and ensure python env is active
# PROJECT_ID=ppltx-m--tutorial-dev
# PATH_TO=/path/to/gaming-bi-system
# 0 7 * * * python $PATH_TO/pipelines/etl_runner.py $PROJECT_ID --job_name fact              --job_action daily   >> $PATH_TO/temp/scheduler/fact_daily.log 2>&1
# 5 7 * * * python $PATH_TO/pipelines/etl_runner.py $PROJECT_ID --job_name daily_user_panel --job_action daily   >> $PATH_TO/temp/scheduler/daily_user_panel_daily.log 2>&1
# 10 7 * * * python $PATH_TO/pipelines/etl_runner.py $PROJECT_ID --job_name user_panel       --job_action daily   >> $PATH_TO/temp/scheduler/user_panel_daily.log 2>&1
# Curated layer (new)
# 20 7 * * * python $PATH_TO/pipelines/etl_runner.py $PROJECT_ID --job_name dim_user       --job_action daily   >> $PATH_TO/temp/scheduler/dim_user_daily.log 2>&1
# 25 7 * * * python $PATH_TO/pipelines/etl_runner.py $PROJECT_ID --job_name fct_sessions    --job_action daily   >> $PATH_TO/temp/scheduler/fct_sessions_daily.log 2>&1
# 30 7 * * * python $PATH_TO/pipelines/etl_runner.py $PROJECT_ID --job_name fct_purchases   --job_action daily   >> $PATH_TO/temp/scheduler/fct_purchases_daily.log 2>&1

# Or single orchestrators:
# 0 7 * * * bash $PATH_TO/scheduler/execute_core_etl.sh     $PROJECT_ID >> $PATH_TO/temp/scheduler/core_etl.log 2>&1
# 0 8 * * * bash $PATH_TO/scheduler/execute_curated_etl.sh  $PROJECT_ID >> $PATH_TO/temp/scheduler/curated_etl.log 2>&1
# 0 9 * * * bash $PATH_TO/scheduler/execute_monitoring.sh    $PROJECT_ID >> $PATH_TO/temp/scheduler/monitoring.log 2>&1
# 0 10 * * * bash $PATH_TO/scheduler/execute_all.sh          $PROJECT_ID >> $PATH_TO/temp/scheduler/all.log 2>&1

