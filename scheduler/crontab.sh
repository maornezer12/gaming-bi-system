# ╔═══════════════════════════════════════════╗
# ║  Final Project – Gaming BI ETL Jobs       ║
# ╚═══════════════════════════════════════════╝

# Run both sequentially (FACT → PANEL)
# ┌───────────── minute (0 - 59)
# │ ┌───────────── hour (0 - 23)
# │ │ ┌───────────── day of the month (1 - 31)
# │ │ │ ┌───────────── month (1 - 12)
# │ │ │ │ ┌───────────── day of the week (0 - 7) (Sunday=0 or 7)
# │ │ │ │ │
# │ │ │ │ │
0 10 * * * bash ~/workspace/ppltx-tutorial/jobs/gaming_bi_system/execute_full_bi_etl.sh >>
