# Gaming BI System 🎮

> **A comprehensive Business Intelligence system for gaming data analysis using Google BigQuery**

## What This System Does

Imagine you're running a mobile game company and you want to understand how players behave, how much money they spend, and how engaged they are with your game. This system does exactly that by:

1. **📊 Pulling Game Data**: The system connects to existing gaming data sources and pulls raw event data (sessions, matches, purchases) into your project
2. **🔄 Organizing the Data**: Takes all this raw data and organizes it into meaningful tables that business analysts can easily understand
3. **👤 Creating User Profiles**: Builds comprehensive profiles for each player showing their lifetime activity, spending, and engagement
4. **📅 Daily Updates**: Every day, it automatically updates these profiles with the latest player activity
5. **🔍 Monitoring**: Keeps track of whether everything is working properly and alerts you if something goes wrong

## 🏗️ System Overview

### Data Flow
```
Raw Game Data (External Source) 
    ↓
FACT Table (Raw Events)
    ↓
Daily User Panel (Daily Aggregations)
    ↓
User Panel (Lifetime User Profiles)
    ↓
Curated Marts (dim_user, fct_sessions, fct_purchases)
```

### Key Features
- **🔄 Automated ETL Pipelines**: Processes gaming data daily with minimal intervention
- **📊 Multi-layered Analytics**: From raw events to user profiles
- **🔍 Smart Monitoring**: Tracks job execution and alerts on failures
- **📈 KPI Monitoring**: Monitors key performance indicators (DAU, Installs, Last Activity) with automated alerts
- **🗃️ Table Monitoring**: Ensures BigQuery tables are fresh and updated within defined thresholds
- **🗂️ Curated Layer (Marts)**: Clean facts and dimensions (`dim_user`, `fct_sessions`, `fct_purchases`) powering KPIs
- **🛠️ Modular Design**: Easy to extend and customize
- **📈 Business Intelligence**: Ready-to-use data for analytics and reporting

## 📁High Level Project Structure

```
gaming-bi-system/
├── pipelines/                    # ETL pipeline configurations and runner
├── monitoring/                   # System monitoring and alerting
├── utilities/                    # Centralized helper functions
├── scheduler/                    # Automated job scheduling
```

## Quick Start

### For New Users
👉 **[Complete Setup Guide](SETUP.md)** - Step-by-step installation and configuration

### For Developers
👉 **[System Documentation](SYSTEM.md)** - Technical details, architecture, and development guide

### Run (Recommended): Orchestrator Scripts
```bash
# Full pipeline (core + curated + monitoring)
./scheduler/execute_all.sh

# Core ETL only (fact → daily_user_panel → user_panel)
./scheduler/execute_core_etl.sh

# Curated ETL only (dim_user → fct_sessions → fct_purchases)
./scheduler/execute_curated_etl.sh

# Monitoring only (logs, KPIs, tables)
./scheduler/execute_monitoring.sh
```

Tip: if needed, make scripts executable once: `chmod +x scheduler/*.sh`.

### Run (Advanced): Direct Python
```bash
# core layer
python pipelines/etl_runner.py <PROJECT_ID> --job_name <fact|daily_user_panel|user_panel> --job_action <init|daily> [--dry-run]

# curated layer
python pipelines/etl_runner.py <PROJECT_ID> --job_name <dim_user|fct_sessions|fct_purchases> --job_action <init|daily> [--dry-run]

# monitoring
python monitoring/logs_monitoring/logs_monitoring.py <PROJECT_ID> --job_name log --job_action daily [--dry-run]
python monitoring/kpis_monitoring/kpis_monitoring.py <PROJECT_ID> --job_name kpis --job_action daily [--dry-run]
python monitoring/table_monitoring/table_monitoring.py <PROJECT_ID> --job_name tables --job_action daily [--dry-run]
```

### Environment Variables
Create `.env` in the project root or export variables:
```
SLACK_WEBHOOK_URL=YOUR/SLACK/WEBHOOK
SLACK_SEND_SUCCESS=false
SLACK_SUMMARY_ONLY=false
BI_VERBOSE=false
```


---

**Ready to get started?** 🚀

👉 **[Begin Setup](SETUP.md)** | 👉 **[Read System Docs](SYSTEM.md)**