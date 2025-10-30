# Setup Guide - Gaming BI System

This guide will walk you through setting up the Gaming BI System from scratch.

## Prerequisites

### Required Software
- **Python 3.8+**: Download from [python.org](https://python.org)
- **Google Cloud SDK**: Download from [cloud.google.com/sdk](https://cloud.google.com/sdk)
- **Git**: Download from [git-scm.com](https://git-scm.com)

### Required Access
- **Google Cloud Project**: You need access to a Google Cloud project with BigQuery enabled
- **BigQuery Permissions**: Your account needs BigQuery Data Editor and Job User roles
- **Source Data Access**: Access to the gaming data source (this system pulls from existing data)

## Step 1: Clone the Repository

```bash
git clone <repository-url>
cd gaming-bi-system
```

## Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```


### Option A: Application Default Credentials (Recommended)
```bash
gcloud auth application-default login
```

### Option B: Service Account (For Production)
1. Create a service account in Google Cloud Console
2. Download the JSON key file
3. Set environment variable:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
```

## Step 4: Configure Your Project

### Update Project ID
Replace `ppltx-m--tutorial-dev` with your actual Google Cloud project ID in:

1. **Pipeline Commands**: Update all commands to use your project ID
2. **Configuration Files**: Check if any hardcoded project IDs need updating

### Verify BigQuery Access
```bash
# Test BigQuery connection
python -c "
from google.cloud import bigquery
client = bigquery.Client(project='your-project-id')
print('BigQuery connection successful!')
"
```

## Step 5: Initialize the System

### Initialize Tables (One-time setup)
```bash
# Initialize FACT table
python pipelines/etl_runner.py your-project-id --job_name fact --job_action init

# Initialize Daily User Panel
python pipelines/etl_runner.py your-project-id --job_name daily_user_panel --job_action init

# Initialize User Panel
python pipelines/etl_runner.py your-project-id --job_name user_panel --job_action init

# Initialize curated layer (dim_user, sessions, purchases)
python pipelines/etl_runner.py your-project-id --job_name dim_user      --job_action init
python pipelines/etl_runner.py your-project-id --job_name fct_sessions  --job_action init
python pipelines/etl_runner.py your-project-id --job_name fct_purchases --job_action init
```

### Test with Dry Run
```bash
# Test FACT pipeline
python pipelines/etl_runner.py your-project-id --job_name fact --job_action daily --dry-run

# Test monitoring system
python monitoring/logs_monitoring/logs_monitoring.py your-project-id --job_name log --job_action daily --dry-run

# Test KPI monitoring system
python monitoring/kpis_monitoring/kpis_monitoring.py your-project-id --job_name kpis --job_action daily --dry-run

# Test table monitoring system
python monitoring/table_monitoring/table_monitoring.py your-project-id --job_name tables --job_action daily --dry-run
```

## Step 6: Run Daily Data Processing

### Manual Execution
```bash
# Process daily data for all pipelines (core)
python pipelines/etl_runner.py your-project-id --job_name fact --job_action daily
python pipelines/etl_runner.py your-project-id --job_name daily_user_panel --job_action daily
python pipelines/etl_runner.py your-project-id --job_name user_panel --job_action daily

# Curated daily
python pipelines/etl_runner.py your-project-id --job_name dim_user      --job_action daily
python pipelines/etl_runner.py your-project-id --job_name fct_sessions  --job_action daily
python pipelines/etl_runner.py your-project-id --job_name fct_purchases --job_action daily
```

### Automated Execution
The system includes shell scripts for automated execution:

```bash
# Make scripts executable
chmod +x scheduler/*.sh

# Run orchestrators
./scheduler/execute_core_etl.sh your-project-id
./scheduler/execute_curated_etl.sh your-project-id
./scheduler/execute_monitoring.sh your-project-id
./scheduler/execute_all.sh your-project-id
```

## Step 7: Monitor the System

### Check Job Status
```bash
# Check if ETL jobs have run within expected timeframes
python monitoring/logs_monitoring/logs_monitoring.py your-project-id --job_name log --job_action daily

# Check KPI metrics and alerts
python monitoring/kpis_monitoring/kpis_monitoring.py your-project-id --job_name kpis --job_action daily

# Check table freshness
python monitoring/table_monitoring/table_monitoring.py your-project-id --job_name tables --job_action daily
```

### View Logs
- **BigQuery Logs**: Check `your-project-id.logs.daily_logs` table
- **Local Logs**: Check `temp/` directory structure
- **Error Logs**: Check `temp/pipelines/{job_name}/errors/`
- **KPI Alerts**: Check `temp/monitoring/kpis_monitoring/alerts/`
- **Table Freshness Reports**: Check `temp/monitoring/table_monitoring/logs/`

## Troubleshooting

### Common Issues

#### 1. Authentication Errors
```bash
# Re-authenticate
gcloud auth application-default login

# Check current account
gcloud auth list
```

#### 2. Permission Errors
- Ensure your account has BigQuery Data Editor and Job User roles
- Check project ID is correct
- Verify BigQuery API is enabled

#### 3. Import Errors
```bash
# Check Python path
python -c "import sys; print(sys.path)"

# Reinstall dependencies
pip install -r requirements.txt --force-reinstall
```

#### 4. Path Issues
- Ensure you're running commands from the project root directory
- Check that all required files exist in the expected locations

### Getting Help

1. **Check Error Logs**: Look in `temp/pipelines/{job_name}/errors/`
2. **Use Dry Run**: Test with `--dry-run` flag first
3. **Verify Configuration**: Check JSON config files are valid
4. **Review Documentation**: See [SYSTEM.md](SYSTEM.md) for technical details



**Setup Complete!** 🎉

👉 **[Read System Documentation](SYSTEM.md)** for technical details and development guide
