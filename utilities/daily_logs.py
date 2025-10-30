"""
Daily logs writing utilities.

Centralizes writing rows into the logs.daily_logs table and maintains
deterministic step_id sequencing for a single process run.
"""

from datetime import datetime
import uuid
import platform
import os
import inspect
from typing import Optional
import pandas as pd

from .constants import LOGS_TABLE
from .bq import get_bq_client


_STEP_COUNTER = 0


def next_step_id() -> int:
    """Return the next sequential step id for the current process.

    This counter is process-local and is not persisted across runs.
    """
    global _STEP_COUNTER
    _STEP_COUNTER += 1
    return _STEP_COUNTER


def insert_log(
    project_id: str,
    job_name: str,
    job_action: str,
    step_name: str,
    message: str = "",
    client=None,
    dry_run: bool = False,
    step_id: Optional[int] = None,
    file_name: Optional[str] = None,
) -> bool:
    """Insert a log record into `logs.daily_logs`.

    Args:
        project_id: Target GCP project
        job_name: Logical job name (e.g., fact)
        job_action: Action type (e.g., daily)
        step_name: Logical step name
        message: Free-form message
        client: Optional BigQuery client; created if None
        dry_run: When True, skip writing
        step_id: Optional step id; auto-incremented if None
        file_name: Optional source filename; auto-inferred if None

    Returns:
        True on success, False on failure or when skipped due to dry_run.
    """
    if dry_run:
        return True

    if client is None:
        client = get_bq_client(project_id)
        if client is None:
            return False

    try:
        log_table = f"{project_id}.{LOGS_TABLE}"

        assigned_step_id = step_id if step_id is not None else next_step_id()

        if file_name is None:
            caller_file = None
            for frame_info in inspect.stack():
                module = inspect.getmodule(frame_info.frame)
                if not module:
                    continue
                module_name = getattr(module, "__name__", "")
                if not module_name.endswith("utilities.daily_logs"):
                    caller_file = os.path.basename(frame_info.filename)
                    break
            file_name_val = caller_file or os.path.basename(__file__)
        else:
            file_name_val = file_name

        log_record = {
            'ts': datetime.now(),
            'dt': datetime.now().strftime("%Y-%m-%d"),
            'uid': str(uuid.uuid4())[:8],
            'username': platform.node(),
            'job_name': job_name,
            'job_type': job_action,
            'file_name': file_name_val,
            'step_id': assigned_step_id,
            'step_name': step_name,
            'message': message
        }

        df = pd.DataFrame(log_record, index=[0])
        client.load_table_from_dataframe(df, log_table).result()
        return True
    except Exception as e:
        print(f"[WARNING] Could not log to BigQuery: {e}")
        return False


