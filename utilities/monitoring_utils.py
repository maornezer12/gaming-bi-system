"""
Shared helpers for monitoring scripts: compose markdown alerts, write files,
and send Slack notifications in a consistent way.
"""

from pathlib import Path
from datetime import datetime
from typing import Optional

from .io import write_file
from .formatting import df_to_string_table
from .slack import send_alert_notification


def compose_alert_markdown(title: str, summary: str, df, run_time: datetime) -> str:
    """Compose a standard markdown alert with a table and timestamp."""
    return (
        f"# {title}\n\n"
        f"## Summary\n{summary}\n\n"
        f"## Alert Details\n\n{df_to_string_table(df)}\n\n"
        f"## Generated at\n{run_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
    )


def write_and_notify(
    alerts_path: Path,
    filename_stem: str,
    content: str,
    alert_type: str,
    count: int,
    details: str,
) -> Path:
    """Write alert markdown to alerts path and send a Slack notification.

    Returns the written file path.
    """
    alert_file = alerts_path / f"{filename_stem}.md"
    write_file(alert_file, content)
    send_alert_notification(
        alert_type=alert_type,
        count=count,
        details=details,
        alert_file_path=alert_file,
    )
    return alert_file


def require_keys(conf: dict, keys: list[str], context: str) -> None:
    """Validate required keys exist; raise ValueError if missing."""
    missing = [k for k in keys if k not in conf]
    if missing:
        raise ValueError(f"Missing keys in {context}: {', '.join(missing)}")


