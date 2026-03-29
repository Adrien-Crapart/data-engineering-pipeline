"""Callback handlers for DAG/task lifecycle events.

Each callback extracts context, determines severity, and dispatches
to the notification router which fans out to Slack / Email channels.
"""

from __future__ import annotations

import logging
import traceback
from typing import Any

from plugins.constants import SEVERITY_CRITICAL, SEVERITY_INFO, SEVERITY_WARNING
from plugins.notifications.router import send_alert

logger = logging.getLogger(__name__)


def _extract_context(context: dict[str, Any]) -> dict[str, Any]:
    """Build a flat payload from the Airflow callback context."""
    ti = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")

    return {
        "dag_id": getattr(ti, "dag_id", "unknown"),
        "task_id": getattr(ti, "task_id", "unknown"),
        "run_id": getattr(dag_run, "run_id", "unknown"),
        "logical_date": str(context.get("logical_date", "")),
        "try_number": getattr(ti, "try_number", 0),
        "max_tries": getattr(ti, "max_tries", 0),
        "duration": getattr(ti, "duration", 0),
        "error": str(exception) if exception else "",
        "traceback": traceback.format_exc() if exception else "",
        "log_url": getattr(ti, "log_url", ""),
    }


def on_failure_callback(context: dict[str, Any]) -> None:
    """Triggered when a task fails — CRITICAL alert (Slack + Email)."""
    payload = _extract_context(context)
    payload["event"] = "failure"
    send_notifications = context.get("params", {}).get("send_notifications", True)
    if not send_notifications:
        logger.info("Notifications disabled via DAG param — skipping failure alert")
        return
    logger.error("TASK FAILED: %s.%s [try %s]", payload["dag_id"], payload["task_id"], payload["try_number"])
    send_alert(SEVERITY_CRITICAL, payload)


def on_retry_callback(context: dict[str, Any]) -> None:
    """Triggered when a task is retried — WARNING alert (Slack only)."""
    payload = _extract_context(context)
    payload["event"] = "retry"
    send_notifications = context.get("params", {}).get("send_notifications", True)
    if not send_notifications:
        return
    logger.warning(
        "TASK RETRY: %s.%s [try %s/%s]",
        payload["dag_id"], payload["task_id"], payload["try_number"], payload["max_tries"],
    )
    send_alert(SEVERITY_WARNING, payload)


def on_success_callback(context: dict[str, Any]) -> None:
    """Triggered when a DAG run succeeds — INFO alert (Email only)."""
    payload = _extract_context(context)
    payload["event"] = "success"
    send_notifications = context.get("params", {}).get("send_notifications", True)
    if not send_notifications:
        return
    logger.info("DAG SUCCESS: %s", payload["dag_id"])
    send_alert(SEVERITY_INFO, payload)
