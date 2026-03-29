"""Alert router — dispatches to Slack and/or Email based on severity.

Routing matrix:
    CRITICAL  →  Slack + Email
    WARNING   →  Slack only
    INFO      →  Email only
"""

from __future__ import annotations

import logging
from typing import Any

from plugins.constants import SEVERITY_CRITICAL, SEVERITY_INFO, SEVERITY_WARNING

logger = logging.getLogger(__name__)


def send_alert(severity: str, payload: dict[str, Any]) -> None:
    """Route an alert to the appropriate notification channels."""
    severity = severity.lower()

    if severity == SEVERITY_CRITICAL:
        _try_slack(severity, payload)
        _try_email(severity, payload)
    elif severity == SEVERITY_WARNING:
        _try_slack(severity, payload)
    elif severity == SEVERITY_INFO:
        _try_email(severity, payload)
    else:
        logger.warning("Unknown severity '%s' — sending to Slack as fallback", severity)
        _try_slack(severity, payload)


def _try_slack(severity: str, payload: dict[str, Any]) -> None:
    try:
        from plugins.notifications.slack_notifier import send_slack_alert

        send_slack_alert(severity, payload)
    except Exception:
        logger.warning("Slack notification dispatch failed", exc_info=True)


def _try_email(severity: str, payload: dict[str, Any]) -> None:
    try:
        from plugins.notifications.email_notifier import send_email_alert

        send_email_alert(severity, payload)
    except Exception:
        logger.warning("Email notification dispatch failed", exc_info=True)
