"""Slack Block Kit notifier using Airflow's SlackWebhookHook.

Renders rich, structured messages via Jinja2 templates and sends
them through the ``slack_webhook`` Airflow connection.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from plugins.constants import CONN_SLACK_WEBHOOK

logger = logging.getLogger(__name__)

_TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "templates"
_jinja_env = Environment(
    loader=FileSystemLoader(str(_TEMPLATE_DIR)),
    autoescape=False,
)

_EVENT_TEMPLATE_MAP = {
    "failure": "slack_failure.json.j2",
    "retry": "slack_retry.json.j2",
    "success": "slack_success.json.j2",
}

_SEVERITY_EMOJI = {
    "critical": "\U0001f6a8",   # 🚨
    "warning": "\u26a0\ufe0f",  # ⚠️
    "info": "\u2705",           # ✅
}

_SEVERITY_COLOR = {
    "critical": "#dc2626",
    "warning": "#f59e0b",
    "info": "#22c55e",
}


def send_slack_alert(severity: str, payload: dict[str, Any]) -> None:
    """Render a Block Kit message and post it via the Slack webhook connection."""
    event = payload.get("event", "failure")
    template_name = _EVENT_TEMPLATE_MAP.get(event, "slack_failure.json.j2")

    try:
        template = _jinja_env.get_template(template_name)
    except Exception:
        logger.warning("Slack template %s not found — falling back to plain text", template_name)
        _send_plain_text(severity, payload)
        return

    render_ctx = {
        **payload,
        "severity": severity,
        "emoji": _SEVERITY_EMOJI.get(severity, "\u2139\ufe0f"),
        "color": _SEVERITY_COLOR.get(severity, "#6b7280"),
    }

    try:
        rendered = template.render(**render_ctx)
        blocks = json.loads(rendered)
    except Exception:
        logger.warning("Failed to render Slack template — falling back to plain text", exc_info=True)
        _send_plain_text(severity, payload)
        return

    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        hook = SlackWebhookHook(slack_webhook_conn_id=CONN_SLACK_WEBHOOK)
        hook.send_dict({"blocks": blocks})
        logger.info("Slack alert sent: %s %s.%s", severity, payload.get("dag_id"), payload.get("task_id"))
    except Exception:
        logger.warning("Failed to send Slack alert — connection '%s' may not be configured", CONN_SLACK_WEBHOOK, exc_info=True)


def _send_plain_text(severity: str, payload: dict[str, Any]) -> None:
    """Fallback: send a simple text message if templates fail."""
    emoji = _SEVERITY_EMOJI.get(severity, "")
    text = (
        f"{emoji} *[{severity.upper()}]* `{payload.get('dag_id')}.{payload.get('task_id')}`\n"
        f"Event: {payload.get('event')} | Try: {payload.get('try_number')}\n"
        f"Error: {payload.get('error', 'N/A')}"
    )
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        hook = SlackWebhookHook(slack_webhook_conn_id=CONN_SLACK_WEBHOOK)
        hook.send_text(text)
    except Exception:
        logger.warning("Slack plain-text fallback also failed", exc_info=True)
