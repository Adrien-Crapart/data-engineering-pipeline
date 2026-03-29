"""Email HTML notifier using Airflow's SmtpHook.

Renders modern HTML emails via Jinja2 templates and sends them
through the configured SMTP connection.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

logger = logging.getLogger(__name__)

_TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "templates"
_jinja_env = Environment(
    loader=FileSystemLoader(str(_TEMPLATE_DIR)),
    autoescape=True,
)

_EVENT_TEMPLATE_MAP = {
    "failure": "email_failure.html.j2",
    "success": "email_success.html.j2",
}

_SUBJECT_MAP = {
    "failure": "[CRITICAL] Pipeline failure: {dag_id}.{task_id}",
    "retry": "[WARNING] Pipeline retry: {dag_id}.{task_id}",
    "success": "[OK] Pipeline completed: {dag_id}",
}


def send_email_alert(severity: str, payload: dict[str, Any]) -> None:
    """Render an HTML email and send it via SMTP."""
    event = payload.get("event", "failure")
    template_name = _EVENT_TEMPLATE_MAP.get(event)
    if not template_name:
        logger.debug("No email template for event '%s' — skipping", event)
        return

    subject = _SUBJECT_MAP.get(event, "[ALERT] {dag_id}").format(**payload)
    to_addr = os.environ.get("ALERT_TO", "admin@weather-pipeline.local")
    from_addr = os.environ.get("ALERT_FROM", "monitoring@weather-pipeline.local")

    try:
        template = _jinja_env.get_template(template_name)
        html_body = template.render(severity=severity, **payload)
    except Exception:
        logger.warning("Failed to render email template %s", template_name, exc_info=True)
        html_body = f"<p><b>{severity.upper()}</b>: {payload.get('dag_id')}.{payload.get('task_id')}<br>Error: {payload.get('error', 'N/A')}</p>"

    try:
        from airflow.providers.smtp.hooks.smtp import SmtpHook

        with SmtpHook() as hook:
            hook.send_email_smtp(
                to=to_addr,
                subject=subject,
                html_content=html_body,
                from_email=from_addr,
            )
        logger.info("Email alert sent to %s: %s", to_addr, subject)
    except Exception:
        logger.warning("Failed to send email alert — SMTP may not be configured", exc_info=True)
