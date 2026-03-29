"""Notification dispatchers — Slack Block Kit and Email HTML."""

from plugins.notifications.router import send_alert

__all__ = ["send_alert"]
