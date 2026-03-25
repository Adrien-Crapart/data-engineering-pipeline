"""
Airflow self-monitoring DAG.

Runs every hour to detect:
- Critical DAGs that are paused/disabled
- Missing DAG executions (no run within expected schedule window)
- Stuck DAG runs (running longer than expected)
- Anomaly detection on run duration vs historical average

Sends a single consolidated HTML email report per run via SMTP (MailHog).
"""

from __future__ import annotations

import logging
import os
import smtplib
from datetime import timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.models import DagModel, DagRun
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import func

logger = logging.getLogger(__name__)

TZ = "Europe/Paris"

CRITICAL_DAGS = [
    "ingestion_pipeline",
    "transformation_pipeline",
]

STUCK_RUN_THRESHOLD = pendulum.duration(hours=4)
ANOMALY_DURATION_FACTOR = 3.0

SMTP_HOST = os.environ.get("SMTP_HOST", "mailhog")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "1025"))
ALERT_FROM = os.environ.get("ALERT_FROM", "monitoring@weather-pipeline.local")
ALERT_TO = os.environ.get("ALERT_TO", "admin@weather-pipeline.local")

TEMPLATE_DIR = Path(__file__).parent / "templates"


def _on_failure_callback(context):
    ti = context.get("task_instance")
    logger.error(
        "TASK FAILED: %s | execution_date=%s",
        getattr(ti, "task_id", "unknown"),
        context.get("execution_date"),
    )


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=5),
    "on_failure_callback": _on_failure_callback,
}


@dag(
    dag_id="airflow_monitoring",
    default_args=default_args,
    schedule="0 * * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz=TZ),
    catchup=False,
    tags=["monitoring", "system"],
    max_active_runs=1,
    doc_md=__doc__,
)
def airflow_monitoring():

    @task
    @provide_session
    def check_critical_dags_paused(session=None) -> dict:
        paused = []
        for dag_id in CRITICAL_DAGS:
            dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
            if dag_model and dag_model.is_paused:
                paused.append(dag_id)
            elif not dag_model:
                paused.append(f"{dag_id} (NOT FOUND)")
        return {"paused_critical_dags": paused, "total_checked": len(CRITICAL_DAGS)}

    @task
    @provide_session
    def check_missed_executions(session=None) -> dict:
        now = pendulum.now(TZ)
        window = now - pendulum.duration(hours=24)
        missed = []
        for dag_id in CRITICAL_DAGS:
            dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
            if not dag_model or not dag_model.timetable_summary:
                continue
            runs_count = (
                session.query(func.count(DagRun.id))
                .filter(DagRun.dag_id == dag_id, DagRun.execution_date >= window)
                .scalar()
            )
            if runs_count == 0:
                missed.append(dag_id)
        return {"missed_dags": missed}

    @task
    @provide_session
    def check_stuck_runs(session=None) -> dict:
        now = pendulum.now(TZ)
        cutoff = now - STUCK_RUN_THRESHOLD
        stuck_runs = (
            session.query(DagRun)
            .filter(DagRun.state == DagRunState.RUNNING, DagRun.start_date < cutoff)
            .all()
        )
        stuck = []
        for run in stuck_runs:
            duration = now - run.start_date
            stuck.append({"dag_id": run.dag_id, "run_id": run.run_id, "duration": str(duration)})
        return {"stuck_runs": stuck}

    @task
    @provide_session
    def check_duration_anomalies(session=None) -> dict:
        anomalies = []
        for dag_id in CRITICAL_DAGS:
            avg_duration = (
                session.query(func.avg(DagRun.end_date - DagRun.start_date))
                .filter(
                    DagRun.dag_id == dag_id,
                    DagRun.state == DagRunState.SUCCESS,
                    DagRun.end_date.isnot(None),
                )
                .scalar()
            )
            if not avg_duration:
                continue
            latest_run = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == dag_id,
                    DagRun.state == DagRunState.SUCCESS,
                    DagRun.end_date.isnot(None),
                )
                .order_by(DagRun.execution_date.desc())
                .first()
            )
            if not latest_run or not latest_run.start_date or not latest_run.end_date:
                continue
            actual = latest_run.end_date - latest_run.start_date
            if actual > avg_duration * ANOMALY_DURATION_FACTOR:
                anomalies.append({
                    "dag_id": dag_id,
                    "actual_duration": str(actual),
                    "avg_duration": str(avg_duration),
                })
        return {"anomalies": anomalies}

    @task
    def send_consolidated_report(
        paused_result: dict,
        missed_result: dict,
        stuck_result: dict,
        anomaly_result: dict,
    ) -> dict:
        paused_dags = paused_result.get("paused_critical_dags", [])
        missed_dags = missed_result.get("missed_dags", [])
        stuck_runs = stuck_result.get("stuck_runs", [])
        anomalies = anomaly_result.get("anomalies", [])
        total_issues = len(paused_dags) + len(missed_dags) + len(stuck_runs) + len(anomalies)
        now = pendulum.now(TZ)

        if total_issues > 0:
            title = f"{total_issues} probleme(s) detecte(s) sur les DAGs critiques"
            icon = "\u26a0\ufe0f"
            header_color = "#dc3545"
            header_color_dark = "#c82333"
            subject = f"[ALERTE] Monitoring Airflow - {total_issues} probleme(s)"
        else:
            title = "Surveillance OK - Aucun probleme"
            icon = "\u2705"
            header_color = "#28a745"
            header_color_dark = "#218838"
            subject = "[OK] Monitoring Airflow - Tout est operationnel"

        env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=True)
        template = env.get_template("monitoring_report.html")
        html = template.render(
            icon=icon,
            title=title,
            header_color=header_color,
            header_color_dark=header_color_dark,
            critical_dags_checked=len(CRITICAL_DAGS),
            total_issues=total_issues,
            paused_dags=paused_dags,
            missed_dags=missed_dags,
            stuck_runs=stuck_runs,
            anomalies=anomalies,
            timestamp=now.format("DD/MM/YYYY HH:mm:ss"),
        )

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = ALERT_FROM
        msg["To"] = ALERT_TO
        msg.attach(MIMEText(html, "html"))

        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                server.sendmail(ALERT_FROM, [ALERT_TO], msg.as_string())
            logger.info("Monitoring report sent to %s via %s:%s", ALERT_TO, SMTP_HOST, SMTP_PORT)
        except Exception:
            logger.exception("Failed to send monitoring report email")

        return {
            "total_issues": total_issues,
            "paused": paused_dags,
            "missed": missed_dags,
            "stuck": [r["dag_id"] for r in stuck_runs],
            "anomalies": [a["dag_id"] for a in anomalies],
            "timestamp": now.isoformat(),
        }

    paused = check_critical_dags_paused()
    missed = check_missed_executions()
    stuck = check_stuck_runs()
    anomalies = check_duration_anomalies()
    send_consolidated_report(paused, missed, stuck, anomalies)


airflow_monitoring()
