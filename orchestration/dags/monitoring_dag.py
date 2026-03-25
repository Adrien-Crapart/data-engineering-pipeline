"""
Airflow self-monitoring DAG.

Runs every hour to detect:
- Critical DAGs that are paused/disabled
- Missing DAG executions (no run within expected schedule window)
- Stuck DAG runs (running longer than expected)
- Anomaly detection on run duration vs historical average

Uses the Airflow REST API (not ORM) — required since Airflow 3.x forbids
direct database access from tasks.

Sends a single consolidated HTML email report per run via SMTP (MailHog).
"""

from __future__ import annotations

import logging
import os
import smtplib
import time
from datetime import timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pendulum
import requests
from airflow.sdk import dag, task
from jinja2 import Environment, FileSystemLoader

logger = logging.getLogger(__name__)

TZ = "Europe/Paris"

CRITICAL_DAGS = [
    "ingestion_pipeline",
    "transformation_pipeline",
]

STUCK_RUN_THRESHOLD_SECONDS = 4 * 3600
ANOMALY_DURATION_FACTOR = 3.0

SMTP_HOST = os.environ.get("SMTP_HOST", "mailhog")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "1025"))
ALERT_FROM = os.environ.get("ALERT_FROM", "monitoring@weather-pipeline.local")
ALERT_TO = os.environ.get("ALERT_TO", "admin@weather-pipeline.local")

TEMPLATE_DIR = Path(__file__).parent / "templates"

AIRFLOW_API_URL = os.environ.get(
    "AIRFLOW__CORE__EXECUTION_API_SERVER_URL",
    "http://airflow-api-server:8080/execution/",
).replace("/execution/", "")
AIRFLOW_API_USER = os.environ.get("AIRFLOW_API_USER", "airflow")
AIRFLOW_API_PASSWORD = os.environ.get("AIRFLOW_API_PASSWORD", "airflow")


def _api_get(endpoint: str, token: str, params: dict | None = None, max_retries: int = 3) -> dict:
    """Make a GET request to the Airflow REST API with retry logic."""
    headers = {"Authorization": f"Bearer {token}"}
    last_exc = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(
                f"{AIRFLOW_API_URL}{endpoint}",
                headers=headers,
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning("API GET %s attempt %d/%d failed: %s", endpoint, attempt + 1, max_retries, exc)
            if attempt < max_retries - 1:
                time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"API GET {endpoint} failed after {max_retries} attempts") from last_exc


def _on_failure_callback(context):
    ti = context.get("task_instance")
    logger.error(
        "TASK FAILED: %s | execution_date=%s",
        getattr(ti, "task_id", "unknown"),
        context.get("execution_date"),
    )


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
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
    def get_api_token() -> str:
        """Authenticate once and return the JWT token for downstream tasks."""
        last_exc = None
        for attempt in range(5):
            try:
                resp = requests.post(
                    f"{AIRFLOW_API_URL}/auth/token",
                    json={"username": AIRFLOW_API_USER, "password": AIRFLOW_API_PASSWORD},
                    timeout=15,
                )
                resp.raise_for_status()
                return resp.json()["access_token"]
            except (requests.RequestException, KeyError) as exc:
                last_exc = exc
                logger.warning("Auth attempt %d/5 failed: %s", attempt + 1, exc)
                if attempt < 4:
                    time.sleep(3 * (attempt + 1))
        raise RuntimeError("Failed to get Airflow API token after 5 attempts") from last_exc

    @task
    def check_critical_dags_paused(token: str) -> dict:
        paused = []
        for dag_id in CRITICAL_DAGS:
            try:
                dag_info = _api_get(f"/api/v2/dags/{dag_id}", token)
                if dag_info.get("is_paused"):
                    paused.append(dag_id)
            except RuntimeError:
                paused.append(f"{dag_id} (NOT FOUND)")
        return {"paused_critical_dags": paused, "total_checked": len(CRITICAL_DAGS)}

    @task
    def check_missed_executions(token: str) -> dict:
        now = pendulum.now(TZ)
        window = now.subtract(hours=24).in_tz("UTC").isoformat()
        missed = []
        for dag_id in CRITICAL_DAGS:
            try:
                dag_info = _api_get(f"/api/v2/dags/{dag_id}", token)
            except RuntimeError:
                continue
            if not dag_info.get("timetable_summary"):
                continue
            try:
                runs = _api_get(
                    f"/api/v2/dags/{dag_id}/dagRuns",
                    token,
                    params={"start_date_gte": window, "limit": 1},
                )
                if runs.get("total_entries", 0) == 0:
                    missed.append(dag_id)
            except RuntimeError:
                logger.warning("Could not check runs for %s", dag_id)
        return {"missed_dags": missed}

    @task
    def check_stuck_runs(token: str) -> dict:
        now = pendulum.now("UTC")
        stuck = []
        try:
            data = _api_get("/api/v2/dagRuns", token, params={"state": "running", "limit": 100})
            for run in data.get("dag_runs", []):
                start = pendulum.parse(run["start_date"])
                elapsed = now - start
                if elapsed.total_seconds() > STUCK_RUN_THRESHOLD_SECONDS:
                    stuck.append({
                        "dag_id": run["dag_id"],
                        "run_id": run["dag_run_id"],
                        "duration": str(elapsed),
                    })
        except RuntimeError:
            logger.warning("Could not check stuck runs")
        return {"stuck_runs": stuck}

    @task
    def check_duration_anomalies(token: str) -> dict:
        anomalies = []
        for dag_id in CRITICAL_DAGS:
            try:
                runs_data = _api_get(
                    f"/api/v2/dags/{dag_id}/dagRuns",
                    token,
                    params={"state": "success", "order_by": "-execution_date", "limit": 20},
                )
            except RuntimeError:
                continue
            durations = []
            for r in runs_data.get("dag_runs", []):
                if r.get("start_date") and r.get("end_date"):
                    start = pendulum.parse(r["start_date"])
                    end = pendulum.parse(r["end_date"])
                    durations.append((end - start).total_seconds())
            if len(durations) < 2:
                continue
            avg_duration = sum(durations) / len(durations)
            latest_duration = durations[0]
            if latest_duration > avg_duration * ANOMALY_DURATION_FACTOR:
                anomalies.append({
                    "dag_id": dag_id,
                    "actual_duration": f"{latest_duration:.0f}s",
                    "avg_duration": f"{avg_duration:.0f}s",
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

    token = get_api_token()
    paused = check_critical_dags_paused(token)
    missed = check_missed_executions(token)
    stuck = check_stuck_runs(token)
    anomalies = check_duration_anomalies(token)
    send_consolidated_report(paused, missed, stuck, anomalies)


airflow_monitoring()
