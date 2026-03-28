"""
Prometheus exporter for Docker container metrics via Docker socket API.

Uses background thread to collect stats asynchronously, avoiding scrape timeouts.
"""

import http.client
import json
import socket
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

DOCKER_SOCKET = "/var/run/docker.sock"
NETWORK_FILTER = "data-engineering-pipeline_pipeline-network"
STATS_COLLECT_INTERVAL = 15

_cached_metrics = ""
_cache_lock = threading.Lock()


class DockerSocketConnection(http.client.HTTPConnection):
    def __init__(self):
        super().__init__("localhost")

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(DOCKER_SOCKET)


def docker_api_get(path: str, timeout: int = 30):
    conn = DockerSocketConnection()
    conn.timeout = timeout
    conn.request("GET", path)
    resp = conn.getresponse()
    data = json.loads(resp.read().decode())
    conn.close()
    return data


def collect_container_stats(container_id: str) -> dict | None:
    """
    Collect stats for a single container.

    stream=false waits ~1s for two samples.
    """
    try:
        return docker_api_get(
            f"/containers/{container_id}/stats?stream=false",
            timeout=10,
        )
    except Exception:
        return None


def collect_metrics() -> str:
    lines = []
    now = time.time()

    lines.append("# HELP docker_container_running 1 if running, 0 otherwise")
    lines.append("# TYPE docker_container_running gauge")
    lines.append("# HELP docker_container_start_time_seconds Unix timestamp of container start")
    lines.append("# TYPE docker_container_start_time_seconds gauge")
    lines.append("# HELP docker_container_uptime_seconds Seconds since container started")
    lines.append("# TYPE docker_container_uptime_seconds gauge")
    lines.append("# HELP docker_container_restart_count Number of restarts")
    lines.append("# TYPE docker_container_restart_count gauge")
    lines.append("# HELP docker_container_cpu_usage_percent CPU usage percentage")
    lines.append("# TYPE docker_container_cpu_usage_percent gauge")
    lines.append("# HELP docker_container_memory_usage_bytes Memory usage in bytes")
    lines.append("# TYPE docker_container_memory_usage_bytes gauge")
    lines.append("# HELP docker_container_memory_limit_bytes Memory limit in bytes")
    lines.append("# TYPE docker_container_memory_limit_bytes gauge")
    lines.append("# HELP docker_container_network_rx_bytes Network bytes received")
    lines.append("# TYPE docker_container_network_rx_bytes gauge")
    lines.append("# HELP docker_container_network_tx_bytes Network bytes transmitted")
    lines.append("# TYPE docker_container_network_tx_bytes gauge")
    lines.append("# HELP docker_container_health_status Health: 1=healthy, 0.5=starting, 0=unhealthy, -1=none")
    lines.append("# TYPE docker_container_health_status gauge")

    try:
        containers = docker_api_get(
            f"/containers/json?all=true&filters="
            f"%7B%22network%22%3A%5B%22{NETWORK_FILTER}%22%5D%7D"
        )
    except Exception:
        containers = docker_api_get("/containers/json?all=true")

    for c in containers:
        name = c.get("Names", ["/unknown"])[0].lstrip("/")
        image = c.get("Image", "unknown").split(":")[0].split("/")[-1]
        state = c.get("State", "unknown")
        running = 1 if state == "running" else 0
        labels = f'name="{name}",image="{image}"'

        lines.append(f"docker_container_running{{{labels}}} {running}")

        try:
            inspect = docker_api_get(f"/containers/{c['Id']}/json")
            started_at = inspect.get("State", {}).get("StartedAt", "")
            restart_count = inspect.get("RestartCount", 0)

            health_data = inspect.get("State", {}).get("Health", {})
            if health_data:
                hs = health_data.get("Status", "none")
                health_status = {"healthy": 1, "starting": 0.5, "unhealthy": 0, "none": -1}.get(hs, -1)
            else:
                health_status = -1
            lines.append(f"docker_container_health_status{{{labels}}} {health_status}")

            if started_at and started_at != "0001-01-01T00:00:00Z":
                start_ts = datetime.fromisoformat(started_at.replace("Z", "+00:00")).timestamp()
                lines.append(f"docker_container_start_time_seconds{{{labels}}} {start_ts:.0f}")
                if running:
                    lines.append(f"docker_container_uptime_seconds{{{labels}}} {now - start_ts:.0f}")
            lines.append(f"docker_container_restart_count{{{labels}}} {restart_count}")

            if running:
                stats = collect_container_stats(c["Id"])
                if stats:
                    try:
                        cpu_delta = (
                            stats["cpu_stats"]["cpu_usage"]["total_usage"]
                            - stats["precpu_stats"]["cpu_usage"]["total_usage"]
                        )
                        sys_delta = (
                            stats["cpu_stats"]["system_cpu_usage"]
                            - stats["precpu_stats"]["system_cpu_usage"]
                        )
                        n_cpus = stats["cpu_stats"].get("online_cpus", 1)
                        cpu_pct = (cpu_delta / sys_delta * n_cpus * 100) if sys_delta > 0 else 0.0
                        lines.append(f"docker_container_cpu_usage_percent{{{labels}}} {cpu_pct:.2f}")
                    except (KeyError, ZeroDivisionError):
                        lines.append(f"docker_container_cpu_usage_percent{{{labels}}} 0")

                    mem_usage = stats.get("memory_stats", {}).get("usage", 0)
                    mem_limit = stats.get("memory_stats", {}).get("limit", 0)
                    lines.append(f"docker_container_memory_usage_bytes{{{labels}}} {mem_usage}")
                    lines.append(f"docker_container_memory_limit_bytes{{{labels}}} {mem_limit}")

                    networks = stats.get("networks", {})
                    rx_total = sum(n.get("rx_bytes", 0) for n in networks.values())
                    tx_total = sum(n.get("tx_bytes", 0) for n in networks.values())
                    lines.append(f"docker_container_network_rx_bytes{{{labels}}} {rx_total}")
                    lines.append(f"docker_container_network_tx_bytes{{{labels}}} {tx_total}")
        except Exception:
            pass

    return "\n".join(lines) + "\n"


def background_collector():
    """
    Periodically collects metrics in the background and caches the result.
    """
    global _cached_metrics
    while True:
        try:
            result = collect_metrics()
            with _cache_lock:
                _cached_metrics = result
        except Exception as e:
            print(f"Collection error: {e}")
        time.sleep(STATS_COLLECT_INTERVAL)


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            with _cache_lock:
                body = _cached_metrics.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h1>Docker Exporter</h1><p><a href='/metrics'>/metrics</a></p>")

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    print("Collecting initial metrics...")
    _cached_metrics = collect_metrics()
    print(f"Initial collection done ({len(_cached_metrics)} bytes)")

    collector_thread = threading.Thread(target=background_collector, daemon=True)
    collector_thread.start()

    server = HTTPServer(("0.0.0.0", 9417), MetricsHandler)
    print("Docker Exporter listening on :9417/metrics")
    server.serve_forever()
