import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest

from slurm_exporter.config import parse_args
from slurm_exporter.collectors.jobs import RunningJobsCollector
from slurm_exporter.collectors.nodes import NodeClusterCollector
from slurm_exporter.collectors.diagnostics import DiagnosticsCollector
from slurm_exporter.collectors.limits import AccountLimitCollector

log = logging.getLogger(__name__)


class MetricsHandler(BaseHTTPRequestHandler):
    registry: CollectorRegistry = None

    def do_GET(self):
        if self.path == "/metrics":
            output = generate_latest(self.registry)
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(output)
        elif self.path == "/healthz":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        pass  # Suppress Prometheus scrape logs.


def _fast_loop(jobs_coll: RunningJobsCollector,
               nodes_coll: NodeClusterCollector,
               interval: int) -> None:
    """Run squeue + sinfo at the configured fast interval."""
    while True:
        try:
            jobs_coll.fetch()
            gpus_alloc = jobs_coll._snapshot().get("gpus_alloc", 0)
            nodes_coll.fetch(gpus_alloc=gpus_alloc)
            log.debug("Fast collect done")
        except Exception as exc:
            log.error("Fast collect error: %s", exc, exc_info=True)
        time.sleep(interval)


def _medium_loop(collectors: list, interval: int) -> None:
    """Run sdiag + sacctmgr at the configured medium interval."""
    while True:
        try:
            for coll in collectors:
                coll.fetch()
            log.debug("Medium collect done")
        except Exception as exc:
            log.error("Medium collect error: %s", exc, exc_info=True)
        time.sleep(interval)


def main() -> None:
    cfg = parse_args()

    logging.basicConfig(
        level=getattr(logging, cfg.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    log.info("Starting SLURM Exporter on port %d", cfg.port)
    log.info("Fast interval: %ds | Medium interval: %ds", cfg.collect_interval, cfg.medium_interval)
    log.info("GPU mode: %s",
             f"fixed {cfg.gpus_per_node} GPUs/node" if cfg.gpus_per_node > 0 else "GRES parsing")

    registry = CollectorRegistry()

    jobs_coll = RunningJobsCollector(cfg)
    nodes_coll = NodeClusterCollector(cfg)
    registry.register(jobs_coll)
    registry.register(nodes_coll)

    medium_collectors = []
    if not cfg.disable_diags:
        diag_coll = DiagnosticsCollector(cfg)
        registry.register(diag_coll)
        medium_collectors.append(diag_coll)

    if not cfg.disable_account_limits:
        limit_coll = AccountLimitCollector(cfg)
        registry.register(limit_coll)
        medium_collectors.append(limit_coll)

    # Initial collection so the first scrape has data ready.
    try:
        jobs_coll.fetch()
        nodes_coll.fetch(gpus_alloc=jobs_coll._snapshot().get("gpus_alloc", 0))
        for coll in medium_collectors:
            coll.fetch()
        log.info("Initial collection complete")
    except Exception as exc:
        log.warning("Initial collection failed (will retry in background): %s", exc)

    # Start background collector threads.
    threading.Thread(
        target=_fast_loop,
        args=(jobs_coll, nodes_coll, cfg.collect_interval),
        daemon=True, name="fast-collector",
    ).start()

    if medium_collectors:
        threading.Thread(
            target=_medium_loop,
            args=(medium_collectors, cfg.medium_interval),
            daemon=True, name="medium-collector",
        ).start()

    # Start the HTTP server.
    MetricsHandler.registry = registry
    server = HTTPServer(("", cfg.port), MetricsHandler)
    log.info("Serving metrics at http://0.0.0.0:%d/metrics", cfg.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down")
        server.server_close()
