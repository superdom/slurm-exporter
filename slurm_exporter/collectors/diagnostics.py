import json
import logging
from typing import Iterator

from prometheus_client.core import GaugeMetricFamily

from slurm_exporter.base import SlurmBaseCollector
from slurm_exporter.config import Config
from slurm_exporter.utils import coerce_int, run_cmd

log = logging.getLogger(__name__)


class DiagnosticsCollector(SlurmBaseCollector):
    """Collect scheduler diagnostics via sdiag --json."""

    def __init__(self, config: Config) -> None:
        super().__init__("diag")
        self._config = config

    # ------------------------------------------------------------------
    # Collection
    # ------------------------------------------------------------------

    def fetch(self) -> None:
        start = self._start_timer()
        cfg = self._config

        out = run_cmd([cfg.sdiag_path, "--json"], timeout=30)
        if out:
            data = self._parse_json(out)
        else:
            # Fall back to text output when JSON collection fails.
            out_text = run_cmd([cfg.sdiag_path], timeout=30)
            if out_text is None:
                # Text fallback also failed; keep the previous snapshot.
                self._record_error()
                log.warning("sdiag both json and text calls failed; retaining previous metrics")
                self._stop_timer(start)
                return
            data = self._parse_text(out_text)

        if not data:
            self._record_error()
            self._stop_timer(start)
            return

        self._update(data)
        self._stop_timer(start)

    def _parse_json(self, out: str) -> dict:
        try:
            raw = json.loads(out)
        except json.JSONDecodeError as e:
            log.warning("sdiag JSON parse error: %s", e)
            self._record_error()
            return {}

        stats = raw.get("statistics", {})

        # Scalar values
        scalars = {
            "thread_count": coerce_int(stats.get("server_thread_count", 0)),
            "dbd_queue_size": coerce_int(stats.get("dbd_agent_queue_size", 0)),
            "bf_backfilled_jobs": coerce_int(stats.get("bf_backfilled_jobs", 0)),
            "bf_cycle_count": coerce_int(stats.get("bf_cycle_sum", 0)),
            "bf_last_depth": coerce_int(stats.get("bf_last_depth", 0)),
            "bf_last_depth_try": coerce_int(stats.get("bf_last_depth_try", 0)),
        }

        # RPC statistics by message type
        rpc_by_type = {}
        for entry in stats.get("rpcs_by_message_type", []):
            msg_type = entry.get("message_type", "unknown")
            rpc_by_type[msg_type] = {
                "count": coerce_int(entry.get("count", 0)),
                "avg_time": coerce_int(entry.get("average_time", 0)),
                "total_time": coerce_int(entry.get("total_time", 0)),
            }

        return {"scalars": scalars, "rpc_by_type": rpc_by_type}

    def _parse_text(self, out: str) -> dict:
        """Parse scalar values from sdiag text output for non-JSON environments."""
        import re
        scalars = {
            "thread_count": 0, "dbd_queue_size": 0,
            "bf_backfilled_jobs": 0, "bf_cycle_count": 0,
            "bf_last_depth": 0, "bf_last_depth_try": 0,
        }
        patterns = {
            "thread_count": r"Server thread count:\s*(\d+)",
            "dbd_queue_size": r"Agent queue size:\s*(\d+)",
            "bf_backfilled_jobs": r"Total backfilled jobs.*?:\s*(\d+)",
            "bf_cycle_count": r"Total cycles:\s*(\d+)",
            "bf_last_depth": r"Last depth cycle:\s*(\d+)",
            "bf_last_depth_try": r"Last depth cycle.*?try.*?:\s*(\d+)",
        }
        for key, pattern in patterns.items():
            m = re.search(pattern, out, re.IGNORECASE)
            if m:
                scalars[key] = int(m.group(1))
        return {"scalars": scalars, "rpc_by_type": {}}

    # ------------------------------------------------------------------
    # Metric exposition
    # ------------------------------------------------------------------

    def collect(self) -> Iterator:
        data = self._snapshot()
        scalars = data.get("scalars", {})
        rpc_by_type = data.get("rpc_by_type", {})

        yield self._scalar("slurm_daemon_thread_count",
                           "SLURM daemon thread count",
                           scalars.get("thread_count", 0))
        yield self._scalar("slurm_dbd_agent_queue_size",
                           "SlurmDBD agent queue size",
                           scalars.get("dbd_queue_size", 0))
        yield self._scalar("slurm_backfill_job_count",
                           "Jobs started via backfilling since last SLURM start",
                           scalars.get("bf_backfilled_jobs", 0))
        yield self._scalar("slurm_backfill_cycle_count",
                           "Backfill scheduling cycles since last reset",
                           scalars.get("bf_cycle_count", 0))
        yield self._scalar("slurm_backfill_last_depth",
                           "Jobs processed in last backfill cycle",
                           scalars.get("bf_last_depth", 0))
        yield self._scalar("slurm_backfill_last_depth_try_sched",
                           "Jobs with chance to start in last backfill cycle",
                           scalars.get("bf_last_depth_try", 0))

        g_count = GaugeMetricFamily("slurm_rpc_msg_type_count",
                                    "SLURM RPC count per message type",
                                    labels=["type"])
        g_avg = GaugeMetricFamily("slurm_rpc_msg_type_avg_time",
                                  "SLURM RPC avg time per message type (ms)",
                                  labels=["type"])
        g_total = GaugeMetricFamily("slurm_rpc_msg_type_total_time",
                                    "SLURM RPC total time per message type (ms)",
                                    labels=["type"])
        for msg_type, vals in rpc_by_type.items():
            g_count.add_metric([msg_type], vals["count"])
            g_avg.add_metric([msg_type], vals["avg_time"])
            g_total.add_metric([msg_type], vals["total_time"])

        yield g_count
        yield g_avg
        yield g_total

        yield from self._health_metrics("slurm_diag")

    @staticmethod
    def _scalar(name: str, doc: str, value: float) -> GaugeMetricFamily:
        g = GaugeMetricFamily(name, doc)
        g.add_metric([], value)
        return g
