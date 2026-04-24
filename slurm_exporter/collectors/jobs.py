import logging
import time
from typing import Dict, Iterator

from prometheus_client.core import GaugeMetricFamily

from slurm_exporter.base import SlurmBaseCollector
from slurm_exporter.config import Config
from slurm_exporter.utils import (
    expand_nodelist,
    parse_epoch,
    parse_gres_gpu_count,
    parse_mem_to_bytes,
    run_cmd,
)

log = logging.getLogger(__name__)

# squeue output field order for a single --state=ALL call.
# %i=JobID %j=Name %u=User %P=Partition %T=State %N=NodeList %D=NumNodes
# %C=NumCPUs %b=GRES %a=Account %S=StartTime %V=SubmitTime %m=MinMemory %R=Reason
_SQUEUE_FMT = "%i|%j|%u|%P|%T|%N|%D|%C|%b|%a|%S|%V|%m|%R"
_SQUEUE_FIELDS = 14


class RunningJobsCollector(SlurmBaseCollector):
    """Collect job and aggregate metrics with a single squeue --state=ALL call."""

    def __init__(self, config: Config) -> None:
        super().__init__("job")
        self._config = config

    # ------------------------------------------------------------------
    # Collection
    # ------------------------------------------------------------------

    def fetch(self) -> None:
        start = self._start_timer()
        cfg = self._config

        out = run_cmd([
            cfg.squeue_path, "--noheader", "--state=ALL",
            "-o", _SQUEUE_FMT,
        ])
        if out is None:
            self._record_error()
            log.warning("squeue command failed; retaining previous metrics")
            self._stop_timer(start)
            return

        # out == "" means the queue is empty — that's valid, parse it as-is
        data = self._parse(out, cfg)
        self._update(data)
        self._stop_timer(start)

    def _parse(self, out: str, cfg: Config) -> dict:
        now_ts = time.time()

        per_job: Dict[str, dict] = {}
        # aggregate dicts keyed by (dim, state) → {count, cpu, mem_bytes}
        user_state: Dict[tuple, dict] = {}
        partition_state: Dict[tuple, dict] = {}
        account_state: Dict[tuple, dict] = {}
        pending_reasons: Dict[str, int] = {}
        gpus_alloc: int = 0

        for line in out.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < _SQUEUE_FIELDS:
                log.debug("Skipping malformed squeue line: %s", line)
                continue

            (job_id, job_name, user, partition, state, nodelist,
             num_nodes, num_cpus, gres, account,
             start_time_str, submit_time_str, min_mem, reason) = parts[:_SQUEUE_FIELDS]

            state_upper = state.upper()

            # --- GPU count ---
            if cfg.gpus_per_node > 0:
                try:
                    gpu_count = int(num_nodes) * cfg.gpus_per_node
                except ValueError:
                    gpu_count = 0
            else:
                gpu_count = parse_gres_gpu_count(gres)

            # --- CPU / memory ---
            try:
                n_nodes = int(num_nodes)
                n_cpus = int(num_cpus)
            except ValueError:
                n_nodes, n_cpus = 0, 0
            mem_bytes = parse_mem_to_bytes(min_mem, si=False)

            # --- Time ---
            start_epoch = parse_epoch(start_time_str)
            submit_epoch = parse_epoch(submit_time_str)
            elapsed = 0
            wait = 0
            if start_epoch:
                elapsed = int(now_ts) - start_epoch
                if submit_epoch:
                    wait = max(0, start_epoch - submit_epoch)

            # --- Aggregates for all states ---
            def _agg(d: dict, key: tuple) -> None:
                if key not in d:
                    d[key] = {"count": 0, "cpu": 0, "mem_bytes": 0}
                d[key]["count"] += 1
                d[key]["cpu"] += n_cpus
                d[key]["mem_bytes"] += mem_bytes

            _agg(user_state, (user, state_upper))
            _agg(partition_state, (partition, state_upper))
            _agg(account_state, (account, state_upper))

            if state_upper == "RUNNING":
                gpus_alloc += gpu_count
            elif state_upper == "PENDING":
                r = reason.strip() or "Unknown"
                pending_reasons[r] = pending_reasons.get(r, 0) + 1

            # --- Per-running-job metrics ---
            if state_upper == "RUNNING":
                nodes = expand_nodelist(
                    nodelist,
                    scontrol_path=cfg.scontrol_path,
                )
                per_job[job_id] = {
                    "job_name": job_name,
                    "user": user,
                    "partition": partition,
                    "account": account,
                    "nodelist": nodelist,
                    "nodes": nodes,
                    "n_nodes": n_nodes,
                    "n_cpus": n_cpus,
                    "gpu_count": gpu_count,
                    "mem_bytes": mem_bytes,
                    "elapsed": elapsed,
                    "start_epoch": start_epoch or 0,
                    "wait": wait,
                }

        return {
            "per_job": per_job,
            "user_state": user_state,
            "partition_state": partition_state,
            "account_state": account_state,
            "pending_reasons": pending_reasons,
            "gpus_alloc": gpus_alloc,
        }

    # ------------------------------------------------------------------
    # Metric exposition
    # ------------------------------------------------------------------

    def collect(self) -> Iterator:
        data = self._snapshot()
        per_job = data.get("per_job", {})
        user_state = data.get("user_state", {})
        partition_state = data.get("partition_state", {})
        account_state = data.get("account_state", {})
        pending_reasons = data.get("pending_reasons", {})

        job_labels = ["job_id", "job_name", "user", "partition", "account"]
        node_alloc_labels = ["job_id", "job_name", "user", "partition", "node", "account"]

        g_node_alloc = GaugeMetricFamily(
            "slurm_job_node_alloc",
            "1 if node is allocated to job (key metric for Grafana dynamic filtering)",
            labels=node_alloc_labels,
        )
        g_nodes_count = GaugeMetricFamily(
            "slurm_job_nodes_count",
            "Number of nodes allocated to job",
            labels=job_labels + ["nodelist"],
        )
        g_gpus = GaugeMetricFamily(
            "slurm_job_gpus_total",
            "Total GPUs allocated to running job",
            labels=job_labels,
        )
        g_cpus = GaugeMetricFamily(
            "slurm_job_cpus_total",
            "Total CPUs allocated to running job",
            labels=job_labels,
        )
        g_mem = GaugeMetricFamily(
            "slurm_job_alloc_mem",
            "Memory allocated to running job in bytes",
            labels=job_labels,
        )
        g_elapsed = GaugeMetricFamily(
            "slurm_job_elapsed_seconds",
            "Running job elapsed time in seconds",
            labels=job_labels,
        )
        g_start = GaugeMetricFamily(
            "slurm_job_start_time",
            "Job start time (Unix epoch)",
            labels=["job_id"],
        )
        g_wait = GaugeMetricFamily(
            "slurm_job_wait_seconds",
            "Job wait time from submit to start in seconds",
            labels=["job_id"],
        )

        for job_id, j in per_job.items():
            lv = [job_id, j["job_name"], j["user"], j["partition"], j["account"]]

            for node in j["nodes"]:
                g_node_alloc.add_metric(
                    [job_id, j["job_name"], j["user"], j["partition"], node, j["account"]], 1
                )

            g_nodes_count.add_metric(lv + [j["nodelist"]], j["n_nodes"])
            g_gpus.add_metric(lv, j["gpu_count"])
            g_cpus.add_metric(lv, j["n_cpus"])
            g_mem.add_metric(lv, j["mem_bytes"])
            g_elapsed.add_metric(lv, j["elapsed"])
            g_start.add_metric([job_id], j["start_epoch"])
            g_wait.add_metric([job_id], j["wait"])

        yield g_node_alloc
        yield g_nodes_count
        yield g_gpus
        yield g_cpus
        yield g_mem
        yield g_elapsed
        yield g_start
        yield g_wait

        # --- Partition aggregates by state ---
        g_part_total = GaugeMetricFamily(
            "slurm_queue_jobs_running",
            "Number of running jobs per partition",
            labels=["partition"],
        )
        g_pending_total = GaugeMetricFamily(
            "slurm_queue_jobs_pending",
            "Number of pending jobs per partition",
            labels=["partition"],
        )
        g_part_state = GaugeMetricFamily(
            "slurm_partition_job_state_total",
            "Job count per partition and state",
            labels=["partition", "state"],
        )
        g_part_cpu = GaugeMetricFamily(
            "slurm_partition_job_state_cpu_alloc",
            "CPUs allocated per partition and state",
            labels=["partition", "state"],
        )
        g_part_mem = GaugeMetricFamily(
            "slurm_partition_job_state_mem_alloc",
            "Memory allocated per partition and state in bytes",
            labels=["partition", "state"],
        )
        for (part, st), v in partition_state.items():
            g_part_state.add_metric([part, st], v["count"])
            g_part_cpu.add_metric([part, st], v["cpu"])
            g_part_mem.add_metric([part, st], v["mem_bytes"])
            if st == "RUNNING":
                g_part_total.add_metric([part], v["count"])
            elif st == "PENDING":
                g_pending_total.add_metric([part], v["count"])
        yield g_part_total
        yield g_pending_total
        yield g_part_state
        yield g_part_cpu
        yield g_part_mem

        # --- User aggregates ---
        g_user_total = GaugeMetricFamily(
            "slurm_user_state_total",
            "Job count per user and state",
            labels=["username", "state"],
        )
        g_user_cpu = GaugeMetricFamily(
            "slurm_user_cpu_alloc",
            "CPUs allocated per user and state",
            labels=["username", "state"],
        )
        g_user_mem = GaugeMetricFamily(
            "slurm_user_mem_alloc",
            "Memory allocated per user and state in bytes",
            labels=["username", "state"],
        )
        for (user, st), v in user_state.items():
            g_user_total.add_metric([user, st], v["count"])
            g_user_cpu.add_metric([user, st], v["cpu"])
            g_user_mem.add_metric([user, st], v["mem_bytes"])
        yield g_user_total
        yield g_user_cpu
        yield g_user_mem

        # --- Account aggregates ---
        g_acct_total = GaugeMetricFamily(
            "slurm_account_job_state_total",
            "Job count per account and state",
            labels=["account", "state"],
        )
        g_acct_cpu = GaugeMetricFamily(
            "slurm_account_job_state_cpu_alloc",
            "CPUs allocated per account and state",
            labels=["account", "state"],
        )
        g_acct_mem = GaugeMetricFamily(
            "slurm_account_job_state_mem_alloc",
            "Memory allocated per account and state in bytes",
            labels=["account", "state"],
        )
        for (acct, st), v in account_state.items():
            g_acct_total.add_metric([acct, st], v["count"])
            g_acct_cpu.add_metric([acct, st], v["cpu"])
            g_acct_mem.add_metric([acct, st], v["mem_bytes"])
        yield g_acct_total
        yield g_acct_cpu
        yield g_acct_mem

        # --- Pending reasons ---
        g_reason = GaugeMetricFamily(
            "slurm_pending_reason_total",
            "Number of pending jobs per reason",
            labels=["reason"],
        )
        for reason, count in pending_reasons.items():
            g_reason.add_metric([reason], count)
        yield g_reason

        yield from self._health_metrics("slurm_job")
