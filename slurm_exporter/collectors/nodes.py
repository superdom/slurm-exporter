import logging
import time
from typing import Iterator

from prometheus_client.core import GaugeMetricFamily

from slurm_exporter.base import SlurmBaseCollector
from slurm_exporter.config import Config
from slurm_exporter.utils import parse_gres_gpu_count, run_cmd

log = logging.getLogger(__name__)

# sinfo -O field order.
# StateCompact | Memory(MB) | NodeHost | CPUsLoad | Partition | FreeMem(MB) | CPUsState(A/I/O/T) | AllocMem(MB)
_SINFO_FMT = (
    "StateCompact:12|,Memory:15|,NodeHost:30|,CPUsLoad:12|"
    ",Partition:15|,FreeMem:15|,CPUsState:15|,AllocMem:15"
)
_SINFO_FIELDS = 8
_MB = 1_000_000  # Go exporter behavior: MB -> bytes (SI).

# mix/mixed nodes have some CPUs allocated, so count them as allocated.
_STATE_ALLOC = {"alloc", "allocated", "mix", "mixed"}
_STATE_IDLE = {"idle"}
_STATE_DOWN = {"down"}
_STATE_DRAIN = {"drain", "draining", "drng"}


class NodeClusterCollector(SlurmBaseCollector):
    """Collect node, partition, and cluster metrics via sinfo -h -O."""

    def __init__(self, config: Config) -> None:
        super().__init__("node")
        self._config = config

    # ------------------------------------------------------------------
    # Collection
    # ------------------------------------------------------------------

    def fetch(self, gpus_alloc: int = 0) -> None:
        start = self._start_timer()
        cfg = self._config

        out = run_cmd([cfg.sinfo_path, "-h", "-O", _SINFO_FMT])
        if out is None:
            self._record_error()
            log.warning("sinfo command failed; retaining previous metrics")
            self._stop_timer(start)
            return

        # Separate call for backward-compatible metrics: slurm_nodes_* and slurm_gpus_total.
        # The -O Gres field can vary by SLURM version/configuration, so keep this path.
        gres_out = run_cmd([cfg.sinfo_path, "--noheader", "-o", "%n|%t|%G"])

        data = self._parse(out, gres_out or "", gpus_alloc, cfg)
        self._update(data)
        self._stop_timer(start)

    def _parse(self, out: str, gres_out: str, gpus_alloc: int, cfg: Config) -> dict:
        partitions: dict = {}
        seen_nodes: set = set()
        cluster = {
            "cpus_total": 0, "cpus_idle": 0, "cpu_load": 0.0,
            "mem_real": 0, "mem_free": 0, "mem_alloc": 0,
        }

        for line in out.strip().split("\n"):
            fields = [f.strip() for f in line.split("|")]
            if len(fields) < _SINFO_FIELDS:
                continue

            state_raw, mem_raw, node_host, cpu_load_raw, partition, \
                free_mem_raw, cpus_state_raw, alloc_mem_raw = fields[:_SINFO_FIELDS]

            state = state_raw.lower().rstrip("*")
            partition = partition.rstrip("*")

            # CPUsState: "16/48/0/64" → alloc/idle/other/total
            try:
                cpu_parts = cpus_state_raw.split("/")
                c_alloc = int(cpu_parts[0])
                c_idle = int(cpu_parts[1])
                c_total = int(cpu_parts[3]) if len(cpu_parts) >= 4 else c_alloc + c_idle
            except (ValueError, IndexError):
                c_alloc = c_idle = c_total = 0

            try:
                mem_real = int(mem_raw) * _MB
            except ValueError:
                mem_real = 0
            try:
                mem_free = int(free_mem_raw) * _MB
            except ValueError:
                mem_free = 0
            try:
                mem_alloc = int(alloc_mem_raw) * _MB
            except ValueError:
                mem_alloc = 0
            try:
                cpu_load = float(cpu_load_raw) if cpu_load_raw not in ("N/A", "") else 0.0
            except ValueError:
                cpu_load = 0.0

            # Partition aggregation
            if partition not in partitions:
                partitions[partition] = {
                    "total_cpus": 0, "idle_cpus": 0, "cpu_load": 0.0,
                    "real_mem": 0, "free_mem": 0,
                    "state_cpus": {}, "state_mem": {}, "state_nodes": {},
                }
            p = partitions[partition]
            p["total_cpus"] += c_total
            p["idle_cpus"] += c_idle
            p["cpu_load"] += cpu_load
            p["real_mem"] += mem_real
            p["free_mem"] += mem_free
            p["state_cpus"][state] = p["state_cpus"].get(state, 0) + c_alloc
            p["state_mem"][state] = p["state_mem"].get(state, 0) + mem_alloc
            p["state_nodes"][state] = p["state_nodes"].get(state, 0) + 1

            # Cluster totals with duplicate nodes removed.
            if node_host not in seen_nodes:
                seen_nodes.add(node_host)
                cluster["cpus_total"] += c_total
                cluster["cpus_idle"] += c_idle
                cluster["cpu_load"] += cpu_load
                cluster["mem_real"] += mem_real
                cluster["mem_free"] += mem_free
                cluster["mem_alloc"] += mem_alloc

        # Backward-compatible simple node states (slurm_nodes_*, slurm_gpus_total).
        # Include mix/drng in the correct state buckets.
        simple = {"idle": 0, "alloc": 0, "down": 0, "drain": 0, "gpus_total": 0}
        for line in gres_out.strip().split("\n"):
            parts = line.strip().split("|")
            if len(parts) < 3:
                continue
            _, st, gres = parts[0], parts[1].lower().rstrip("*"), parts[2]

            if cfg.gpus_per_node > 0:
                if "gpu" in gres.lower():
                    simple["gpus_total"] += cfg.gpus_per_node
            else:
                simple["gpus_total"] += parse_gres_gpu_count(gres)

            if st in _STATE_IDLE:
                simple["idle"] += 1
            elif st in _STATE_ALLOC:
                simple["alloc"] += 1
            elif st in _STATE_DOWN or st.startswith("down"):
                simple["down"] += 1
            elif st in _STATE_DRAIN:
                simple["drain"] += 1

        return {
            "cluster": cluster,
            "partitions": partitions,
            "simple": simple,
            "gpus_alloc": gpus_alloc,
            "last_update_ts": time.time(),
        }

    # ------------------------------------------------------------------
    # Metric exposition
    # ------------------------------------------------------------------

    def collect(self) -> Iterator:
        data = self._snapshot()
        cluster = data.get("cluster", {})
        partitions = data.get("partitions", {})
        simple = data.get("simple", {})
        gpus_alloc = data.get("gpus_alloc", 0)

        # Backward-compatible existing metrics.
        yield self._scalar("slurm_nodes_idle", "Number of idle SLURM nodes", simple.get("idle", 0))
        yield self._scalar("slurm_nodes_alloc", "Number of allocated SLURM nodes", simple.get("alloc", 0))
        yield self._scalar("slurm_nodes_down", "Number of down SLURM nodes", simple.get("down", 0))
        yield self._scalar("slurm_nodes_drain", "Number of draining SLURM nodes", simple.get("drain", 0))
        yield self._scalar("slurm_gpus_total", "Total GPUs in cluster", simple.get("gpus_total", 0))
        yield self._scalar("slurm_gpus_alloc", "Allocated GPUs", gpus_alloc)

        # Cluster-wide CPU and memory metrics.
        yield self._scalar("slurm_cpus_total", "Total CPUs across all nodes", cluster.get("cpus_total", 0))
        yield self._scalar("slurm_cpus_idle", "Total idle CPUs across all nodes", cluster.get("cpus_idle", 0))
        yield self._scalar("slurm_cpu_load", "Total CPU load across all nodes", cluster.get("cpu_load", 0))
        yield self._scalar("slurm_mem_real", "Total real memory in bytes", cluster.get("mem_real", 0))
        yield self._scalar("slurm_mem_free", "Total free memory in bytes", cluster.get("mem_free", 0))
        yield self._scalar("slurm_mem_alloc", "Total allocated memory in bytes", cluster.get("mem_alloc", 0))

        # Per-partition metrics.
        g_ptotal = GaugeMetricFamily("slurm_partition_total_cpus", "Total CPUs per partition", labels=["partition"])
        g_pidle = GaugeMetricFamily("slurm_partition_idle_cpus", "Idle CPUs per partition", labels=["partition"])
        g_pload = GaugeMetricFamily("slurm_partition_cpu_load", "CPU load per partition", labels=["partition"])
        g_pmem_real = GaugeMetricFamily("slurm_partition_real_mem", "Real memory per partition (bytes)", labels=["partition"])
        g_pmem_free = GaugeMetricFamily("slurm_partition_free_mem", "Free memory per partition (bytes)", labels=["partition"])
        g_pmem_alloc = GaugeMetricFamily("slurm_partition_alloc_mem", "Allocated memory per partition per state (bytes)", labels=["partition", "state"])
        g_pcpus_alloc = GaugeMetricFamily("slurm_partition_alloc_cpus", "Allocated CPUs per partition per state", labels=["partition", "state"])
        g_pnodes = GaugeMetricFamily("slurm_partition_node_count", "Node count per partition per state", labels=["partition", "state"])

        for part, p in partitions.items():
            g_ptotal.add_metric([part], p["total_cpus"])
            g_pidle.add_metric([part], p["idle_cpus"])
            g_pload.add_metric([part], p["cpu_load"])
            g_pmem_real.add_metric([part], p["real_mem"])
            g_pmem_free.add_metric([part], p["free_mem"])
            for st, val in p["state_mem"].items():
                g_pmem_alloc.add_metric([part, st], val)
            for st, val in p["state_cpus"].items():
                g_pcpus_alloc.add_metric([part, st], val)
            for st, val in p["state_nodes"].items():
                g_pnodes.add_metric([part, st], val)

        yield g_ptotal
        yield g_pidle
        yield g_pload
        yield g_pmem_real
        yield g_pmem_free
        yield g_pmem_alloc
        yield g_pcpus_alloc
        yield g_pnodes

        # sinfo health metrics. Stale threshold is collect_interval * 4.
        last_ts = data.get("last_update_ts", 0.0)
        age = time.time() - last_ts if last_ts else float("inf")
        stale_threshold = self._config.collect_interval * 4
        yield self._scalar("slurm_sinfo_last_update", "Unix timestamp of last successful sinfo collection", last_ts)
        yield self._scalar("slurm_sinfo_stale", "1 if sinfo data is stale (older than 4× collect-interval)", 1.0 if age > stale_threshold else 0.0)

        yield from self._health_metrics("slurm_node")

    @staticmethod
    def _scalar(name: str, doc: str, value: float) -> GaugeMetricFamily:
        g = GaugeMetricFamily(name, doc)
        g.add_metric([], value)
        return g
