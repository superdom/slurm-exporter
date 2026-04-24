from pathlib import Path

from slurm_exporter.collectors.nodes import NodeClusterCollector, _MB
from slurm_exporter.config import Config

SINFO_FIXTURE = (Path(__file__).parent / "fixtures" / "sinfo.txt").read_text()
GRES_FIXTURE = (Path(__file__).parent / "fixtures" / "sinfo_gres.txt").read_text()

_BASE_CONFIG = Config(
    port=9410,
    collect_interval=15,
    medium_interval=300,
    log_level="info",
    gpus_per_node=0,
    disable_diags=False,
    disable_account_limits=False,
    squeue_path="squeue",
    sinfo_path="sinfo",
    scontrol_path="scontrol",
    sdiag_path="sdiag",
    sacctmgr_path="sacctmgr",
)


def _parse(gpus_alloc: int = 4) -> dict:
    coll = NodeClusterCollector(_BASE_CONFIG)
    return coll._parse(SINFO_FIXTURE, GRES_FIXTURE, gpus_alloc=gpus_alloc, cfg=_BASE_CONFIG)


class TestNodesParse:
    def test_partition_gpu_present(self):
        data = _parse()
        assert "gpu" in data["partitions"]
        assert "cpu" in data["partitions"]

    def test_partition_total_cpus(self):
        data = _parse()
        # gpu partition: node01 8/0/0/8 → total=8
        assert data["partitions"]["gpu"]["total_cpus"] == 8

    def test_partition_idle_cpus(self):
        data = _parse()
        # cpu partition: node02(4/4/0/8) + node03(0/8/0/8) + node04(0/0/8/8)
        # + node01 duplicate row → idle=4+8+0+0=12
        assert data["partitions"]["cpu"]["idle_cpus"] == 12

    def test_cluster_dedup(self):
        # node01 appears in both gpu and cpu partitions → cluster counts it once
        # node01: 8, node02: 8, node03: 8, node04: 8 → 32 total
        data = _parse()
        assert data["cluster"]["cpus_total"] == 32

    def test_cluster_mem_real(self):
        data = _parse()
        # node01: 1048576 MB, node02: 524288 MB, node03: 524288 MB, node04: 524288 MB
        expected = (1048576 + 524288 + 524288 + 524288) * _MB
        assert data["cluster"]["mem_real"] == expected

    def test_simple_node_states(self):
        data = _parse()
        simple = data["simple"]
        # sinfo_gres.txt: node01=alloc, node02=mix (→alloc), node03=idle, node04=drain
        assert simple["alloc"] == 2  # alloc(node01) + mix(node02)
        assert simple["idle"] == 1
        assert simple["drain"] == 1

    def test_simple_gpu_total(self):
        data = _parse()
        # sinfo_gres.txt: node01 has gpu:a100:4
        assert data["simple"]["gpus_total"] == 4

    def test_gpus_alloc_passthrough(self):
        data = _parse(gpus_alloc=4)
        assert data["gpus_alloc"] == 4

    def test_last_update_ts_set(self):
        data = _parse()
        assert data["last_update_ts"] > 0


class TestNodesCollect:
    def test_collect_yields_metrics(self):
        coll = NodeClusterCollector(_BASE_CONFIG)
        data = coll._parse(SINFO_FIXTURE, GRES_FIXTURE, gpus_alloc=4, cfg=_BASE_CONFIG)
        coll._update(data)

        metrics = list(coll.collect())
        names = {m.name for m in metrics}
        assert "slurm_nodes_idle" in names
        assert "slurm_nodes_alloc" in names
        assert "slurm_cpus_total" in names
        assert "slurm_partition_total_cpus" in names
        assert "slurm_sinfo_last_update" in names
        assert "slurm_sinfo_stale" in names

    def test_sinfo_stale_zero_when_fresh(self):
        coll = NodeClusterCollector(_BASE_CONFIG)
        data = coll._parse(SINFO_FIXTURE, GRES_FIXTURE, gpus_alloc=0, cfg=_BASE_CONFIG)
        coll._update(data)

        metrics = {m.name: m for m in coll.collect()}
        stale_samples = metrics["slurm_sinfo_stale"].samples
        assert stale_samples[0].value == 0.0

    def test_stale_threshold_uses_collect_interval(self):
        # collect_interval=15 → threshold=60s. 방금 수집한 데이터는 stale이 아니어야 함
        coll = NodeClusterCollector(_BASE_CONFIG)
        data = coll._parse(SINFO_FIXTURE, GRES_FIXTURE, gpus_alloc=0, cfg=_BASE_CONFIG)
        coll._update(data)
        metrics = {m.name: m for m in coll.collect()}
        assert metrics["slurm_sinfo_stale"].samples[0].value == 0.0
