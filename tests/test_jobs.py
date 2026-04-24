import time
from pathlib import Path
from typing import Dict, Optional
from unittest.mock import patch

from slurm_exporter.collectors.jobs import RunningJobsCollector
from slurm_exporter.config import Config

FIXTURE = (Path(__file__).parent / "fixtures" / "squeue.txt").read_text()

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


def _make_collector() -> RunningJobsCollector:
    return RunningJobsCollector(_BASE_CONFIG)


def _parse_fixture(nodelist_map: Optional[Dict[str, list]] = None) -> dict:
    coll = _make_collector()
    nodes_map = nodelist_map or {"node01": ["node01"], "node02": ["node02"]}

    def fake_expand(nodelist, scontrol_path, ttl=300):
        return nodes_map.get(nodelist, [nodelist])

    with patch("slurm_exporter.collectors.jobs.expand_nodelist", side_effect=fake_expand):
        return coll._parse(FIXTURE, _BASE_CONFIG)


class TestJobsParse:
    def test_running_jobs_present(self):
        data = _parse_fixture()
        assert "1001" in data["per_job"]
        assert "1002" in data["per_job"]

    def test_pending_jobs_not_in_per_job(self):
        data = _parse_fixture()
        assert "1003" not in data["per_job"]
        assert "1004" not in data["per_job"]

    def test_running_job_fields(self):
        data = _parse_fixture()
        j = data["per_job"]["1001"]
        assert j["user"] == "alice"
        assert j["partition"] == "gpu"
        assert j["account"] == "ml-team"
        assert j["job_name"] == "training"
        assert j["n_cpus"] == 8
        assert j["gpu_count"] == 4  # gpu:a100:4(IDX:0-3) → 4
        assert j["mem_bytes"] == 256 * 1024 ** 3

    def test_no_gpu_job(self):
        data = _parse_fixture()
        j = data["per_job"]["1002"]
        assert j["gpu_count"] == 0
        assert j["n_cpus"] == 16

    def test_gpus_alloc_total(self):
        data = _parse_fixture()
        # job 1001 has 4 GPUs, job 1002 has 0 GPUs
        assert data["gpus_alloc"] == 4

    def test_pending_reason_counted(self):
        data = _parse_fixture()
        reasons = data["pending_reasons"]
        assert reasons.get("Resources", 0) == 1
        assert reasons.get("Priority", 0) == 2

    def test_user_state_aggregates(self):
        data = _parse_fixture()
        us = data["user_state"]
        # alice: 1 RUNNING + 1 PENDING
        assert us[("alice", "RUNNING")]["count"] == 1
        assert us[("alice", "PENDING")]["count"] == 1
        # bob: 1 RUNNING + 1 PENDING
        assert us[("bob", "RUNNING")]["count"] == 1
        assert us[("bob", "PENDING")]["count"] == 1

    def test_partition_state_aggregates(self):
        data = _parse_fixture()
        ps = data["partition_state"]
        assert ps[("gpu", "RUNNING")]["count"] == 1
        assert ps[("gpu", "PENDING")]["count"] == 2
        assert ps[("cpu", "RUNNING")]["count"] == 1

    def test_account_state_aggregates(self):
        data = _parse_fixture()
        acct = data["account_state"]
        assert acct[("ml-team", "RUNNING")]["count"] == 1
        assert acct[("ml-team", "PENDING")]["count"] == 2
        assert acct[("data-team", "RUNNING")]["count"] == 1
        assert acct[("data-team", "PENDING")]["count"] == 1

    def test_user_cpu_aggregates(self):
        data = _parse_fixture()
        us = data["user_state"]
        assert us[("alice", "RUNNING")]["cpu"] == 8

    def test_elapsed_positive_for_running(self):
        data = _parse_fixture()
        j = data["per_job"]["1001"]
        assert j["elapsed"] >= 0

    def test_wait_positive_for_running(self):
        data = _parse_fixture()
        j = data["per_job"]["1001"]
        assert j["wait"] >= 0

    def test_malformed_line_skipped(self):
        bad_fixture = FIXTURE + "\nBAD_LINE\n"
        coll = _make_collector()
        with patch("slurm_exporter.collectors.jobs.expand_nodelist",
                   side_effect=lambda n, scontrol_path, ttl=300: [n]):
            data = coll._parse(bad_fixture, _BASE_CONFIG)
        assert len(data["per_job"]) == 2


class TestJobsCollect:
    def test_collect_yields_metrics(self):
        coll = _make_collector()
        with patch("slurm_exporter.collectors.jobs.expand_nodelist",
                   side_effect=lambda n, scontrol_path, ttl=300: [n]):
            data = coll._parse(FIXTURE, _BASE_CONFIG)
        coll._update(data)

        metrics = list(coll.collect())
        names = {m.name for m in metrics}
        assert "slurm_job_gpus_total" in names
        assert "slurm_job_elapsed_seconds" in names
        assert "slurm_user_state_total" in names
        assert "slurm_partition_job_state_total" in names
        assert "slurm_account_job_state_total" in names
        assert "slurm_pending_reason_total" in names
        assert "slurm_queue_jobs_running" in names
        assert "slurm_queue_jobs_pending" in names

    def test_collect_empty_data_no_crash(self):
        coll = _make_collector()
        metrics = list(coll.collect())
        assert len(metrics) > 0
