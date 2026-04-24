import json
from pathlib import Path

from slurm_exporter.collectors.diagnostics import DiagnosticsCollector
from slurm_exporter.config import Config

SDIAG_JSON = (Path(__file__).parent / "fixtures" / "sdiag.json").read_text()
SDIAG_SLURM24_JSON = (Path(__file__).parent / "fixtures" / "sdiag_slurm24.json").read_text()

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

SDIAG_TEXT = """\
Server thread count: 3
Agent queue size: 5
Total backfilled jobs (since last slurm start): 2488
Total cycles: 72
Last depth cycle: 113
Last depth cycle (try sched): 89
"""


class TestDiagnosticsJsonParse:
    def _parse(self, text: str) -> dict:
        coll = DiagnosticsCollector(_BASE_CONFIG)
        return coll._parse_json(text)

    def test_thread_count(self):
        data = self._parse(SDIAG_JSON)
        assert data["scalars"]["thread_count"] == 3

    def test_dbd_queue_size(self):
        data = self._parse(SDIAG_JSON)
        assert data["scalars"]["dbd_queue_size"] == 5

    def test_backfill_jobs(self):
        data = self._parse(SDIAG_JSON)
        assert data["scalars"]["bf_backfilled_jobs"] == 2488

    def test_bf_cycle_count(self):
        data = self._parse(SDIAG_JSON)
        assert data["scalars"]["bf_cycle_count"] == 72

    def test_bf_last_depth(self):
        data = self._parse(SDIAG_JSON)
        assert data["scalars"]["bf_last_depth"] == 113

    def test_rpc_by_type(self):
        data = self._parse(SDIAG_JSON)
        assert "REQUEST_JOB_INFO" in data["rpc_by_type"]
        rpc = data["rpc_by_type"]["REQUEST_JOB_INFO"]
        assert rpc["count"] == 1200
        assert rpc["avg_time"] == 45
        assert rpc["total_time"] == 54000


class TestDiagnosticsSlurm24Parse:
    """SLURM 24 dual-format int: {"set": true, "number": N}."""

    def _parse(self, text: str) -> dict:
        coll = DiagnosticsCollector(_BASE_CONFIG)
        return coll._parse_json(text)

    def test_thread_count_struct(self):
        data = self._parse(SDIAG_SLURM24_JSON)
        assert data["scalars"]["thread_count"] == 4

    def test_dbd_queue_size_struct(self):
        data = self._parse(SDIAG_SLURM24_JSON)
        assert data["scalars"]["dbd_queue_size"] == 2

    def test_rpc_count_struct(self):
        data = self._parse(SDIAG_SLURM24_JSON)
        rpc = data["rpc_by_type"]["REQUEST_JOB_INFO"]
        assert rpc["count"] == 500
        assert rpc["avg_time"] == 20


class TestDiagnosticsTextFallback:
    def _parse(self, text: str) -> dict:
        coll = DiagnosticsCollector(_BASE_CONFIG)
        return coll._parse_text(text)

    def test_thread_count_text(self):
        data = self._parse(SDIAG_TEXT)
        assert data["scalars"]["thread_count"] == 3

    def test_dbd_queue_size_text(self):
        data = self._parse(SDIAG_TEXT)
        assert data["scalars"]["dbd_queue_size"] == 5

    def test_backfill_jobs_text(self):
        data = self._parse(SDIAG_TEXT)
        assert data["scalars"]["bf_backfilled_jobs"] == 2488

    def test_rpc_empty_in_text_fallback(self):
        data = self._parse(SDIAG_TEXT)
        assert data["rpc_by_type"] == {}


class TestDiagnosticsCollect:
    def test_collect_yields_metrics(self):
        coll = DiagnosticsCollector(_BASE_CONFIG)
        data = coll._parse_json(SDIAG_JSON)
        coll._update(data)

        metrics = list(coll.collect())
        names = {m.name for m in metrics}
        assert "slurm_daemon_thread_count" in names
        assert "slurm_backfill_job_count" in names
        assert "slurm_rpc_msg_type_count" in names
