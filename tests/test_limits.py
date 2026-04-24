from pathlib import Path

from slurm_exporter.collectors.limits import AccountLimitCollector, _MB
from slurm_exporter.config import Config

FIXTURE = (Path(__file__).parent / "fixtures" / "sacctmgr.txt").read_text()

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


def _parse() -> dict:
    coll = AccountLimitCollector(_BASE_CONFIG)
    return coll._parse(FIXTURE)


class TestLimitsParse:
    def test_ml_team_parsed(self):
        data = _parse()
        assert "ml-team" in data["accounts"]

    def test_data_team_parsed(self):
        data = _parse()
        assert "data-team" in data["accounts"]

    def test_user_rows_skipped(self):
        # "shouldignore|ml-team|..." has user="shouldignore", must not overwrite ml-team
        data = _parse()
        # ml-team values should come from account-level row only
        assert data["accounts"]["ml-team"]["cpu_limit"] == 993

    def test_ml_team_cpu_limit(self):
        data = _parse()
        assert data["accounts"]["ml-team"]["cpu_limit"] == 993

    def test_ml_team_mem_limit(self):
        data = _parse()
        # 15917500 MB × 1_000_000
        assert data["accounts"]["ml-team"]["mem_limit"] == 15917500 * _MB

    def test_ml_team_job_alloc_limit(self):
        data = _parse()
        assert data["accounts"]["ml-team"]["job_alloc_limit"] == 8000

    def test_ml_team_job_submit_limit(self):
        data = _parse()
        assert data["accounts"]["ml-team"]["job_limit"] == 60000

    def test_empty_limits_zero(self):
        data = _parse()
        # empty-team has no limits
        empty = data["accounts"].get("empty-team", {})
        assert empty.get("cpu_limit", 0) == 0
        assert empty.get("mem_limit", 0) == 0

    def test_root_account_parsed(self):
        # root row: |root|||| — all limits empty → zero
        data = _parse()
        root = data["accounts"].get("root", {})
        assert root.get("cpu_limit", 0) == 0


class TestLimitsCollect:
    def test_collect_yields_metrics(self):
        coll = AccountLimitCollector(_BASE_CONFIG)
        data = coll._parse(FIXTURE)
        coll._update(data)

        metrics = list(coll.collect())
        names = {m.name for m in metrics}
        assert "slurm_account_cpu_limit" in names
        assert "slurm_account_mem_limit" in names
        assert "slurm_account_job_alloc_limit" in names
        assert "slurm_account_job_limit" in names
