import logging
import re
from typing import Iterator

from prometheus_client.core import GaugeMetricFamily

from slurm_exporter.base import SlurmBaseCollector
from slurm_exporter.config import Config
from slurm_exporter.utils import run_cmd

log = logging.getLogger(__name__)

_SACCTMGR_FMT = "format=User,Account,GrpCPU,GrpMem,GrpJobs,GrpSubmit"
_MB = 1_000_000  # GrpMem 단위 없음 = MB (SI)
_MEM_LIMIT_RE = re.compile(r"^(\d+(?:\.\d+)?)([KMGT]?)$", re.IGNORECASE)
_UNLIMITED_VALS = {"UNLIMITED", "INFINITE", "N/A", "-1", ""}


def _parse_mem_limit(s: str) -> int:
    """sacctmgr GrpMem 필드를 바이트로 변환. UNLIMITED/빈값은 0 반환."""
    s = s.strip()
    if not s or s.upper() in _UNLIMITED_VALS:
        return 0
    m = _MEM_LIMIT_RE.match(s)
    if not m:
        return 0
    val = float(m.group(1))
    unit = m.group(2).upper()
    mult = {"K": 1_000, "M": 1_000_000, "G": 1_000_000_000, "T": 1_000_000_000_000}.get(unit, _MB)
    return int(val * mult)


def _safe_int(s: str) -> int:
    """UNLIMITED/비어있는 문자열을 0으로 처리하는 정수 변환."""
    s = s.strip()
    if not s or s.upper() in _UNLIMITED_VALS:
        return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


class AccountLimitCollector(SlurmBaseCollector):
    """sacctmgr 호출로 계정 한도 메트릭 수집."""

    def __init__(self, config: Config) -> None:
        super().__init__("account")
        self._config = config

    # ------------------------------------------------------------------
    # 수집
    # ------------------------------------------------------------------

    def fetch(self) -> None:
        start = self._start_timer()
        cfg = self._config

        out = run_cmd([
            cfg.sacctmgr_path, "show", "assoc",
            _SACCTMGR_FMT,
            "--noheader", "--parsable2",
        ], timeout=30)

        if not out:
            self._record_error()
            log.warning("sacctmgr returned no output; retaining previous metrics")
            self._stop_timer(start)
            return

        data = self._parse(out)
        self._update(data)
        self._stop_timer(start)

    def _parse(self, out: str) -> dict:
        # user 필드가 비어있는 행이 계정 레벨 행
        # 필드 순서: user|account|grp_cpu|grp_mem|grp_jobs|grp_submit
        accounts: dict = {}
        for line in out.strip().split("\n"):
            parts = line.strip().split("|")
            if len(parts) < 6:
                continue
            user, account, grp_cpu, grp_mem, grp_jobs, grp_submit = parts[:6]
            if user:  # 사용자 레벨 행은 건너뜀
                continue
            if not account:
                continue

            accounts[account] = {
                "cpu_limit": _safe_int(grp_cpu),
                "mem_limit": _parse_mem_limit(grp_mem),
                "job_alloc_limit": _safe_int(grp_jobs),
                "job_limit": _safe_int(grp_submit),
            }

        return {"accounts": accounts}

    # ------------------------------------------------------------------
    # 메트릭 노출
    # ------------------------------------------------------------------

    def collect(self) -> Iterator:
        data = self._snapshot()
        accounts = data.get("accounts", {})

        g_cpu = GaugeMetricFamily("slurm_account_cpu_limit",
                                  "Account CPU limit", labels=["account"])
        g_mem = GaugeMetricFamily("slurm_account_mem_limit",
                                  "Account memory limit (bytes)", labels=["account"])
        g_jobs = GaugeMetricFamily("slurm_account_job_alloc_limit",
                                   "Account limit on running jobs", labels=["account"])
        g_submit = GaugeMetricFamily("slurm_account_job_limit",
                                     "Account limit on total submitted jobs", labels=["account"])

        for account, vals in accounts.items():
            g_cpu.add_metric([account], vals["cpu_limit"])
            g_mem.add_metric([account], vals["mem_limit"])
            g_jobs.add_metric([account], vals["job_alloc_limit"])
            g_submit.add_metric([account], vals["job_limit"])

        yield g_cpu
        yield g_mem
        yield g_jobs
        yield g_submit

        yield from self._health_metrics("slurm_account")
