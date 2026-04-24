import logging
import re
import subprocess
import time
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# nodelist 캐시: { "skt-dgx[001-003]": (["skt-dgx001", ...], timestamp) }
_nodelist_cache: Dict[str, Tuple[List[str], float]] = {}

# GRES GPU 파싱 정규식 — gpu:a100:sxm5:8(IDX:0-7) 등 모든 형태 처리
_GPU_GRES_RE = re.compile(r"^gpu(?::\w+)*:(\d+)", re.IGNORECASE)

# 메모리 단위 변환 (squeue %m 필드: 1024 기반 MiB)
_MEM_UNITS_BINARY = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4,
                     "k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}
# 메모리 단위 변환 (sinfo -O Memory 필드: SI 기반 MB, Go exporter 방식 일치)
_MEM_UNITS_SI = {"": 1, "K": 1_000, "M": 1_000_000, "G": 1_000_000_000, "T": 1_000_000_000_000,
                 "k": 1_000, "m": 1_000_000, "g": 1_000_000_000, "t": 1_000_000_000_000}
_MEM_RE = re.compile(r"^(\d+(?:\.\d+)?)([KMGTkmgt]?)$")


def run_cmd(cmd: List[str], timeout: int = 30) -> Optional[str]:
    """SLURM CLI 명령을 실행하고 stdout을 반환. 실패 시 None."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            log.warning("Command %s returned %d: %s", cmd, result.returncode, result.stderr.strip())
            return None
        return result.stdout
    except subprocess.TimeoutExpired:
        log.error("Command %s timed out after %ds", cmd, timeout)
        return None
    except FileNotFoundError:
        log.error("Command not found: %s", cmd[0])
        return None
    except Exception as exc:
        log.error("Command %s failed: %s", cmd, exc)
        return None


def expand_nodelist(nodelist: str, scontrol_path: str = "scontrol", ttl: int = 300) -> List[str]:
    """압축된 nodelist를 개별 호스트명 목록으로 확장. TTL 캐시 적용.

    캐시 없이 running job 100개 × 15s 주기 = 1시간에 scontrol 24,000회 호출.
    TTL 300s 적용 시 1,200회로 감소 (20배).
    캐시 크기가 1000 초과 시 만료 항목을 정리하여 메모리 무한 증가를 방지.
    """
    if not nodelist or nodelist in ("(null)", "None", ""):
        return []

    now = time.time()
    cached = _nodelist_cache.get(nodelist)
    if cached:
        nodes, cached_at = cached
        if now - cached_at < ttl:
            return nodes

    # 캐시가 너무 커지면 만료 항목 정리
    if len(_nodelist_cache) > 1000:
        expired = [k for k, (_, ts) in _nodelist_cache.items() if now - ts >= ttl]
        for k in expired:
            del _nodelist_cache[k]

    out = run_cmd([scontrol_path, "show", "hostnames", nodelist])
    nodes = [n for n in out.strip().split("\n") if n] if out else [nodelist]
    _nodelist_cache[nodelist] = (nodes, now)
    return nodes


def parse_gres_gpu_count(gres: str) -> int:
    """SLURM GRES 문자열에서 GPU 수 추출.

    지원 형태: 'gpu:8', 'gpu:a100:8', 'gpu:a100:sxm5:8', 'gpu:a100:sxm5:8(IDX:0-7)'
    복수 GRES: 'gpu:8,cpu:32' → 8
    """
    if not gres or gres in ("(null)", "N/A", ""):
        return 0
    total = 0
    for token in gres.split(","):
        token = token.split("(")[0]  # IDX suffix 제거
        m = _GPU_GRES_RE.match(token.strip())
        if m:
            total += int(m.group(1))
    return total


_GPU_TRES_RE = re.compile(r"gres/gpu(?::[^=,]+)?=(\d+)", re.IGNORECASE)


def parse_gres_gpu_count_from_tres(tres: str) -> int:
    """AllocTRES 문자열에서 GPU 수 추출.

    지원 형태: 'gres/gpu=8', 'gres/gpu:a100=4', 'gres/gpu:h100:sxm5=8'
    """
    if not tres or tres in ("(null)", "N/A", ""):
        return 0
    m = _GPU_TRES_RE.search(tres)
    return int(m.group(1)) if m else 0


def parse_mem_to_bytes(mem_str: str, si: bool = False) -> int:
    """메모리 문자열을 바이트로 변환.

    si=False (기본): squeue %m 필드 — MiB 기반 (1024^2)
    si=True: sinfo -O Memory 필드 — MB 기반 (1e6, Go exporter 방식)
    """
    if not mem_str or mem_str in ("N/A", "0", ""):
        return 0
    # squeue %m은 숫자만 오면 MiB 단위
    if mem_str.isdigit():
        factor = 1_000_000 if si else 1024 ** 2
        return int(mem_str) * factor
    m = _MEM_RE.match(mem_str.strip())
    if not m:
        return 0
    value = float(m.group(1))
    unit = m.group(2)
    units = _MEM_UNITS_SI if si else _MEM_UNITS_BINARY
    return int(value * units.get(unit, 1))


def parse_epoch(slurm_time: str) -> Optional[int]:
    """SLURM 시간 문자열(YYYY-MM-DDTHH:MM:SS)을 Unix epoch으로 변환."""
    if not slurm_time or slurm_time in ("N/A", "Unknown", "None", ""):
        return None
    from datetime import datetime
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return int(datetime.strptime(slurm_time, fmt).timestamp())
        except ValueError:
            continue
    return None


def coerce_int(val) -> int:
    """SLURM 24의 dual-format int 처리: int 또는 {"set": bool, "number": N}."""
    if isinstance(val, int):
        return val
    if isinstance(val, dict):
        if val.get("set") and not val.get("infinite"):
            return val.get("number", 0)
        return 0
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0
