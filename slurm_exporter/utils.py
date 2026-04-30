import logging
import re
import subprocess
import time
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# Nodelist cache: { "skt-dgx[001-003]": (["skt-dgx001", ...], timestamp) }
_nodelist_cache: Dict[str, Tuple[List[str], float]] = {}

# GRES GPU parsing regex. Handles forms such as gpu:a100:sxm5:8(IDX:0-7).
_GPU_GRES_RE = re.compile(r"^gpu(?::[^:,()]+)*:(\d+)", re.IGNORECASE)

# Memory unit conversion for squeue %m: 1024-based MiB.
_MEM_UNITS_BINARY = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4,
                     "k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}
# Memory unit conversion for sinfo -O Memory: SI-based MB, matching the Go exporter.
_MEM_UNITS_SI = {"": 1, "K": 1_000, "M": 1_000_000, "G": 1_000_000_000, "T": 1_000_000_000_000,
                 "k": 1_000, "m": 1_000_000, "g": 1_000_000_000, "t": 1_000_000_000_000}
_MEM_RE = re.compile(r"^(\d+(?:\.\d+)?)([KMGTkmgt]?)$")


def run_cmd(cmd: List[str], timeout: int = 30) -> Optional[str]:
    """Run a SLURM CLI command and return stdout. Return None on failure."""
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
    """Expand a compressed nodelist into individual hostnames with a TTL cache.

    Without caching, 100 running jobs at a 15s interval call scontrol 24,000
    times per hour. A 300s TTL reduces this to 1,200 calls (20x less).
    Expired entries are pruned once the cache exceeds 1000 entries.
    """
    if not nodelist or nodelist in ("(null)", "None", ""):
        return []

    now = time.time()
    cached = _nodelist_cache.get(nodelist)
    if cached:
        nodes, cached_at = cached
        if now - cached_at < ttl:
            return nodes

    # Prune expired entries when the cache grows too large.
    if len(_nodelist_cache) > 1000:
        expired = [k for k, (_, ts) in _nodelist_cache.items() if now - ts >= ttl]
        for k in expired:
            del _nodelist_cache[k]

    out = run_cmd([scontrol_path, "show", "hostnames", nodelist])
    nodes = [n for n in out.strip().split("\n") if n] if out else [nodelist]
    _nodelist_cache[nodelist] = (nodes, now)
    return nodes


def parse_gres_gpu_count(gres: str) -> int:
    """Extract the GPU count from a SLURM GRES string.

    Supported forms: 'gpu:8', 'gpu:a100:8', 'gpu:a100:sxm5:8',
    'gpu:a100:sxm5:8(IDX:0-7)'.
    Multiple GRES example: 'gpu:8,cpu:32' -> 8.
    """
    if not gres or gres in ("(null)", "N/A", ""):
        return 0
    total = 0
    for token in gres.split(","):
        token = token.split("(")[0]  # Strip IDX suffix.
        m = _GPU_GRES_RE.match(token.strip())
        if m:
            total += int(m.group(1))
    return total


_GPU_TRES_RE = re.compile(r"gres/gpu(?::[^=,]+)?=(\d+)", re.IGNORECASE)


def parse_gres_gpu_count_from_tres(tres: str) -> int:
    """Extract the GPU count from an AllocTRES string.

    Supported forms: 'gres/gpu=8', 'gres/gpu:a100=4',
    'gres/gpu:h100:sxm5=8'.
    """
    if not tres or tres in ("(null)", "N/A", ""):
        return 0
    m = _GPU_TRES_RE.search(tres)
    return int(m.group(1)) if m else 0


def parse_mem_to_bytes(mem_str: str, si: bool = False) -> int:
    """Convert a memory string to bytes.

    si=False (default): squeue %m field, MiB-based (1024^2).
    si=True: sinfo -O Memory field, MB-based (1e6), matching the Go exporter.
    """
    if not mem_str or mem_str in ("N/A", "0", ""):
        return 0
    # Numeric-only squeue %m values are in MiB.
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
    """Convert a SLURM time string (YYYY-MM-DDTHH:MM:SS) to Unix epoch."""
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
    """Handle SLURM 24 dual-format ints: int or {"set": bool, "number": N}."""
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
