import argparse
import dataclasses
import re


@dataclasses.dataclass
class Config:
    port: int
    collect_interval: int
    medium_interval: int
    gpus_per_node: int
    disable_diags: bool
    disable_account_limits: bool
    log_level: str
    squeue_path: str
    sinfo_path: str
    scontrol_path: str
    sacctmgr_path: str
    sdiag_path: str


def _parse_retention(s: str) -> int:
    """Parse '6h' → 6, '25h' → 25, '12' → 12."""
    m = re.match(r"^(\d+)h?$", s.strip())
    if m:
        return int(m.group(1))
    raise argparse.ArgumentTypeError(f"Invalid retention format: {s} (use '6h' or '6')")


def parse_args() -> Config:
    parser = argparse.ArgumentParser(
        description="Prometheus exporter for SLURM workload manager",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--port", type=int, default=9410,
                        help="Listen port")
    parser.add_argument("--collect-interval", type=int, default=15,
                        help="squeue/sinfo refresh interval in seconds")
    parser.add_argument("--medium-interval", type=int, default=300,
                        help="sdiag/sacctmgr refresh interval in seconds")
    parser.add_argument("--gpus-per-node", type=int, default=0,
                        help="Fixed GPUs per node (0=parse from GRES)")

    parser.add_argument("--disable-diags", action="store_true",
                        help="Disable sdiag collector (requires SLURM 20.11+)")
    parser.add_argument("--disable-account-limits", action="store_true",
                        help="Disable sacctmgr account limits collector")

    parser.add_argument("--squeue-path", default="squeue", help="Path to squeue binary")
    parser.add_argument("--sinfo-path", default="sinfo", help="Path to sinfo binary")
    parser.add_argument("--scontrol-path", default="scontrol", help="Path to scontrol binary")
    parser.add_argument("--sacctmgr-path", default="sacctmgr", help="Path to sacctmgr binary")
    parser.add_argument("--sdiag-path", default="sdiag", help="Path to sdiag binary")

    parser.add_argument("--log-level", default="info",
                        choices=["debug", "info", "warning", "error"],
                        help="Log level")

    args = parser.parse_args()
    return Config(
        port=args.port,
        collect_interval=args.collect_interval,
        medium_interval=args.medium_interval,
        gpus_per_node=args.gpus_per_node,
        disable_diags=args.disable_diags,
        disable_account_limits=args.disable_account_limits,
        log_level=args.log_level,
        squeue_path=args.squeue_path,
        sinfo_path=args.sinfo_path,
        scontrol_path=args.scontrol_path,
        sacctmgr_path=args.sacctmgr_path,
        sdiag_path=args.sdiag_path,
    )
