# slurm-exporter

Prometheus exporter for the [SLURM](https://slurm.schedmd.com/) workload manager.

Collects per-job, per-node, partition, user, and account metrics by invoking
SLURM CLI tools (`squeue`, `sinfo`, `scontrol`, `sdiag`, `sacctmgr`) directly —
no REST API dependency required.

## Features

- **~55 metrics** covering jobs, nodes, partitions, users, accounts, and scheduler diagnostics
- **Race-condition-free** via Prometheus Custom Collector pattern (atomic snapshot swap)
- **Efficient**: single `squeue --state=ALL` call replaces multiple separate calls; `scontrol` nodelist cache (TTL 300s) reduces calls by 20×
- **Container-ready**: ships as a `python:3.11-slim` image with SLURM binaries mounted from the host
- **Backward compatible**: preserves existing `slurm_nodes_*`, `slurm_gpus_*`, `slurm_queue_jobs_*` metric names
- **Grafana-friendly**: `slurm_job_node_alloc` enables PromQL join with `dcgm-exporter` and `node_exporter` via the `node` label

## Quick start

### Local install (pip + systemd)

```bash
pip install .
slurm-exporter --port=9410 --log-level=info
```

See [docs/installation.md](docs/installation.md) for the full systemd setup.

### Container (Docker / Compose)

```bash
docker compose up -d
```

See [docs/container.md](docs/container.md) for configuration details.

## Metrics

Full metric reference: [docs/metrics.md](docs/metrics.md)

Key metrics at a glance:

| Metric | Labels | Description |
|--------|--------|-------------|
| `slurm_job_node_alloc` | `job_id, job_name, user, partition, node, account` | 1 if node is allocated to job (Grafana join key) |
| `slurm_job_gpus_total` | `job_id, job_name, user, partition, account` | GPUs allocated to running job |
| `slurm_job_elapsed_seconds` | `job_id, job_name, user, partition, account` | Running job elapsed time |
| `slurm_user_state_total` | `username, state` | Job count per user and state |
| `slurm_partition_job_state_total` | `partition, state` | Job count per partition and state |
| `slurm_pending_reason_total` | `reason` | Pending job count per reason |
| `slurm_cpus_total` | — | Total CPUs across all nodes |
| `slurm_backfill_job_count` | — | Jobs started via backfilling |

### Grafana — GPU hour history (example)

Because running job metrics carry rich labels (`user`, `account`, `partition`,
`job_name`) and Prometheus retains them after job completion, you can query
past GPU usage without sacct:

```promql
# GPU hours per user over the last 24h
sum by (user) (
  max_over_time(slurm_job_gpus_total[24h])
  * max_over_time(slurm_job_elapsed_seconds[24h]) / 3600
)
```

## CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `9410` | Listen port |
| `--collect-interval` | `15` | squeue/sinfo refresh interval (seconds) |
| `--medium-interval` | `300` | sdiag/sacctmgr refresh interval (seconds) |
| `--gpus-per-node` | `0` | Fixed GPUs/node (0 = parse from GRES) |
| `--disable-diags` | off | Disable sdiag collector (SLURM < 20.11) |
| `--disable-account-limits` | off | Disable sacctmgr collector |
| `--log-level` | `info` | `debug`/`info`/`warning`/`error` |
| `--squeue-path` | `squeue` | Path to squeue binary |
| `--sinfo-path` | `sinfo` | Path to sinfo binary |
| `--scontrol-path` | `scontrol` | Path to scontrol binary |
| `--sdiag-path` | `sdiag` | Path to sdiag binary |
| `--sacctmgr-path` | `sacctmgr` | Path to sacctmgr binary |

## Requirements

- Python 3.8+
- `prometheus_client >= 0.17.0`
- SLURM CLI tools on `$PATH` (or specify paths via `--*-path` flags)
- Munge authentication configured (for remote SLURM access)

## License

This project is licensed under the MIT License.
