# Metrics Reference

All metrics are prefixed with `slurm_`. Scrape endpoint: `http://<host>:9410/metrics`

---

## Job metrics (fast loop, 15s)

Collected via `squeue --state=ALL`. Only **RUNNING** jobs appear in per-job metrics;
all states contribute to aggregate metrics.

### Per-job metrics (RUNNING only)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `slurm_job_node_alloc` | Gauge | `job_id, job_name, user, partition, node, account` | 1 per node allocated to job. Key metric for Grafana join with `dcgm-exporter`/`node_exporter` via `node` label |
| `slurm_job_nodes_count` | Gauge | `job_id, job_name, user, partition, account, nodelist` | Number of nodes allocated to job |
| `slurm_job_gpus_total` | Gauge | `job_id, job_name, user, partition, account` | GPUs allocated to job (parsed from GRES or nodes × `--gpus-per-node`) |
| `slurm_job_cpus_total` | Gauge | `job_id, job_name, user, partition, account` | CPUs allocated to job |
| `slurm_job_alloc_mem` | Gauge | `job_id, job_name, user, partition, account` | Memory allocated to job (bytes) |
| `slurm_job_elapsed_seconds` | Gauge | `job_id, job_name, user, partition, account` | Elapsed time since job start (seconds) |
| `slurm_job_start_time` | Gauge | `job_id` | Job start time (Unix epoch) |
| `slurm_job_wait_seconds` | Gauge | `job_id` | Wait time from submit to start (seconds) |

### Queue aggregate metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `slurm_queue_jobs_running` | Gauge | `partition` | Running job count per partition |
| `slurm_queue_jobs_pending` | Gauge | `partition` | Pending job count per partition |
| `slurm_pending_reason_total` | Gauge | `reason` | Pending job count per reason (e.g. `Priority`, `Resources`) |

### User aggregate metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `slurm_user_state_total` | Gauge | `username, state` | Job count per user and state |
| `slurm_user_cpu_alloc` | Gauge | `username, state` | CPU count per user and state |
| `slurm_user_mem_alloc` | Gauge | `username, state` | Memory per user and state (bytes) |

### Partition aggregate metrics (from squeue)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `slurm_partition_job_state_total` | Gauge | `partition, state` | Job count per partition and state |
| `slurm_partition_job_state_cpu_alloc` | Gauge | `partition, state` | CPUs per partition and state |
| `slurm_partition_job_state_mem_alloc` | Gauge | `partition, state` | Memory per partition and state (bytes) |

### Account aggregate metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `slurm_account_job_state_total` | Gauge | `account, state` | Job count per account and state |
| `slurm_account_job_state_cpu_alloc` | Gauge | `account, state` | CPUs per account and state |
| `slurm_account_job_state_mem_alloc` | Gauge | `account, state` | Memory per account and state (bytes) |

---

## Node/cluster metrics (fast loop, 15s)

Collected via `sinfo -h -O ...` and node GRES/GresUsed output from `sinfo`.

### Cluster totals

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `slurm_nodes_idle` | Gauge | — | Idle node count |
| `slurm_nodes_alloc` | Gauge | — | Allocated node count |
| `slurm_nodes_down` | Gauge | — | Down node count |
| `slurm_nodes_drain` | Gauge | — | Draining node count |
| `slurm_gpus_total` | Gauge | — | Total GPUs in cluster |
| `slurm_gpus_alloc` | Gauge | — | Allocated GPUs from node `GresUsed` when available; falls back to running-job GPU sum |
| `slurm_cpus_total` | Gauge | — | Total CPUs across all nodes |
| `slurm_cpus_idle` | Gauge | — | Total idle CPUs |
| `slurm_cpu_load` | Gauge | — | Total CPU load |
| `slurm_mem_real` | Gauge | — | Total real memory (bytes) |
| `slurm_mem_free` | Gauge | — | Total free memory (bytes) |
| `slurm_mem_alloc` | Gauge | — | Total allocated memory (bytes) |

### Partition-level node metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `slurm_partition_total_cpus` | Gauge | `partition` | Total CPUs in partition |
| `slurm_partition_idle_cpus` | Gauge | `partition` | Idle CPUs in partition |
| `slurm_partition_cpu_load` | Gauge | `partition` | CPU load in partition |
| `slurm_partition_real_mem` | Gauge | `partition` | Real memory in partition (bytes) |
| `slurm_partition_free_mem` | Gauge | `partition` | Free memory in partition (bytes) |
| `slurm_partition_alloc_mem` | Gauge | `partition, state` | Allocated memory per partition/node-state (bytes) |
| `slurm_partition_alloc_cpus` | Gauge | `partition, state` | Allocated CPUs per partition/node-state |
| `slurm_partition_node_count` | Gauge | `partition, state` | Node count per partition and node-state |

### sinfo health

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `slurm_sinfo_last_update` | Gauge | — | Unix timestamp of last successful sinfo collection |
| `slurm_sinfo_stale` | Gauge | — | 1 if sinfo data is older than 4× collect-interval (default 60s) |

---

## Scheduler diagnostics (medium loop, 300s)

Collected via `sdiag --json`. Requires SLURM 20.11+. Disable with `--disable-diags`.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `slurm_daemon_thread_count` | Gauge | — | SLURM daemon thread count |
| `slurm_dbd_agent_queue_size` | Gauge | — | SlurmDBD agent queue size |
| `slurm_backfill_job_count` | Gauge | — | Jobs started via backfilling since last start |
| `slurm_backfill_cycle_count` | Gauge | — | Backfill scheduling cycles since last reset |
| `slurm_backfill_last_depth` | Gauge | — | Jobs processed in last backfill cycle |
| `slurm_backfill_last_depth_try_sched` | Gauge | — | Jobs with chance to start in last backfill cycle |
| `slurm_rpc_msg_type_count` | Gauge | `type` | RPC count per message type |
| `slurm_rpc_msg_type_avg_time` | Gauge | `type` | RPC average time per message type (ms) |
| `slurm_rpc_msg_type_total_time` | Gauge | `type` | RPC total time per message type (ms) |

---

## Account limits (medium loop, 300s)

Collected via `sacctmgr show assoc`. Account-level rows only (user rows skipped).
Disable with `--disable-account-limits`.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `slurm_account_cpu_limit` | Gauge | `account` | GrpCPU limit (0 = unlimited) |
| `slurm_account_mem_limit` | Gauge | `account` | GrpMem limit in bytes (0 = unlimited) |
| `slurm_account_job_alloc_limit` | Gauge | `account` | GrpJobs limit — max concurrently running jobs |
| `slurm_account_job_limit` | Gauge | `account` | GrpSubmit limit — max total submitted jobs |

---

## Scrape health metrics

Each collector exposes two health metrics. `<prefix>` is `slurm_job`, `slurm_node`,
`slurm_diag`, or `slurm_account`.

| Metric | Type | Description |
|--------|------|-------------|
| `<prefix>_scrape_error` | Counter | Total collection errors since start |
| `<prefix>_scrape_duration` | Gauge | Last collection duration (milliseconds) |

---

## Grafana query examples

```promql
# GPU hours per user (last 24h, using Prometheus retention)
sum by (user) (
  max_over_time(slurm_job_gpus_total[24h])
  * max_over_time(slurm_job_elapsed_seconds[24h]) / 3600
)

# GPU utilization per node (join with dcgm-exporter)
slurm_job_node_alloc * on(node) group_left()
  avg by (node) (DCGM_FI_DEV_GPU_UTIL)

# Pending jobs by reason
sort_desc(slurm_pending_reason_total)

# Partition CPU utilization
1 - (slurm_partition_idle_cpus / slurm_partition_total_cpus)

# Backfill effectiveness (jobs per cycle)
slurm_backfill_job_count / slurm_backfill_cycle_count
```
