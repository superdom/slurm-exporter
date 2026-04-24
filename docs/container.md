# Container Deployment (Docker / Compose)

The container image contains only Python and `prometheus_client`. SLURM binaries
and Munge authentication are provided by the host at runtime via volume mounts —
no SLURM installation inside the image is required.

## Prerequisites

- Docker 20.10+ or compatible container runtime
- SLURM binaries installed on the host (typically `/usr/bin/squeue` etc.)
- Munge running on the host (`/run/munge/munge.socket.2`)

## Build

```bash
docker build -t slurm-exporter:latest .
```

## Run with Docker Compose

```bash
docker compose up -d
docker compose logs -f slurm-exporter
```

The `docker-compose.yml` mounts the necessary host paths:

```yaml
volumes:
  - /usr/bin/squeue:/usr/bin/squeue:ro
  - /usr/bin/sinfo:/usr/bin/sinfo:ro
  - /usr/bin/scontrol:/usr/bin/scontrol:ro
  - /usr/bin/sdiag:/usr/bin/sdiag:ro
  - /usr/bin/sacctmgr:/usr/bin/sacctmgr:ro
  - /etc/slurm:/etc/slurm:ro
  - /run/munge:/run/munge:ro
  - /etc/munge:/etc/munge:ro
```

`network_mode: host` is required so that SLURM commands can reach the `slurmctld`
socket without NAT.

## Run with docker run

```bash
docker run -d \
  --name slurm-exporter \
  --network host \
  --restart unless-stopped \
  -v /usr/bin/squeue:/usr/bin/squeue:ro \
  -v /usr/bin/sinfo:/usr/bin/sinfo:ro \
  -v /usr/bin/scontrol:/usr/bin/scontrol:ro \
  -v /usr/bin/sdiag:/usr/bin/sdiag:ro \
  -v /usr/bin/sacctmgr:/usr/bin/sacctmgr:ro \
  -v /etc/slurm:/etc/slurm:ro \
  -v /run/munge:/run/munge:ro \
  -v /etc/munge:/etc/munge:ro \
  slurm-exporter:latest \
  --port=9410 \
  --log-level=info \
  --collect-interval=15 \
  --medium-interval=300
```

## SLURM binaries in a non-standard path

If your SLURM tools are not in `/usr/bin`:

```yaml
# docker-compose.yml
volumes:
  - /usr/lib64/bin/squeue:/usr/bin/squeue:ro
  - /usr/lib64/bin/sinfo:/usr/bin/sinfo:ro
  # ...
```

Or mount them at their original path and pass the path flags:

```yaml
command:
  - "--squeue-path=/usr/lib64/bin/squeue"
  - "--sinfo-path=/usr/lib64/bin/sinfo"
```

## Disable optional collectors

```yaml
command:
  - "--port=9410"
  - "--disable-diags"           # SLURM < 20.11
  - "--disable-account-limits"  # no sacctmgr access
```

## Verify

```bash
curl http://localhost:9410/metrics | grep slurm_nodes
curl http://localhost:9410/healthz
```

## Prometheus scrape config

Same as the local install — see [installation.md](installation.md#prometheus-scrape-config).

## Image details

| Layer | Size |
|-------|------|
| `python:3.11-slim` base | ~130 MB |
| `prometheus_client` | ~2 MB |
| `slurm_exporter` package | < 1 MB |

The final image is ~133 MB. No SLURM, no C libraries, no secrets embedded.
