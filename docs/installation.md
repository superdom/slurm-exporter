# Local Installation (pip + systemd)

Deploy `slurm-exporter` directly on the SLURM controller node alongside `slurmctld`.

## Prerequisites

- Python 3.8+
- SLURM CLI tools (`squeue`, `sinfo`, `scontrol`, `sdiag`, `sacctmgr`) on `$PATH`
- Munge authentication running (`/run/munge/munge.socket.2`)
- A user with permission to run SLURM CLI commands (typically the `slurm` system user)

## Install

```bash
# As root on the SLURM controller
pip install git+https://github.com/<org>/slurm-exporter.git

# Or from a local clone
git clone https://github.com/<org>/slurm-exporter.git
pip install slurm-exporter/
```

## Verify

```bash
slurm-exporter --help
slurm-exporter --port=9410 --log-level=debug
# Then in another terminal:
curl http://localhost:9410/metrics | grep slurm_nodes
```

## systemd service

Copy the unit file and enable the service:

```bash
cp deploy/slurm-exporter.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now slurm-exporter
systemctl status slurm-exporter
```

The unit file (`deploy/slurm-exporter.service`) runs as `User=slurm` and starts
after `slurmctld.service`:

```ini
[Unit]
Description=Prometheus SLURM Exporter
After=slurmctld.service
Wants=slurmctld.service

[Service]
User=slurm
Group=slurm
ExecStart=/usr/local/bin/slurm-exporter \
    --port=9410 \
    --log-level=info \
    --collect-interval=15 \
    --medium-interval=300
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

Adjust `ExecStart` flags as needed (e.g. `--disable-diags` for SLURM < 20.11,
or `--gpus-per-node=8` for clusters with a fixed GPU topology).

## Prometheus scrape config

Add to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: slurm
    scrape_interval: 15s
    scrape_timeout: 10s
    static_configs:
      - targets:
          - "slurm-controller:9410"
    metric_relabel_configs:
      # Extract hostname from instance label for Grafana join
      - source_labels: [instance]
        regex: "([^:]+):.+"
        target_label: hostname
        action: replace
```

Or copy `deploy/prometheus-scrape.yml` and merge into your Prometheus config.

## SLURM binaries in a non-standard path

If SLURM tools are not on `$PATH`:

```bash
slurm-exporter \
  --squeue-path=/usr/lib64/bin/squeue \
  --sinfo-path=/usr/lib64/bin/sinfo \
  --scontrol-path=/usr/lib64/bin/scontrol \
  --sdiag-path=/usr/lib64/bin/sdiag \
  --sacctmgr-path=/usr/lib64/bin/sacctmgr
```

## HA (high-availability) controllers

In an HA SLURM setup, run `slurm-exporter` on the **active** controller only.
Use a floating VIP or Prometheus `file_sd_configs` to point at the active node.
The exporter will return stale data (`slurm_sinfo_stale=1`) if `sinfo` fails,
which can serve as an alerting signal.

## Uninstall

```bash
systemctl disable --now slurm-exporter
rm /etc/systemd/system/slurm-exporter.service
pip uninstall slurm-exporter
```
