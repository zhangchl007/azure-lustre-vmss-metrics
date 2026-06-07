# Azure VMSS and Managed Lustre Metrics Exporter

Prometheus exporter for Azure VM Scale Sets and Azure Managed Lustre filesystems.

The exporter discovers resources with Azure Resource Graph, reads Managed Lustre metrics from Azure Monitor, and exposes cached Prometheus metrics on `/metrics`.

## Features

- Discover VM Scale Sets across one or more Azure subscriptions.
- Export actual VMSS instance count and desired VMSS capacity.
- Discover Azure Managed Lustre filesystems.
- Export key Managed Lustre OST and MDT metrics.
- Export filesystem inventory metrics so every discovered Lustre filesystem is visible in Grafana.
- Support local Azure CLI auth, Service Principal auth, Managed Identity, and AKS Workload Identity.
- Optional Kubernetes leader election for HA deployments.

## Metrics

The exporter emits three metric families:

- VMSS inventory and desired-vs-actual capacity.
- Standalone, non-VMSS Azure VM inventory by VM size and power state.
- Azure Managed Lustre filesystem, OST, MDT, HSM, and derived AV/HPC signals.

See the full catalog and PromQL examples in [docs/metrics.md](docs/metrics.md).

Quick samples:

```promql
# VMSS count currently observed
azure_vmss_exporter_vmss_total

# Standalone VM count by size
azure_vm_count_by_size

# Managed Lustre filesystem capacity and health
azure_managed_lustre_filesystem_storage_capacity_tib
azure_managed_lustre_filesystem_sample_max_age_seconds
```

## Configuration

Set configuration with environment variables. A local `.env` file is also supported.

| Variable | Default | Description |
| --- | --- | --- |
| `AZURE_SUBSCRIPTION_IDS` | required | Comma-separated subscription IDs to query. |
| `POLL_INTERVAL_SECONDS` | `300` | VMSS collection interval. |
| `HOST` | `0.0.0.0` | HTTP bind host. |
| `PORT` | `8000` | HTTP bind port. |
| `LOG_LEVEL` | `INFO` | Log level. |
| `VMSS_METRICS_AUTH_MODE` | `auto` | `auto`, `service_principal`, or `workload_identity`. |
| `ENABLE_MANAGED_LUSTRE_METRICS` | `true` | Enable Managed Lustre discovery and metrics. |
| `LUSTRE_POLL_INTERVAL_SECONDS` | `60` | Managed Lustre collection interval. |
| `LUSTRE_METRICS_LOOKBACK_MINUTES` | `15` | Azure Monitor lookback window. The 15-minute default tolerates isolated Azure Monitor sample gaps at the cost of more API work; shorter windows detect sustained stalls faster but can return empty windows during transient ingestion delays. |
| `LUSTRE_METRICS_INTERVAL` | `PT1M` | Azure Monitor metric granularity. |
| `LUSTRE_METRICS_MAX_WORKERS` | `4` | Concurrent Managed Lustre metric queries. |
| `LUSTRE_METRICS_REQUEST_JITTER_SECONDS` | `0.5` | Maximum per-filesystem jitter before Azure Monitor metric queries, used to spread bursts and reduce 429 risk at scale. |
| `ENABLE_STANDALONE_VM_INVENTORY` | `true` | Enable discovery and exposition of standalone (non-VMSS) Azure VM inventory (`azure_vm_info`, `azure_vm_power_state`, `azure_vm_count_by_size`). |
| `STANDALONE_VM_MAX_INVENTORY` | `5000` | Cardinality guardrail. When the discovered standalone-VM count exceeds this value, per-VM series are suppressed and only `azure_vm_count_by_size` aggregates are emitted. Range: 100–100000. |
| `LEADER_ELECTION_ENABLED` | `false` | Enable active/standby Kubernetes leader election. |
| `LEADER_ELECTION_LOCK_NAME` | `vmss-metrics-exporter` | Leader-election lock name. |
| `LEADER_ELECTION_NAMESPACE` | `default` | Leader-election namespace. |
| `SHUTDOWN_DRAIN_SECONDS` | `0` | After graceful Lease release on shutdown, keep serving cached `/metrics` for this many seconds to smooth rolling-update handoff. |

For Service Principal auth, set:

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_CLIENT_SECRET`

The identity needs Reader access to the target subscription(s).

## Local run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'

az login
export AZURE_SUBSCRIPTION_IDS=<subscription-id>

vmss-metrics-exporter --once
vmss-metrics-exporter
```

Open the metrics endpoint:

```bash
curl http://localhost:8000/metrics
```

## Docker

```bash
make image-multiarch IMAGE=<repo>/vmss-metrics-exporter TAG=<tag>
```

`make image-multiarch` builds and pushes a `linux/amd64,linux/arm64` manifest.
Use this target for AKS deployments; the cluster nodes are `amd64`, so a
single-arch image built on an `aarch64` workstation will fail to pull with
`no match for platform in manifest`. A `docker-container` buildx builder is
required for true multi-arch builds, for example:

```bash
docker buildx create --name multi --use --driver docker-container
```

`make image` remains useful for local smoke tests only because it builds for the
host architecture. `make push` pushes that single-arch local image and should not
be used for AKS rollout images.

Run locally with Docker:

```bash
make docker-run IMAGE=<repo>/vmss-metrics-exporter TAG=<tag> SUBSCRIPTION_IDS=<subscription-id>
```

## Kubernetes

Update `deploy/kubernetes.yaml` for your environment:

- container image
- `AZURE_SUBSCRIPTION_IDS`
- authentication mode and identity settings
- leader-election settings, if using multiple replicas

Deploy:

```bash
make deploy
make rollout
```

The checked-in AKS manifest currently runs two replicas with native Lease leader
election, leader-only Service routing, a 15-second graceful drain, and
`publishNotReadyAddresses: true` on the metrics Service so the terminating leader
can keep serving cached metrics during rollout handoff. The current deployed
image tag is `zhangchl007/vmss-metrics-exporter:v36-rollout-handoff`.

View logs:

```bash
make logs
```

Port-forward the exporter:

```bash
make port-forward
```

## Grafana

Import the dashboards from `deploy/`:

- [deploy/grafana-dashboard-vmss.json](deploy/grafana-dashboard-vmss.json)
- [deploy/grafana-dashboard-lustre.json](deploy/grafana-dashboard-lustre.json)

The Managed Lustre dashboard uses filesystem inventory metrics for dropdowns, so discovered filesystems remain visible even when Azure Monitor has no current OST or MDT sample for a filesystem.

The Managed Lustre dashboard uses an AV/HPC Lustre operations layout with rows for overview, metadata triage, OST data path, storage capacity, client I/O, metadata performance, reliability and freshness, and inventory. The `Metadata triage` row groups metadata amplification, MDT latency, MDT operations, MDT inode usage, and MDT byte usage for FSx-for-Lustre versus AMLFS customer discussions, while the `OST data path` row keeps data-path latency, operations, and connected-client visibility for large sensor logs and write-heavy phases.

For AV-specific interpretation of metadata-heavy workload signals, see [docs/av-industry.md](docs/av-industry.md).

## Prometheus alert rules

[deploy/lustre-alert-rules.yaml](deploy/lustre-alert-rules.yaml) ships ready-to-use alert rules for the Managed Lustre signals, including:

- `AzureManagedLustreCollectorStale` — collection has not completed recently.
- `AzureManagedLustreSampleStale` — per-OST Azure Monitor sample is stale.
- `AzureManagedLustreMdtSampleStale` and `AzureManagedLustreFilesystemSampleStale` — MDT or derived filesystem sample freshness is stale.
- `AzureManagedLustreCollectionErrors` — non-zero collection error rate.
- `AzureManagedLustreOstAvailablePercentLow` — OST available capacity below threshold.
- `AzureManagedLustreOstUsedPercentWarn` and `AzureManagedLustreOstUsedPercentCritical` — OST used-capacity guardrails.
- `AzureManagedLustreMdtBytesUsedPercentWarn` and `AzureManagedLustreMdtBytesUsedPercentCritical` — MDT byte-capacity guardrails.
- `AzureManagedLustreMdtInodeUsedPercentWarn` and `AzureManagedLustreMdtInodeUsedPercentCritical` — MDT inode/file-count guardrails.
- `AzureManagedLustreMdtLatencyWarn`, `AzureManagedLustreMdtLatencySerious`, and `AzureManagedLustreMdtLatencyHang` — MDT latency risk bands.
- `AzureManagedLustreHsmActionErrors`, `AzureManagedLustreHsmBacklog`, and `AzureManagedLustreHsmBacklogCritical` — HSM reliability and backlog alerts.
- `AzureManagedLustreClientEvictions` and `AzureManagedLustreClientEvictionBurst` — client reconnect/disruption alerts.
- `AzureManagedLustreMetadataAmplificationHigh` and `AzureManagedLustreMetadataAmplificationCritical` — AV/HPC metadata-heavy workload alerts.
- `AzureManagedLustreExporterNoLeader` — leader-election failure, which pauses collection.

[deploy/vmss-alert-rules.yaml](deploy/vmss-alert-rules.yaml) ships VMSS alert rules, including:

- `AzureVmssExporterStale` — VMSS Resource Graph collection has not completed recently.
- `AzureVmssCollectionErrors` — non-zero VMSS collection error rate.
- `AzureVmssCapacityDrift` — desired VMSS capacity differs from observed VM instances for a sustained period.
- `AzureVmInventoryStale` — standalone VM Resource Graph inventory has not completed recently.
- `AzureVmInventoryCollectionErrors` — non-zero standalone VM inventory collection error rate.

Apply with `kubectl apply -f deploy/lustre-alert-rules.yaml` and `kubectl apply -f deploy/vmss-alert-rules.yaml` against your Prometheus operator namespace, or import the groups into your alert manager of choice.

## Azure Monitor managed Prometheus

If you are scraping the exporter from Azure Monitor managed Prometheus, the
[deploy/ama-metrics-settings-configmap-v1.yaml](deploy/ama-metrics-settings-configmap-v1.yaml)
ConfigMap (namespace `kube-system`) defines a custom scrape job that targets the
stable Kubernetes Service DNS name (`vmss-metrics-exporter.default.svc.cluster.local:8000`).
This avoids pod-target churn during rollouts and keeps dashboard time series
continuous. The Service itself selects only the elected leader pod and publishes
not-ready addresses so a terminating leader remains scrapeable during the
configured shutdown drain window. Apply the custom scrape config once per
cluster:

```bash
kubectl apply -f deploy/ama-metrics-settings-configmap-v1.yaml
```

## Prometheus examples

See [docs/metrics.md](docs/metrics.md#promql-examples) for VMSS, standalone VM,
Managed Lustre, and collection-health PromQL examples.

## High availability

When running more than one replica, enable Kubernetes leader election so only one Pod actively queries Azure Resource Graph and Azure Monitor:

```yaml
env:
  - name: LEADER_ELECTION_ENABLED
    value: "true"
  - name: LEADER_ELECTION_LOCK_NAME
    value: vmss-metrics-exporter
  - name: LEADER_ELECTION_NAMESPACE
    valueFrom:
      fieldRef:
        fieldPath: metadata.namespace
```

The leader holds a `coordination.k8s.io/Lease`; standby replicas keep `/metrics` served but skip collection until they win the lease. RBAC for the lease is in [deploy/kubernetes.yaml](deploy/kubernetes.yaml).

For smoother rolling updates, set `SHUTDOWN_DRAIN_SECONDS` (for example `15`) and
ensure `terminationGracePeriodSeconds` is larger than that value. This lets the
terminating leader continue serving cached metrics briefly after releasing the
Lease while the new leader acquires and warms up.

The metrics Service should keep `publishNotReadyAddresses: true` together with
the leader-only selector. Kubernetes marks a terminating pod not-ready before the
new leader has necessarily acquired the Lease and collected metrics; publishing
not-ready addresses keeps the old leader reachable only during the drain window.
It does not expose idle followers because the Service selector still requires
`vmss-metrics-exporter-leader=true`.

## Pressure testing Azure Managed Lustre

The `scripts/` and `deploy/pressure-test/` directories contain an AV-simulation
workload used to validate Azure Managed Lustre throughput, tail latency, and
stability from AKS:

- [scripts/av_lustre_workload.py](scripts/av_lustre_workload.py) — simulator with four modes: `discover`, `read-only`, `read-write-output`, `verify-output`. Enforces three immutability guardrails on the source dataset.
- [scripts/av_pressure_phase.sh](scripts/av_pressure_phase.sh) — drives a single phase (smoke, ramp-N, hotset, metadata-heavy, heavy-write, soak) and collects per-pod summaries.
- [scripts/av_pressure_all_phases.sh](scripts/av_pressure_all_phases.sh) — runs the full phase ladder end-to-end.
- [scripts/lustre_safe_write_budget.py](scripts/lustre_safe_write_budget.py) — pre-phase capacity / write-budget calculator (default policy: stop before 80 % used).
- [deploy/pressure-test/](deploy/pressure-test/) — Job manifests, PVC example, Azure Lustre CSI node RBAC, and the workload ConfigMap.

The full procedure, safety rules, and pass/fail criteria live in
[docs/lustre-pressure-test.md](docs/lustre-pressure-test.md). Production tuning
guidance is in
[docs/azure-managed-lustre-production-best-practices-v2.md](docs/azure-managed-lustre-production-best-practices-v2.md).

> Do not run any phase except `discover` against a shared production filesystem
> without prior approval and a documented rollback window. The dataset root is
> read-only; all writes are confined to `RESULT_ROOT/<RUN_ID>/<pod-name>/`.

## Development

This repository ships an existing virtualenv at `.venv/`. Activate it before
running local validation:

```bash
source .venv/bin/activate

make install   # editable install with dev extras
make test      # pytest -q
make lint      # ruff check .
make validate  # test + lint
```

Prefer the `Makefile` targets over ad-hoc commands. For small changes, run
targeted tests first (for example `pytest tests/test_collector.py -q`) and then
`make validate` once before opening a PR.
