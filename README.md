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

### VMSS metrics

| Metric | Description |
| --- | --- |
| `azure_vmss_instance_count` | Actual VM count for each VMSS. |
| `azure_vmss_capacity` | Desired VMSS capacity from Azure. |
| `azure_vmss_info` | VMSS metadata. Value is always `1`. |
| `azure_vmss_exporter_vmss_total` | Number of VMSS discovered in the latest successful collection. |
| `azure_vmss_exporter_last_success_timestamp_seconds` | Last successful VMSS collection timestamp. |
| `azure_vmss_exporter_collection_duration_seconds` | Latest VMSS collection duration. |
| `azure_vmss_exporter_collection_errors_total` | VMSS collection error counter. |

### Managed Lustre inventory metrics

| Metric | Description |
| --- | --- |
| `azure_managed_lustre_discovered_filesystem_info` | Table-friendly identity metadata for each discovered Managed Lustre filesystem. Labels: `subscription_id`, `resource_group`, `filesystem_name`, `location`, and `sku_tier`. Value is always `1`. |
| `azure_managed_lustre_filesystem_info` | Metadata for each discovered Managed Lustre filesystem. Value is always `1`. |
| `azure_managed_lustre_filesystem_storage_capacity_tib` | Configured filesystem capacity in TiB. |
| `azure_managed_lustre_filesystem_total` | Number of Managed Lustre filesystems discovered. |

### Managed Lustre key metrics

| Metric | Description |
| --- | --- |
| `azure_managed_lustre_ost_bytes_available` | OST bytes available. |
| `azure_managed_lustre_ost_bytes_used` | OST bytes used. |
| `azure_managed_lustre_ost_bytes_total` | OST bytes total. |
| `azure_managed_lustre_ost_bytes_available_percent` | Derived OST available percentage. |
| `azure_managed_lustre_ost_bytes_used_percent` | Derived OST used percentage. |
| `azure_managed_lustre_client_read_ops` | Client read operations. |
| `azure_managed_lustre_client_read_throughput_bytes_per_second` | Client read throughput. |
| `azure_managed_lustre_client_write_ops` | Client write operations. |
| `azure_managed_lustre_client_write_throughput_bytes_per_second` | Client write throughput. |
| `azure_managed_lustre_ost_connected_clients` | OST connected client count / exports (`OSTConnectedClients`). Approximates clients seen by each OST; per-OST variance flags failover or eviction. |
| `azure_managed_lustre_ost_sample_timestamp_seconds` | Azure Monitor sample timestamp for each OST metric series. |
| `azure_managed_lustre_ost_client_latency_milliseconds` | OST client latency. |
| `azure_managed_lustre_ost_client_ops` | OST client operations. |
| `azure_managed_lustre_ost_operation_sample_timestamp_seconds` | Azure Monitor sample timestamp for each OST operation metric series. |
| `azure_managed_lustre_mdt_bytes_available` | MDT bytes available. |
| `azure_managed_lustre_mdt_bytes_used` | MDT bytes used. |
| `azure_managed_lustre_mdt_bytes_total` | MDT bytes total. |
| `azure_managed_lustre_mdt_bytes_available_percent` | Derived MDT available percentage. |
| `azure_managed_lustre_mdt_bytes_used_percent` | Derived MDT used percentage. |
| `azure_managed_lustre_mdt_files_free` | MDT free file/inode count. |
| `azure_managed_lustre_mdt_files_used` | MDT used file/inode count. |
| `azure_managed_lustre_mdt_files_total` | MDT total file/inode count. |
| `azure_managed_lustre_mdt_files_free_percent` | Derived MDT file/inode free percentage. |
| `azure_managed_lustre_mdt_files_used_percent` | Derived MDT file/inode used percentage. |
| `azure_managed_lustre_mdt_sample_timestamp_seconds` | Azure Monitor sample timestamp for each MDT metric series. |
| `azure_managed_lustre_mdt_client_latency_milliseconds` | MDT client latency. |
| `azure_managed_lustre_mdt_client_ops` | MDT client operations. |
| `azure_managed_lustre_mdt_operation_sample_timestamp_seconds` | Azure Monitor sample timestamp for each MDT operation metric series. |
| `azure_managed_lustre_hsm_action_errors` | HSM action errors (`HSMActionErrors`). |
| `azure_managed_lustre_hsm_current_requests` | HSM in-flight requests (`HSMCurrentRequests`). |
| `azure_managed_lustre_mdt_client_evictions` | Client evictions (`LustreClientEvictions`) from Azure Monitor. This is the latest Azure Monitor interval-total sample per MDT, or `mdtnum="all"` when Azure returns an aggregate series. |
| `azure_managed_lustre_filesystem_client_evictions` | Derived filesystem-level client evictions, summed across MDTs or copied from the aggregate `mdtnum="all"` series. This is an interval-total gauge, not a Prometheus counter; use `max_over_time` or `sum_over_time` in PromQL. |
| `azure_managed_lustre_mdt_connected_clients` | MDT connected client count / exports (`MDTConnectedClients`). Each mounted client opens one export per MDT, so `max by (filesystem_name)` of this metric approximates the per-filesystem client count. |
| `azure_managed_lustre_filesystem_connected_clients` | Derived per-filesystem connected client count, computed as the maximum MDT connected-client value for the filesystem. |
| `azure_managed_lustre_metadata_amplification_ratio` | Derived AV/HPC metadata pressure signal: `sum(MDTClientOps) / max(sum(ClientReadOps + ClientWriteOps), 1)`. |
| `azure_managed_lustre_filesystem_sample_max_age_seconds` | Derived age of the oldest Azure Monitor OST/MDT sample for each filesystem. |
| `azure_managed_lustre_ost_sample_count` | Number of OST sample series produced by the latest collection. This approximates OST count only when every OST reports at least one metric. |
| `azure_managed_lustre_last_success_timestamp_seconds` | Last successful Managed Lustre collection timestamp. |
| `azure_managed_lustre_collection_duration_seconds` | Latest Managed Lustre collection duration. |
| `azure_managed_lustre_collection_errors_total` | Managed Lustre collection error counter. |

Managed Lustre labels include `subscription_id`, `resource_group`, `filesystem_name`, and `location`. OST metrics also include `ostnum`; MDT metrics also include `mdtnum`. If Azure Monitor returns an aggregate series without OST or MDT dimensions, the exporter uses `ostnum="all"` or `mdtnum="all"`.

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
make image IMAGE=<repo>/vmss-metrics-exporter TAG=<tag>
make push IMAGE=<repo>/vmss-metrics-exporter TAG=<tag>
```

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
make deploy-image IMAGE=<repo>/vmss-metrics-exporter TAG=<tag>
make rollout
```

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

Apply with `kubectl apply -f deploy/lustre-alert-rules.yaml` and `kubectl apply -f deploy/vmss-alert-rules.yaml` against your Prometheus operator namespace, or import the groups into your alert manager of choice.

## Azure Monitor managed Prometheus

If you are scraping the exporter from Azure Monitor managed Prometheus, the
[deploy/ama-metrics-settings-configmap-v1.yaml](deploy/ama-metrics-settings-configmap-v1.yaml)
ConfigMap (namespace `kube-system`) defines a custom scrape job that targets the
stable Kubernetes Service DNS name (`vmss-metrics-exporter.default.svc.cluster.local:8000`).
This avoids pod-target churn during rollouts and keeps dashboard time series
continuous. Apply it once per cluster:

```bash
kubectl apply -f deploy/ama-metrics-settings-configmap-v1.yaml
```

## Prometheus examples

```promql
# VMSS desired vs actual
azure_vmss_capacity
azure_vmss_instance_count

# VMSS count by subscription
sum by (subscription_id) (azure_vmss_instance_count)

# Managed Lustre inventory
azure_managed_lustre_filesystem_info

# Managed Lustre OST available percentage
azure_managed_lustre_ost_bytes_available_percent

# Managed Lustre MDT file free percentage
azure_managed_lustre_mdt_files_free_percent

# Managed Lustre metadata amplification for AV/tiny-file workloads
azure_managed_lustre_metadata_amplification_ratio

# Managed Lustre filesystem telemetry freshness
azure_managed_lustre_filesystem_sample_max_age_seconds

# Managed Lustre filesystem connected clients and evictions
azure_managed_lustre_filesystem_connected_clients
azure_managed_lustre_filesystem_client_evictions

# Managed Lustre read/write throughput by filesystem
sum by (filesystem_name) (azure_managed_lustre_client_read_throughput_bytes_per_second)
sum by (filesystem_name) (azure_managed_lustre_client_write_throughput_bytes_per_second)

# Collection health
time() - azure_managed_lustre_last_success_timestamp_seconds
rate(azure_managed_lustre_collection_errors_total[5m])
```

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
