# Release Notes

## Unreleased - 2026-06-07 - Standalone VM inventory and rollout handoff

### Overview

Adds standalone (non-VMSS) Azure VM inventory to the exporter and the VMSS Grafana dashboard so operators can see the full Azure VM footprint (not just scale set members) without spinning up a second tool. This update also hardens rolling updates for the HA exporter deployment and documents the required multi-arch image workflow for AKS.

### Highlights

- New backend collector `AzureResourceGraphStandaloneVmCollector` discovers standalone Azure VMs via Resource Graph, reusing the existing `ResourceGraphClient` (shared credential, token cache, and throttling bucket).
- New metrics:
  - `azure_vm_info{subscription_id, resource_group, vm_name, vm_id, location, zone, vm_size, os_type}` — stable per-VM identity, value `1`.
  - `azure_vm_power_state{subscription_id, resource_group, vm_name, state}` — one series per VM per normalized state in `{running, stopped, deallocated, starting, stopping, unknown}`; exactly one series per VM has value `1`. Splitting state off the info metric keeps Prometheus from creating brand-new series on every start/stop transition.
  - `azure_vm_count_by_size{vm_size}` — bounded aggregate that always emits, even when per-VM series are suppressed by the cardinality guardrail.
  - `azure_vm_exporter_vm_total`, `azure_vm_exporter_last_success_timestamp_seconds`, `azure_vm_exporter_collection_duration_seconds`, `azure_vm_exporter_collection_errors_total`.
- New collapsible `Standalone VM inventory (non-VMSS)` row added to `deploy/grafana-dashboard-vmss.json` (panels: stat-strip by power state, total observed, table by `vm_size`, per-VM table joined with current state).
- New alert rules in `deploy/vmss-alert-rules.yaml`:
  - `AzureVmInventoryStale` — fires when the last successful inventory collection is older than 10 minutes.
  - `AzureVmInventoryCollectionErrors` — fires when `azure_vm_exporter_collection_errors_total` is increasing.
- Cardinality guardrail `STANDALONE_VM_MAX_INVENTORY` (default `5000`) suppresses per-VM `azure_vm_info` / `azure_vm_power_state` series above the threshold but keeps the bounded `azure_vm_count_by_size` aggregate and `azure_vm_exporter_vm_total` scalar.
- New configuration:
  - `ENABLE_STANDALONE_VM_INVENTORY` (default `true`)
  - `STANDALONE_VM_MAX_INVENTORY` (default `5000`, range 100–100000)
- Standalone-VM collection runs on the existing VMSS poll cadence and is isolated from VMSS / Managed Lustre via `with suppress(Exception)` plus a dedicated error counter, so a Resource Graph failure on the inventory query can never break the VMSS or Lustre exporters.
- Kubernetes manifest now uses image tag `zhangchl007/vmss-metrics-exporter:v36-rollout-handoff`, keeps two replicas with native Lease leader election, and sets `publishNotReadyAddresses: true` on the leader-only metrics Service. This keeps the terminating leader reachable during `SHUTDOWN_DRAIN_SECONDS` while the new leader acquires the Lease, collects once, and patches the `vmss-metrics-exporter-leader=true` label.
- Leader election now follows Kubernetes projected ServiceAccount token rotation best practice: the Kubernetes client refresh path rereads the mounted token file and keeps both Python-client auth keys (`authorization` and `BearerToken`) synchronized. If the API still returns `401 Unauthorized`, the exporter reloads in-cluster credentials, rebuilds the Lease client, and retries the failed Lease read/create/replace once. This prevents stale-token 401 loops from pausing acquisition or renewal.
- VMSS Grafana panels that are sensitive to brief rollout scrape gaps now use `last_over_time(...[10m])`:
  - `VMSS observed`
  - `Top VMSS instance count`
  - `Top VMSS timeline`
- The `Standalone VMs by power state` dashboard query now explicitly parenthesizes `(azure_vm_power_state == 1)` before joining to `azure_vm_info`, avoiding ambiguous PromQL operator precedence.
- Image build guidance now requires `make image-multiarch TAG=<tag>` for AKS images. A `docker-container` buildx builder is required for true `linux/amd64,linux/arm64` manifests; single-arch `make image` is only for local smoke tests.
- README now stays sample-oriented; the detailed metric catalog lives in `docs/metrics.md`, and Grafana / Prometheus operations live in `docs/grafana-prometheus.md`.

### Notes

- Worst-case per-VM series budget: `8` (info) + `6` (power_state) = `14` series per VM, plus one shared `azure_vm_count_by_size` series per distinct SKU.
- Verified live v36 exporter scrape after rollout: 20 VMSS, 14 standalone VMs, 84 standalone power-state series, 6 VM-size buckets, and zero VMSS / standalone-VM collection errors.
- Resolves issue [#11](https://github.com/zhangchl007/azure-lustre-vmss-metrics/issues/11).


## Unreleased - 2026-06-07

### Overview

This release focuses on making Azure Managed Lustre and VMSS observability easier to use during operations and customer discussions, especially for AV/HPC workloads where metadata pressure can be more important than raw throughput.

The main theme is dashboard clarity: the Managed Lustre dashboard now follows an AV/HPC Lustre operations flow from health, to metadata triage, OST data path, storage capacity, client I/O, metadata performance, reliability and freshness, and inventory.

### Highlights

- Reworked the Managed Lustre Grafana dashboard into 49 AV/HPC workflow-oriented panels.
- Added a `Metadata triage` dashboard row for FSx-for-Lustre versus AMLFS metadata-bottleneck discussions.
- Added a dedicated `OST data path` row so data-path latency, OST operation volume, and OST connected-client visibility remain first-class dashboard signals.
- Fixed Grafana freshness panels showing an epoch-sized age, about 56 years, when exporter timestamp gauges are reset to `0`.
- Added VMSS alert rules for stale collection, collection errors, and sustained desired-versus-actual capacity drift.
- Expanded Managed Lustre alert coverage for derived filesystem sample freshness and metadata amplification.
- Updated README metric and alert documentation for derived Managed Lustre signals.
- Added AV workload guidance that maps customer symptoms to exported metrics and dashboard panels.

### Managed Lustre Dashboard

Updated `deploy/grafana-dashboard-lustre.json`:

- Reorganized the dashboard into eight AV/HPC Lustre operational rows:
  - `Overview`
  - `Metadata triage`
  - `OST data path`
  - `Storage capacity`
  - `Client I/O`
  - `Metadata performance`
  - `Reliability and freshness`
  - `Inventory`
- Removed redundant panels that repeated the same signal as separate worst-value, count, and trend views.
- Kept the highest-value signals for AV/HPC triage:
  - metadata amplification
  - MDT latency
  - MDT operation volume
  - MDT inode usage
  - MDT byte usage
  - OST used capacity
  - OST latency and operation volume
  - OST connected clients
  - client read/write throughput and ops
  - MDT ops per connected client
  - HSM backlog and HSM action errors
  - client evictions
  - filesystem sample freshness
  - discovered Lustre filesystem inventory with an explicit identity metric, deduplicated OST/MDT metric-series fallback, and label-to-column transformation
- Simplified the dashboard Overview row into 5 high-signal Grafana stat cards: collector age, collection errors, OST used max, MDT latency max, and evictions.
- Removed the redundant `OST free min` card from Overview; detailed free-space context remains in the `Lowest OST free-space` table.
- Simplified the Metadata triage row into 4 focused metadata-pressure cards: amplification, MDT ops per client, MDT inode used, and MDT bytes used.
- Removed the duplicated scalar `Lowest OST free` card from Storage capacity and kept the detailed `Lowest OST free-space` table.
- Replaced the count-only OST free-space card with a `Lowest OST free-space` table that lists the filesystem, OST, free bytes, resource group, region, and subscription, with warning/critical coloring at 1 TiB and 100 GiB.
- Replaced non-actionable `Top MDT latency now` / `Top MDT ops now` ranking tables with a single `MDT latency incidents now` table that only shows MDT operations currently above the 100 ms warning threshold.
- Replaced non-actionable `Top OST latency now` / `Top OST ops now` ranking tables with a single `OST latency incidents now` table that only shows OST operations currently above the 100 ms warning threshold.
- Restored explicit warning/critical threshold coloring for capacity-count cards and HSM backlog panels.
- Removed a misleading warning threshold from `Provisioned TiB` and renamed the lower metadata trend panel to avoid duplicating the triage stat title.
- Updated the dashboard description to reflect the AV/HPC Lustre operational workflow.
- Fixed `Collector freshness` to ignore zero-valued reset timestamps before calculating age.
- Incremented the dashboard version to `34`.

Updated `deploy/grafana-dashboard-vmss.json`:

- Fixed `Exporter freshness` to ignore zero-valued reset timestamps before calculating age.

### Alert Rules

Updated `deploy/lustre-alert-rules.yaml`:

- Added `AzureManagedLustreFilesystemSampleStale` for stale derived filesystem-level telemetry.
- Added `AzureManagedLustreMetadataAmplificationHigh` for metadata-heavy workloads above a warning threshold.
- Added `AzureManagedLustreMetadataAmplificationCritical` for severe metadata amplification.

Added `deploy/vmss-alert-rules.yaml`:

- `AzureVmssExporterStale` warns when VMSS Resource Graph collection is stale.
- `AzureVmssCollectionErrors` warns on non-zero VMSS collection error rate.
- `AzureVmssCapacityDrift` warns when desired VMSS capacity differs from observed instance count for 15 minutes.

### Documentation

Updated `README.md`:

- Documented derived Managed Lustre metrics that are now important dashboard and alert inputs:
  - `azure_managed_lustre_filesystem_connected_clients`
  - `azure_managed_lustre_metadata_amplification_ratio`
  - `azure_managed_lustre_filesystem_sample_max_age_seconds`
  - `azure_managed_lustre_ost_sample_count`
  - OST/MDT sample timestamp metrics
- Documented the AV/HPC Managed Lustre dashboard layout, `Metadata triage` row, and `OST data path` row.
- Expanded alert-rule documentation for Managed Lustre and VMSS alert groups.
- Added PromQL examples for metadata amplification, filesystem sample age, connected clients, and evictions.

Updated `docs/av-industry.md` in the workspace:

- Added FSx for Lustre metadata IOPS context.
- Added Lustre MDT/OST architecture context.
- Added AV metadata storm examples.
- Added small-file reduction guidance for tar shards, WebDataset, Parquet, TFRecord, and RecordIO.
- Added a customer discussion guide for workloads that behave differently on FSx for Lustre and AMLFS.
- Added a metric mapping from AV symptoms to exporter metrics.

Note: `docs/av-industry.md` is currently ignored by `.gitignore`. If this document should ship with the release, remove the ignore rule or force-add the file intentionally.

### Operational Impact

- No exporter metric names or label shapes changed.
- No Python collector behavior changed.
- Existing Prometheus series remain compatible with the updated dashboard and alert rules.
- The new VMSS alert file is independent of the Managed Lustre alert file and can be applied separately.
- The AV/HPC operations dashboard should be re-imported into Grafana to replace the older, more redundant Managed Lustre dashboard.

### Validation

Performed local validation:

- Parsed `deploy/grafana-dashboard-lustre.json` with `python3 -m json.tool`.
- Parsed alert rule YAML files with Python/YAML tooling during implementation.
- Ran `git diff --check` on edited dashboard and documentation files.
- Checked VS Code diagnostics for the edited dashboard, alert rules, README, and AV documentation.

Not performed:

- `promtool check rules`, because `promtool` is not installed in the current environment.
- End-to-end Grafana import test against a live Grafana instance.
- Live Prometheus query execution against a cluster.

### Upgrade Notes

1. Re-import `deploy/grafana-dashboard-lustre.json` into Grafana.
2. Apply Managed Lustre alerts with `kubectl apply -f deploy/lustre-alert-rules.yaml` or import the rule group into your alerting system.
3. Apply VMSS alerts with `kubectl apply -f deploy/vmss-alert-rules.yaml` if VMSS drift and collection health alerting are desired.
4. Review metadata amplification thresholds (`10` warning, `100` critical) against real workload baselines and tune if needed.
5. Decide whether `docs/av-industry.md` should be tracked in Git for release packaging.
