# Release Notes

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
