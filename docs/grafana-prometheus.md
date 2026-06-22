# Grafana and Prometheus Operations

This document covers the Grafana dashboards, Prometheus alert rules, Azure Monitor managed Prometheus scrape configuration, and common query examples for the exporter.

For the full metric catalog, see [metrics.md](metrics.md). For deployment and runtime configuration, see [../README.md](../README.md).

## Grafana dashboards

Import the dashboards from `deploy/`:

- [../deploy/grafana-dashboard-vmss.json](../deploy/grafana-dashboard-vmss.json)
- [../deploy/grafana-dashboard-lustre.json](../deploy/grafana-dashboard-lustre.json)

### VMSS dashboard

The VMSS dashboard covers:

- VMSS inventory and actual instance counts.
- Desired capacity versus observed instances.
- Standalone, non-VMSS Azure VM inventory by VM size and power state.
- Per-VM standalone inventory joined with current power state.
- Rollout-tolerant VMSS overview panels using `last_over_time(...[10m])` for short scrape gaps.

The standalone VM panels use `azure_vm_info`, `azure_vm_power_state`, and `azure_vm_count_by_size`. VMSS members are intentionally excluded from standalone VM inventory so VMSS and standalone VM counts do not double-count.

### Managed Lustre dashboard

The Managed Lustre dashboard uses filesystem inventory metrics for dropdowns, so discovered filesystems remain visible even when Azure Monitor has no current OST or MDT sample for a filesystem.

The dashboard follows an AV/HPC Lustre operations layout with rows for overview, metadata triage, OST data path, storage capacity, client I/O, metadata performance, reliability and freshness, and inventory.

The `Metadata triage` row groups metadata amplification, MDT latency, MDT operations, MDT inode usage, and MDT byte usage for FSx-for-Lustre versus AMLFS customer discussions. The `OST data path` row keeps data-path latency, operations, and connected-client visibility for large sensor logs and write-heavy phases.

For AV-specific interpretation of metadata-heavy workload signals, see [av-industry.md](av-industry.md).

## Prometheus alert rules

### Managed Lustre alerts

[../deploy/lustre-alert-rules.yaml](../deploy/lustre-alert-rules.yaml) ships alert rules for Managed Lustre signals, including:

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

### VMSS and standalone VM alerts

[../deploy/vmss-alert-rules.yaml](../deploy/vmss-alert-rules.yaml) ships alert rules for VMSS and standalone VM inventory, including:

- `AzureVmssExporterStale` — VMSS Resource Graph collection has not completed recently.
- `AzureVmssCollectionErrors` — non-zero VMSS collection error rate.
- `AzureVmssCapacityDrift` — desired VMSS capacity differs from observed VM instances for a sustained period.
- `AzureVmInventoryStale` — standalone VM Resource Graph inventory has not completed recently.
- `AzureVmInventoryCollectionErrors` — non-zero standalone VM inventory collection error rate.

Apply the rule files with `kubectl apply -f deploy/lustre-alert-rules.yaml` and `kubectl apply -f deploy/vmss-alert-rules.yaml` against your Prometheus operator namespace, or import the groups into your alert manager of choice.

## Azure Monitor managed Prometheus

If you are scraping the exporter from Azure Monitor managed Prometheus, [../deploy/ama-metrics-settings-configmap-v1.yaml](../deploy/ama-metrics-settings-configmap-v1.yaml) defines a custom scrape job that targets the stable Kubernetes Service DNS name:

```text
vmss-metrics-exporter.default.svc.cluster.local:8000
```

This avoids pod-target churn during rollouts and keeps dashboard time series continuous. The Service itself selects only the elected leader pod and publishes not-ready addresses so a terminating leader remains scrapeable during the configured shutdown drain window.

Apply the custom scrape config once per cluster:

```bash
kubectl apply -f deploy/ama-metrics-settings-configmap-v1.yaml
```

## PromQL examples

More metric-specific examples are in [metrics.md](metrics.md#promql-examples).

```promql
# VMSS observed, bridged across short rollout scrape gaps
max(last_over_time(azure_vmss_exporter_vmss_total{job="$job"}[10m]))

# Top VMSS timeline query shape
last_over_time(azure_vmss_instance_count{job="$job"}[10m])

# Standalone VM count by power state
sum by (state) (
  (azure_vm_power_state == 1)
  * on (subscription_id, resource_group, vm_name) group_left ()
  azure_vm_info
)

# Standalone VM count by size
sum by (vm_size) (azure_vm_count_by_size)

# Managed Lustre collector freshness
time() - max(azure_managed_lustre_last_success_timestamp_seconds > 0)

# VMSS collection errors
rate(azure_vmss_exporter_collection_errors_total[5m])

# Standalone VM inventory errors
rate(azure_vm_exporter_collection_errors_total[5m])
```
