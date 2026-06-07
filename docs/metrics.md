# Metrics Reference

This document is the detailed Prometheus metric catalog for the Azure VMSS, standalone VM, and Azure Managed Lustre exporter. For quick setup and deployment examples, see [../README.md](../README.md).

## VMSS metrics

| Metric | Description |
| --- | --- |
| `azure_vmss_instance_count` | Actual VM count for each VMSS. |
| `azure_vmss_capacity` | Desired VMSS capacity from Azure. |
| `azure_vmss_info` | VMSS metadata. Value is always `1`. |
| `azure_vmss_exporter_vmss_total` | Number of VMSS discovered in the latest successful collection. |
| `azure_vmss_exporter_last_success_timestamp_seconds` | Last successful VMSS collection timestamp. |
| `azure_vmss_exporter_collection_duration_seconds` | Latest VMSS collection duration. |
| `azure_vmss_exporter_collection_errors_total` | VMSS collection error counter. |
| `azure_vmss_exporter_is_leader` | Whether this replica is the active leader. `1` means leader, `0` means follower. |

VMSS resource labels include `subscription_id`, `resource_group`, `vmss_name`, `location`, and `orchestration_mode`. `azure_vmss_info` also includes `vm_size` and `sku_tier`.

## Standalone VM inventory metrics

Standalone VM metrics include only non-VMSS Azure VMs. VMSS members are intentionally excluded so these series never double-count against `azure_vmss_instance_count`.

| Metric | Description |
| --- | --- |
| `azure_vm_info` | Static inventory of standalone Azure VMs. Labels: `subscription_id`, `resource_group`, `vm_name`, `vm_id`, `location`, `zone`, `vm_size`, `os_type`. Value is always `1`. Join via `* on (subscription_id, resource_group, vm_name) group_left(...)`. |
| `azure_vm_power_state` | Current normalized power state per VM. Extra label `state` in `{running, stopped, deallocated, starting, stopping, unknown}`. Exactly one state series per VM has value `1` at a time. Splitting state off `azure_vm_info` keeps the info metric churn-free across power cycles. |
| `azure_vm_count_by_size` | Number of standalone VMs aggregated by `vm_size`. Always emitted, even when per-VM series are suppressed by the cardinality guardrail. |
| `azure_vm_exporter_vm_total` | Number of standalone VMs observed in the latest successful collection. |
| `azure_vm_exporter_last_success_timestamp_seconds` | Last successful standalone VM inventory collection timestamp. |
| `azure_vm_exporter_collection_duration_seconds` | Latest standalone VM inventory collection duration. |
| `azure_vm_exporter_collection_errors_total` | Standalone VM inventory collection error counter. |

Worst-case series budget per VM: 8 (`azure_vm_info`) + 6 (`azure_vm_power_state`, one per state) = **14 series per VM**, plus one shared `azure_vm_count_by_size` series per distinct SKU. Subscriptions with more than `STANDALONE_VM_MAX_INVENTORY` standalone VMs (default `5000`) trip the cardinality guardrail: the per-VM `azure_vm_info` and `azure_vm_power_state` series are suppressed and only the bounded `azure_vm_count_by_size` aggregate plus `azure_vm_exporter_vm_total` scalar are emitted.

## Managed Lustre inventory metrics

| Metric | Description |
| --- | --- |
| `azure_managed_lustre_discovered_filesystem_info` | Table-friendly identity metadata for each discovered Managed Lustre filesystem. Labels: `subscription_id`, `resource_group`, `filesystem_name`, `location`, and `sku_tier`. Value is always `1`. |
| `azure_managed_lustre_filesystem_info` | Metadata for each discovered Managed Lustre filesystem. Value is always `1`. |
| `azure_managed_lustre_filesystem_storage_capacity_tib` | Configured filesystem capacity in TiB. |
| `azure_managed_lustre_filesystem_total` | Number of Managed Lustre filesystems discovered. |

## Managed Lustre OST metrics

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

## Managed Lustre MDT metrics

| Metric | Description |
| --- | --- |
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
| `azure_managed_lustre_mdt_connected_clients` | MDT connected client count / exports (`MDTConnectedClients`). Each mounted client opens one export per MDT, so `max by (filesystem_name)` of this metric approximates the per-filesystem client count. |

## Managed Lustre derived filesystem metrics

| Metric | Description |
| --- | --- |
| `azure_managed_lustre_filesystem_client_evictions` | Derived filesystem-level client evictions, summed across MDTs or copied from the aggregate `mdtnum="all"` series. This is an interval-total gauge, not a Prometheus counter; use `max_over_time` or `sum_over_time` in PromQL. |
| `azure_managed_lustre_filesystem_connected_clients` | Derived per-filesystem connected client count, computed as the maximum MDT connected-client value for the filesystem. |
| `azure_managed_lustre_metadata_amplification_ratio` | Derived AV/HPC metadata pressure signal: `sum(MDTClientOps) / max(sum(ClientReadOps + ClientWriteOps), 1)`. |
| `azure_managed_lustre_filesystem_sample_max_age_seconds` | Derived age of the oldest Azure Monitor OST/MDT sample for each filesystem. |
| `azure_managed_lustre_ost_sample_count` | Number of OST sample series produced by the latest collection. This approximates OST count only when every OST reports at least one metric. |
| `azure_managed_lustre_last_success_timestamp_seconds` | Last successful Managed Lustre collection timestamp. |
| `azure_managed_lustre_collection_duration_seconds` | Latest Managed Lustre collection duration. |
| `azure_managed_lustre_collection_errors_total` | Managed Lustre collection error counter. |

Managed Lustre labels include `subscription_id`, `resource_group`, `filesystem_name`, and `location`. OST metrics also include `ostnum`; MDT metrics also include `mdtnum`. If Azure Monitor returns an aggregate series without OST or MDT dimensions, the exporter uses `ostnum="all"` or `mdtnum="all"`.

## PromQL examples

```promql
# VMSS desired vs actual
azure_vmss_capacity
azure_vmss_instance_count

# VMSS count by subscription
sum by (subscription_id) (azure_vmss_instance_count)

# VMSS top-N panels that tolerate short rollout scrape gaps
last_over_time(azure_vmss_instance_count[10m])

# Standalone VM inventory
azure_vm_exporter_vm_total
azure_vm_info
azure_vm_power_state
azure_vm_count_by_size

# Standalone VM power-state count
sum by (state) (
  (azure_vm_power_state == 1)
  * on (subscription_id, resource_group, vm_name) group_left ()
  azure_vm_info
)

# Standalone VM count by size
sum by (vm_size) (azure_vm_count_by_size)

# Managed Lustre inventory
azure_managed_lustre_filesystem_info
azure_managed_lustre_discovered_filesystem_info

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
time() - azure_vmss_exporter_last_success_timestamp_seconds
rate(azure_vmss_exporter_collection_errors_total[5m])
time() - azure_vm_exporter_last_success_timestamp_seconds
rate(azure_vm_exporter_collection_errors_total[5m])
time() - azure_managed_lustre_last_success_timestamp_seconds
rate(azure_managed_lustre_collection_errors_total[5m])
```
