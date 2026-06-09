# Managed Lustre Metadata Dashboard Parameters

This document explains the metadata-related parameters in the Managed Lustre Grafana dashboard. It focuses on MDT/MDS behavior: namespace lookups, directory scans, `stat` and `getattr`, file creation and removal, rename, open and close, inode allocation, and metadata-side locking.

For the raw metric catalog, see [metrics.md](metrics.md). For dashboard import and alert-rule operations, see [grafana-prometheus.md](grafana-prometheus.md).

Official references:

- [Azure Managed Lustre monitoring data reference](https://learn.microsoft.com/en-us/azure/azure-managed-lustre/monitor-azure-managed-lustre-reference)
- [Azure Monitor Metrics overview: multi-dimensional metrics](https://learn.microsoft.com/en-us/azure/azure-monitor/metrics/data-platform-metrics#multi-dimensional-metrics)

## Azure Monitor / Exporter Scope

This dashboard uses AMLFS Azure Monitor-derived metrics exported to Prometheus. It can show OST/MDT capacity, sampled MDT/OST latency and ops, client read/write throughput and ops, metadata amplification, evictions, HSM gauges, and telemetry freshness.

It does **not** expose MDS CPU, MDS queue depth, client-side LNet counters, D-state processes, CSI mount latency, pod churn, or simulator/application counters. Use MDT latency, MDT ops, metadata amplification, client write/read ops, evictions, and sample age as proxy signals only. If service-side MDS CPU is available, correlate it separately.

Azure Monitor's AMLFS client operation metrics expose target and operation dimensions (`ostnum` + `operation` for OST ops/latency, `mdtnum` + `operation` for MDT ops/latency). Operation panels query both dimensions, so legends should normally show target-specific series such as `OST0000 read` or `MDT0000 rename`. If an operation panel shows `ostnum="all"` or `mdtnum="all"`, treat it as a fallback or query/collection issue, not the expected split view.

Some non-operation target metrics, such as capacity, HSM, or eviction gauges, can still be aggregate-only depending on the Azure Monitor response. In that case `ostnum="all"` or `mdtnum="all"` means the panel is showing a filesystem aggregate series rather than physical target imbalance. Azure Monitor samples may also lag short workload runs; compare workload time with `*_sample_timestamp_seconds` and filesystem sample age before concluding that a workload produced no signal.

## Key Interpretation Notes

The Azure Managed Lustre metrics exported by this project are Azure Monitor-derived gauges. Operation and throughput series usually represent sampled values from a recent Azure Monitor time window. They are not monotonically increasing Prometheus counters, so do not wrap MDT operation or throughput gauges in `rate()`.

Azure Monitor's AMLFS client operation metrics expose both target and operation dimensions: `mdtnum` plus `operation` for MDT client operation metrics, and `ostnum` plus `operation` for OST client operation metrics. The exporter queries both dimensions, so operation panels should normally show target-specific series such as `MDT0000 rename` or `OST0001 read`.

Some non-operation target metrics, such as capacity, HSM, or eviction gauges, can still be aggregate-only depending on the Azure Monitor response. In those cases the exporter uses labels such as `mdtnum="all"`. A dashboard panel grouped by `mdtnum` then shows the aggregate filesystem series, not a true physical per-MDT imbalance view. Use a mounted Lustre client and commands such as `lfs df -i /mnt/lustre` when physical MDT inode distribution must be verified.

The dashboard is a triage view. It can show metadata pressure, sampled MDT latency, operation volume, inode usage, byte usage, client evictions, HSM backpressure, and sample freshness. It cannot prove MDS CPU saturation, MDS queue depth, lock queue depth, client D-state processes, CSI mount latency, or application-side operation counts by itself.

## MDT Latency Max

Dashboard panel: `MDT latency max (Azure Monitor sample)`

Metric:

```promql
azure_managed_lustre_mdt_client_latency_milliseconds
```

Dashboard query shape:

```promql
max(azure_managed_lustre_mdt_client_latency_milliseconds{...})
```

This panel shows the worst selected MDT operation latency in milliseconds. It represents client-observed metadata latency from Azure Monitor `MDTClientLatency`. It is useful for quickly answering whether the metadata path is slow right now.

This value is a sampled Azure Monitor aggregate, commonly a mean or aggregate over the Azure Monitor sample window. It is not a true p50, p95, or p99 distribution, and it is not MDS CPU. If this sampled value is already high, real tail latency may be worse.

Recommended interpretation:

| Value | Meaning |
| --- | --- |
| `< 10 ms` | Healthy metadata latency. |
| `10-50 ms` | Early warning, especially if sustained. |
| `> 100 ms` | Serious metadata latency worth investigation. |
| `> 500 ms` | Reconnect or stall risk. |
| `> 1000 ms` | Hang-risk range for metadata operations. |

Common causes of elevated MDT latency include directory scans, small-file storms, hot directories, lock contention, HSM restore/archive backpressure, reconnect stalls, client/network issues, and service-side metadata saturation.

## Metadata Amplification

Dashboard panels: `Amplification`, `Metadata amplification`

Metric:

```promql
azure_managed_lustre_metadata_amplification_ratio
```

Definition:

```text
sum(MDTClientOps) / max(sum(ClientReadOps + ClientWriteOps), 1)
```

This derived metric compares metadata operation volume with data-path read and write operation volume. It answers: for each data operation, how many metadata operations are being generated?

Recommended interpretation:

| Value | Meaning |
| --- | --- |
| `< 10` | Usually normal for many mixed workloads. |
| `>= 10` | Metadata-heavy behavior is visible. |
| `>= 100` | Strong metadata amplification, often a metadata storm. |

High metadata amplification commonly indicates tiny-file workloads, excessive `stat()` calls, recursive directory scans, AI dataloader file discovery, cleanup or indexing jobs, or applications that repeatedly touch metadata without doing much data I/O.

Use this metric to distinguish an OST throughput problem from a workload that is actually blocked by metadata work.

## MDT Ops Per Client

Dashboard panels: `MDT ops per client`, `MDT ops/client trend`

Metrics:

```promql
azure_managed_lustre_mdt_client_ops
azure_managed_lustre_filesystem_connected_clients
```

Dashboard query shape:

```promql
sum(MDTClientOps) / clamp_min(max(connected_clients), 1)
```

This panel normalizes metadata operation volume by the number of connected clients. It helps separate a naturally large workload from a small set of clients generating unusually heavy metadata pressure.

Recommended interpretation:

| Value | Meaning |
| --- | --- |
| `< 100 ops/client` | Usually acceptable. |
| `>= 100 ops/client` | Metadata-heavy client behavior is likely. |
| `>= 1000 ops/client` | Strong signal for directory scans, tiny-file storms, cleanup jobs, indexing, or aggressive dataloaders. |

If MDT ops per client rises at the same time as MDT latency, the filesystem is likely under active metadata pressure. If ops per client is low but latency is high, investigate locks, reconnects, HSM, network behavior, hot directories, or service-side issues.

## MDT Inode Used Percent

Dashboard panels: `MDT inode used`, `MDT inode used %`, `MDT series >70% inode`

Metric:

```promql
azure_managed_lustre_mdt_files_used_percent
```

This metric shows MDT file/inode usage. In this context, `files` means metadata namespace objects such as files and directories. AI, HPC, and AV workloads often exhaust inode capacity before they exhaust OST byte capacity.

Recommended thresholds:

| Value | Meaning |
| --- | --- |
| `< 70%` | Healthy. |
| `>= 70%` | Warning. Plan cleanup, compaction, archiving, or namespace changes. |
| `>= 85%` | Critical. New file or directory creation may soon fail or become operationally risky. |

The `MDT series >70% inode` panel counts selected MDT metric series above the warning threshold. If `mdtnum="all"`, this is a count of aggregate filesystem series above the threshold, not a count of physical MDTs.

## MDT Bytes Used Percent

Dashboard panels: `MDT bytes used`, `MDT bytes used %`

Metric:

```promql
azure_managed_lustre_mdt_bytes_used_percent
```

This metric shows metadata byte-capacity usage on the MDT. It is different from inode usage: inode percent measures object-count pressure, while byte percent measures metadata storage capacity. In many small-file workloads inode pressure appears first, but MDT byte exhaustion can also block metadata growth.

Recommended thresholds:

| Value | Meaning |
| --- | --- |
| `< 75%` | Healthy. |
| `>= 75%` | Warning. Track growth rate and cleanup options. |
| `>= 90%` | Critical. Metadata growth may be blocked if capacity is exhausted. |

Correlate this panel with inode usage. High inode usage with moderate byte usage points to object-count pressure. High byte usage may indicate metadata is growing in size, not just count.

## MDT Latency By Operation

Dashboard panel: `MDT latency by operation`

Metric:

```promql
azure_managed_lustre_mdt_client_latency_milliseconds{operation="..."}
```

Dashboard query shape:

```promql
topk($top_n,
  max by (filesystem_name, mdtnum, operation) (
    azure_managed_lustre_mdt_client_latency_milliseconds{...}
  )
)
```

This panel breaks metadata latency down by operation. It is usually the best place to identify what kind of metadata work is slow.

Typical interpretation:

| High-latency operation pattern | Common implication |
| --- | --- |
| `lookup`, `getattr`, `stat` | Directory scans, file discovery, dataloader metadata checks, very large directories. |
| `open`, `close` | Large numbers of small files or repeated open/close cycles. |
| `create`, `unlink`, `rename` | Concurrent file generation, cleanup, shuffle output, checkpoint movement, temp-file workflows. |
| Many operations high at once | Broad metadata-path pressure, network/client stalls, HSM effects, or service-side saturation. |

Use the same latency bands as the MDT latency max panel: above `100 ms` is serious, above `500 ms` is reconnect/stall risk, and above `1000 ms` is hang-risk territory.

## MDT Ops By Operation

Dashboard panel: `MDT ops by operation`

Metric:

```promql
azure_managed_lustre_mdt_client_ops{operation="..."}
```

Dashboard query shape:

```promql
topk($top_n,
  max by (filesystem_name, mdtnum, operation) (
    azure_managed_lustre_mdt_client_ops{..., mdtnum=~"$mdtnum", operation=~"$operation"}
  )
)
```

This panel shows which metadata operations are generating the most volume. It should be read together with `MDT latency by operation`.

The dashboard includes an `MDT operation` variable populated from the `operation` label on `azure_managed_lustre_mdt_client_ops`. Azure Monitor's AMLFS metric definitions expose both the target dimension (`mdtnum` for MDT metrics, `ostnum` for OST metrics) and the `operation` dimension for client operation metrics. The exporter queries both dimensions, so operation panels should normally show target-specific series such as `MDT0000 rename` or `OST0001 read` when Azure Monitor returns those dimensions. Selecting one or more operation values filters the operation latency, operation volume, and current latency incident panels. Selecting `All` keeps the Azure Monitor-style split-by-operation view.

If the dropdown only contains `all`, treat it as a fallback signal that Azure Monitor did not return operation dimensions for the selected metric response, or the exporter query did not request the target and operation dimensions correctly.

Common patterns:

| Pattern | Meaning |
| --- | --- |
| High ops and low latency | The workload is metadata-heavy, but the metadata path is currently handling it. |
| High ops and high latency | Active metadata pressure or contention. This is the classic metadata storm pattern. |
| Low ops and high latency | Investigate locks, reconnects, HSM backpressure, hot directories, client issues, network issues, or service-side symptoms. |
| High `lookup` / `getattr` / `stat` ops | File discovery, recursive scans, AI dataloaders, validators, indexers. |
| High `create` / `unlink` / `rename` ops | Temp-file workflows, cleanup, checkpoint churn, shuffle output, concurrent writers. |

Because this metric is Azure Monitor-derived, treat the value as a sampled operation gauge, not as a counter that needs `rate()`.

## MDT Latency Incidents Now

Dashboard panel: `MDT latency incidents now`

Metric condition:

```promql
azure_managed_lustre_mdt_client_latency_milliseconds > 100
```

This table shows current MDT operation latency incidents above `100 ms`. An empty table means there is no current selected MDT operation above the warning threshold.

Use this as the first table during incident triage. Check the filesystem, region, resource group, `mdtnum`, operation, and current latency value. Then correlate with MDT operation volume, metadata amplification, connected clients, evictions, HSM metrics, and sample freshness.

## Related Metadata Signals

The following related signals can help explain metadata symptoms:

| Signal | Metric | Why it matters |
| --- | --- | --- |
| Connected clients | `azure_managed_lustre_filesystem_connected_clients` | Sudden changes can correlate with workload phase changes, client reconnects, failover, or evictions. |
| Client evictions | `azure_managed_lustre_filesystem_client_evictions` | Evictions indicate disruption and can appear with reconnect stalls or client-side errors. |
| HSM current requests | `azure_managed_lustre_hsm_current_requests` | HSM backlog can surface as elevated metadata latency for affected files. |
| HSM action errors | `azure_managed_lustre_hsm_action_errors` | HSM failures can correlate with restore/archive stalls. |
| MDT sample freshness | `azure_managed_lustre_mdt_sample_timestamp_seconds` | Confirms whether the displayed MDT capacity sample is current. |
| MDT operation sample freshness | `azure_managed_lustre_mdt_operation_sample_timestamp_seconds` | Confirms whether latency and operation panels reflect recent Azure Monitor samples. |
| Filesystem sample max age | `azure_managed_lustre_filesystem_sample_max_age_seconds` | Detects stale Azure Monitor/exporter data at the filesystem level. |

## Suggested Triage Flow

1. Check `MDT latency max` to confirm whether metadata latency is elevated.
2. Open `MDT latency incidents now` to identify the affected filesystem, MDT label, and operation.
3. Compare `MDT latency by operation` with `MDT ops by operation` to see whether the slow operation is also high-volume.
4. Check `MDT ops/client trend` to understand whether a small number of clients are generating heavy metadata work.
5. Check `Metadata amplification` to determine whether data I/O is being amplified by metadata operations.
6. Check `MDT inode used %` and `MDT bytes used %` to rule out metadata capacity pressure.
7. Correlate with connected clients, client evictions, HSM metrics, and sample freshness before concluding that the MDS itself is saturated.

## Practical Reading Examples

High `Metadata amplification`, high `lookup/getattr/stat` ops, and rising MDT latency usually means the workload is scanning or discovering many small files. Look for recursive directory walks, dataloader initialization, validators, or indexers.

High `create/unlink/rename` ops and elevated latency usually points to concurrent file churn: temp files, cleanup, shuffle output, checkpoint rewrites, or many jobs writing into the same namespace.

High MDT latency with low MDT ops may indicate lock contention, reconnect stalls, HSM restore/archive waits, hot directories, client-side problems, network behavior, or service-side metadata issues that are not directly visible in this dashboard.

High MDT inode usage with low OST byte usage is a classic small-file capacity problem. The filesystem may have plenty of data capacity left while still being at risk of failing new file or directory creation due to inode pressure.
