# Azure Managed Lustre Production Best Practices
## AKS / CSI / AI-HPC Large-Scale Client Environments

---

# Official References

- Azure Managed Lustre Documentation
- Optimize Azure Managed Lustre Performance
- Optimize AMLFS File Layouts
- Azure Lustre CSI Driver

Official Links:
- https://learn.microsoft.com/en-us/azure/azure-managed-lustre/
- https://learn.microsoft.com/en-us/azure/azure-managed-lustre/optimize-performance
- https://learn.microsoft.com/en-us/azure/azure-managed-lustre/optimize-file-layouts
- https://github.com/kubernetes-sigs/azurelustre-csi-driver

---

# 1. Architecture Best Practices

## 1.1 Same Availability Zone Placement (CRITICAL)

For best performance and lowest metadata latency, Microsoft recommends placing AMLFS clients and the filesystem in the same Availability Zone whenever possible.

AMLFS is created into a single Availability Zone and is **immutable** post-creation; you cannot move an existing filesystem to a different zone. Zone failover requires provisioning a new filesystem (typically restored from HSM-archived data in Azure Blob).

### Risks of Cross-Zone Access

| Risk | Impact |
|---|---|
| Increased RTT | Higher metadata latency |
| Jitter | Lock instability |
| TCP retries | LNet reconnect amplification |
| Recovery delay | Mount storms |
| Cross-zone bandwidth limits | Throughput degradation |

### Recommended Design

| Component | Recommendation |
|---|---|
| AMLFS | Single AZ |
| AKS AMLFS nodepool | Same AZ |
| GPU nodepool | Same AZ |
| CSI workloads | Same AZ |
| Accelerated Networking | Enabled |

---

## 1.2 Network Topology Best Practices

AMLFS traffic should use the most direct network path possible.

### Avoid

- NVAs
- Azure Firewall inline routing
- Forced tunneling
- Extra UDR hops
- Overlay network detours

### Goal

Minimize:
- RTT
- jitter
- retransmissions
- reconnect amplification

---

## 1.3 Accelerated Networking

Accelerated Networking is strongly recommended on all AMLFS clients.

Benefits:
- lower RTT
- lower jitter
- lower latency
- improved throughput stability
- improved LNet stability

---

## 1.4 Dedicated AMLFS Node Pool

Avoid running AMLFS-heavy workloads on shared regional node pools.

### Recommended

- Dedicated zonal node pool
- Taints + tolerations
- Node affinity
- Limit PVC density

---

## 1.5 Limit Mount Density Per Node

Excessive mounts can cause:
- LNet connection explosion
- Privileged port exhaustion
- Reconnect storms
- Metadata contention

Operational experience in large-scale AI/CSI environments suggests keeping mount density per node as low as possible to reduce reconnect amplification and LNet pressure.

---

# 2. Storage Capacity Best Practices

## 2.1 OST Capacity Monitoring

### Monitor

- azure_managed_lustre_ost_bytes_used
- azure_managed_lustre_ost_bytes_available
- azure_managed_lustre_ost_bytes_used_percent

### Best Practice

Monitor:
- Total capacity
- Growth trends
- 80% used-capacity safety boundary (hard stop for write-heavy workloads)

### Per-OST imbalance — important caveat

AMLFS Azure Monitor publishes a single per-filesystem aggregate (this repo's exporter emits it as `ostnum="all"`); per-OST series are not available. Therefore `max(OST used %) - min(OST used %)` is **not derivable** from Azure Monitor / Prometheus alone.

To measure per-OST imbalance, query the client:

```bash
lfs df -h /mnt/lustre        # per-OST used %
lfs df -i /mnt/lustre        # per-MDT inode usage
```

If you need this in Prometheus, export it from a privileged DaemonSet pod that runs `lfs df` periodically and emits text-file metrics scraped by node-exporter.

### Recommended Thresholds (client-side per-OST imbalance)

| Threshold | Severity |
|---|---|
| >20% imbalance | Warning |
| >35% imbalance | Critical |

---

## 2.2 MDT Capacity Monitoring

### Monitor

- azure_managed_lustre_mdt_bytes_used
- azure_managed_lustre_mdt_files_used
- azure_managed_lustre_mdt_files_free

### Why MDT Matters

AI/HPC workloads frequently exhaust inode capacity before storage capacity.

### Recommended Thresholds

| Metric | Warning | Critical |
|---|---|---|
| MDT inode usage | 70% | 85% |
| MDT byte usage | 75% | 90% |

---

# 3. Metadata Performance Best Practices

## 3.1 MDT Latency Monitoring

### Critical Metric

azure_managed_lustre_mdt_client_latency_milliseconds

This is a Gauge populated from Azure Monitor's `MDTClientLatency` sample, typically the aggregate/mean for the ~1-minute sample window per operation label. It is **not** a true percentile distribution; do not call it P50/P95/P99. For real percentiles, instrument the client (e.g., `lctl get_param mdc.*.stats`) and export as a histogram.

### Why This Matters

Many "read hangs" are actually:
- metadata lookup hangs
- lock contention
- reconnect stalls
- inode lookup delays

rather than OST throughput issues.

### Recommended Thresholds (sampled MDT latency from Azure Monitor)

| MDT sampled latency | Status |
|---|---|
| <10ms | Healthy |
| 10-50ms | Warning |
| >100ms | Serious |
| >500ms | Reconnect risk |
| >1000ms | Hang risk |

---

## 3.2 Metadata Amplification Detection

### Recommended Derived Metric

Prefer this repo's exported per-filesystem gauge when available:

```
azure_managed_lustre_metadata_amplification_ratio
```

If you need to derive the ratio in PromQL, aggregate each side to the same filesystem label set before dividing:

```
sum by (subscription_id, resource_group, filesystem_name, location) (
	azure_managed_lustre_mdt_client_ops
)
/
clamp_min(
	sum by (subscription_id, resource_group, filesystem_name, location) (
		azure_managed_lustre_client_read_ops + azure_managed_lustre_client_write_ops
	),
	1
)
```

Do **not** wrap these Azure Monitor-derived series in `rate()`: they are already per-sample 1-minute average gauges, not monotonically increasing counters. Replace `clamp_min(..., 1)` only if your Prometheus-compatible backend uses a different divide-by-zero guard.

### High Ratio Indicates

- Tiny file storms
- Excessive stat()
- Directory scans
- AI dataloader amplification

## 3.3 HSM Monitoring (when HSM is enabled)

### Recommended Metrics

- `azure_managed_lustre_hsm_current_requests`
- `azure_managed_lustre_hsm_action_errors`

### Why This Matters

HSM queue growth and action-error spikes correlate with archive/restore backpressure that surfaces to clients as elevated MDT latency on the affected files. If HSM is not enabled on the filesystem, both series stay flat at 0.

---

## 3.4 Azure Monitor Dashboard Interpretation

The Grafana dashboard in `deploy/grafana-dashboard-lustre.json` is an Azure Monitor / exporter view of AMLFS. It is useful for filesystem-level triage, but it is not a complete Lustre client or service-internal diagnostic view.

### What the dashboard can show

- OST and MDT capacity gauges.
- Sampled OST and MDT latency and operation gauges.
- Client read/write throughput and operation gauges.
- Metadata amplification ratio.
- Connected clients and client evictions.
- HSM in-flight requests and action errors.
- Exporter collection health and Azure Monitor sample freshness.

### What the dashboard cannot prove by itself

- MDS CPU saturation.
- MDS queue depth, lock queue depth, or RPC queue depth.
- Client-side LNet reconnects, retransmits, or privileged-port pressure.
- D-state process counts or kernel hung-task symptoms.
- CSI mount latency, mount errors, or pod churn.
- Application or simulator-side operation counters.

When service-side MDS CPU is available, treat it as the strongest signal for metadata-server saturation and correlate it with `MDTClientLatency`, `MDTClientOps`, metadata amplification, client write/read ops, connected clients, and evictions. Without MDS CPU, those Azure Monitor metrics are proxy signals only.

### Aggregate-label caveat

In many AMLFS environments, Azure Monitor exposes filesystem aggregate series rather than physical per-target series. The exporter represents this as labels such as:

```text
ostnum="all"
mdtnum="all"
```

When labels are aggregate-only, dashboard panels grouped by `ostnum` or `mdtnum` do not show true physical OST or MDT imbalance. They show the aggregate filesystem series. Use client-side `lfs df -h` / `lfs df -i` from a mounted Lustre client if physical target imbalance must be measured.

### Sampling and short-test caveat

Azure Monitor-derived AMLFS metrics are sampled gauges, commonly representing a recent one-minute sample window. They can lag workload activity by several minutes. For short experiments or simulator runs, compare the workload start/end time with:

```text
azure_managed_lustre_ost_sample_timestamp_seconds
azure_managed_lustre_mdt_sample_timestamp_seconds
azure_managed_lustre_mdt_operation_sample_timestamp_seconds
azure_managed_lustre_filesystem_sample_max_age_seconds
```

Low dashboard write throughput does not necessarily mean no workload ran. It can also mean the workload was metadata-bound, waiting on locks, or that the Azure Monitor sample window did not align with the workload burst. Do not apply `rate()` to Azure Monitor-derived ops or throughput gauges; they are already sampled values, not monotonically increasing counters.

---

# 4. File and Directory Layout Best Practices

## 4.1 File Striping Best Practices

Large sequential workloads:
- wider striping
- more OSTs

Small-file workloads:
- narrower striping
- fewer OSTs

Avoid over-striping small files.

Use Progressive File Layouts (PFL) when appropriate (AMLFS runs Lustre 2.15, so PFL is available).

### Operational caveats on AKS / CSI

- Layout changes (`lfs setstripe`) apply only to **newly created** files. Existing files retain their layout unless explicitly migrated with `lfs migrate`.
- The Azure Lustre CSI sidecar image generally does not ship the `lfs` userspace. Run layout changes from a separate privileged debugging pod that mounts the same PVC and has the AMLFS client packages installed.
- Non-root pods typically cannot change layouts of files they do not own.

---

## 4.2 Small File Workload Guidance

Lustre performs best with:
- large sequential I/O
- parallel streaming workloads

Large quantities of tiny files can:
- overload MDTs
- increase metadata latency
- amplify stat()/lookup traffic
- cause directory scan slowdowns

### Recommended Mitigations

- tar sharding
- parquet
- WebDataset
- larger file aggregation

---

## 4.3 Directory Layout Best Practices

Large directories containing millions of files can create metadata bottlenecks.

### Recommended

- Shard large directories (e.g., two-level prefix hashing: `aa/ab/<file>`)
- Avoid flat namespace layouts
- Reduce tiny-file fanout via tar/parquet/WebDataset aggregation

### Note on DNE / multiple MDTs

AMLFS today exposes a **single user-visible MDT** per filesystem. Distributed namespace (DNE/PSME) is not user-configurable, so you cannot pin directories to specific MDTs. The mitigations above (directory sharding, fanout reduction) are the only practical levers.

---

# 5. LNet Best Practices

## 5.1 Monitor LNet Reconnect Storms

### Strongly Recommended Metrics

- lnet_reconnect_count
- lnet_timeout_count
- lnet_resend_count
- lnet_connection_failures
- lnet_peer_state

### Source — IMPORTANT

These metrics are **not** published by Azure Monitor and are **not** in this repo's exporter. They live on the client and must be scraped on each AMLFS node, e.g.:

- `lnetctl net show -v` / `lnetctl peer show -v`
- `/sys/kernel/debug/lnet/stats`
- `/proc/sys/lnet/*` counters (kernel-version dependent)

Typical implementation: a privileged DaemonSet on the AMLFS nodepool that periodically parses the above and writes a node-exporter textfile collector file. See [Appendix A](#a-lnet--d-state-textfile-collector-daemonset-sketch) for a minimal textfile-collector sketch.

### Why This Matters

Reconnect storms can cause:
- PVC hangs
- metadata freezes
- client eviction
- mount failures

---

## 5.2 Monitor Privileged Port Exhaustion

### Critical Symptom

LNetError: No privileged ports available

### Root Cause

LNet requires privileged source ports (<1024).

Large-scale client environments may exhaust:
- reserved ports
- reconnect pools
- TIME_WAIT sockets

### Recommended Monitoring

```bash
ss -tan sport lt :1024 | wc -l
```

For Prometheus ingestion, publish this as a node-local textfile-collector metric. See [Appendix A](#a-lnet--d-state-textfile-collector-daemonset-sketch).

### Recommended Thresholds

| Metric | Threshold |
|---|---|
| Privileged ports used >70% | Warning |
| Rapid reconnect spikes | Critical |
| TIME_WAIT spikes | Critical |

---

## 5.3 Recommended LNet Tuning

There is **no Linux sysctl that increases the privileged-port pool** (ports `1-1023` are a hard kernel constant). The realistic mitigations for LNet privileged-port pressure are operational, not kernel-tuning:

1. Reduce mount density per node (see 1.5 and 6.2).
2. Stagger pod restarts / node reboots to avoid simultaneous LNet reconnects.
3. Use a dedicated zonal AMLFS nodepool so non-AMLFS workloads do not consume sockets on the same node.
4. Enable Accelerated Networking so each reconnect completes faster and releases the source port sooner.

### Sysctls — use with care

Some environments report benefit from the following, but each has caveats. Validate in a staging cluster before enabling in production.

```bash
# Reuse TIME_WAIT sockets for outbound connections. Can break flows behind
# SNAT/conntrack (common with AKS Standard LB egress). Do NOT enable
# blindly in shared-egress environments.
net.ipv4.tcp_tw_reuse = 1

# Shorten TIME_WAIT-related buffers. Safer than tcp_tw_reuse but only helps
# under heavy short-lived TCP load.
net.ipv4.tcp_fin_timeout = 15
```

### Anti-patterns to avoid

- `net.ipv4.ip_local_reserved_ports = 988` does **not** help LNet. Port 988 is the Lustre LND server port and is below `ip_local_port_range.min`, so reserving it is a no-op for ephemeral allocation and is unrelated to the privileged source-port pool LNet uses.
- Raising `ip_local_port_range` does not add privileged ports; LNet allocates from `1-1023` only.
- Disabling `tcp_timestamps` is sometimes suggested online but tends to make `tcp_tw_reuse` less safe; leave it on.

---

# 6. Kubernetes / CSI Best Practices

## Strongly Recommended Metrics

| Metric | Importance | Typical source |
|---|---|---|
| Mounts per node | CRITICAL | node-exporter (`node_filesystem_*` filtered by fstype=`lustre`) or kubelet `volume_manager_total_volumes` |
| PVC count per node | CRITICAL | kube-state-metrics (`kube_pod_spec_volumes_persistentvolumeclaims_info` joined to pod→node) |
| Pod churn | HIGH | kube-state-metrics (`kube_pod_status_phase`, `kube_pod_created`) |
| Remount frequency | HIGH | csi-azurelustre-node container logs / events |
| CSI mount latency | HIGH | csi-driver Prometheus metrics (`csi_operations_seconds`) |
| Node drain events | HIGH | kube-state-metrics / Kubernetes events |

None of these come from AMLFS Azure Monitor or from this repo's exporter; deploy kube-state-metrics, node-exporter, and the CSI driver's own metrics endpoint to surface them.

---

## 6.1 CSI Metrics Guidance

The Azure Lustre CSI driver is the best source for mount-path health. At minimum, scrape the `csi-azurelustre-node` metrics endpoint and dashboard these views:

| Signal | Example PromQL | Why it matters |
|---|---|---|
| Mount operation rate | `sum by (grpc_method, grpc_status_code) (rate(csi_operations_seconds_count{grpc_method=~"NodePublishVolume|NodeUnpublishVolume"}[5m]))` | Separates normal pod churn from mount storms and remount loops. |
| Mount latency | `histogram_quantile(0.95, sum by (le, grpc_method) (rate(csi_operations_seconds_bucket{grpc_method="NodePublishVolume"}[5m])))` | Detects slow mounts before application pods report I/O hangs. |
| Mount errors | `sum by (grpc_status_code) (rate(csi_operations_seconds_count{grpc_method="NodePublishVolume",grpc_status_code!="OK"}[5m]))` | Surfaces CSI failures that do not appear in AMLFS Azure Monitor metrics. |

If the deployed CSI image does not expose `csi_operations_seconds`, fall back to `csi-azurelustre-node` logs and Kubernetes events, but treat that as a temporary visibility gap rather than a steady-state design.

---

## 6.2 Avoid Mount Explosion

Large CSI environments may create:
- thousands of Lustre exports
- excessive LNet connections
- reconnect amplification

Note on shared mounts: multiple pods on the same node mounting the same AMLFS filesystem reuse the underlying client/LNet peer; the multiplier is **distinct AMLFS instances per node**, not pods per PVC. Limiting the number of distinct AMLFS PVCs (and AMLFS filesystems) per node is what reduces LNet/peer pressure.

### Best Practices

- Reuse PVCs (one PVC per AMLFS instance per namespace where possible)
- Reduce pod churn
- Limit dynamic mounts
- Use zonal nodepools

## 6.3 Mount Options

AMLFS clients accept standard Lustre client mount options. Two that frequently matter:

- `flock` / `localflock` / (omit): default behaviour is **no** distributed POSIX file locking. Workloads that rely on `flock()` across multiple clients must explicitly mount with `flock`. Workloads that only need locks within a single client can mount with `localflock` (lower MDT overhead).
- `user_xattr`: required by some AI dataloaders that store extended attributes; verify your dataset import path needs it before enabling, since it costs MDT bandwidth.

---

# 7. Client Health Best Practices

## 7.1 Monitor D-State Processes

### Recommended Command

```bash
ps -eo state,pid,cmd | grep "^D"
```

### Recommended Metric

`lustre_dstate_process_count` from a node-local textfile collector. See [Appendix A](#a-lnet--d-state-textfile-collector-daemonset-sketch) for the collection pattern.

---

## 7.2 Monitor Hung Tasks

### Recommended Commands

```bash
dmesg | grep hung
journalctl -k
```

For alerting, publish a short-window counter/gauge such as `lustre_kernel_hung_task_recent` through the same node-local textfile collector described in [Appendix A](#a-lnet--d-state-textfile-collector-daemonset-sketch).

---

# 8. Client Software Best Practices

Keep AMLFS Lustre client packages updated to the latest supported versions to benefit from:
- bug fixes
- reconnect improvements
- performance optimizations
- kernel compatibility updates

### Kernel / client version pinning

The AMLFS client (`amlfs-lustre-client`) is built per kernel. On AKS:

- Pin AMLFS nodepools to a supported image/kernel pair (e.g., Ubuntu 22.04 with `5.15.0-*-azure` is currently supported; newer Azure Linux kernels may not yet have a published client).
- The Azure Lustre CSI DaemonSet image (`mcr.microsoft.com/oss/v2/kubernetes-csi/azurelustre-csi:<tag>`) bundles a matching client; do not mix CSI image tags across nodes with different kernels.
- After kernel autoupgrade, validate the client still loads (`lsmod | grep lustre`) before resuming production load.

---

# 9. Workload Segregation Best Practices

Avoid mixing:
- latency-sensitive metadata workloads
- large sequential streaming workloads
- tiny-file AI datasets
- checkpoint-heavy workloads

on the same filesystem whenever possible.

Benefits:
- improved MDT stability
- predictable latency
- better OST balance

## 9.1 Capacity Safety Boundary

Keep filesystem-wide used capacity **below 80%** during normal operation. Above that line Lustre allocation behavior degrades (OST imbalance grows, write tail latency rises, recovery windows widen). Make 80% used a hard stop for write-heavy phases:

- Pre-flight every write-heavy job with a budget calculator (planned-write + safety-reserve must fit under the 80% line).
- Alert at >75% used; page at >80% used.
- Reject new write-heavy workloads above 80% until cleanup reclaims headroom.

## 9.2 Backup / Disaster Recovery

AMLFS does **not** support native snapshots. The supported durability paths are:

- **HSM archive to Azure Blob**: configure HSM to tier cold data to Blob, then apply Blob lifecycle / soft-delete / versioning for retention.
- **External periodic copy**: out-of-band `rclone`/`azcopy` from a mounted client to Blob/ADLS.

Plan recovery as "provision a new AMLFS in another zone and rehydrate from Blob", not "restore an AMLFS snapshot".

---

# 10. Networking Best Practices

## Monitor TCP Retransmissions

Recommended Metrics:
- TCP retransmissions
- RTT
- Conntrack usage
- SNAT usage

---

# 11. Operational Best Practices

## Avoid Large Simultaneous Reconnects

Examples:
- mass pod restart
- node reboot storms
- AKS upgrade waves
- CSI remount storms

---

## Stagger Large Operations

Recommended:
- rolling restarts
- staggered node upgrades
- gradual scaling

---

# 12. Recommended Dashboard Structure

## Executive Dashboard

- MDT sampled latency (from Azure Monitor; not a percentile)
- OST throughput
- Connected clients
- D-state process count
- Reconnect rate
- Mounts per node
- OST used % vs the 80% safety boundary

---

## Metadata Dashboard

- getattr rate
- lookup rate
- open rate
- inode usage
- MDT queue depth
- lock contention

---

## LNet Dashboard

- reconnect count
- timeout count
- resend count
- peer state
- privileged port usage
- TCP retransmissions

---

## AKS / CSI Dashboard

- mounts per node
- PVC count per node
- pod churn
- remount frequency
- CSI `NodePublishVolume` latency and error rate (`csi_operations_seconds`)
- node drain events

---

## Alert ↔ Doc Threshold Map

Use this table to verify that the production thresholds in this document are represented by concrete Prometheus rules in `deploy/lustre-alert-rules.yaml`.

| Doc reference | Operational threshold | Prometheus alert rule |
|---|---|---|
| Exporter freshness | No successful Managed Lustre collection for >180s | `AzureManagedLustreCollectorStale` |
| Azure Monitor sample freshness | OST sample age >300s | `AzureManagedLustreSampleStale` |
| Azure Monitor sample freshness | MDT sample age >300s | `AzureManagedLustreMdtSampleStale` |
| Collector reliability | Collection errors observed over 5m | `AzureManagedLustreCollectionErrors` |
| §2.1 / §9.1 OST capacity | OST available <10% / <5% | `AzureManagedLustreOstAvailablePercentLow`, `AzureManagedLustreOstAvailablePercentCritical` |
| §2.1 / §9.1 OST capacity | OST available <1 TiB / <100 GiB | `AzureManagedLustreOstBytesAvailableLow`, `AzureManagedLustreOstBytesAvailableCritical` |
| §9.1 OST safety boundary | OST used >75% / >80% | `AzureManagedLustreOstUsedPercentWarn`, `AzureManagedLustreOstUsedPercentCritical` |
| §2.2 MDT byte usage | MDT bytes used >75% / >90% | `AzureManagedLustreMdtBytesUsedPercentWarn`, `AzureManagedLustreMdtBytesUsedPercentCritical` |
| §2.2 MDT inode usage | MDT inodes used >70% / >85% | `AzureManagedLustreMdtInodeUsedPercentWarn`, `AzureManagedLustreMdtInodeUsedPercentCritical` |
| §3.1 MDT sampled latency | MDT latency >100ms / >500ms / >1000ms | `AzureManagedLustreMdtLatencyWarn`, `AzureManagedLustreMdtLatencySerious`, `AzureManagedLustreMdtLatencyHang` |
| §3.3 HSM reliability | HSM action errors >0 | `AzureManagedLustreHsmActionErrors` |
| §3.3 HSM backlog | HSM in-flight requests >100 / >500 | `AzureManagedLustreHsmBacklog`, `AzureManagedLustreHsmBacklogCritical` |
| Exporter HA | No leader elected for >2m | `AzureManagedLustreExporterNoLeader` |

The LNet and D-state signals in §5 and §7 are client-side metrics and are not emitted by Azure Monitor. See [Appendix A](#a-lnet--d-state-textfile-collector-daemonset-sketch) for the textfile-collector ingestion sketch.

---

# 13. Final Recommendations

The most common AMLFS production failures in AKS/AI-HPC environments are NOT caused by:
- raw throughput limits
- OST capacity exhaustion

Instead, they are commonly caused by:

Client-side scaling collapse

Driven by:
- LNet reconnect storms
- Privileged port exhaustion
- Metadata amplification
- CSI mount explosion
- Cross-zone latency amplification
- AKS pod churn

Therefore, production monitoring should prioritize:
- Metadata latency
- LNet stability
- Client scaling
- CSI behavior
- Mount density
- Reconnect visibility

---

# A. LNet / D-state Textfile-Collector DaemonSet Sketch

This appendix sketches the minimal client-side ingestion path for the LNet and client-health signals called out in §5 and §7. It is intentionally **documentation only**: do not ship this as-is without adapting the image, security policy, node labels, and scrape path to your cluster.

## A.1 Placement and Security Model

Run one privileged pod per AMLFS client node and keep it off general-purpose nodes.

Recommended placement:

- `nodeSelector` or node affinity that targets only the dedicated AMLFS nodepool, for example `workload.azure.com/amlfs-client: "true"`.
- Tolerations for the AMLFS nodepool taint, for example `workload=amlfs:NoSchedule`.
- `hostPID: true` so D-state process counts reflect the host, not only the collector container.
- `securityContext.privileged: true` so the collector can read LNet debugfs and kernel logs.
- Read-only host mounts for `/sys` and `/proc`.
- A read-write hostPath mount for the node-exporter textfile directory, commonly `/var/lib/node_exporter/textfile_collector`.

Pod Security / RBAC notes:

- Kubernetes Pod Security Standards must allow the collector namespace to run privileged pods. On OpenShift, bind the collector service account to the privileged SCC.
- Keep the service account narrow: it usually does not need Kubernetes API permissions if it only writes local textfile metrics.
- Pin AMLFS nodepools to an AMLFS-supported kernel and client image pair as described in §8. On AKS, Ubuntu 22.04 with `5.15.0-*-azure` is a common supported baseline, but always verify against the current Azure Managed Lustre client support matrix.

Partial pod spec sketch:

```yaml
hostPID: true
nodeSelector:
	workload.azure.com/amlfs-client: "true"
tolerations:
	- key: workload
		operator: Equal
		value: amlfs
		effect: NoSchedule
containers:
	- name: lustre-client-textfile-collector
		securityContext:
			privileged: true
			readOnlyRootFilesystem: true
		volumeMounts:
			- name: host-sys
				mountPath: /host/sys
				readOnly: true
			- name: host-proc
				mountPath: /host/proc
				readOnly: true
			- name: textfile-dir
				mountPath: /textfile
volumes:
	- name: host-sys
		hostPath:
			path: /sys
	- name: host-proc
		hostPath:
			path: /proc
	- name: textfile-dir
		hostPath:
			path: /var/lib/node_exporter/textfile_collector
			type: DirectoryOrCreate
```

## A.2 Collection Loop

Run a short interval loop, for example every 30 seconds. Keep the loop local, bounded, and fail-open: if one probe fails, write the other metrics and expose a collector error metric.

Recommended probes:

| Source | Purpose | Example metric |
|---|---|---|
| `lnetctl net show -v` / `lnetctl peer show -v` | Peer state and reconnect visibility | `lustre_lnet_peer_state`, `lustre_lnet_reconnect_count`, `lustre_lnet_timeout_count`, `lustre_lnet_resend_count`, `lustre_lnet_connection_failures` |
| `/sys/kernel/debug/lnet/stats` | LNet transport counters | `lustre_lnet_send_count`, `lustre_lnet_recv_count`, `lustre_lnet_drop_count` |
| `ss -tan sport lt :1024 | wc -l` | Privileged source-port pressure | `lustre_privileged_ports_in_use` |
| `ps -eo state | awk '/^D/ {n++} END {print n+0}'` | Host D-state process pressure | `lustre_dstate_process_count` |
| `dmesg --since="-5min" | grep -ci 'hung_task\|lustre'` | Recent kernel hangs or Lustre client errors | `lustre_kernel_hung_task_recent` |

Shell-style sketch:

```bash
#!/usr/bin/env bash
set -euo pipefail

TEXTFILE_DIR=${TEXTFILE_DIR:-/textfile}
OUT="${TEXTFILE_DIR}/lustre_client.prom"
TMP="${OUT}.$$"

while true; do
	{
		echo "# HELP lustre_privileged_ports_in_use TCP sockets currently using privileged source ports."
		echo "# TYPE lustre_privileged_ports_in_use gauge"
		ss -tan sport lt :1024 | awk 'END {print "lustre_privileged_ports_in_use " NR-1}'

		echo "# HELP lustre_dstate_process_count Host processes currently in D state."
		echo "# TYPE lustre_dstate_process_count gauge"
		ps -eo state | awk '/^D/ {n++} END {print "lustre_dstate_process_count " n+0}'

		echo "# HELP lustre_kernel_hung_task_recent Recent kernel log lines matching hung_task or lustre."
		echo "# TYPE lustre_kernel_hung_task_recent gauge"
		dmesg --since="-5min" | grep -ci 'hung_task\|lustre' | awk '{print "lustre_kernel_hung_task_recent " $1}'

		# Parse lnetctl JSON/YAML/text output in the real collector image and emit:
		# lustre_lnet_reconnect_count{peer="..."} N
		# lustre_lnet_timeout_count{peer="..."} N
		# lustre_lnet_resend_count{peer="..."} N
		# lustre_lnet_connection_failures{peer="..."} N
		# lustre_lnet_peer_state{peer="...",state="up|down|recovery"} 0|1
		# lustre_lnet_send_count N
		# lustre_lnet_recv_count N
		# lustre_lnet_drop_count N
	} > "${TMP}"

	mv "${TMP}" "${OUT}"
	sleep 30
done
```

Write the file atomically (`write temp` then `mv`) so node-exporter never scrapes a partially written `.prom` file.

## A.3 Scrape Integration

Use the existing node-exporter textfile-collector path when possible. That keeps the AMLFS client collector simple: it only writes `lustre_client.prom`; the existing Prometheus / Azure Monitor managed Prometheus scrape configuration picks it up with the rest of the node metrics.

If node-exporter runs with a different textfile path, mount that hostPath instead and keep the same atomic write pattern.

## A.4 Operational Guardrails

- Alert on reconnect or timeout spikes, not only absolute counts.
- Page on sustained privileged-port pressure above 70%, especially during node upgrades or pod restart storms.
- Treat non-zero `lustre_dstate_process_count` plus increasing `lustre_kernel_hung_task_recent` as a client-health incident, even if AMLFS Azure Monitor server-side metrics still look healthy.
- Keep labels low-cardinality. Peer labels are useful; process command labels are not.
