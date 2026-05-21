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

```
sum(azure_managed_lustre_mdt_client_ops) / clamp_min(sum(azure_managed_lustre_client_read_ops + azure_managed_lustre_client_write_ops), 1)
```

(Use this repo's exporter metric names; replace `clamp_min(...,1)` with whatever you use to avoid divide-by-zero.)

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

Typical implementation: a privileged DaemonSet on the AMLFS nodepool that periodically parses the above and writes a node-exporter textfile collector file.

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

D-state process count

---

## 7.2 Monitor Hung Tasks

### Recommended Commands

```bash
dmesg | grep hung
journalctl -k
```

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
- node drain events

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
