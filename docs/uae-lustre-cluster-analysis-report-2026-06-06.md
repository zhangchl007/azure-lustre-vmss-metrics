# UAE Azure Managed Lustre Cluster Analysis Report

Date: 2026-06-06

Scope: Azure Managed Lustre filesystems in the UAE environment, reviewed from Azure Monitor 24-hour history using the exporter metric model and repository alert thresholds.

Review window: last 24 hours, `PT5M` Azure Monitor granularity, namespace `Microsoft.StorageCache/amlFilesystems`.

## Filesystems Reviewed

| Filesystem | Location | Summary status |
| --- | --- | --- |
| `ide-cache` | `uaenorth` | Warning: near capacity threshold |
| `yun-training` | `uaenorth` | Critical: OST capacity issue |
| `l4-service` | `uaenorth` | Critical: OST capacity issue |
| `laser` | `uaenorth` | Warning: client evictions observed |
| `training` | `uaenorth` | Warning: client evictions observed |
| `bigdata` | `uaenorth` | Warning: client evictions observed |
| `map` | `uaenorth` | Critical: MDT latency incident and heavy client evictions |
| `cep2` | `uaenorth` | Healthy in reviewed metrics |

## Executive Summary

The UAE Lustre fleet has three important risk areas:

1. `yun-training` and `l4-service` are in critical OST capacity pressure.
2. `map` had a severe metadata latency incident and a large client eviction burst.
3. Several filesystems had smaller client eviction events, suggesting transient client, network, or storage disruption.

The most urgent operational actions are to stop or reduce write-heavy workload on `yun-training` and `l4-service`, reclaim or expand capacity, and investigate the `map` incident window.

## Critical Findings

| Severity | Filesystem | Finding | Evidence |
| --- | --- | --- | --- |
| Critical | `yun-training` | OST almost full | `OST0000` latest and peak used `98.5%`; latest and minimum available `59.21 GiB`. This is above the `80%` critical safety boundary and below the `100 GiB` critical free-space boundary. |
| Critical | `l4-service` | OST critically full | `OST0000` latest and peak used `95.2%`; latest and minimum available about `194 GiB`. This is above the `80%` critical safety boundary. |
| Critical | `map` | Severe MDT latency spike | `MDT0000` metadata operations peaked far above the `1000 ms` hang-risk threshold. Worst observed: `getxattr` `322,291.8 ms`, `samedir_rename` `183,737.4 ms`, `rename` `91,868.8 ms`, `mkdir` `58,046.1 ms`. |
| Critical / High | `map` | Large client eviction burst | `796` client evictions over 24h; max 5-minute interval `160`. This strongly correlates with the MDT latency incident. |

## Warnings

| Severity | Filesystem | Finding | Evidence |
| --- | --- | --- | --- |
| Warning | `ide-cache` | Near capacity threshold | `OST0000` reached `74.7%` used and minimum available `1016.48 GiB`, close to the `75%` warning threshold and just below the `1 TiB` free-space warning. |
| Warning | `laser` | Client evictions | `10` evictions over 24h, max interval `5`, latest interval `0`. |
| Warning | `training` | Client evictions | `16` evictions over 24h, max interval `8`, latest interval `0`. |
| Warning | `bigdata` | Client evictions | `6` evictions over 24h, max interval `3`, latest interval `0`. |
| Warning | `map` | Sparse capacity samples | Some `map` capacity series had fewer than expected samples, example `149/288`. Latency series had full samples, so the incident signal is still valid. |

## Healthy Signals

| Filesystem | Observation |
| --- | --- |
| `cep2` | OSTs around `44%` used, MDT bytes and inodes low, no latency, HSM, or capacity issue found. |
| `bigdata` | OST around `38.5%` used, MDT low. Evictions were present but small. |
| `training` | Top OST around `70.4%` used, below capacity warning. Evictions were present but small. |
| `laser` | OSTs around `56%` used, MDT low. Evictions were present but small. |

## HSM Status

No HSM threshold breach was observed in the reviewed data.

No clear issue was found for:

- `HSMActionErrors`
- `HSMCurrentRequests`
- HSM backlog thresholds

## Capacity Risk Assessment

`yun-training` and `l4-service` should be treated as capacity-critical.

Recommended actions:

1. Pause or avoid new write-heavy phases on these filesystems.
2. Reclaim old data, snapshots, temporary output, or abandoned workload artifacts.
3. If workload growth is expected, expand capacity or migrate write-heavy workloads.
4. Watch `azure_managed_lustre_ost_bytes_used_percent` and `azure_managed_lustre_ost_bytes_available`.

`ide-cache` is not yet critical, but it is close enough to warning thresholds that it should be monitored.

## Map Incident Assessment

`map` is the most important stability incident in the 24-hour review.

Observed pattern:

- Very high MDT latency on metadata operations.
- Large client eviction burst.
- Latest latency samples returned to normal, so the issue appears transient rather than currently sustained.

Likely impact:

- Client reconnects.
- Application stalls.
- Metadata operations delayed.
- Potential D-state or hung I/O symptoms during the incident window.
- Possible job failures or slowdowns.

Recommended investigation:

1. Correlate `map` latency spikes with workload or job timeline.
2. Check AKS node logs and affected workload pods around the incident window.
3. Check Lustre client logs for reconnects and evictions.
4. Review whether metadata-heavy operations occurred: `getxattr`, rename storms, directory scans, small-file workload, dataset indexing, or cleanup jobs.
5. Watch the new eviction metrics after exporter rollout:
   - `azure_managed_lustre_filesystem_client_evictions`
   - `azure_managed_lustre_mdt_client_evictions`

## Dashboard Gap And Fix Status

The existing dashboard could already show:

- Capacity issues on `yun-training` and `l4-service`.
- MDT latency spike on `map`, if viewing the last 24 hours.
- HSM status.
- Freshness and collection health.

The dashboard previously could not clearly show client eviction counts because `LustreClientEvictions` was not exported.

Implemented fix:

- Added Azure Monitor `LustreClientEvictions` collection using `Total` aggregation.
- Added Prometheus metrics:
  - `azure_managed_lustre_mdt_client_evictions`
  - `azure_managed_lustre_filesystem_client_evictions`
- Added Grafana panels for eviction visibility.
- Added alert rules for eviction occurrence and burst detection.
- Updated the Kubernetes manifest image tag to:
  - `zhangchl007/vmss-metrics-exporter:v26-client-evictions-multiarch`

## Deployment Notes

The current Kubernetes manifest now points to the multi-arch image tag:

```yaml
image: zhangchl007/vmss-metrics-exporter:v26-client-evictions-multiarch
```

Deployment verification depends on stable access to the AKS API and Docker Hub from the workstation or deployment environment. Earlier rollout attempts were affected by:

- Global Secure Access / network routing timeouts to the AKS API endpoint.
- Docker Hub connectivity issues.
- An initial image platform mismatch where AKS `amd64` nodes attempted to pull an `arm64`-only tag.

## Recommended Next Steps

1. Complete exporter rollout once AKS API and Docker Hub connectivity are stable.
2. Verify the image manifest includes both `linux/amd64` and `linux/arm64`.
3. Verify new metrics after rollout:

```bash
kubectl port-forward -n default service/vmss-metrics-exporter 8000:8000
curl -s localhost:8000/metrics | grep azure_managed_lustre_filesystem_client_evictions
curl -s localhost:8000/metrics | grep azure_managed_lustre_mdt_client_evictions
curl -s localhost:8000/metrics | grep azure_managed_lustre_last_success_timestamp_seconds
```

4. In Grafana, review the last 24 hours:
   - OST used % vs 75/80 guardrails
   - Lowest OST available %
   - MDT client latency
   - Client evictions in range
   - Client evictions trend
5. Take immediate capacity action for:
   - `yun-training`
   - `l4-service`
6. Investigate the `map` metadata latency and eviction incident as a stability event.

## Appendix: Useful Commands

Check rollout:

```bash
make deploy KUBE_NAMESPACE=default KUBE_MANIFEST=deploy/kubernetes.yaml
make rollout KUBE_NAMESPACE=default
kubectl get pods -n default -l app.kubernetes.io/name=vmss-metrics-exporter -o wide
```

Inspect image pull or pod runtime failures:

```bash
kubectl get events -n default --sort-by=.lastTimestamp | tail -n 40
kubectl describe pod -n default -l app.kubernetes.io/name=vmss-metrics-exporter
kubectl logs -n default -l app.kubernetes.io/name=vmss-metrics-exporter --tail=100
```

Verify Docker image platforms:

```bash
docker buildx imagetools inspect zhangchl007/vmss-metrics-exporter:v26-client-evictions-multiarch
```
