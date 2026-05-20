# Azure Managed Lustre — AV simulation pressure test runbook

This runbook is the canonical procedure for pressure-testing an Azure Managed Lustre filesystem from AKS using a realistic **autonomous-vehicle (AV) simulation** workload. It is the single source of truth for the manifests under `deploy/pressure-test/` and the simulator at `scripts/av_lustre_workload.py`.

> **Do not run any phase except discovery against a shared production filesystem without prior approval, a quota guard, and a documented rollback window.**

> **Dataset immutability is a hard rule.** The source dataset at `DATASET_ROOT` (currently `/mnt/lustre/waymo_v2/`, 682.7 GiB / 19,618 files across the four Waymo Open Dataset v2.0.1 splits `training`, `validation`, `testing_location`, `testing`) is read-only for every phase. The simulator, discovery Job, and cleanup Job only ever write under `RESULT_ROOT/<RUN_ID>/<pod-name>/`. The two roots are disjoint by configuration **and** enforced at runtime by a startup preflight and a per-write path gate that aborts the pod if any output resolves under `DATASET_ROOT`.

## 1. Goals and scope

The pressure test answers three questions for AV simulation:

1. **Throughput** — can the filesystem sustain the aggregate read/write bandwidth that N concurrent simulation pods produce against the real dataset?
2. **Tail latency** — does per-file (and per-bucket) p95/p99 latency stay within the AV SLO under sustained pressure, including hotset (shared popular files) reads and cold-cache epochs?
3. **Stability** — does the cluster (CSI driver, exporter, AKS nodes) remain healthy through ramps, sustained load, and burst phases without metric staleness, evictions, or capacity breach?

What this runbook explicitly does **not** validate:

- Source dataset semantics or AV algorithm correctness.
- HSM tiering behavior.
- Production failover / region-pair behavior.
- Destructive fill-to-full tests.

## 2. AV workload model

The simulator is configured to match a typical AV simulation read profile:

| Trait | Real AV simulation | Simulator behavior |
| --- | --- | --- |
| Sensor logs (rosbag/h5/parquet) | Medium–large sequential streaming | `read-pattern: full`, large `CHUNK_SIZE_BYTES`, throughput-oriented |
| Per-frame label / calibration files | Many small open/read/close ops | small bucket reads driven by the real dataset; head-tail pattern is even cheaper if needed |
| Shared maps / models / lookup tables | Same files read by many pods every epoch | `HOTSET_COUNT > 0`; top-K largest files re-read by every pod each epoch |
| Index/metadata lookups | Random seeks inside large files | `read-pattern: random-offset`, configurable `RANDOM_OFFSET_READS` |
| Multi-epoch training-like access | Several passes over the dataset | `EPOCHS > 1`, reshuffled deterministically per epoch |
| Output traces written back | Per-pod derived artifacts | `MODE: read-write-output`, outputs isolated under `RESULT_ROOT/<RUN_ID>/<pod-name>/` |

File sizes are classified into buckets so that per-bucket latency is reported and SLO'd independently. Defaults:

| Bucket | Max size | Typical AV content |
| --- | --- | --- |
| `small` | ≤ 1 MiB | Label files, calibration metadata, tiny indexes |
| `medium` | ≤ 64 MiB | Short sensor segments, image batches |
| `large` | ≤ 512 MiB | Rosbag chunks, log shards |
| `xlarge` | > 512 MiB | Long sensor recordings, archive bundles |

> For the current Waymo v2 dataset, expect `medium` and `large` to dominate (Parquet shards typically tens to hundreds of MiB) and `xlarge` to be near-zero. The `discover` mode prints the actual `bucket_counts` / `bucket_bytes`; re-check after every download or dataset refresh before relying on the default SLOs.

## 3. Safety policy

The default policy is to stop before the test filesystem exceeds **80 % used capacity**.

Calculate the allowed output budget before every write-output phase:

```text
safe_write_budget_bytes = capacity_bytes * 0.80 - used_bytes - safety_reserve_bytes
```

Block the phase if `OUTPUT_BYTES_PER_INPUT * total_dataset_bytes * epochs_planned` exceeds that budget. Use [scripts/lustre_safe_write_budget.py](scripts/lustre_safe_write_budget.py).

Stop a running phase if any of the following occurs:

- projected or observed filesystem usage reaches 80 %;
- available bytes fall below the safety reserve;
- AV pods report source read errors or output write errors;
- per-bucket p95 latency exceeds the agreed SLO and the trend is rising;
- OST or MDT client latency sustained > 5× baseline;
- `rate(azure_managed_lustre_collection_errors_total[5m]) > 0`;
- `time() - azure_managed_lustre_last_success_timestamp_seconds` exceeds the stale threshold;
- AKS nodes report CPU/memory/disk pressure or AV pods repeatedly fail or get evicted.

Output writes are constrained to:

```text
<RESULT_ROOT>/<RUN_ID>/<pod-name>/
```

Cleanup is scoped to `RESULT_ROOT/<RUN_ID>` only and refuses to touch the dataset or anything outside that path.

The simulator additionally enforces three runtime guardrails so a misconfigured `DATASET_ROOT` / `RESULT_ROOT` pair never produces a write into the dataset:

1. **Disjoint-roots preflight** — at startup, `Path.resolve()` is run on both roots and the pod aborts with a clear error if they are equal, if `RESULT_ROOT` is nested inside `DATASET_ROOT`, or if `DATASET_ROOT` is nested inside `RESULT_ROOT`. The check fires in every mode (`discover`, `read-only`, `read-write-output`, `verify-output`).
2. **Per-write path gate** — every output path produced by the simulator must resolve under `RESULT_ROOT/<RUN_ID>/<pod-name>/`; any resolved path whose prefix matches `DATASET_ROOT` (plus separator) is rejected before `open()`.
3. **Dataset-immutability sample** — before launching Phase E (the pressure ramps), capture a 50-file random sample from `DATASET_ROOT` to `reports/<run-base>/dataset-immutability-baseline.tsv` with `(path, size, mtime, sha256-of-first-MiB)`. Re-check the same sample after every phase; any mismatch is a hard abort and indicates a write escape.

## 4. Validated AKS / Lustre / CSI setup

| Item | Value |
| --- | --- |
| AKS cluster | `aks-storage-test` |
| AKS resource group | `aks-test-rg` |
| Lustre filesystem | `lustrenewfile` (`lustrefs`) |
| Lustre resource group | `lustre-rg` |
| Region | `westus3` |
| MGS address | `10.10.16.6` |
| Capacity | `8.0Ti` |
| Static StorageClass | `sc-lustrenewfile-static` |
| Pressure-test PVC | `lustre-pressure-test/lustre-pressure-test-pvc` |
| Dataset path on Lustre | `/mnt/lustre/waymo_v2/` (Waymo Open Dataset v2.0.1) |
| Dataset size / file count | 682.7 GiB / 19,618 files / 4 splits (`training`, `validation`, `testing_location`, `testing`) |
| Validated CSI image | `mcr.microsoft.com/oss/v2/kubernetes-csi/azurelustre-csi:v0.4.0-jammy-3` |
| Validated `LUSTRE_VERSION` | `2.15.7` |
| Validated `CLIENT_SHA_SUFFIX` | `33-g79ddf99` |
| Validated node pool | `juicefspool` (Ubuntu 22.04, kernel `5.15.0-1110-azure`, SKU `Standard_D8d_v5`, 8 vCPU / 32 GiB each) |
| Node count | 2 baseline · ≥ 4 during the pressure window (each node is `Standard_D8d_v5` with ~7 allocatable vCPU; 4 nodes ≈ 28 pod slots at 1 vCPU/pod, 6 nodes ≈ 42 slots for `ramp-40`). Scaled out via `az aks nodepool scale`, scaled back after Phase F. |

Pressure-test Jobs pin `nodeSelector: kubernetes.azure.com/agentpool: juicefspool`. Azure Linux 3 (`dexpool`) is out of scope until Microsoft publishes a working AMLFS Lustre client for kernel `6.6.130.1-3.azl3`; see the previous test summary captured in `/memories/session/plan.md`.

## 5. Files

| File | Purpose |
| --- | --- |
| [deploy/pressure-test/namespace.yaml](deploy/pressure-test/namespace.yaml) | Dedicated `lustre-pressure-test` namespace. |
| [deploy/pressure-test/pvc-example.yaml](deploy/pressure-test/pvc-example.yaml) | Static StorageClass/PV/PVC for the test filesystem. |
| [deploy/pressure-test/azurelustre-csi-node-rbac.yaml](deploy/pressure-test/azurelustre-csi-node-rbac.yaml) | RBAC for the Azure Lustre CSI node DaemonSet. |
| [deploy/pressure-test/av-workload-configmap.yaml](deploy/pressure-test/av-workload-configmap.yaml) | Knobs ConfigMap + stub script ConfigMap. |
| [deploy/pressure-test/av-dataset-discovery-job.yaml](deploy/pressure-test/av-dataset-discovery-job.yaml) | Single-pod read-only dataset profiler. |
| [deploy/pressure-test/av-dataset-validation-job.yaml](deploy/pressure-test/av-dataset-validation-job.yaml) | Indexed pressure-test Job (default parallelism 10). |
| [deploy/pressure-test/av-output-cleanup-job.yaml](deploy/pressure-test/av-output-cleanup-job.yaml) | Scoped cleanup for `RESULT_ROOT/<RUN_ID>` only. |
| [deploy/pressure-test/waymo-download-job.yaml](deploy/pressure-test/waymo-download-job.yaml) | One-shot Job that downloads Waymo Open Dataset v2.0.1 from `gs://waymo_open_dataset_v_2_0_1/` into `/mnt/lustre/waymo_v2/`. **Kept in the repo for reproducibility**, but the Job object and its `gcp-credentials` Secret are deleted from the cluster after the download completes (see § 13). Never re-run unless the dataset must be replaced. |
| [scripts/av_lustre_workload.py](scripts/av_lustre_workload.py) | AV simulator (modes: `discover`, `read-only`, `read-write-output`, `verify-output`). |
| [scripts/av_pressure_phase.sh](scripts/av_pressure_phase.sh) | Phase driver: patch Job, wait, collect summaries. |
| [scripts/lustre_safe_write_budget.py](scripts/lustre_safe_write_budget.py) | Local capacity budget calculator. |

## 6. Configuration reference

All knobs are exposed via `ConfigMap/av-lustre-workload-config`. The validation Job reads them through `envFrom`. Override per-phase via `kubectl set env job/av-dataset-validation KEY=VALUE` (or via [scripts/av_pressure_phase.sh](scripts/av_pressure_phase.sh)).

| Key | Default | Effect |
| --- | --- | --- |
| `DATASET_ROOT` | `/mnt/lustre/waymo_v2` | Real AV dataset directory. The disjoint-roots preflight (§ 3) verifies this is not nested with `RESULT_ROOT`. |
| `RESULT_ROOT` | `/mnt/lustre/pressure-tests/av-results` | Where output artifacts go. Must be disjoint from `DATASET_ROOT`; the simulator aborts at startup otherwise. |
| `SPLIT_FILTER` | `` (empty = no filter) | Comma-separated list of first-level directories under `DATASET_ROOT` to descend into (e.g. `training,validation`). Empty walks all children. Used to scope phases. |
| `SUBPATH` | `` (empty = no override) | Relative path under `DATASET_ROOT` that becomes the effective walk root for this phase (e.g. `training/camera_image`). Must not contain `..` or start with `/`; the simulator rejects either. |
| `RUN_ID` | `av-run-001` | One path segment; used in result subpath and as random seed input. |
| `POD_COUNT` | `10` | Sets `parallelism`/`completions`. Override per phase. |
| `FILES_PER_POD` | `0` (no cap) | Cap files processed per pod per epoch. |
| `MAX_BYTES_PER_POD` | `0` (no cap) | Cap bytes read per pod per epoch. |
| `CHUNK_SIZE_BYTES` | `4MiB` | Streaming chunk size and random/head-tail read width. |
| `VERIFY_READS` | `true` | SHA-256 hashing during full reads. Disable for pure throughput phases. |
| `READ_PATTERN` | `full` | `full` \| `random-offset` \| `head-tail`. |
| `RANDOM_OFFSET_READS` | `4` | Random offsets per file when `READ_PATTERN=random-offset`. |
| `EPOCHS` | `1` | Number of shard passes per pod. |
| `WARMUP_SECONDS` | `0` | Latency samples in the first N seconds go into a `warmup_latency_ms` bucket and are excluded from steady-state percentiles. |
| `HOTSET_COUNT` | `0` | Each pod additionally reads the dataset-wide top-K largest files every epoch. |
| `OUTPUT_BYTES_PER_INPUT` | `0.001` | Synthetic output payload size, as a fraction of input bytes. |
| `MAX_OUTPUT_BYTES_PER_FILE` | `1MiB` | Cap synthetic output payload per input file. |
| `SMALL_MAX_BYTES`, `MEDIUM_MAX_BYTES`, `LARGE_MAX_BYTES` | `1MiB / 64MiB / 512MiB` | Size-bucket boundaries. |
| `EXCLUDE_PATHS` | `/mnt/lustre/pressure-tests` | Colon-separated paths excluded from enumeration. Do **not** add `DATASET_ROOT` here — reads must traverse it. The metadata-heavy phase extends this list to skip `training/camera_image`, `training/lidar`, and the `*_segmentation` directories. |
| `STATS_INTERVAL_SECONDS` | `30` | Per-pod JSON progress interval. |
| `BUCKET_SLO_P95_MS` | `small=20,medium=300,large=3000,xlarge=8000` | p95 latency SLO per bucket. Defaults are loosened relative to the original runbook because Waymo `medium` files are Parquet shards with per-row-group seeks. |
| `FAIL_ON_SLO` | `false` | When `true`, pods exit with code 4 if any bucket's p95 exceeds its threshold. |
| `FAIL_ON_READ_ERROR` | `true` | First source read error aborts the pod. |
| `FAIL_ON_WRITE_ERROR` | `true` | First output write error aborts the pod. Also fires (exit 3) if any output path resolves under `DATASET_ROOT` — the per-write path gate from § 3. |

### Pod summary schema

Each pod emits one JSON summary on stdout at the end of its run. Key fields:

```json
{
  "event": "summary",
  "pod": "av-dataset-validation-0",
  "pod_index": 0,
  "pod_count": 10,
  "mode": "read-write-output",
  "read_pattern": "full",
  "epochs_completed": 2,
  "warmup_seconds": 60.0,
  "hotset_count": 25,
  "files_attempted": 1234,
  "files_succeeded": 1234,
  "files_failed": 0,
  "bytes_read": 12345678901,
  "bytes_written": 12345678,
  "elapsed_seconds": 612.3,
  "throughput_mib_s": 192.7,
  "per_bucket_counts": {"small": 600, "medium": 400, "large": 200, "xlarge": 34},
  "per_bucket_bytes":  {"small": ...},
  "per_file_latency_ms": {
    "min": 0.3, "p50": 4.2, "p95": 88.0, "p99": 230.1, "max": 540.0, "mean": 17.6,
    "per_bucket": {"small": {"count": 600, "p50": 1.1, "p95": 5.4, "p99": 9.8, "max": 14.2}, ...}
  },
  "warmup_latency_ms": {"count": 312, "p50": 12.8, "p95": 130.0, ...},
  "hotset_latency_ms": {
    "overall": {"count": 50, "p50": 70.0, "p95": 240.0, ...},
    "per_bucket": {"large": {"count": 30, "p50": 80.0, ...}}
  },
  "slo": {
    "pass": true,
    "failures": [],
    "thresholds_ms": {"small": 20, "medium": 300, "large": 3000, "xlarge": 8000},
    "per_bucket": {"small": {"count": 600, "p95_ms": 5.4, "threshold_ms": 20, "pass": true}, ...}
  },
  "errors": []
}
```

Pod exit codes: `0` ok · `1` per-mode failure default · `2` first source read error · `3` first output write error · `4` SLO breach with `FAIL_ON_SLO=true`.

## 7. Phased pressure-test plan

The test is structured as ordered **phases**. Each phase is a dedicated `RUN_ID`, applied via [scripts/av_pressure_phase.sh](scripts/av_pressure_phase.sh). Phases share the same dataset; they differ in concurrency, read pattern, hotset, scope (`--split` / `--subpath`), and epoch settings.

| # | Phase | Pods | Mode | Pattern | Epochs | Hotset | Warmup (s) | Scope (`--split` / `--subpath`) | Goal |
| --: | --- | --: | --- | --- | --: | --: | --: | --- | --- |
| 0 | `baseline` | – | metrics only | – | – | – | – | – | Quiet 30–60 min idle baseline before any phase. |
| 1 | `discovery` | 1 | `discover` | – | – | – | – | – | Profile `DATASET_ROOT`; confirm ≈19,618 files across the four Waymo splits. |
| 2 | `pre-E-immutability` | – | offline | – | – | – | – | – | Capture 50-file `(path, size, mtime, sha256-of-first-MiB)` immutability sample (§ 8.5). |
| 3 | `smoke` | 1 | `read-only` | `full` | 1 | 0 | 0 | `--split training`, `MAX_BYTES_PER_POD=10GiB` | End-to-end smoke on real data; validate filter logic. |
| 4 | `ramp-10-ro` | 10 | `read-only` | `full` | 1 | 0 | 30 | `--split training` | 10-pod baseline; per-bucket p95 + throughput floor. |
| 5 | `ramp-10-rwo` | 10 | `read-write-output` | `full` | 1 | 0 | 30 | `--split training` | Same load with output amplification; capacity-delta check. |
| 6 | `ramp-20` | 20 | `read-write-output` | `full` | 1 | 0 | 60 | `--split training,validation` | First real concurrency stress. |
| 7 | `ramp-40` | 40 | `read-write-output` | `full` | 1 | 0 | 60 | `--split training,validation` | Peak throughput target (requires juicefspool scaled to 4 nodes). |
| 8 | `metadata-heavy` | 20 | `read-only` | `full` | 1 | 0 | 60 | `--subpath training`, `EXCLUDE_PATHS` extended to skip `training/camera_image`, `training/lidar`, `training/*_segmentation` | Force the walk onto tiny-file directories (`camera_box`, `camera_calibration`, `camera_hkp`, `camera_to_lidar_box_association`, `stats`, etc.) to stress MDT operations. Tight SLO: `small` p95 ≤ 30 ms. |
| 9 | `hotset` | 20 | `read-only` | `full` | 3 | 50 | 60 | `--subpath training/camera_image` | All pods deterministically re-read the top-50 largest camera_image shards across 3 epochs. Look for warm-cache p95 improvement on epochs 2/3. |
| 10 | `soak` | 20 | `read-write-output` | `full` | 4 | 25 | 120 | `--split training,validation` | Multi-epoch sustained run; tail-latency and leak surface. Gate: epoch-4 p95 ≤ 1.2 × epoch-1 p95. |
| 11 | `cool-down` | – | – | – | – | – | – | – | Watch capacity/latency return toward baseline. |
| 12 | `verify+cleanup` | 1 | `verify-output` then cleanup Job | – | – | – | – | – | Validate outputs, then delete `RESULT_ROOT/<RUN_ID>` per phase. |
| 13 | `report` | – | – | – | – | – | – | – | Produce the structured report (§ 12). |

You can selectively run a subset; phases 0–5 are the minimum recommended after any dataset refresh.

If a phase trips a kill switch (capacity > 80 %, exporter stale, EIO bursts, pod crashes, node pressure, **or** immutability-sample mismatch — see § 11) the sweep stops and the remaining phases are not run until root cause is fixed.

## 8. Day-of execution

### 8.0 Cluster scaling (before Phase 4)

The ramp phases need pod slots beyond the 2-node baseline of `juicefspool`. Each node is `Standard_D8d_v5` with ~7 allocatable vCPU, so plan node-count by phase:

| Phase | Pods | Min nodes (1 vCPU/pod) |
| --- | --: | --: |
| ramp-10-ro / ramp-10-rwo | 10 | 2 (baseline) |
| ramp-20 | 20 | 3–4 |
| ramp-40 | 40 | 6 |

Scale out before each ramp window and scale back after Phase 13.

```bash
# Scale out before ramp-40 (use 4 nodes for ramp-20, 6 for ramp-40)
az aks nodepool scale \
   --resource-group aks-test-rg \
   --cluster-name aks-storage-test \
   --name juicefspool \
   --node-count 6

# Wait for new nodes + CSI DaemonSet to converge
kubectl get nodes -l kubernetes.azure.com/agentpool=juicefspool
kubectl rollout status -n kube-system daemonset/csi-azurelustre-node --timeout=300s
kubectl get pods -n kube-system -l app=csi-azurelustre-node -o wide   # expect N Running, 3/3 each
```

After Phase 13 (report), scale back:

```bash
az aks nodepool scale \
   --resource-group aks-test-rg \
   --cluster-name aks-storage-test \
   --name juicefspool \
   --node-count 2
```

### 8.1 One-time setup

```bash
# Base resources: namespace, RBAC, PVC, ConfigMaps
kubectl apply -k deploy/pressure-test

# Publish the simulator script as a ConfigMap (single source of truth: scripts/av_lustre_workload.py)
make av-workload-script
```

Confirm the Azure Lustre CSI driver is Running on `juicefspool`:

```bash
kubectl get pods -n kube-system -l app=csi-azurelustre-node -o wide
```

### 8.2 Phase 0 — baseline (30–60 min, no load)

Capture exporter health and Lustre baseline in the **PromQL pack** below. Record values; they become the comparison floor for every later phase.

### 8.3 Phase 1 — discovery

```bash
kubectl delete job -n lustre-pressure-test av-dataset-discovery --ignore-not-found
kubectl apply -f deploy/pressure-test/av-dataset-discovery-job.yaml
kubectl wait -n lustre-pressure-test --for=condition=complete \
   job/av-dataset-discovery --timeout=900s
kubectl logs -n lustre-pressure-test job/av-dataset-discovery \
   | tee reports/av-press-$(date +%Y%m%d)/discovery.json
```

Confirm the JSON profile shows the current Waymo dataset shape:

- `dataset_root` equals `/mnt/lustre/waymo_v2`
- `file_count` between 19,000 and 20,000 (expected ≈ 19,618)
- `directory_count` ≥ 50 (4 splits × ~17 component dirs)
- `bucket_counts.medium` + `bucket_counts.large` > 0
- `bucket_counts.xlarge` is near zero (if `xlarge` > 5 % of bytes, raise `LARGE_MAX_BYTES` to 1024 MiB in the ConfigMap and rerun discovery)

If `DATASET_ROOT` is wrong, update [deploy/pressure-test/av-workload-configmap.yaml](deploy/pressure-test/av-workload-configmap.yaml) and rerun phase 1.

### 8.4 Phase 2 — capture the dataset-immutability sample

Before launching any Phase E run, baseline a fixed 50-file sample of `DATASET_ROOT` for the post-phase immutability re-check. Run this once per `RUN_BASE`.

```bash
RUN_BASE=av-press-$(date +%Y%m%d-%H%M)
mkdir -p reports/${RUN_BASE}
BASELINE=reports/${RUN_BASE}/dataset-immutability-baseline.tsv

kubectl run av-immutability-baseline --rm -i --restart=Never \
   --image=alpine:3.20 -n lustre-pressure-test \
   --overrides='{"apiVersion":"v1","spec":{"nodeSelector":{"kubernetes.azure.com/agentpool":"juicefspool"},"containers":[{"name":"baseline","image":"alpine:3.20","command":["sh","-c","apk add --no-cache coreutils findutils >/dev/null 2>&1; set -e; find /mnt/lustre/waymo_v2 -type f | shuf -n 50 | while read f; do printf \"%s\\t%s\\t%s\\t\" \"$f\" \"$(stat -c %s \"$f\")\" \"$(stat -c %Y \"$f\")\"; head -c $((1024*1024)) \"$f\" | sha256sum | awk \"{print \\$1}\"; done"],"volumeMounts":[{"name":"lustre","mountPath":"/mnt/lustre"}]}],"volumes":[{"name":"lustre","persistentVolumeClaim":{"claimName":"lustre-pressure-test-pvc"}}]}}' \
   > ${BASELINE}

wc -l ${BASELINE}   # must be 50
```

Re-check after each Phase E phase by re-running the same pod (rename it) and `diff`-ing the output against `${BASELINE}`; any non-zero diff is a hard abort (see § 11).

### 8.5 Phase 3 — smoke + dry runs

```bash
RUN_BASE=av-press-$(date +%Y%m%d-%H%M)
OUTDIR=reports/${RUN_BASE}
SLO="small=20,medium=300,large=3000,xlarge=8000"

# Single-pod smoke on training, 10 GiB cap
scripts/av_pressure_phase.sh --phase smoke --parallelism 1 \
   --run-id ${RUN_BASE}-smoke --mode read-only \
   --read-pattern full --split training \
   --bucket-slo "${SLO}" --output-dir ${OUTDIR}
```

Required outcomes:

- `files_failed: 0`.
- Every output line in pod logs resolves under `${RESULT_ROOT}/${RUN_ID}/<pod-name>/`.
- The immutability sample (§ 8.4) is unchanged after the phase.

### 8.6 Phases 4–10 — pressure ramps

Run each phase via the helper. **After every phase**, re-run the immutability check from § 8.4 and `diff` against the baseline before launching the next phase. Example for the full sequence:

```bash
RUN_BASE=av-press-$(date +%Y%m%d-%H%M)
OUTDIR=reports/${RUN_BASE}
SLO="small=20,medium=300,large=3000,xlarge=8000"
META_EXCLUDE="/mnt/lustre/pressure-tests:/mnt/lustre/waymo_v2/training/camera_image:/mnt/lustre/waymo_v2/training/lidar:/mnt/lustre/waymo_v2/training/lidar_segmentation:/mnt/lustre/waymo_v2/training/camera_segmentation"

scripts/av_pressure_phase.sh --phase ramp-10-ro --parallelism 10 \
   --run-id ${RUN_BASE}-ramp-10-ro --mode read-only \
   --read-pattern full --split training \
   --warmup-seconds 30 --bucket-slo "${SLO}" --output-dir ${OUTDIR}

scripts/av_pressure_phase.sh --phase ramp-10-rwo --parallelism 10 \
   --run-id ${RUN_BASE}-ramp-10-rwo --mode read-write-output \
   --read-pattern full --split training \
   --warmup-seconds 30 --bucket-slo "${SLO}" --output-dir ${OUTDIR}

scripts/av_pressure_phase.sh --phase ramp-20 --parallelism 20 \
   --run-id ${RUN_BASE}-ramp-20 --mode read-write-output \
   --read-pattern full --split training,validation \
   --warmup-seconds 60 --bucket-slo "${SLO}" --output-dir ${OUTDIR}

# Requires juicefspool scaled to 4 nodes (§ 8.0)
scripts/av_pressure_phase.sh --phase ramp-40 --parallelism 40 \
   --run-id ${RUN_BASE}-ramp-40 --mode read-write-output \
   --read-pattern full --split training,validation \
   --warmup-seconds 60 --bucket-slo "${SLO}" --output-dir ${OUTDIR}

# Metadata-heavy: walk training/, exclude the large directories
kubectl set env -n lustre-pressure-test configmap/av-lustre-workload-config EXCLUDE_PATHS="${META_EXCLUDE}"
scripts/av_pressure_phase.sh --phase metadata-heavy --parallelism 20 \
   --run-id ${RUN_BASE}-metadata-heavy --mode read-only \
   --read-pattern full --subpath training \
   --warmup-seconds 60 \
   --bucket-slo "small=30,medium=300,large=3000,xlarge=8000" --output-dir ${OUTDIR}
kubectl set env -n lustre-pressure-test configmap/av-lustre-workload-config EXCLUDE_PATHS="/mnt/lustre/pressure-tests"

scripts/av_pressure_phase.sh --phase hotset --parallelism 20 \
   --run-id ${RUN_BASE}-hotset --mode read-only \
   --read-pattern full --epochs 3 --hotset-count 50 \
   --subpath training/camera_image \
   --warmup-seconds 60 --bucket-slo "${SLO}" --output-dir ${OUTDIR}

scripts/av_pressure_phase.sh --phase soak --parallelism 20 \
   --run-id ${RUN_BASE}-soak --mode read-write-output \
   --read-pattern full --epochs 4 --hotset-count 25 \
   --split training,validation --warmup-seconds 120 \
   --bucket-slo "${SLO}" --fail-on-slo --output-dir ${OUTDIR}
```

Each invocation:

1. Cleans any prior `job/av-dataset-validation`.
2. Re-applies the manifest, patches `parallelism`/`completions` and env vars (including `SPLIT_FILTER` / `SUBPATH` when `--split` / `--subpath` are passed).
3. Waits for completion.
4. Saves pod logs to `${OUTDIR}/<run-id>-<phase>.log`.
5. Extracts per-pod `event=summary` objects to `${OUTDIR}/<run-id>-<phase>-summaries.jsonl`.

### 8.7 Phase 12 — verify and cleanup

Verify the outputs (optional, but recommended after a long endurance run):

```bash
kubectl run av-verify --rm -it --restart=Never \
   --image=python:3.12-slim -n lustre-pressure-test \
   --overrides='{"apiVersion":"v1","spec":{"nodeSelector":{"kubernetes.azure.com/agentpool":"juicefspool"},"containers":[{"name":"av-verify","image":"python:3.12-slim","command":["python","/opt/av/av_lustre_workload.py","--mode","verify-output"],"envFrom":[{"configMapRef":{"name":"av-lustre-workload-config"}}],"volumeMounts":[{"name":"lustre","mountPath":"/mnt/lustre"},{"name":"av-script","mountPath":"/opt/av","readOnly":true}]}],"volumes":[{"name":"lustre","persistentVolumeClaim":{"claimName":"lustre-pressure-test-pvc"}},{"name":"av-script","configMap":{"name":"av-lustre-workload-script","defaultMode":493}}]}}'
```

Then clean up per `RUN_ID`:

```bash
kubectl set env -n lustre-pressure-test job/av-output-cleanup --containers=cleanup \
   RUN_ID=<the run id you want to delete>
kubectl apply -f deploy/pressure-test/av-output-cleanup-job.yaml
kubectl wait -n lustre-pressure-test --for=condition=complete \
   job/av-output-cleanup --timeout=900s
kubectl logs -n lustre-pressure-test job/av-output-cleanup
```

The cleanup Job refuses to delete `DATASET_ROOT`, ancestors of `DATASET_ROOT`, or paths outside `RESULT_ROOT`, and it walks for symlink escapes before removing anything.

## 9. PromQL pack

These queries use metric names emitted by [src/vmss_metrics_exporter/collector.py](src/vmss_metrics_exporter/collector.py). Replace `lustrefs` with your filesystem label as needed.

### 9.1 Health

```promql
# Exporter freshness (must stay below the agreed stale threshold during the run)
time() - azure_managed_lustre_last_success_timestamp_seconds

# Exporter collection errors (must remain flat at 0)
rate(azure_managed_lustre_collection_errors_total[5m])

# Exporter collection duration
azure_managed_lustre_collection_duration_seconds

# Discovered filesystems
azure_managed_lustre_filesystem_total
```

### 9.2 Capacity (hard safety boundary)

```promql
# Used % must stay below 80
max by (filesystem_name) (azure_managed_lustre_ost_bytes_used_percent)

# Available % must stay above 20
min by (filesystem_name) (azure_managed_lustre_ost_bytes_available_percent)

# Absolute available bytes
min by (filesystem_name) (azure_managed_lustre_ost_bytes_available)
```

### 9.3 Throughput

```promql
# Aggregate read throughput by filesystem
sum by (filesystem_name) (
  azure_managed_lustre_client_read_throughput_bytes_per_second
)

# Aggregate write throughput by filesystem
sum by (filesystem_name) (
  azure_managed_lustre_client_write_throughput_bytes_per_second
)

# Read ops/s and write ops/s
sum by (filesystem_name) (azure_managed_lustre_client_read_ops)
sum by (filesystem_name) (azure_managed_lustre_client_write_ops)
```

### 9.4 Latency

```promql
# OST client latency by operation
max by (filesystem_name, operation) (
  azure_managed_lustre_ost_client_latency_milliseconds
)

# MDT client latency by operation
max by (filesystem_name, operation) (
  azure_managed_lustre_mdt_client_latency_milliseconds
)
```

### 9.5 MDT pressure

```promql
azure_managed_lustre_mdt_files_used_percent
azure_managed_lustre_mdt_files_free
azure_managed_lustre_hsm_action_errors
azure_managed_lustre_hsm_current_requests
```

### 9.6 AKS scale-out (validation Job at high parallelism)

```promql
azure_vmss_capacity
azure_vmss_instance_count
azure_vmss_exporter_is_leader
```

## 10. Pass / fail criteria

A phase passes when **all** of the following hold:

1. Every pod summary reports `files_failed == 0`.
2. Every pod summary's `slo.pass` is `true` (or the SLO entry is absent for phases without SLO).
3. `max(azure_managed_lustre_ost_bytes_used_percent) < 80` throughout the phase.
4. `time() - azure_managed_lustre_last_success_timestamp_seconds < 180` throughout the phase.
5. `rate(azure_managed_lustre_collection_errors_total[5m]) == 0` throughout the phase.
6. No AKS node went `NotReady` or reported sustained memory/disk pressure.
7. Aggregate read throughput met the per-phase target (recorded against the filesystem SKU envelope).
8. Per-bucket steady-state latency stayed below the agreed SLO:

   | Bucket | Default p95 SLO | Default p99 budget |
   | --- | --: | --: |
   | small  | 20 ms   | 60 ms |
   | medium | 300 ms  | 750 ms |
   | large  | 3 000 ms | 7 500 ms |
   | xlarge | 8 000 ms | 16 000 ms |

   These defaults match `BUCKET_SLO_P95_MS` in the ConfigMap; tune for the actual AV SLO before reporting.

9. **Dataset immutability:** the 50-file sample baselined in § 8.4 hashes identically after the phase (same `size`, `mtime`, and `sha256-of-first-MiB`). Every output path observed in pod logs resolves under `RESULT_ROOT/<RUN_ID>/<pod-name>/` and **never** under `DATASET_ROOT`.

Any single criterion missing => the phase **fails**.

## 11. Abort and rollback

Stop conditions (any one triggers an immediate abort and a halt to the sweep):

- A criterion in § 10 fails.
- The 50-file dataset-immutability sample shows any mismatch versus `reports/<run-base>/dataset-immutability-baseline.tsv`. **Treat this as a hard abort — it indicates a write escape into `DATASET_ROOT`.** Capture full pod logs and the failing diff before proceeding.
- The simulator emits the disjoint-roots preflight error or the per-write path-gate error (exit code 3). Inspect `DATASET_ROOT` / `RESULT_ROOT` in the ConfigMap before retrying.
- Capacity exceeds 80 %, exporter metrics go stale, or AKS nodes go `NotReady`.

If any stop condition fires:

1. **Immediately cancel** the in-flight Job:
   ```bash
   kubectl delete job -n lustre-pressure-test av-dataset-validation
   ```
2. **Check capacity recovery** with `azure_managed_lustre_ost_bytes_used_percent`. If usage is climbing, do not run a new phase — run cleanup first.
3. **Run scoped cleanup** for the current `RUN_ID` using phase 12.
4. **Collect forensics**:
   ```bash
   kubectl get events -n lustre-pressure-test --sort-by=.lastTimestamp | tail -100
   kubectl describe pods -n lustre-pressure-test -l app.kubernetes.io/name=av-lustre-workload
   kubectl logs  -n kube-system -l app=csi-azurelustre-node --tail=400
   ```
5. **Pause further phases** until the abort cause is documented and fixed.

Cluster-level rollback (cluster left healthy) is the validated Ubuntu `juicefspool` configuration documented in § 4.

## 12. Report template

Create the report at `reports/av-press-<run-base>.md` after each full run. Required sections:

```markdown
# AV Lustre Pressure Test — <RUN_BASE>

## Run metadata
- Run base ID
- Date (UTC)
- Filesystem name, region, SKU, capacity
- AKS cluster, node pool, node count, node SKU
- CSI image, LUSTRE_VERSION, CLIENT_SHA_SUFFIX
- Operator(s), reviewer(s)

## Dataset profile (from phase 1)
- Dataset root
- File count, directory count, total bytes
- Bucket counts and bytes
- Top-10 largest files (size + redacted path)

## Phase results
| Phase | Pods | Mode | Pattern | Epochs | Hotset | Warmup | Files OK / Failed | Read MiB/s (sum) | Write MiB/s (sum) | p95 small/med/large (ms) | p99 small/med/large (ms) | SLO | Result |
| --- | --: | --- | --- | --: | --: | --: | --- | --: | --: | --- | --- | --- | --- |
| steady-10 | 10 | rwo | full | 1 | 0 | 60 | 12340 / 0 | 1180 | 12 | 4.2 / 88 / 1280 | 9.8 / 230 / 3100 | pass | PASS |
| ramp-20   | 20 | rwo | full | 1 | 0 | 60 | ... | ... | ... | ... | ... | ... | ... |
| ramp-40   | 40 | rwo | full | 1 | 0 | 60 | ... | ... | ... | ... | ... | ... | ... |
| metadata-storm | 20 | ro | head-tail | 1 | 0 | 60 | ... | ... | – | ... | ... | – | ... |
| index-lookup   | 20 | ro | random-offset | 1 | 0 | 60 | ... | ... | – | ... | ... | – | ... |
| hotset    | 20 | ro | full | 2 | 25 | 60 | ... | ... | – | ... | ... | – | ... |
| endurance | 10 | rwo | full | 4 | 25 | 120 | ... | ... | ... | ... | ... | ... | ... |

## Capacity and exporter health
- Start / peak / end values of `azure_managed_lustre_ost_bytes_used_percent`
- Max `time() - azure_managed_lustre_last_success_timestamp_seconds`
- Cumulative `azure_managed_lustre_collection_errors_total` delta

## Dataset immutability
- 50-file baseline location: `reports/<RUN_BASE>/dataset-immutability-baseline.tsv`
- Per-phase re-check result (pass / fail + first mismatching path if any)

## Issues, observations, next actions
- ...

## Cleanup
- Run ID(s) cleaned up
- Post-cleanup `azure_managed_lustre_ost_bytes_used_percent`
- Node pool scaled back to 2 nodes (`az aks nodepool scale --name juicefspool --node-count 2`)
```

Use [reports/uae-lustre-24h-analysis-2026-05-19.md](reports/uae-lustre-24h-analysis-2026-05-19.md) as the narrative style reference.

## 13. Post-run cleanup

Run these once the report is finalized. They restore the cluster to its pre-test state without touching `DATASET_ROOT`.

```bash
# 1. Per-RUN_ID result trees (one invocation per RUN_ID you want removed)
kubectl set env -n lustre-pressure-test job/av-output-cleanup --containers=cleanup RUN_ID=<run-id>
kubectl apply -f deploy/pressure-test/av-output-cleanup-job.yaml
kubectl wait -n lustre-pressure-test --for=condition=complete job/av-output-cleanup --timeout=900s

# 2. Drop the Waymo download Job from the cluster (the YAML stays in the repo)
kubectl delete job -n lustre-pressure-test waymo-dataset-download --ignore-not-found

# 3. Delete the GCP credentials Secret — it is only needed during a download window
kubectl delete secret -n lustre-pressure-test gcp-credentials --ignore-not-found

# 4. Scale juicefspool back to its baseline size
az aks nodepool scale \
   --resource-group aks-test-rg \
   --cluster-name aks-storage-test \
   --name juicefspool \
   --node-count 2
```

[deploy/pressure-test/waymo-download-job.yaml](deploy/pressure-test/waymo-download-job.yaml) is **kept in the repo** for reproducibility. Re-running it requires re-creating the `gcp-credentials` Secret (authorized_user ADC JSON, key `adc.json`) in the `lustre-pressure-test` namespace before `kubectl apply`. Never run it unless the dataset must be replaced — a stray re-run could overwrite `/mnt/lustre/waymo_v2/`.
