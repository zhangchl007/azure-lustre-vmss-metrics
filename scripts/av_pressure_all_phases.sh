#!/usr/bin/env bash
# Run the full AV Lustre pressure-test phase ladder with safety checks.
#
# This script orchestrates:
#   - optional pressure-test resource apply + simulator ConfigMap publish
#   - discovery
#   - fixed-path dataset immutability baseline and per-phase re-checks
#   - optional AKS nodepool scale-out / scale-back
#   - smoke, ramp, heavy-write, metadata-heavy, hotset, strict soak, and soak-collect phases
#   - summary validation and aggregate report snippets
#
# It intentionally does not delete result trees by default; inspect reports first,
# then run cleanup explicitly or pass --cleanup-results.

set -euo pipefail

NAMESPACE="lustre-pressure-test"
RUN_BASE="av-press-$(date -u +%Y%m%d-%H%M)"
OUTDIR=""
SLO="small=20,medium=300,large=3000,xlarge=8000"
METADATA_SLO="small=30,medium=300,large=3000,xlarge=8000"
HEAVY_WRITE_SLO="small=50,medium=500,large=5000,xlarge=10000"
META_EXCLUDE="/mnt/lustre/pressure-tests:/mnt/lustre/waymo_v2/training/camera_image:/mnt/lustre/waymo_v2/training/lidar:/mnt/lustre/waymo_v2/training/lidar_segmentation:/mnt/lustre/waymo_v2/training/camera_segmentation"

AKS_RESOURCE_GROUP="aks-test-rg"
AKS_CLUSTER="aks-storage-test"
NODEPOOL="juicefspool"
NODEPOOL_MAX_COUNT="20"
NODEPOOL_BASELINE_MIN="2"

LUSTRE_CAPACITY="8.0TiB"
HEAVY_PLANNED_WRITE="600GiB"
HEAVY_RESERVE="200GiB"
CURRENT_USED="${CURRENT_USED:-}"

SKIP_SETUP="false"
SKIP_DISCOVERY="false"
SKIP_SCALE="false"
SKIP_HEAVY_WRITE="false"
SKIP_STRICT_SOAK="false"
SKIP_SOAK_COLLECT="false"
CLEANUP_RESULTS="false"
RESTORE_SCALE="true"
ASSUME_YES="false"
PHASE_TIMEOUT="7200s"

BASELINE=""
BASELINE_PATHS=""
SUMMARY_INDEX=""
RUN_IDS=()

usage() {
   cat <<'EOF'
Usage: scripts/av_pressure_all_phases.sh [options]

Runs the full AV Lustre pressure-test ladder automatically.

Common options:
  --run-base ID                 Run base ID. Default: av-press-<UTC timestamp>
  --output-dir DIR              Report/output directory. Default: reports/<run-base>
  --yes                         Do not prompt before launching high-pressure phases

Safety / setup options:
  --skip-setup                  Do not apply kustomize resources or publish script ConfigMap
  --skip-discovery              Do not run discovery
  --skip-scale                  Do not scale AKS nodepool
  --no-restore-scale            Do not restore nodepool min/count on exit
  --current-used SIZE           Current used capacity for heavy-write budget, e.g. 1.25TiB
                                If omitted, the script tries to read df -B1 from the Lustre mount.
  --lustre-capacity SIZE        Capacity for budget check. Default: 8.0TiB
  --heavy-planned-write SIZE    Heavy-write planned payload. Default: 600GiB
  --heavy-reserve SIZE          Extra reserve for budget check. Default: 200GiB

Phase selection options:
  --skip-heavy-write            Skip the heavy-write phase
  --skip-strict-soak            Skip the strict --fail-on-slo soak gate
  --skip-soak-collect           Skip the non-strict soak collection run
  --cleanup-results             Run scoped cleanup Jobs for all generated RUN_IDs at the end

AKS scaling options:
  --aks-resource-group RG       Default: aks-test-rg
  --aks-cluster NAME            Default: aks-storage-test
  --nodepool NAME               Default: juicefspool
  --nodepool-max-count N        Default: 20
  --nodepool-baseline-min N     Default: 2

Examples:
  scripts/av_pressure_all_phases.sh --yes
  scripts/av_pressure_all_phases.sh --yes --current-used 1.25TiB
  scripts/av_pressure_all_phases.sh --yes --skip-scale --skip-heavy-write
EOF
}

log() {
   printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

fail() {
   echo "ERROR: $*" >&2
   exit 1
}

require_cmd() {
   command -v "$1" >/dev/null 2>&1 || fail "$1 not found on PATH"
}

while [[ $# -gt 0 ]]; do
   case "$1" in
      --run-base) RUN_BASE="$2"; shift 2 ;;
      --output-dir) OUTDIR="$2"; shift 2 ;;
      --yes|-y) ASSUME_YES="true"; shift ;;
      --skip-setup) SKIP_SETUP="true"; shift ;;
      --skip-discovery) SKIP_DISCOVERY="true"; shift ;;
      --skip-scale) SKIP_SCALE="true"; shift ;;
      --no-restore-scale) RESTORE_SCALE="false"; shift ;;
      --current-used) CURRENT_USED="$2"; shift 2 ;;
      --lustre-capacity) LUSTRE_CAPACITY="$2"; shift 2 ;;
      --heavy-planned-write) HEAVY_PLANNED_WRITE="$2"; shift 2 ;;
      --heavy-reserve) HEAVY_RESERVE="$2"; shift 2 ;;
      --skip-heavy-write) SKIP_HEAVY_WRITE="true"; shift ;;
      --skip-strict-soak) SKIP_STRICT_SOAK="true"; shift ;;
      --skip-soak-collect) SKIP_SOAK_COLLECT="true"; shift ;;
      --cleanup-results) CLEANUP_RESULTS="true"; shift ;;
      --aks-resource-group) AKS_RESOURCE_GROUP="$2"; shift 2 ;;
      --aks-cluster) AKS_CLUSTER="$2"; shift 2 ;;
      --nodepool) NODEPOOL="$2"; shift 2 ;;
      --nodepool-max-count) NODEPOOL_MAX_COUNT="$2"; shift 2 ;;
      --nodepool-baseline-min) NODEPOOL_BASELINE_MIN="$2"; shift 2 ;;
      --timeout) PHASE_TIMEOUT="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) usage >&2; fail "unknown argument: $1" ;;
   esac
done

OUTDIR="${OUTDIR:-reports/${RUN_BASE}}"
BASELINE="${OUTDIR}/dataset-immutability-baseline.tsv"
BASELINE_PATHS="${OUTDIR}/dataset-immutability-baseline.paths"
SUMMARY_INDEX="${OUTDIR}/phase-summary-index.tsv"

if [[ "$ASSUME_YES" != "true" ]]; then
   cat <<EOF
This will run the AV Lustre pressure-test phase ladder against namespace ${NAMESPACE}.
Run base: ${RUN_BASE}
Output dir: ${OUTDIR}
Heavy-write: $([[ "$SKIP_HEAVY_WRITE" == "true" ]] && echo skipped || echo enabled)
Scaling: $([[ "$SKIP_SCALE" == "true" ]] && echo skipped || echo enabled for ${AKS_CLUSTER}/${NODEPOOL})

Re-run with --yes to proceed.
EOF
   exit 64
fi

require_cmd kubectl
require_cmd python3
require_cmd jq

mkdir -p "$OUTDIR"
printf 'phase\trun_id\tsummary_jsonl\n' > "$SUMMARY_INDEX"

restore_excludes() {
   kubectl patch configmap -n "$NAMESPACE" av-lustre-workload-config \
      --type merge \
      -p '{"data":{"EXCLUDE_PATHS":"/mnt/lustre/pressure-tests"}}' >/dev/null 2>&1 || true
}

restore_nodepool() {
   if [[ "$SKIP_SCALE" == "true" || "$RESTORE_SCALE" != "true" ]]; then
      return 0
   fi
   if ! command -v az >/dev/null 2>&1; then
      log "az not found; skipping nodepool restore"
      return 0
   fi
   log "restoring nodepool ${NODEPOOL} baseline min/count to ${NODEPOOL_BASELINE_MIN}"
   local enabled
   enabled="$(az aks nodepool show \
      --resource-group "$AKS_RESOURCE_GROUP" \
      --cluster-name "$AKS_CLUSTER" \
      --name "$NODEPOOL" \
      --query enableAutoScaling -o tsv 2>/dev/null || true)"
   if [[ "$enabled" == "true" ]]; then
      az aks nodepool update \
         --resource-group "$AKS_RESOURCE_GROUP" \
         --cluster-name "$AKS_CLUSTER" \
         --name "$NODEPOOL" \
         --update-cluster-autoscaler \
         --min-count "$NODEPOOL_BASELINE_MIN" \
         --max-count "$NODEPOOL_MAX_COUNT" >/dev/null || true
   elif [[ -n "$enabled" ]]; then
      az aks nodepool scale \
         --resource-group "$AKS_RESOURCE_GROUP" \
         --cluster-name "$AKS_CLUSTER" \
         --name "$NODEPOOL" \
         --node-count "$NODEPOOL_BASELINE_MIN" >/dev/null || true
   fi
}

on_exit() {
   restore_excludes
   restore_nodepool
}
trap on_exit EXIT

scale_nodepool() {
   local min_count="$1"
   if [[ "$SKIP_SCALE" == "true" ]]; then
      log "skipping scale request to ${min_count} nodes/min-count"
      return 0
   fi
   require_cmd az
   log "scaling nodepool ${NODEPOOL} for at least ${min_count} nodes/min-count"
   local enabled
   enabled="$(az aks nodepool show \
      --resource-group "$AKS_RESOURCE_GROUP" \
      --cluster-name "$AKS_CLUSTER" \
      --name "$NODEPOOL" \
      --query enableAutoScaling -o tsv)"
   if [[ "$enabled" == "true" ]]; then
      az aks nodepool update \
         --resource-group "$AKS_RESOURCE_GROUP" \
         --cluster-name "$AKS_CLUSTER" \
         --name "$NODEPOOL" \
         --update-cluster-autoscaler \
         --min-count "$min_count" \
         --max-count "$NODEPOOL_MAX_COUNT" >/dev/null
   else
      az aks nodepool scale \
         --resource-group "$AKS_RESOURCE_GROUP" \
         --cluster-name "$AKS_CLUSTER" \
         --name "$NODEPOOL" \
         --node-count "$min_count" >/dev/null
   fi
   kubectl rollout status -n kube-system daemonset/csi-azurelustre-node --timeout=300s
}

apply_setup() {
   if [[ "$SKIP_SETUP" == "true" ]]; then
      log "skipping setup"
      return 0
   fi
   log "applying pressure-test resources"
   kubectl apply -k deploy/pressure-test
   log "publishing simulator script ConfigMap"
   make av-workload-script
   kubectl rollout status -n kube-system daemonset/csi-azurelustre-node --timeout=300s
}

run_discovery() {
   if [[ "$SKIP_DISCOVERY" == "true" ]]; then
      log "skipping discovery"
      return 0
   fi
   log "running discovery"
   kubectl delete job -n "$NAMESPACE" av-dataset-discovery --ignore-not-found --wait=true
   kubectl apply -f deploy/pressure-test/av-dataset-discovery-job.yaml >/dev/null
   kubectl wait -n "$NAMESPACE" --for=condition=complete job/av-dataset-discovery --timeout=900s
   kubectl logs -n "$NAMESPACE" job/av-dataset-discovery | tee "${OUTDIR}/discovery.json" >/dev/null
}

immutability_overrides() {
   local mode="$1"
   local configmap_name="${2:-}"
   python3 - "$mode" "$configmap_name" <<'PY'
import json
import sys

mode = sys.argv[1]
configmap_name = sys.argv[2]
if mode == "baseline":
    command = """
apk add --no-cache coreutils findutils >/dev/null 2>&1
set -eu
find /mnt/lustre/waymo_v2 -type f | shuf -n 50 | while IFS= read -r f; do
  printf "%s\t%s\t%s\t" "$f" "$(stat -c %s "$f")" "$(stat -c %Y "$f")"
  head -c $((1024*1024)) "$f" | sha256sum | awk '{print $1}'
done
""".strip()
    volumes = [{"name": "lustre", "persistentVolumeClaim": {"claimName": "lustre-pressure-test-pvc"}}]
    mounts = [{"name": "lustre", "mountPath": "/mnt/lustre"}]
else:
    command = """
apk add --no-cache coreutils >/dev/null 2>&1
set -eu
while IFS= read -r f; do
  [ -n "$f" ] || continue
  printf "%s\t%s\t%s\t" "$f" "$(stat -c %s "$f")" "$(stat -c %Y "$f")"
  head -c $((1024*1024)) "$f" | sha256sum | awk '{print $1}'
done < /config/paths
""".strip()
    volumes = [
        {"name": "lustre", "persistentVolumeClaim": {"claimName": "lustre-pressure-test-pvc"}},
        {"name": "paths", "configMap": {"name": configmap_name}},
    ]
    mounts = [
        {"name": "lustre", "mountPath": "/mnt/lustre"},
        {"name": "paths", "mountPath": "/config", "readOnly": True},
    ]

overrides = {
    "apiVersion": "v1",
    "spec": {
        "nodeSelector": {"kubernetes.azure.com/agentpool": "juicefspool"},
        "restartPolicy": "Never",
        "containers": [
            {
                "name": "immutability",
                "image": "alpine:3.20",
                "command": ["sh", "-c", command],
                "volumeMounts": mounts,
            }
        ],
        "volumes": volumes,
    },
}
print(json.dumps(overrides))
PY
}

wait_pod_succeeded() {
   local pod="$1"
   if ! kubectl wait -n "$NAMESPACE" --for=jsonpath='{.status.phase}'=Succeeded "pod/${pod}" --timeout=900s; then
      kubectl describe pod -n "$NAMESPACE" "$pod" || true
      kubectl logs -n "$NAMESPACE" "$pod" || true
      return 1
   fi
}

capture_immutability_baseline() {
   log "capturing fixed 50-file immutability baseline"
   local pod="av-immutability-baseline-${RUN_BASE}"
   kubectl delete pod -n "$NAMESPACE" "$pod" --ignore-not-found --wait=true >/dev/null
   kubectl run "$pod" --restart=Never --image=alpine:3.20 -n "$NAMESPACE" \
      --overrides="$(immutability_overrides baseline)" >/dev/null
   wait_pod_succeeded "$pod"
   kubectl logs -n "$NAMESPACE" "$pod" > "$BASELINE"
   kubectl delete pod -n "$NAMESPACE" "$pod" --ignore-not-found >/dev/null
   local count
   count="$(wc -l < "$BASELINE" | tr -d ' ')"
   [[ "$count" == "50" ]] || fail "immutability baseline has ${count} rows, expected 50"
   cut -f1 "$BASELINE" > "$BASELINE_PATHS"
}

check_immutability() {
   local phase="$1"
   local pod="av-immutability-check-${RUN_BASE}-${phase}"
   local cm="av-immutability-paths-${RUN_BASE}-${phase}"
   local check_file="${OUTDIR}/dataset-immutability-after-${phase}.tsv"
   log "checking immutability after ${phase}"
   kubectl delete pod -n "$NAMESPACE" "$pod" --ignore-not-found --wait=true >/dev/null
   kubectl delete configmap -n "$NAMESPACE" "$cm" --ignore-not-found >/dev/null
   kubectl create configmap -n "$NAMESPACE" "$cm" \
      --from-file=paths="$BASELINE_PATHS" \
      --dry-run=client -o yaml | kubectl apply -f - >/dev/null
   kubectl run "$pod" --restart=Never --image=alpine:3.20 -n "$NAMESPACE" \
      --overrides="$(immutability_overrides check "$cm")" >/dev/null
   wait_pod_succeeded "$pod"
   kubectl logs -n "$NAMESPACE" "$pod" > "$check_file"
   kubectl delete pod -n "$NAMESPACE" "$pod" --ignore-not-found >/dev/null
   kubectl delete configmap -n "$NAMESPACE" "$cm" --ignore-not-found >/dev/null
   diff -u "$BASELINE" "$check_file" >/dev/null || fail "dataset immutability check failed after ${phase}; see ${check_file}"
}

summary_path() {
   local run_id="$1"
   local phase="$2"
   printf '%s/%s-%s-summaries.jsonl' "$OUTDIR" "$run_id" "$phase"
}

validate_summary() {
   local phase="$1"
   local run_id="$2"
   local expected_count="$3"
   local allow_partial="$4"
   local summary
   summary="$(summary_path "$run_id" "$phase")"
   [[ -f "$summary" ]] || fail "missing summary file ${summary}"
   python3 - "$summary" "$expected_count" "$allow_partial" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
expected = int(sys.argv[2])
allow_partial = sys.argv[3] == "true"
rows = []
for line in path.read_text(encoding="utf-8").splitlines():
    if line.strip():
        rows.append(json.loads(line))
count = len(rows)
if allow_partial:
    if count == 0:
        raise SystemExit(f"no summaries in {path}")
elif count != expected:
    raise SystemExit(f"expected {expected} summaries in {path}, found {count}")
bad = [r for r in rows if r.get("files_failed", 0) or r.get("errors")]
if bad:
    raise SystemExit(f"{len(bad)} pod summaries reported file failures/errors in {path}")
print(json.dumps({
    "summary": str(path),
    "pods": count,
    "bytes_read": sum(int(r.get("bytes_read", 0)) for r in rows),
    "bytes_written": sum(int(r.get("bytes_written", 0)) for r in rows),
    "planned_output_bytes": sum(int(r.get("planned_output_bytes", 0)) for r in rows),
    "write_throughput_mib_s_sum": round(sum(float(r.get("write_throughput_mib_s", 0)) for r in rows), 3),
}, sort_keys=True))
PY
   printf '%s\t%s\t%s\n' "$phase" "$run_id" "$summary" >> "$SUMMARY_INDEX"
}

run_phase() {
   local phase="$1"
   local parallelism="$2"
   local run_id="$3"
   local allow_partial="${4:-false}"
   shift 4
   log "running phase=${phase} run_id=${run_id} parallelism=${parallelism}"
   RUN_IDS+=("$run_id")
   scripts/av_pressure_phase.sh \
      --phase "$phase" \
      --parallelism "$parallelism" \
      --run-id "$run_id" \
      --output-dir "$OUTDIR" \
      --timeout "$PHASE_TIMEOUT" \
      "$@"
   validate_summary "$phase" "$run_id" "$parallelism" "$allow_partial"
   check_immutability "$phase"
}

patch_metadata_excludes() {
   log "patching metadata-heavy EXCLUDE_PATHS"
   local patch
   patch="$(python3 - "$META_EXCLUDE" <<'PY'
import json
import sys
print(json.dumps({"data": {"EXCLUDE_PATHS": sys.argv[1]}}))
PY
)"
   kubectl patch configmap -n "$NAMESPACE" av-lustre-workload-config --type merge -p "$patch" >/dev/null
}

read_lustre_used_bytes() {
   if [[ -n "$CURRENT_USED" ]]; then
      echo "$CURRENT_USED"
      return 0
   fi
   log "CURRENT_USED not provided; reading used bytes from df -B1 inside a Lustre-mounted pod" >&2
   local pod="av-capacity-check-${RUN_BASE}"
   local overrides
   overrides="$(python3 - <<'PY'
import json
print(json.dumps({
  "apiVersion": "v1",
  "spec": {
    "nodeSelector": {"kubernetes.azure.com/agentpool": "juicefspool"},
    "restartPolicy": "Never",
    "containers": [{
      "name": "capacity",
      "image": "alpine:3.20",
      "command": ["sh", "-c", "df -B1 /mnt/lustre | awk 'END { if (NF == 6) print $3; else if (NF == 5) print $2; else exit 2 }'"],
      "volumeMounts": [{"name": "lustre", "mountPath": "/mnt/lustre"}],
    }],
    "volumes": [{"name": "lustre", "persistentVolumeClaim": {"claimName": "lustre-pressure-test-pvc"}}],
  },
}))
PY
)"
   kubectl delete pod -n "$NAMESPACE" "$pod" --ignore-not-found --wait=true >/dev/null
   kubectl run "$pod" --restart=Never --image=alpine:3.20 -n "$NAMESPACE" --overrides="$overrides" >/dev/null
   wait_pod_succeeded "$pod" >/dev/null
   local used
   used="$(kubectl logs -n "$NAMESPACE" "$pod" | tail -1 | tr -d '[:space:]')"
   kubectl delete pod -n "$NAMESPACE" "$pod" --ignore-not-found >/dev/null
   [[ "$used" =~ ^[0-9]+$ ]] || fail "could not parse used bytes from df output: ${used}"
   echo "${used}B"
}

check_heavy_write_budget() {
   if [[ "$SKIP_HEAVY_WRITE" == "true" ]]; then
      return 0
   fi
   local used
   used="$(read_lustre_used_bytes)"
   log "checking heavy-write budget: capacity=${LUSTRE_CAPACITY} used=${used} planned=${HEAVY_PLANNED_WRITE} reserve=${HEAVY_RESERVE}"
   python3 scripts/lustre_safe_write_budget.py \
      --capacity "$LUSTRE_CAPACITY" \
      --used "$used" \
      --planned-write "$HEAVY_PLANNED_WRITE" \
      --reserve "$HEAVY_RESERVE" \
      --max-used-percent 80
}

cleanup_run_id() {
   local run_id="$1"
   log "cleaning result tree for ${run_id}"
   kubectl delete job -n "$NAMESPACE" av-output-cleanup --ignore-not-found --wait=true >/dev/null
   local rendered_job
   rendered_job="$(mktemp)"
   kubectl create --dry-run=client -f deploy/pressure-test/av-output-cleanup-job.yaml -o json > "$rendered_job"
   python3 - "$run_id" "$rendered_job" <<'PY' | kubectl apply -f - >/dev/null
import json
import sys
from pathlib import Path

run_id = sys.argv[1]
path = Path(sys.argv[2])
job = json.loads(path.read_text(encoding="utf-8"))
container = job["spec"]["template"]["spec"]["containers"][0]
env = container.setdefault("env", [])
for item in env:
    if item.get("name") == "RUN_ID":
        item.pop("valueFrom", None)
        item["value"] = run_id
        break
else:
    env.append({"name": "RUN_ID", "value": run_id})
print(json.dumps(job))
PY
   rm -f "$rendered_job"
   kubectl wait -n "$NAMESPACE" --for=condition=complete job/av-output-cleanup --timeout=900s
   kubectl logs -n "$NAMESPACE" job/av-output-cleanup | tee "${OUTDIR}/${run_id}-cleanup.log" >/dev/null
}

aggregate_all_summaries() {
   log "writing aggregate summary to ${OUTDIR}/aggregate-summary.json"
   python3 - "$OUTDIR" "$SUMMARY_INDEX" <<'PY'
import json
import sys
from pathlib import Path

outdir = Path(sys.argv[1])
index = Path(sys.argv[2])
phases = []
for line in index.read_text(encoding="utf-8").splitlines()[1:]:
    phase, run_id, summary_path = line.split("\t")
    rows = [json.loads(x) for x in Path(summary_path).read_text(encoding="utf-8").splitlines() if x.strip()]
    phases.append({
        "phase": phase,
        "run_id": run_id,
        "pods": len(rows),
        "files_failed": sum(int(r.get("files_failed", 0)) for r in rows),
        "bytes_read": sum(int(r.get("bytes_read", 0)) for r in rows),
        "bytes_written": sum(int(r.get("bytes_written", 0)) for r in rows),
        "planned_output_bytes": sum(int(r.get("planned_output_bytes", 0)) for r in rows),
        "write_throughput_mib_s_sum": round(sum(float(r.get("write_throughput_mib_s", 0)) for r in rows), 3),
        "slo_failures": sorted({bucket for r in rows for bucket in r.get("slo", {}).get("failures", [])}),
    })
(outdir / "aggregate-summary.json").write_text(json.dumps({"phases": phases}, indent=2, sort_keys=True), encoding="utf-8")
PY
}

main() {
   log "starting full pressure test: RUN_BASE=${RUN_BASE} OUTDIR=${OUTDIR}"
   apply_setup
   run_discovery
   capture_immutability_baseline

   run_phase smoke 1 "${RUN_BASE}-smoke" false \
      --mode read-only --read-pattern full --split training \
      --max-bytes-per-pod 10GiB --bucket-slo "$SLO"

   run_phase ramp-10-ro 10 "${RUN_BASE}-ramp-10-ro" false \
      --mode read-only --read-pattern full --split training \
      --warmup-seconds 30 --bucket-slo "$SLO"

   run_phase ramp-10-rwo 10 "${RUN_BASE}-ramp-10-rwo" false \
      --mode read-write-output --read-pattern full --split training \
      --warmup-seconds 30 --bucket-slo "$SLO"

   scale_nodepool 4
   run_phase ramp-20 20 "${RUN_BASE}-ramp-20" false \
      --mode read-write-output --read-pattern full --split training,validation \
      --warmup-seconds 60 --bucket-slo "$SLO"

   scale_nodepool 6
   run_phase ramp-40 40 "${RUN_BASE}-ramp-40" false \
      --mode read-write-output --read-pattern full --split training,validation \
      --warmup-seconds 60 --bucket-slo "$SLO"

   if [[ "$SKIP_HEAVY_WRITE" != "true" ]]; then
      check_heavy_write_budget
      run_phase heavy-write 20 "${RUN_BASE}-heavy-write" false \
         --mode read-write-output --read-pattern full --split training,validation \
         --warmup-seconds 60 \
         --output-bytes-per-input 1.0 \
         --max-output-bytes-per-file 512MiB \
         --bucket-slo "$HEAVY_WRITE_SLO"
   else
      log "skipping heavy-write"
   fi

   patch_metadata_excludes
   run_phase metadata-heavy 20 "${RUN_BASE}-metadata-heavy" false \
      --mode read-only --read-pattern full --subpath training \
      --warmup-seconds 60 --bucket-slo "$METADATA_SLO"
   restore_excludes

   run_phase hotset 20 "${RUN_BASE}-hotset" false \
      --mode read-only --read-pattern full --epochs 3 --hotset-count 50 \
      --subpath training/camera_image --warmup-seconds 60 --bucket-slo "$SLO"

   if [[ "$SKIP_STRICT_SOAK" != "true" ]]; then
      log "running strict soak gate; SLO exit code 4/partial summaries are allowed here"
      run_phase soak 20 "${RUN_BASE}-soak" true \
         --mode read-write-output --read-pattern full --epochs 4 --hotset-count 25 \
         --split training,validation --warmup-seconds 120 --bucket-slo "$SLO" --fail-on-slo
   else
      log "skipping strict soak"
   fi

   if [[ "$SKIP_SOAK_COLLECT" != "true" ]]; then
      run_phase soak 20 "${RUN_BASE}-soak-collect" false \
         --mode read-write-output --read-pattern full --epochs 4 --hotset-count 25 \
         --split training,validation --warmup-seconds 120 --bucket-slo "$SLO"
   else
      log "skipping soak-collect"
   fi

   aggregate_all_summaries

   if [[ "$CLEANUP_RESULTS" == "true" ]]; then
      for run_id in "${RUN_IDS[@]}"; do
         cleanup_run_id "$run_id"
      done
   fi

   log "pressure test complete. Outputs: ${OUTDIR}"
   log "summary index: ${SUMMARY_INDEX}"
   log "aggregate summary: ${OUTDIR}/aggregate-summary.json"
}

main "$@"
