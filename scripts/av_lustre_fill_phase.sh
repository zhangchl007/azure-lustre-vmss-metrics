#!/usr/bin/env bash
# Scenario G (200-client fill-to-ENOSPC) phase orchestrator.
# See docs/lustre-pressure-test.md § 15.
#
# This helper:
#   1. Generates per-PVC single-pod Job manifests via gen_lustre_fill_pvcs.py.
#   2. Applies them in batches (avoid 200-document kubectl-apply spikes).
#   3. Polls Job conditions every 10 s and samples Prometheus every 30 s
#      (snapshots written to <output-dir>/fill-prom-snapshots.jsonl).
#   4. Collates per-pod summary JSON lines into <output-dir>/fill-pod-summaries.jsonl.
#   5. Emits an aggregate-summary.json with the headline numbers.
#
# Usage:
#   scripts/av_lustre_fill_phase.sh \
#     --run-id fill-200-20260525-1700 \
#     --pod-count 200 \
#     --target-bytes-per-pod 42949672960 \
#     --output-dir reports/fill-200-20260525-1700 \
#     [--mode dedicated|subpath] \
#     [--shared-pvc lustre-pressure-test-pvc] \
#     [--timeout 4h]

set -euo pipefail

NAMESPACE="lustre-pressure-test"
EXPORTER_NAMESPACE="default"
EXPORTER_DEPLOY="vmss-metrics-exporter"
FILESYSTEM_NAME="${FILESYSTEM_NAME:-almfstestcluster02}"

RUN_ID=""
POD_COUNT="200"
TARGET_BYTES_PER_POD="42949672960"
OUTPUT_DIR=""
TIMEOUT="4h"
MODE="dedicated"
SHARED_PVC="lustre-pressure-test-pvc"
SKIP_PREFLIGHT="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id) RUN_ID="$2"; shift 2 ;;
    --pod-count) POD_COUNT="$2"; shift 2 ;;
    --target-bytes-per-pod) TARGET_BYTES_PER_POD="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --timeout) TIMEOUT="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --shared-pvc) SHARED_PVC="$2"; shift 2 ;;
    --skip-preflight) SKIP_PREFLIGHT="true"; shift ;;
    -h|--help) sed -n '1,22p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

[[ -z "${RUN_ID}" ]] && { echo "ERROR: --run-id required" >&2; exit 2; }
case "${RUN_ID}" in *"/"*|"."|"..") echo "ERROR: bad --run-id" >&2; exit 2 ;; esac
[[ -z "${OUTPUT_DIR}" ]] && OUTPUT_DIR="reports/${RUN_ID}"
mkdir -p "${OUTPUT_DIR}"

JOB_MANIFEST="${OUTPUT_DIR}/lustre-fill-jobs-${RUN_ID}.yaml"
PROM_SNAPSHOTS="${OUTPUT_DIR}/fill-prom-snapshots.jsonl"
POD_SUMMARIES="${OUTPUT_DIR}/fill-pod-summaries.jsonl"
AGGREGATE="${OUTPUT_DIR}/aggregate-summary.json"
GRAFANA_URL_FILE="${OUTPUT_DIR}/grafana-url.txt"

# Pre-flight.
if [[ "${SKIP_PREFLIGHT}" != "true" ]]; then
  echo "[$(date -u +%H:%M:%S)] running pre-flight"
  bash scripts/lustre_preflight.sh --check active-writers --check capacity-baseline \
    || { echo "ABORT: pre-flight failed (use --skip-preflight to override)" >&2; exit 1; }
fi

echo "[$(date -u +%H:%M:%S)] generating ${JOB_MANIFEST} (${POD_COUNT} jobs, mode=${MODE})"
python scripts/gen_lustre_fill_pvcs.py --jobs --mode "${MODE}" \
  --count "${POD_COUNT}" \
  --run-id "${RUN_ID}" \
  --target-bytes-per-pod "${TARGET_BYTES_PER_POD}" \
  --shared-pvc "${SHARED_PVC}" \
  --out "${JOB_MANIFEST}"

# Record run start + grafana url placeholder for retrospective lookup.
START_EPOCH=$(date -u +%s)
START_ISO=$(date -u -d @"${START_EPOCH}" '+%Y-%m-%dT%H:%M:%SZ')
{
  echo "Grafana dashboard hint for ${RUN_ID}:"
  echo "  open the Lustre dashboard with time range from=${START_ISO} to=<after-cleanup>"
  echo "  filter filesystem_name=${FILESYSTEM_NAME}"
} > "${GRAFANA_URL_FILE}"

echo "[$(date -u +%H:%M:%S)] applying jobs"
kubectl -n "${NAMESPACE}" apply -f "${JOB_MANIFEST}"

# Background Prometheus sampler.
PROM_SAMPLER_PID=""
prom_sampler() {
  local query
  : > "${PROM_SNAPSHOTS}"
  while sleep 30; do
    local ts
    ts=$(date -u +%s)
    query=$(kubectl exec -n "${EXPORTER_NAMESPACE}" "deploy/${EXPORTER_DEPLOY}" -- \
      python -c "import urllib.request; print(urllib.request.urlopen('http://localhost:8000/metrics').read().decode())" 2>/dev/null \
      | awk -v fs="${FILESYSTEM_NAME}" '
          /azure_managed_lustre_ost_bytes_used_percent/ && $0 ~ fs { gsub("\n",""); print "ost_pct:"$NF }
          /azure_managed_lustre_client_write_throughput_bytes_per_second/ && $0 ~ fs { print "write_bps:"$NF }
          /azure_managed_lustre_mdt_client_latency_milliseconds/ && $0 ~ fs { print "mdt_lat_ms:"$NF }
        ' | tr '\n' ' ')
    printf '{"ts":%d,"metrics":"%s"}\n' "${ts}" "${query}" >> "${PROM_SNAPSHOTS}"
  done
}
prom_sampler &
PROM_SAMPLER_PID=$!
trap 'kill ${PROM_SAMPLER_PID} 2>/dev/null || true' EXIT

# Wait for terminal Job condition on each Job.
TIMEOUT_SECONDS=$(python3 -c "
import re,sys
v='${TIMEOUT}'; m=re.fullmatch(r'(\d+)([smhd]?)',v); n,u=int(m.group(1)),m.group(2) or 's'
print(n*{'s':1,'m':60,'h':3600,'d':86400}[u])
")
DEADLINE=$(( START_EPOCH + TIMEOUT_SECONDS ))

echo "[$(date -u +%H:%M:%S)] polling Job conditions every 10s (deadline=$(date -u -d @${DEADLINE}))"
: > "${POD_SUMMARIES}"
COLLECTED=()
while [[ $(date -u +%s) -lt ${DEADLINE} ]]; do
  # Snapshot all fill Jobs and partition by terminal condition.
  ALL=$(kubectl -n "${NAMESPACE}" get jobs \
    -l app.kubernetes.io/name=lustre-fill,"scenario-g/run-id=${RUN_ID}" \
    -o json)
  TERMINAL=$(echo "${ALL}" | python3 -c '
import json,sys
doc=json.load(sys.stdin)
out=[]
for j in doc.get("items", []):
    name=j["metadata"]["name"]
    conds=j.get("status",{}).get("conditions",[]) or []
    state=None
    for c in conds:
        if c.get("type")=="Complete" and c.get("status")=="True": state="complete"
        elif c.get("type")=="Failed"   and c.get("status")=="True": state=state or "failed"
    if state: out.append(f"{name}\t{state}")
print("\n".join(out))
')
  ACTIVE_COUNT=$(echo "${ALL}" | python3 -c '
import json,sys
doc=json.load(sys.stdin); print(sum(1 for j in doc["items"] if (j.get("status",{}).get("active") or 0)>0))
')

  # Collect pod summaries for newly-terminal Jobs we have not collected yet.
  if [[ -n "${TERMINAL}" ]]; then
    while IFS=$'\t' read -r job_name state; do
      if [[ -z "${job_name}" ]]; then continue; fi
      printf '%s\n' "${COLLECTED[@]:-}" | grep -qxF "${job_name}" && continue
      POD=$(kubectl -n "${NAMESPACE}" get pod -l job-name="${job_name}" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
      if [[ -z "${POD}" ]]; then
        # Pod GC'd before we collected; mark as collected so we don't loop.
        COLLECTED+=("${job_name}")
        continue
      fi
      LOG=$(kubectl -n "${NAMESPACE}" logs "${POD}" 2>/dev/null || true)
      # Extract the final {"event":"summary",...} JSON object from the log.
      SUMMARY=$(echo "${LOG}" | python3 -c '
import json,sys,re
text=sys.stdin.read()
# Find the last balanced JSON object containing "event": "summary".
last_idx=-1
for m in re.finditer(r"\{[^{}]*\"event\":\s*\"summary\"", text):
    last_idx=m.start()
if last_idx<0: sys.exit(0)
depth=0; end=None
for i,ch in enumerate(text[last_idx:], start=last_idx):
    if ch=="{": depth+=1
    elif ch=="}":
        depth-=1
        if depth==0:
            end=i+1; break
if not end: sys.exit(0)
print(text[last_idx:end])
' || true)
      if [[ -n "${SUMMARY}" ]]; then
        # Add job_name + state for the aggregator.
        echo "${SUMMARY}" | python3 -c "
import json,sys
o=json.loads(sys.stdin.read())
o['job_name']='${job_name}'
o['job_state']='${state}'
print(json.dumps(o))
" >> "${POD_SUMMARIES}"
      fi
      COLLECTED+=("${job_name}")
    done <<< "${TERMINAL}"
  fi

  TOTAL=$(echo "${ALL}" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["items"]))')
  DONE=${#COLLECTED[@]}
  echo "[$(date -u +%H:%M:%S)] total=${TOTAL} done=${DONE} active=${ACTIVE_COUNT}"
  if [[ "${DONE}" -ge "${TOTAL}" ]] && [[ "${TOTAL}" -ge "${POD_COUNT}" ]] && [[ "${ACTIVE_COUNT}" -eq 0 ]]; then
    break
  fi
  sleep 10
done

END_EPOCH=$(date -u +%s)

# Aggregate.
python3 - <<PY > "${AGGREGATE}"
import json
from pathlib import Path
rows=[]
with open("${POD_SUMMARIES}") as fh:
    for line in fh:
        line=line.strip()
        if not line: continue
        try: rows.append(json.loads(line))
        except Exception: pass
def safe_int(x, default=0):
    try: return int(x)
    except Exception: return default
agg={
    "run_id": "${RUN_ID}",
    "pod_count_target": int("${POD_COUNT}"),
    "pod_count_collected": len(rows),
    "wall_clock_seconds": ${END_EPOCH} - ${START_EPOCH},
    "terminal_reasons": {},
    "total_bytes_written": sum(safe_int(r.get("bytes_written")) for r in rows),
    "pods_enospc": sum(1 for r in rows if r.get("enospc_reached")),
    "pods_target_bytes": sum(1 for r in rows if r.get("terminal_reason")=="target_bytes"),
    "pods_sigterm": sum(1 for r in rows if r.get("terminal_reason")=="sigterm"),
    "pods_failed": sum(1 for r in rows if r.get("job_state")=="failed"),
}
for r in rows:
    tr=r.get("terminal_reason","unknown")
    agg["terminal_reasons"][tr]=agg["terminal_reasons"].get(tr,0)+1
print(json.dumps(agg, indent=2, sort_keys=True))
PY

echo "[$(date -u +%H:%M:%S)] aggregate written to ${AGGREGATE}"
cat "${AGGREGATE}"

# Pass criteria sanity (see docs § 15.8): exit 0 if at least one ENOSPC, else 1.
PODS_ENOSPC=$(python3 -c "import json; print(json.load(open('${AGGREGATE}'))['pods_enospc'])")
if [[ "${PODS_ENOSPC}" -lt 1 ]]; then
  echo "WARN: zero pods reached ENOSPC. Pass criterion 1 in § 15.8 is NOT satisfied." >&2
  exit 1
fi
echo "[$(date -u +%H:%M:%S)] Scenario G complete (pods_enospc=${PODS_ENOSPC})"
