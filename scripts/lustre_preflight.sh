#!/usr/bin/env bash
# Shared pre-flight checks for the Lustre pressure-test runbook
# (docs/lustre-pressure-test.md § 14.4, § 15.5, § 16.2).
#
# Usage:
#   scripts/lustre_preflight.sh --check active-writers
#   scripts/lustre_preflight.sh --check capacity-baseline
#   scripts/lustre_preflight.sh --check top-level-listing
#   scripts/lustre_preflight.sh --check all
#
# Exit codes:
#   0 = all selected checks passed
#   1 = at least one check failed (cause printed to stderr)
#   2 = bad invocation

set -euo pipefail

NAMESPACE="lustre-pressure-test"
EXPORTER_NAMESPACE="default"
EXPORTER_DEPLOY="vmss-metrics-exporter"
FILESYSTEM_NAME="${FILESYSTEM_NAME:-almfstestcluster02}"
CAPACITY_BASELINE_PCT="${CAPACITY_BASELINE_PCT:-10}"
NODE_POOL="${NODE_POOL:-juicefspool}"

CHECKS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --check)
      CHECKS+=("$2"); shift 2 ;;
    -h|--help)
      sed -n '1,18p' "$0"; exit 0 ;;
    *)
      echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [[ ${#CHECKS[@]} -eq 0 ]]; then
  echo "ERROR: at least one --check is required" >&2; exit 2
fi
if [[ " ${CHECKS[*]} " == *" all "* ]]; then
  CHECKS=("active-writers" "capacity-baseline" "top-level-listing")
fi

FAIL=0

_check_active_writers() {
  echo "[pre-flight] checking for active Lustre writers in ns/${NAMESPACE}..."
  # Match any Job that writes Lustre: av-dataset-validation*, waymo-blob-to-lustre*,
  # lustre-fill-*, waymo-dataset-download*. A Job with .status.active > 0 is alive.
  local active
  active=$(kubectl -n "${NAMESPACE}" get jobs -o json 2>/dev/null \
    | python3 -c '
import json, sys
patterns = ("av-dataset-validation", "waymo-blob-to-lustre",
            "lustre-fill-", "waymo-dataset-download")
doc = json.load(sys.stdin)
hits = []
for j in doc.get("items", []):
    name = j["metadata"]["name"]
    if not any(name.startswith(p) for p in patterns):
        continue
    if (j.get("status", {}).get("active") or 0) > 0:
        hits.append(name)
print("\n".join(hits))
') || active=""
  if [[ -n "${active}" ]]; then
    echo "FAIL: active Lustre-writer Jobs found:" >&2
    echo "${active}" >&2
    return 1
  fi
  echo "OK: no active Lustre-writer Jobs."
}

_check_capacity_baseline() {
  echo "[pre-flight] checking starting Lustre capacity on ${FILESYSTEM_NAME}..."
  local used
  # The exporter image has no curl; use python urllib instead.
  used=$(kubectl exec -n "${EXPORTER_NAMESPACE}" "deploy/${EXPORTER_DEPLOY}" -- \
    python -c "import urllib.request; print(urllib.request.urlopen('http://localhost:8000/metrics').read().decode())" 2>/dev/null \
    | awk -v fs="${FILESYSTEM_NAME}" '
      $0 ~ "azure_managed_lustre_ost_bytes_used_percent" && $0 ~ fs {
        print $NF
        exit
      }') || used=""
  if [[ -z "${used}" ]]; then
    echo "WARN: could not read ost_bytes_used_percent (exporter not available?)" >&2
    return 1
  fi
  echo "starting used%=${used} (cap=${CAPACITY_BASELINE_PCT})"
  if python3 -c "import sys; sys.exit(0 if float('${used}') <= float('${CAPACITY_BASELINE_PCT}') else 1)"; then
    echo "OK: capacity within baseline."
  else
    echo "FAIL: ost_bytes_used_percent ${used} > baseline ${CAPACITY_BASELINE_PCT}" >&2
    return 1
  fi
}

_check_top_level_listing() {
  echo "[pre-flight] listing top-level entries under /mnt/lustre..."
  kubectl run lustre-ls-preflight --rm -i --restart=Never \
    -n "${NAMESPACE}" --image=python:3.12-slim \
    --overrides="{\"apiVersion\":\"v1\",\"spec\":{\"nodeSelector\":{\"kubernetes.azure.com/agentpool\":\"${NODE_POOL}\"},\"containers\":[{\"name\":\"ls\",\"image\":\"python:3.12-slim\",\"command\":[\"sh\",\"-c\",\"ls -la /mnt/lustre; echo; du -sh /mnt/lustre/*/ 2>/dev/null\"],\"volumeMounts\":[{\"name\":\"lustre\",\"mountPath\":\"/mnt/lustre\",\"readOnly\":true}]}],\"volumes\":[{\"name\":\"lustre\",\"persistentVolumeClaim\":{\"claimName\":\"lustre-pressure-test-pvc\"}}]}}" \
    --command -- sh -c 'true' 2>/dev/null || true
}

for c in "${CHECKS[@]}"; do
  case "${c}" in
    active-writers)     _check_active_writers || FAIL=1 ;;
    capacity-baseline)  _check_capacity_baseline || FAIL=1 ;;
    top-level-listing)  _check_top_level_listing || FAIL=1 ;;
    *) echo "unknown --check value: ${c}" >&2; exit 2 ;;
  esac
done

if [[ "${FAIL}" -ne 0 ]]; then
  echo "[pre-flight] one or more checks FAILED" >&2
  exit 1
fi
echo "[pre-flight] all selected checks passed"
