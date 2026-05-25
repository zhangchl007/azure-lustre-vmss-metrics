#!/usr/bin/env bash
# Scenario F Leg 2 driver (docs/lustre-pressure-test.md § 14): renders
# deploy/pressure-test/waymo-blob-to-lustre-job.yaml with the requested
# RUN_ID / SOURCE_SUFFIX / rclone tunings, applies it, waits for terminal
# Job condition, and collects a per-run summary into ${OUTPUT_DIR}.
#
# Usage:
#   scripts/av_blob_to_lustre.sh \
#     --run-id blob-ingest-20260525-1700 \
#     [--source-suffix testing|validation|...] \
#     [--transfers 32] [--checkers 16] [--buffer-size 32M] \
#     [--output-dir reports/blob-ingest-20260525-1700] \
#     [--timeout 6h]

set -euo pipefail

NAMESPACE="lustre-pressure-test"
JOB_NAME="waymo-blob-to-lustre"
JOB_MANIFEST="deploy/pressure-test/waymo-blob-to-lustre-job.yaml"

RUN_ID=""
SOURCE_SUFFIX=""
TRANSFERS=""
CHECKERS=""
BUFFER_SIZE=""
OUTPUT_DIR=""
TIMEOUT="6h"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id) RUN_ID="$2"; shift 2 ;;
    --source-suffix) SOURCE_SUFFIX="$2"; shift 2 ;;
    --transfers) TRANSFERS="$2"; shift 2 ;;
    --checkers) CHECKERS="$2"; shift 2 ;;
    --buffer-size) BUFFER_SIZE="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --timeout) TIMEOUT="$2"; shift 2 ;;
    -h|--help)
      sed -n '1,15p' "$0"; exit 0 ;;
    *)
      echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "${RUN_ID}" ]]; then
  echo "ERROR: --run-id is required" >&2; exit 2
fi
case "${RUN_ID}" in
  *"/"*|"."|"..") echo "ERROR: --run-id must be a single path segment" >&2; exit 2 ;;
esac
if [[ -z "${OUTPUT_DIR}" ]]; then
  OUTPUT_DIR="reports/${RUN_ID}"
fi
mkdir -p "${OUTPUT_DIR}"

# Render the Job manifest with the requested env overrides via kubectl create
# --dry-run, then patch the container env array with jq. Job pod templates are
# immutable, so all overrides must be applied before apply.
echo "[$(date -u +%H:%M:%S)] rendering ${JOB_MANIFEST} for RUN_ID=${RUN_ID}"
RENDERED="${OUTPUT_DIR}/${JOB_NAME}-${RUN_ID}.yaml"

python3 - <<PY > "${RENDERED}"
import os, sys, yaml
with open("${JOB_MANIFEST}") as fh:
    doc = next(d for d in yaml.safe_load_all(fh) if d and d.get("kind") == "Job")
doc["metadata"]["name"] = "${JOB_NAME}"
overrides = {
    "RUN_ID": "${RUN_ID}",
    "SOURCE_SUFFIX": "${SOURCE_SUFFIX}",
}
if "${TRANSFERS}":
    overrides["RCLONE_TRANSFERS"] = "${TRANSFERS}"
if "${CHECKERS}":
    overrides["RCLONE_CHECKERS"] = "${CHECKERS}"
if "${BUFFER_SIZE}":
    overrides["RCLONE_BUFFER_SIZE"] = "${BUFFER_SIZE}"
container = doc["spec"]["template"]["spec"]["containers"][0]
env_list = container.get("env", [])
existing = {e["name"]: i for i, e in enumerate(env_list)}
for k, v in overrides.items():
    if k in existing:
        env_list[existing[k]] = {"name": k, "value": v}
    else:
        env_list.append({"name": k, "value": v})
container["env"] = env_list
yaml.safe_dump(doc, sys.stdout, sort_keys=False)
PY

echo "[$(date -u +%H:%M:%S)] applying ${RENDERED}"
kubectl -n "${NAMESPACE}" delete job/"${JOB_NAME}" --ignore-not-found
kubectl -n "${NAMESPACE}" apply -f "${RENDERED}"

LOG_FILE="${OUTPUT_DIR}/blob-ingest-${RUN_ID}.log"
SUMMARY_FILE="${OUTPUT_DIR}/blob-ingest-${RUN_ID}-summary.txt"
START_EPOCH=$(date -u +%s)

echo "[$(date -u +%H:%M:%S)] waiting for Job pod, then streaming logs to ${LOG_FILE}"
# Wait for pod creation.
for _ in $(seq 1 60); do
  if kubectl -n "${NAMESPACE}" get pod -l job-name="${JOB_NAME}" \
      -o jsonpath='{.items[0].metadata.name}' >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
POD=$(kubectl -n "${NAMESPACE}" get pod -l job-name="${JOB_NAME}" \
  -o jsonpath='{.items[0].metadata.name}')
echo "[$(date -u +%H:%M:%S)] pod=${POD}"

# Stream logs in the background.
kubectl -n "${NAMESPACE}" logs -f "${POD}" 2>&1 | tee "${LOG_FILE}" &
LOG_PID=$!

# Poll for terminal Job condition (Complete or Failed). The helper deliberately
# polls instead of `kubectl wait --for=condition=complete` so that a Failed Job
# does not hang until --timeout.
TIMEOUT_SECONDS=$(python3 -c "
import re, sys
v = '${TIMEOUT}'
m = re.fullmatch(r'(\d+)([smhd]?)', v)
if not m: sys.exit(f'bad timeout: {v}')
n, u = int(m.group(1)), m.group(2) or 's'
print(n * {'s':1,'m':60,'h':3600,'d':86400}[u])
")
DEADLINE=$(( START_EPOCH + TIMEOUT_SECONDS ))
FINAL_STATUS=""
while [[ $(date -u +%s) -lt ${DEADLINE} ]]; do
  STATUS=$(kubectl -n "${NAMESPACE}" get job "${JOB_NAME}" \
    -o jsonpath='{range .status.conditions[*]}{.type}={.status} {end}' 2>/dev/null || true)
  if [[ "${STATUS}" == *"Complete=True"* ]]; then
    FINAL_STATUS="complete"; break
  fi
  if [[ "${STATUS}" == *"Failed=True"* ]]; then
    FINAL_STATUS="failed"; break
  fi
  sleep 10
done
if [[ -z "${FINAL_STATUS}" ]]; then
  FINAL_STATUS="timeout"
fi
END_EPOCH=$(date -u +%s)
wait "${LOG_PID}" 2>/dev/null || true

WALL_CLOCK=$(( END_EPOCH - START_EPOCH ))
{
  echo "RUN_ID=${RUN_ID}"
  echo "SOURCE_SUFFIX=${SOURCE_SUFFIX}"
  echo "TRANSFERS=${TRANSFERS:-32}"
  echo "CHECKERS=${CHECKERS:-16}"
  echo "BUFFER_SIZE=${BUFFER_SIZE:-32M}"
  echo "START_UTC=$(date -u -d @${START_EPOCH} '+%Y-%m-%dT%H:%M:%SZ')"
  echo "END_UTC=$(date -u -d @${END_EPOCH} '+%Y-%m-%dT%H:%M:%SZ')"
  echo "WALL_CLOCK_SECONDS=${WALL_CLOCK}"
  echo "JOB_STATUS=${FINAL_STATUS}"
  # Extract rclone end-of-run stats from the log (last "Transferred:" line).
  grep -E '^Transferred:' "${LOG_FILE}" | tail -3 || true
  echo "--- SOURCE / DEST tally (from container) ---"
  grep -E '^(SOURCE_BYTES|SOURCE_COUNT|DEST_BYTES|DEST_COUNT|byte_delta)=' "${LOG_FILE}" || true
} | tee "${SUMMARY_FILE}"

if [[ "${FINAL_STATUS}" != "complete" ]]; then
  echo "ERROR: Job ended with status=${FINAL_STATUS}" >&2
  exit 1
fi
echo "[$(date -u +%H:%M:%S)] Scenario F Leg 2 complete"
