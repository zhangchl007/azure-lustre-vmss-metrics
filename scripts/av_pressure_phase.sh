#!/usr/bin/env bash
# Launch one phase of the AV Lustre pressure test against an existing AKS
# cluster. The validation Job manifest stays unchanged on disk; each phase is
# applied by patching a fresh copy with the requested concurrency and env vars,
# waiting for completion, and collecting structured pod summaries.
#
# Usage:
#   scripts/av_pressure_phase.sh \
#     --phase ramp-20 \
#     --parallelism 20 \
#     --run-id av-press-20260519-01-ramp-20 \
#     --mode read-write-output \
#     --read-pattern full \
#     --epochs 2 \
#     --warmup-seconds 60 \
#     --hotset-count 25 \
#     --split training,validation \
#     --subpath training/camera_image \
#     --bucket-slo small=20,medium=300,large=3000 \
#     --fail-on-slo \
#     --output-dir reports/av-press-20260519-01
#
# All flags are optional except --phase, --parallelism, and --run-id.
# --split sets SPLIT_FILTER (comma-separated list of first-level dirs to
#   descend into under the walk root).
# --subpath sets SUBPATH (relative path under DATASET_ROOT to use as the walk
#   root). Must not contain '..' and must not start with '/'.

set -euo pipefail

NAMESPACE="lustre-pressure-test"
JOB_NAME="av-dataset-validation"
JOB_MANIFEST="deploy/pressure-test/av-dataset-validation-job.yaml"
TIMEOUT="7200s"

PHASE=""
PARALLELISM=""
RUN_ID=""
MODE=""
READ_PATTERN=""
EPOCHS=""
WARMUP_SECONDS=""
HOTSET_COUNT=""
BUCKET_SLO=""
FAIL_ON_SLO="false"
SPLIT_FILTER=""
SUBPATH=""
OUTPUT_DIR="reports"

usage() {
   grep '^# ' "$0" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
   case "$1" in
      --phase) PHASE="$2"; shift 2 ;;
      --parallelism) PARALLELISM="$2"; shift 2 ;;
      --run-id) RUN_ID="$2"; shift 2 ;;
      --mode) MODE="$2"; shift 2 ;;
      --read-pattern) READ_PATTERN="$2"; shift 2 ;;
      --epochs) EPOCHS="$2"; shift 2 ;;
      --warmup-seconds) WARMUP_SECONDS="$2"; shift 2 ;;
      --hotset-count) HOTSET_COUNT="$2"; shift 2 ;;
      --bucket-slo) BUCKET_SLO="$2"; shift 2 ;;
      --fail-on-slo) FAIL_ON_SLO="true"; shift ;;
      --split) SPLIT_FILTER="$2"; shift 2 ;;
      --subpath) SUBPATH="$2"; shift 2 ;;
      --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
      --timeout) TIMEOUT="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) echo "unknown argument: $1" >&2; usage >&2; exit 64 ;;
   esac
done

if [[ -z "$PHASE" || -z "$PARALLELISM" || -z "$RUN_ID" ]]; then
   echo "ERROR: --phase, --parallelism, and --run-id are required" >&2
   usage >&2
   exit 64
fi

if [[ -n "$SUBPATH" ]]; then
   if [[ "$SUBPATH" == /* ]]; then
      echo "ERROR: --subpath must be relative, not absolute ($SUBPATH)" >&2
      exit 64
   fi
   case "/${SUBPATH}/" in
      */../*)
         echo "ERROR: --subpath must not contain '..' segments ($SUBPATH)" >&2
         exit 64
         ;;
   esac
fi

if [[ ! -f "$JOB_MANIFEST" ]]; then
   echo "ERROR: missing manifest $JOB_MANIFEST" >&2
   exit 66
fi

command -v kubectl >/dev/null || { echo "ERROR: kubectl not on PATH" >&2; exit 127; }

mkdir -p "$OUTPUT_DIR"
phase_log="$OUTPUT_DIR/${RUN_ID}-${PHASE}.log"
phase_summary="$OUTPUT_DIR/${RUN_ID}-${PHASE}-summaries.jsonl"

echo "[phase=$PHASE] cleaning any previous instance of job/$JOB_NAME"
kubectl delete job -n "$NAMESPACE" "$JOB_NAME" --ignore-not-found

echo "[phase=$PHASE] applying job manifest"
kubectl apply -f "$JOB_MANIFEST" >/dev/null

echo "[phase=$PHASE] patching parallelism/completions to $PARALLELISM"
kubectl patch job -n "$NAMESPACE" "$JOB_NAME" --type=merge -p \
   "$(printf '{"spec":{"parallelism":%s,"completions":%s}}' "$PARALLELISM" "$PARALLELISM")"

# Patch ConfigMap-driven env vars on the Job template via kubectl set env.
set_env=("kubectl" "set" "env" "-n" "$NAMESPACE" "job/$JOB_NAME"
         "POD_COUNT=$PARALLELISM"
         "RUN_ID=$RUN_ID")
[[ -n "$MODE" ]]            && set_env+=("MODE=$MODE")
[[ -n "$READ_PATTERN" ]]    && set_env+=("READ_PATTERN=$READ_PATTERN")
[[ -n "$EPOCHS" ]]          && set_env+=("EPOCHS=$EPOCHS")
[[ -n "$WARMUP_SECONDS" ]]  && set_env+=("WARMUP_SECONDS=$WARMUP_SECONDS")
[[ -n "$HOTSET_COUNT" ]]    && set_env+=("HOTSET_COUNT=$HOTSET_COUNT")
[[ -n "$BUCKET_SLO" ]]      && set_env+=("BUCKET_SLO_P95_MS=$BUCKET_SLO")
[[ -n "$SPLIT_FILTER" ]]    && set_env+=("SPLIT_FILTER=$SPLIT_FILTER")
[[ -n "$SUBPATH" ]]         && set_env+=("SUBPATH=$SUBPATH")
set_env+=("FAIL_ON_SLO=$FAIL_ON_SLO")
echo "[phase=$PHASE] setting env: ${set_env[*]:5}"
"${set_env[@]}" >/dev/null

echo "[phase=$PHASE] waiting up to $TIMEOUT for completion"
if ! kubectl wait -n "$NAMESPACE" --for=condition=complete \
      "job/$JOB_NAME" --timeout="$TIMEOUT"; then
   echo "[phase=$PHASE] WARNING: job did not reach Complete; collecting failure context" >&2
fi

echo "[phase=$PHASE] capturing pod logs to $phase_log"
kubectl logs -n "$NAMESPACE" -l app.kubernetes.io/name=av-lustre-workload \
   --tail=-1 --prefix=true > "$phase_log"

echo "[phase=$PHASE] extracting per-pod summary JSON objects to $phase_summary"
python3 - "$phase_log" "$phase_summary" <<'PY'
import json
import sys
from pathlib import Path

src = Path(sys.argv[1])
dst = Path(sys.argv[2])
text = src.read_text(encoding="utf-8", errors="replace")
dst.write_text("", encoding="utf-8")

decoder = json.JSONDecoder()
i = 0
count = 0
with dst.open("a", encoding="utf-8") as handle:
    while i < len(text):
        start = text.find("{", i)
        if start < 0:
            break
        try:
            obj, end = decoder.raw_decode(text, start)
        except json.JSONDecodeError:
            i = start + 1
            continue
        if isinstance(obj, dict) and obj.get("event") == "summary":
            handle.write(json.dumps(obj, sort_keys=True))
            handle.write("\n")
            count += 1
        i = end
print(f"summaries={count} -> {dst}")
PY

echo "[phase=$PHASE] done"
