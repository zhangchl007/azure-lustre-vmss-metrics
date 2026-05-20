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
#     --output-bytes-per-input 1.0 \
#     --max-output-bytes-per-file 512MiB \
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
OUTPUT_BYTES_PER_INPUT=""
MAX_OUTPUT_BYTES_PER_FILE=""
FILES_PER_POD=""
MAX_BYTES_PER_POD=""
VERIFY_READS=""
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
      --output-bytes-per-input) OUTPUT_BYTES_PER_INPUT="$2"; shift 2 ;;
      --max-output-bytes-per-file) MAX_OUTPUT_BYTES_PER_FILE="$2"; shift 2 ;;
      --files-per-pod) FILES_PER_POD="$2"; shift 2 ;;
      --max-bytes-per-pod) MAX_BYTES_PER_POD="$2"; shift 2 ;;
      --verify-reads) VERIFY_READS="true"; shift ;;
      --no-verify-reads) VERIFY_READS="false"; shift ;;
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
kubectl delete job -n "$NAMESPACE" "$JOB_NAME" --ignore-not-found --wait=true

# Render a fresh Job locally before applying it. Job pod templates are
# immutable after creation, so parallelism and env overrides must be present in
# the manifest that creates the Job instead of patched afterward.
rendered_job="$(mktemp)"
trap 'rm -f "$rendered_job"' EXIT
kubectl create --dry-run=client -f "$JOB_MANIFEST" -o json > "$rendered_job"

env_overrides=("POD_COUNT=$PARALLELISM" "RUN_ID=$RUN_ID")
[[ -n "$MODE" ]]            && env_overrides+=("MODE=$MODE")
[[ -n "$READ_PATTERN" ]]    && env_overrides+=("READ_PATTERN=$READ_PATTERN")
[[ -n "$EPOCHS" ]]          && env_overrides+=("EPOCHS=$EPOCHS")
[[ -n "$WARMUP_SECONDS" ]]  && env_overrides+=("WARMUP_SECONDS=$WARMUP_SECONDS")
[[ -n "$HOTSET_COUNT" ]]    && env_overrides+=("HOTSET_COUNT=$HOTSET_COUNT")
[[ -n "$BUCKET_SLO" ]]      && env_overrides+=("BUCKET_SLO_P95_MS=$BUCKET_SLO")
[[ -n "$SPLIT_FILTER" ]]    && env_overrides+=("SPLIT_FILTER=$SPLIT_FILTER")
[[ -n "$SUBPATH" ]]         && env_overrides+=("SUBPATH=$SUBPATH")
[[ -n "$OUTPUT_BYTES_PER_INPUT" ]]   && env_overrides+=("OUTPUT_BYTES_PER_INPUT=$OUTPUT_BYTES_PER_INPUT")
[[ -n "$MAX_OUTPUT_BYTES_PER_FILE" ]] && env_overrides+=("MAX_OUTPUT_BYTES_PER_FILE=$MAX_OUTPUT_BYTES_PER_FILE")
[[ -n "$FILES_PER_POD" ]]            && env_overrides+=("FILES_PER_POD=$FILES_PER_POD")
[[ -n "$MAX_BYTES_PER_POD" ]]        && env_overrides+=("MAX_BYTES_PER_POD=$MAX_BYTES_PER_POD")
[[ -n "$VERIFY_READS" ]]             && env_overrides+=("VERIFY_READS=$VERIFY_READS")
env_overrides+=("FAIL_ON_SLO=$FAIL_ON_SLO")

echo "[phase=$PHASE] rendering parallelism/completions to $PARALLELISM"
echo "[phase=$PHASE] rendering env: ${env_overrides[*]}"
python3 - "$rendered_job" "$PARALLELISM" "${env_overrides[@]}" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
parallelism = int(sys.argv[2])
overrides = dict(item.split("=", 1) for item in sys.argv[3:])

job = json.loads(path.read_text(encoding="utf-8"))
job["spec"]["parallelism"] = parallelism
job["spec"]["completions"] = parallelism

containers = job["spec"]["template"]["spec"]["containers"]
container = next((c for c in containers if c.get("name") == "av-workload"), containers[0])
env = container.setdefault("env", [])

for name, value in overrides.items():
   for item in env:
      if item.get("name") == name:
         item.pop("valueFrom", None)
         item["value"] = value
         break
   else:
      env.append({"name": name, "value": value})

path.write_text(json.dumps(job), encoding="utf-8")
PY

echo "[phase=$PHASE] applying rendered job manifest"
kubectl apply -f "$rendered_job" >/dev/null

echo "[phase=$PHASE] waiting up to $TIMEOUT for completion"
if ! kubectl wait -n "$NAMESPACE" --for=condition=complete \
      "job/$JOB_NAME" --timeout="$TIMEOUT"; then
   echo "[phase=$PHASE] WARNING: job did not reach Complete; collecting failure context" >&2
fi

echo "[phase=$PHASE] capturing pod logs to $phase_log"
kubectl logs -n "$NAMESPACE" \
   -l app.kubernetes.io/name=av-lustre-workload,app.kubernetes.io/component=validation \
   --tail=-1 --prefix=true > "$phase_log"

echo "[phase=$PHASE] extracting per-pod summary JSON objects to $phase_summary"
python3 - "$phase_log" "$phase_summary" <<'PY'
import json
import sys
from pathlib import Path

src = Path(sys.argv[1])
dst = Path(sys.argv[2])
lines = []
for line in src.read_text(encoding="utf-8", errors="replace").splitlines():
   if line.startswith("["):
      _, sep, rest = line.partition("] ")
      if sep:
         line = rest
   lines.append(line)
text = "\n".join(lines)
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
