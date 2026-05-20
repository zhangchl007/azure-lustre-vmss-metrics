#!/usr/bin/env python3
"""Autonomous-vehicle dataset workload simulator for Azure Managed Lustre on AKS.

This script is designed to be launched as a Kubernetes Job (preferably an
Indexed Job) where each pod processes a deterministic shard of an existing
Lustre dataset. Source files are read but never modified. Derived outputs are
written only under the pod's isolated result directory.

Modes:
  discover           Enumerate a Lustre subtree and print a dataset profile.
  read-only          Read the pod's shard of files and report timings.
  read-write-output  Read the pod's shard and write derived outputs into the
                     per-pod result directory.
  verify-output      Re-read derived outputs and validate their summaries.

Path safety:
  Output writes are constrained to ``<RESULT_ROOT>/<RUN_ID>/<pod-name>/``.
  Attempts to resolve outside of this directory raise an error before any
  filesystem write occurs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import re
import socket
import statistics
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_CHUNK_SIZE_BYTES = 4 * 1024 * 1024
DEFAULT_SMALL_MAX_BYTES = 1 * 1024 * 1024
DEFAULT_MEDIUM_MAX_BYTES = 64 * 1024 * 1024
DEFAULT_LARGE_MAX_BYTES = 512 * 1024 * 1024
DEFAULT_OUTPUT_BYTES_PER_INPUT = 0.001
DEFAULT_RANDOM_OFFSET_READS = 4
DEFAULT_EPOCHS = 1
DEFAULT_HOTSET_COUNT = 0
DEFAULT_WARMUP_SECONDS = 0

SIZE_BUCKETS = ("small", "medium", "large", "xlarge")
READ_PATTERNS = ("full", "random-offset", "head-tail")

_SIZE_RE = re.compile(r"^\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>[a-zA-Z]+)?\s*$")
_BINARY_UNITS = {
    "b": 1,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
}
_DECIMAL_UNITS = {
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
}


def parse_size(value: str | int) -> int:
    """Parse a human-friendly size string (e.g. ``500MiB``) into bytes."""
    if isinstance(value, int):
        if value < 0:
            raise ValueError("size must be non-negative")
        return value
    match = _SIZE_RE.match(str(value))
    if not match:
        raise ValueError(f"invalid size: {value!r}")
    number = float(match.group("value"))
    unit = (match.group("unit") or "b").lower()
    multiplier = _BINARY_UNITS.get(unit) or _DECIMAL_UNITS.get(unit)
    if multiplier is None:
        raise ValueError(f"unsupported size unit: {unit!r}")
    return int(number * multiplier)


def classify_size(
    size_bytes: int,
    *,
    small_max: int = DEFAULT_SMALL_MAX_BYTES,
    medium_max: int = DEFAULT_MEDIUM_MAX_BYTES,
    large_max: int = DEFAULT_LARGE_MAX_BYTES,
) -> str:
    """Return the bucket name for a given file size."""
    if size_bytes < 0:
        raise ValueError("size_bytes must be non-negative")
    if size_bytes <= small_max:
        return "small"
    if size_bytes <= medium_max:
        return "medium"
    if size_bytes <= large_max:
        return "large"
    return "xlarge"


@dataclass
class FileEntry:
    path: Path
    size: int
    bucket: str


def enumerate_dataset(
    root: Path,
    *,
    exclude_paths: Iterable[Path] = (),
    follow_symlinks: bool = False,
    small_max: int = DEFAULT_SMALL_MAX_BYTES,
    medium_max: int = DEFAULT_MEDIUM_MAX_BYTES,
    large_max: int = DEFAULT_LARGE_MAX_BYTES,
    split_filter: Iterable[str] | None = None,
) -> list[FileEntry]:
    """Walk ``root`` and return a deterministic list of regular files.

    When ``split_filter`` is provided, only first-level children of ``root``
    whose directory name appears in the filter are descended into. Files
    directly inside ``root`` are always considered. The filter must be a
    non-empty iterable of names; pass ``None`` (the default) to walk every
    child directory.

    The returned list is sorted by path so independent pods derive identical
    shards from the same dataset.
    """
    if not root.exists():
        raise FileNotFoundError(f"dataset root does not exist: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"dataset root is not a directory: {root}")

    resolved_root = root.resolve()
    excluded_resolved: list[Path] = []
    for excluded in exclude_paths:
        try:
            excluded_resolved.append(excluded.resolve())
        except OSError:
            continue

    split_names: set[str] | None = None
    if split_filter is not None:
        split_names = {name for name in split_filter if name}
        if not split_names:
            split_names = None

    entries: list[FileEntry] = []
    for dirpath, dirnames, filenames in os.walk(resolved_root, followlinks=follow_symlinks):
        current = Path(dirpath).resolve()
        is_walk_root = current == resolved_root
        # Prune excluded directories in-place so os.walk stops descending.
        kept_dirs: list[str] = []
        for name in dirnames:
            # When a split filter is set, only descend into matching first-level dirs.
            if is_walk_root and split_names is not None and name not in split_names:
                continue
            child = (current / name).resolve()
            if any(child == ex or _is_inside(child, ex) for ex in excluded_resolved):
                continue
            kept_dirs.append(name)
        dirnames[:] = kept_dirs

        for filename in filenames:
            full = current / filename
            if any(full == ex or _is_inside(full, ex) for ex in excluded_resolved):
                continue
            try:
                if not follow_symlinks and full.is_symlink():
                    continue
                if not full.is_file():
                    continue
                size = full.stat().st_size
            except OSError:
                continue
            bucket = classify_size(
                size,
                small_max=small_max,
                medium_max=medium_max,
                large_max=large_max,
            )
            entries.append(FileEntry(path=full, size=size, bucket=bucket))

    entries.sort(key=lambda entry: str(entry.path))
    return entries


def _is_inside(child: Path, parent: Path) -> bool:
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False


def shard_files(
    entries: list[FileEntry],
    *,
    pod_index: int,
    pod_count: int,
) -> list[FileEntry]:
    """Deterministically slice ``entries`` for ``pod_index``.

    Uses round-robin assignment so every pod sees a similar size distribution
    even if files are sorted by path or size.
    """
    if pod_count <= 0:
        raise ValueError("pod_count must be positive")
    if not 0 <= pod_index < pod_count:
        raise ValueError("pod_index must satisfy 0 <= pod_index < pod_count")
    return [entry for index, entry in enumerate(entries) if index % pod_count == pod_index]


def select_files(
    shard: list[FileEntry],
    *,
    files_per_pod: int | None,
    max_bytes_per_pod: int | None,
    seed: int,
) -> list[FileEntry]:
    """Return the files this pod will actually process.

    The shard is shuffled deterministically by ``seed`` so different runs vary
    workload order, then limited by file count and/or byte budget.
    """
    if files_per_pod is not None and files_per_pod < 0:
        raise ValueError("files_per_pod must be non-negative")
    if max_bytes_per_pod is not None and max_bytes_per_pod < 0:
        raise ValueError("max_bytes_per_pod must be non-negative")

    rng = random.Random(seed)
    ordered = list(shard)
    rng.shuffle(ordered)

    selected: list[FileEntry] = []
    total_bytes = 0
    for entry in ordered:
        if files_per_pod is not None and len(selected) >= files_per_pod:
            break
        if max_bytes_per_pod is not None and total_bytes + entry.size > max_bytes_per_pod:
            continue
        selected.append(entry)
        total_bytes += entry.size
    return selected


def percentile(values: list[float], percent: float) -> float:
    """Return a percentile in milliseconds using linear interpolation."""
    if not values:
        return 0.0
    if not 0 <= percent <= 100:
        raise ValueError("percent must be in [0, 100]")
    ordered = sorted(values)
    if percent == 100:
        return ordered[-1]
    rank = (percent / 100.0) * (len(ordered) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[int(rank)]
    weight = rank - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def ensure_within(base: Path, candidate: Path) -> Path:
    """Resolve ``candidate`` and assert it lives under ``base``.

    Raises ``ValueError`` if the resolved path escapes ``base``. This is the
    only sanctioned way to compute an output path inside this module.
    """
    base_resolved = base.resolve()
    try:
        candidate_resolved = candidate.resolve()
    except OSError as exc:
        raise ValueError(f"unable to resolve {candidate}: {exc}") from exc
    if not _is_inside(candidate_resolved, base_resolved) and candidate_resolved != base_resolved:
        raise ValueError(
            f"refusing to operate on {candidate_resolved} outside of {base_resolved}"
        )
    return candidate_resolved


def assert_disjoint_roots(dataset_root: Path, result_root: Path) -> tuple[Path, Path]:
    """Resolve both roots and assert neither is nested inside the other.

    Returns the resolved ``(dataset_root, result_root)`` tuple. Raises
    ``ValueError`` if the two roots resolve to the same path, if
    ``result_root`` is nested inside ``dataset_root``, or if ``dataset_root``
    is nested inside ``result_root``. This is the configuration-level
    guardrail that prevents the simulator from writing into the source
    dataset; it must be called from every mode before any I/O.
    """
    dataset_resolved = dataset_root.resolve()
    result_resolved = result_root.resolve()
    if dataset_resolved == result_resolved:
        raise ValueError(
            f"refusing to run: DATASET_ROOT and RESULT_ROOT resolve to the same path "
            f"({dataset_resolved}). RESULT_ROOT must be a sibling location, never "
            f"identical to or nested with DATASET_ROOT."
        )
    if _is_inside(result_resolved, dataset_resolved):
        raise ValueError(
            f"refusing to run: RESULT_ROOT ({result_resolved}) is nested inside "
            f"DATASET_ROOT ({dataset_resolved}). Move RESULT_ROOT outside the dataset "
            f"so output writes can never touch source files."
        )
    if _is_inside(dataset_resolved, result_resolved):
        raise ValueError(
            f"refusing to run: DATASET_ROOT ({dataset_resolved}) is nested inside "
            f"RESULT_ROOT ({result_resolved}). Move DATASET_ROOT outside the result "
            f"tree so the cleanup Job cannot reach source files."
        )
    return dataset_resolved, result_resolved


def resolve_walk_root(dataset_root: Path, subpath: str | None) -> Path:
    """Compute the effective walk root, validating ``subpath`` first.

    Returns ``dataset_root`` resolved when ``subpath`` is empty/None. When
    ``subpath`` is provided it must (a) not be absolute, (b) not contain a
    ``..`` segment, and (c) resolve to a directory strictly under
    ``dataset_root``. Any violation raises ``ValueError``.
    """
    dataset_resolved = dataset_root.resolve()
    if not subpath:
        return dataset_resolved
    raw = subpath.strip()
    if not raw:
        return dataset_resolved
    candidate = Path(raw)
    if candidate.is_absolute():
        raise ValueError(
            f"refusing to use SUBPATH {subpath!r}: must be a relative path, not absolute."
        )
    if ".." in candidate.parts:
        raise ValueError(
            f"refusing to use SUBPATH {subpath!r}: '..' segments are not allowed."
        )
    walk_root = (dataset_resolved / candidate).resolve()
    if walk_root != dataset_resolved and not _is_inside(walk_root, dataset_resolved):
        raise ValueError(
            f"refusing to use SUBPATH {subpath!r}: resolved walk root {walk_root} "
            f"escapes DATASET_ROOT {dataset_resolved}."
        )
    return walk_root


def parse_split_filter(value: str | None) -> list[str]:
    """Parse the comma-separated ``SPLIT_FILTER`` env / flag value."""
    if value is None:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass
class FileReadResult:
    path: str
    size: int
    bucket: str
    elapsed_seconds: float
    chunk_count: int
    bytes_read: int
    checksum: str | None
    pattern: str = "full"
    error: str | None = None


@dataclass
class WorkloadSummary:
    pod_name: str
    pod_index: int
    pod_count: int
    mode: str
    dataset_root: str
    result_root: str | None
    read_pattern: str = "full"
    epochs_completed: int = 0
    warmup_seconds: float = 0.0
    hotset_count: int = 0
    files_attempted: int = 0
    files_succeeded: int = 0
    files_failed: int = 0
    bytes_read: int = 0
    bytes_written: int = 0
    elapsed_seconds: float = 0.0
    per_file_latency_ms: dict[str, object] = field(default_factory=dict)
    warmup_latency_ms: dict[str, float] = field(default_factory=dict)
    hotset_latency_ms: dict[str, object] = field(default_factory=dict)
    per_bucket_counts: dict[str, int] = field(default_factory=dict)
    per_bucket_bytes: dict[str, int] = field(default_factory=dict)
    slo: dict[str, object] = field(default_factory=dict)
    errors: list[dict[str, str]] = field(default_factory=list)


def parse_slo_pairs(value: str | None) -> dict[str, float]:
    """Parse ``bucket=ms,bucket=ms,…`` into a dictionary of thresholds."""
    if value is None or value.strip() == "":
        return {}
    result: dict[str, float] = {}
    for raw in value.split(","):
        item = raw.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"invalid SLO entry {item!r}; expected bucket=ms")
        bucket, ms = item.split("=", 1)
        bucket = bucket.strip()
        if bucket not in SIZE_BUCKETS:
            raise ValueError(
                f"invalid SLO bucket {bucket!r}; expected one of {SIZE_BUCKETS}"
            )
        result[bucket] = float(ms)
    return result


def read_file_pattern(
    entry: FileEntry,
    *,
    chunk_size: int,
    verify_checksum: bool,
    pattern: str = "full",
    random_offset_reads: int = DEFAULT_RANDOM_OFFSET_READS,
    rng: random.Random | None = None,
) -> FileReadResult:
    """Read ``entry`` using one of the supported AV access patterns.

    - ``full``: stream the entire file in ``chunk_size`` chunks (current default).
    - ``random-offset``: read ``random_offset_reads`` random ``chunk_size``
      regions (useful for frame-index / sensor lookup workloads).
    - ``head-tail``: read the first and last ``chunk_size`` bytes only
      (cheap metadata-style access used by manifests and small index files).
    """
    if pattern not in READ_PATTERNS:
        raise ValueError(f"unknown read pattern {pattern!r}")
    if pattern == "full":
        return read_file_chunked(
            entry, chunk_size=chunk_size, verify_checksum=verify_checksum
        )
    if entry.size <= chunk_size:
        # Short files behave the same regardless of pattern; just read fully.
        result = read_file_chunked(
            entry, chunk_size=chunk_size, verify_checksum=False
        )
        result.pattern = pattern
        return result

    rng = rng or random.Random(str(entry.path))
    chunks = 0
    bytes_read = 0
    started = time.monotonic()
    try:
        with entry.path.open("rb") as handle:
            if pattern == "random-offset":
                max_offset = max(0, entry.size - chunk_size)
                count = max(1, random_offset_reads)
                for _ in range(count):
                    offset = rng.randint(0, max_offset)
                    handle.seek(offset)
                    chunk = handle.read(chunk_size)
                    if not chunk:
                        break
                    chunks += 1
                    bytes_read += len(chunk)
            else:  # head-tail
                head = handle.read(chunk_size)
                if head:
                    chunks += 1
                    bytes_read += len(head)
                tail_offset = max(0, entry.size - chunk_size)
                handle.seek(tail_offset)
                tail = handle.read(chunk_size)
                if tail:
                    chunks += 1
                    bytes_read += len(tail)
    except OSError as exc:
        elapsed = time.monotonic() - started
        return FileReadResult(
            path=str(entry.path),
            size=entry.size,
            bucket=entry.bucket,
            elapsed_seconds=elapsed,
            chunk_count=chunks,
            bytes_read=bytes_read,
            checksum=None,
            pattern=pattern,
            error=f"{type(exc).__name__}: {exc}",
        )
    elapsed = time.monotonic() - started
    return FileReadResult(
        path=str(entry.path),
        size=entry.size,
        bucket=entry.bucket,
        elapsed_seconds=elapsed,
        chunk_count=chunks,
        bytes_read=bytes_read,
        checksum=None,
        pattern=pattern,
    )


def read_file_chunked(
    entry: FileEntry,
    *,
    chunk_size: int,
    verify_checksum: bool,
) -> FileReadResult:
    """Stream ``entry`` from Lustre in fixed-size chunks."""
    sha = hashlib.sha256() if verify_checksum else None
    chunks = 0
    bytes_read = 0
    started = time.monotonic()
    try:
        with entry.path.open("rb") as handle:
            while True:
                chunk = handle.read(chunk_size)
                if not chunk:
                    break
                chunks += 1
                bytes_read += len(chunk)
                if sha is not None:
                    sha.update(chunk)
    except OSError as exc:
        elapsed = time.monotonic() - started
        return FileReadResult(
            path=str(entry.path),
            size=entry.size,
            bucket=entry.bucket,
            elapsed_seconds=elapsed,
            chunk_count=chunks,
            bytes_read=bytes_read,
            checksum=None,
            error=f"{type(exc).__name__}: {exc}",
        )
    elapsed = time.monotonic() - started
    return FileReadResult(
        path=str(entry.path),
        size=entry.size,
        bucket=entry.bucket,
        elapsed_seconds=elapsed,
        chunk_count=chunks,
        bytes_read=bytes_read,
        checksum=sha.hexdigest() if sha is not None else None,
    )


def write_per_pod_output(
    *,
    pod_output_dir: Path,
    file_result: FileReadResult,
    output_bytes_per_input: float,
    max_output_bytes_per_file: int,
    dataset_root: Path | None = None,
) -> int:
    """Write a small derived artifact describing ``file_result``.

    Returns the number of bytes written. The artifact is a JSON sidecar plus,
    when ``output_bytes_per_input`` > 0, a synthetic payload sized as a fraction
    of the input bytes (capped at ``max_output_bytes_per_file``).

    When ``dataset_root`` is provided the function applies a defense-in-depth
    check: every output path must resolve under ``pod_output_dir`` (already
    enforced by :func:`ensure_within`) **and** must not resolve under
    ``dataset_root``. This guards against a misconfigured ``RESULT_ROOT`` that
    somehow bypassed :func:`assert_disjoint_roots`.
    """
    dataset_resolved = dataset_root.resolve() if dataset_root is not None else None

    def _check_output(candidate: Path) -> Path:
        resolved = ensure_within(pod_output_dir, candidate)
        if dataset_resolved is not None and (
            resolved == dataset_resolved or _is_inside(resolved, dataset_resolved)
        ):
            raise ValueError(
                f"refusing to write output {resolved}: resolves under DATASET_ROOT "
                f"{dataset_resolved}."
            )
        return resolved

    artifact_name = hashlib.sha1(file_result.path.encode("utf-8")).hexdigest()[:16]
    artifact_dir = _check_output(pod_output_dir / artifact_name[:2])
    artifact_dir.mkdir(parents=True, exist_ok=True)

    summary_path = _check_output(artifact_dir / f"{artifact_name}.json")
    summary_payload = {
        "source_path": file_result.path,
        "size_bytes": file_result.size,
        "bucket": file_result.bucket,
        "elapsed_seconds": file_result.elapsed_seconds,
        "chunk_count": file_result.chunk_count,
        "checksum": file_result.checksum,
        "error": file_result.error,
    }
    summary_bytes = json.dumps(summary_payload, sort_keys=True).encode("utf-8")
    bytes_written = 0
    with summary_path.open("wb") as handle:
        handle.write(summary_bytes)
        handle.flush()
        os.fsync(handle.fileno())
    bytes_written += len(summary_bytes)

    if output_bytes_per_input > 0 and file_result.size > 0:
        payload_bytes = int(file_result.size * output_bytes_per_input)
        payload_bytes = min(payload_bytes, max_output_bytes_per_file)
        if payload_bytes > 0:
            payload_path = _check_output(artifact_dir / f"{artifact_name}.bin")
            block = b"AV-LUSTRE-OUTPUT" * 64  # 1 KiB block
            with payload_path.open("wb") as handle:
                remaining = payload_bytes
                while remaining > 0:
                    write_chunk = block if remaining >= len(block) else block[:remaining]
                    handle.write(write_chunk)
                    remaining -= len(write_chunk)
                handle.flush()
                os.fsync(handle.fileno())
            bytes_written += payload_bytes
    return bytes_written


def aggregate_summary(
    summary: WorkloadSummary,
    latencies_ms: list[float],
    per_bucket_latencies_ms: dict[str, list[float]],
) -> None:
    if latencies_ms:
        summary.per_file_latency_ms = {
            "min": min(latencies_ms),
            "p50": percentile(latencies_ms, 50),
            "p95": percentile(latencies_ms, 95),
            "p99": percentile(latencies_ms, 99),
            "max": max(latencies_ms),
            "mean": statistics.fmean(latencies_ms),
        }
    per_bucket_summary: dict[str, dict[str, float]] = {}
    for bucket, samples in per_bucket_latencies_ms.items():
        if not samples:
            continue
        per_bucket_summary[bucket] = {
            "count": len(samples),
            "p50": percentile(samples, 50),
            "p95": percentile(samples, 95),
            "p99": percentile(samples, 99),
            "max": max(samples),
        }
    if per_bucket_summary:
        summary.per_file_latency_ms["per_bucket"] = per_bucket_summary


def _summarize_simple(samples: list[float]) -> dict[str, float]:
    if not samples:
        return {}
    return {
        "count": len(samples),
        "min": min(samples),
        "p50": percentile(samples, 50),
        "p95": percentile(samples, 95),
        "p99": percentile(samples, 99),
        "max": max(samples),
        "mean": statistics.fmean(samples),
    }


def evaluate_slo(
    per_bucket_latencies_ms: dict[str, list[float]],
    thresholds_ms: dict[str, float],
) -> dict[str, object]:
    """Compare per-bucket p95 latency against configured thresholds.

    Returns a structured result with overall ``pass`` and per-bucket detail.
    Buckets without samples or without thresholds are reported but do not fail
    the overall result.
    """
    per_bucket: dict[str, dict[str, object]] = {}
    failures: list[str] = []
    for bucket in SIZE_BUCKETS:
        samples = per_bucket_latencies_ms.get(bucket) or []
        threshold = thresholds_ms.get(bucket)
        p95 = percentile(samples, 95) if samples else 0.0
        entry: dict[str, object] = {
            "count": len(samples),
            "p95_ms": p95,
            "threshold_ms": threshold,
        }
        if threshold is None or not samples:
            entry["pass"] = None
        else:
            ok = p95 <= threshold
            entry["pass"] = ok
            if not ok:
                failures.append(bucket)
        per_bucket[bucket] = entry
    return {
        "pass": not failures,
        "failures": failures,
        "thresholds_ms": thresholds_ms,
        "per_bucket": per_bucket,
    }


def run_discover(args: argparse.Namespace) -> WorkloadSummary:
    dataset_root = Path(args.dataset_root)
    # Disjoint-roots preflight runs in every mode — even discover — so a
    # misconfigured pair fails fast before any I/O against the dataset.
    if args.result_root:
        assert_disjoint_roots(dataset_root, Path(args.result_root))
    walk_root = resolve_walk_root(dataset_root, getattr(args, "subpath", None))
    split_names = parse_split_filter(getattr(args, "split_filter", None))
    excludes = [Path(p) for p in args.exclude or []]
    entries = enumerate_dataset(
        walk_root,
        exclude_paths=excludes,
        small_max=args.small_max,
        medium_max=args.medium_max,
        large_max=args.large_max,
        split_filter=split_names or None,
    )
    summary = WorkloadSummary(
        pod_name=args.pod_name,
        pod_index=args.pod_index,
        pod_count=args.pod_count,
        mode="discover",
        dataset_root=str(walk_root),
        result_root=None,
    )
    total_bytes = 0
    bucket_counts: dict[str, int] = dict.fromkeys(SIZE_BUCKETS, 0)
    bucket_bytes: dict[str, int] = dict.fromkeys(SIZE_BUCKETS, 0)
    directories: set[str] = set()
    largest: list[tuple[int, str]] = []
    for entry in entries:
        total_bytes += entry.size
        bucket_counts[entry.bucket] += 1
        bucket_bytes[entry.bucket] += entry.size
        directories.add(str(entry.path.parent))
        largest.append((entry.size, str(entry.path)))
    largest.sort(reverse=True)
    summary.files_attempted = len(entries)
    summary.files_succeeded = len(entries)
    summary.bytes_read = 0
    summary.per_bucket_counts = bucket_counts
    summary.per_bucket_bytes = bucket_bytes
    profile = {
        "dataset_root": summary.dataset_root,
        "file_count": len(entries),
        "total_bytes": total_bytes,
        "directory_count": len(directories),
        "bucket_counts": bucket_counts,
        "bucket_bytes": bucket_bytes,
        "largest_files": [{"size": size, "path": path} for size, path in largest[:10]],
    }
    print(json.dumps({"profile": profile}, indent=2, sort_keys=True))
    return summary


def run_read(args: argparse.Namespace, *, write_outputs: bool) -> WorkloadSummary:
    dataset_root = Path(args.dataset_root)
    # Disjoint-roots preflight: aborts before any I/O if DATASET_ROOT and
    # RESULT_ROOT overlap. Applied to read-only mode too because a misconfigured
    # RESULT_ROOT would otherwise be discovered only on the first write.
    dataset_resolved: Path
    if args.result_root:
        dataset_resolved, _ = assert_disjoint_roots(dataset_root, Path(args.result_root))
    else:
        dataset_resolved = dataset_root.resolve()
    walk_root = resolve_walk_root(dataset_root, getattr(args, "subpath", None))
    split_names = parse_split_filter(getattr(args, "split_filter", None))
    excludes = [Path(p) for p in args.exclude or []]
    result_root: Path | None = None
    pod_output_dir: Path | None = None
    if write_outputs:
        if not args.result_root or not args.run_id:
            raise ValueError("result-root and run-id are required for write modes")
        result_root_resolved = Path(args.result_root).resolve()
        run_dir = (result_root_resolved / args.run_id).resolve()
        try:
            run_dir.relative_to(result_root_resolved)
        except ValueError as exc:
            raise ValueError("run-id must not contain path separators") from exc
        pod_output_dir = (run_dir / args.pod_name).resolve()
        ensure_within(run_dir, pod_output_dir)
        pod_output_dir.mkdir(parents=True, exist_ok=True)
        # exclude the result directory from enumeration so we never read our outputs
        excludes.append(result_root_resolved)
        result_root = run_dir

    entries = enumerate_dataset(
        walk_root,
        exclude_paths=excludes,
        small_max=args.small_max,
        medium_max=args.medium_max,
        large_max=args.large_max,
        split_filter=split_names or None,
    )
    shard = shard_files(entries, pod_index=args.pod_index, pod_count=args.pod_count)

    hotset: list[FileEntry] = []
    if args.hotset_count and args.hotset_count > 0:
        # Top-K largest files across the whole dataset (shared by all pods).
        hotset = sorted(entries, key=lambda e: e.size, reverse=True)[: args.hotset_count]

    base_seed = args.seed if args.seed is not None else hash((args.run_id, args.pod_index))
    epochs = max(1, args.epochs)
    warmup_deadline_seconds = max(0.0, float(args.warmup_seconds))
    read_pattern = args.read_pattern
    pattern_rng = random.Random(f"{base_seed}-pattern")

    summary = WorkloadSummary(
        pod_name=args.pod_name,
        pod_index=args.pod_index,
        pod_count=args.pod_count,
        mode="read-write-output" if write_outputs else "read-only",
        dataset_root=str(walk_root),
        result_root=str(result_root) if result_root is not None else None,
        read_pattern=read_pattern,
        warmup_seconds=warmup_deadline_seconds,
        hotset_count=len(hotset),
    )
    summary.per_bucket_counts = dict.fromkeys(SIZE_BUCKETS, 0)
    summary.per_bucket_bytes = dict.fromkeys(SIZE_BUCKETS, 0)

    steady_latencies_ms: list[float] = []
    warmup_latencies_ms: list[float] = []
    hotset_latencies_ms: list[float] = []
    per_bucket_steady_ms: dict[str, list[float]] = {b: [] for b in SIZE_BUCKETS}
    per_bucket_hotset_ms: dict[str, list[float]] = {b: [] for b in SIZE_BUCKETS}
    started = time.monotonic()
    last_progress = started
    progress_interval = max(1, args.stats_interval_seconds)

    def _abort_with_summary(exit_code: int) -> None:
        summary.elapsed_seconds = time.monotonic() - started
        aggregate_summary(summary, steady_latencies_ms, per_bucket_steady_ms)
        summary.warmup_latency_ms = _summarize_simple(warmup_latencies_ms)
        summary.hotset_latency_ms = {
            "overall": _summarize_simple(hotset_latencies_ms),
            "per_bucket": {
                bucket: _summarize_simple(samples)
                for bucket, samples in per_bucket_hotset_ms.items()
                if samples
            },
        }
        thresholds = parse_slo_pairs(args.bucket_slo_p95_ms)
        if thresholds:
            summary.slo = evaluate_slo(per_bucket_steady_ms, thresholds)
        _print_summary(summary)
        raise SystemExit(exit_code)

    def _record(result: FileReadResult, *, is_hotset: bool) -> None:
        latency_ms = result.elapsed_seconds * 1000.0
        bucket = result.bucket
        summary.files_attempted += 1
        if result.error is not None:
            summary.files_failed += 1
            if len(summary.errors) < args.max_recorded_errors:
                summary.errors.append({"path": result.path, "error": result.error})
            if args.fail_on_read_error:
                _abort_with_summary(2)
            return
        summary.files_succeeded += 1
        summary.bytes_read += result.bytes_read
        summary.per_bucket_counts[bucket] = summary.per_bucket_counts.get(bucket, 0) + 1
        summary.per_bucket_bytes[bucket] = (
            summary.per_bucket_bytes.get(bucket, 0) + result.bytes_read
        )
        if is_hotset:
            hotset_latencies_ms.append(latency_ms)
            per_bucket_hotset_ms.setdefault(bucket, []).append(latency_ms)
            return
        now_rel = time.monotonic() - started
        if now_rel < warmup_deadline_seconds:
            warmup_latencies_ms.append(latency_ms)
        else:
            steady_latencies_ms.append(latency_ms)
            per_bucket_steady_ms.setdefault(bucket, []).append(latency_ms)

    def _maybe_write_output(result: FileReadResult) -> None:
        if not write_outputs or pod_output_dir is None or result.error is not None:
            return
        try:
            bytes_written = write_per_pod_output(
                pod_output_dir=pod_output_dir,
                file_result=result,
                output_bytes_per_input=args.output_bytes_per_input,
                max_output_bytes_per_file=args.max_output_bytes_per_file,
                dataset_root=dataset_resolved,
            )
            summary.bytes_written += bytes_written
        except (OSError, ValueError) as exc:
            summary.files_failed += 1
            if len(summary.errors) < args.max_recorded_errors:
                summary.errors.append({"path": result.path, "error": f"output: {exc}"})
            if args.fail_on_write_error:
                _abort_with_summary(3)

    def _maybe_progress() -> None:
        nonlocal last_progress
        now = time.monotonic()
        if now - last_progress >= progress_interval:
            last_progress = now
            _print_progress(summary, now - started)

    for epoch in range(epochs):
        epoch_seed = hash(f"{base_seed}-epoch-{epoch}")
        selected = select_files(
            shard,
            files_per_pod=args.files_per_pod,
            max_bytes_per_pod=args.max_bytes_per_pod,
            seed=epoch_seed,
        )
        for entry in selected:
            result = read_file_pattern(
                entry,
                chunk_size=args.chunk_size,
                verify_checksum=args.verify_reads and read_pattern == "full",
                pattern=read_pattern,
                random_offset_reads=args.random_offset_reads,
                rng=pattern_rng,
            )
            _record(result, is_hotset=False)
            _maybe_write_output(result)
            _maybe_progress()
        for entry in hotset:
            result = read_file_pattern(
                entry,
                chunk_size=args.chunk_size,
                verify_checksum=False,
                pattern=read_pattern,
                random_offset_reads=args.random_offset_reads,
                rng=pattern_rng,
            )
            _record(result, is_hotset=True)
            _maybe_progress()
        summary.epochs_completed = epoch + 1

    summary.elapsed_seconds = time.monotonic() - started
    aggregate_summary(summary, steady_latencies_ms, per_bucket_steady_ms)
    summary.warmup_latency_ms = _summarize_simple(warmup_latencies_ms)
    summary.hotset_latency_ms = {
        "overall": _summarize_simple(hotset_latencies_ms),
        "per_bucket": {
            bucket: _summarize_simple(samples)
            for bucket, samples in per_bucket_hotset_ms.items()
            if samples
        },
    }
    thresholds = parse_slo_pairs(args.bucket_slo_p95_ms)
    if thresholds:
        summary.slo = evaluate_slo(per_bucket_steady_ms, thresholds)
    _print_summary(summary)
    return summary


def run_verify(args: argparse.Namespace) -> WorkloadSummary:
    if not args.result_root or not args.run_id:
        raise ValueError("result-root and run-id are required for verify-output")
    if args.dataset_root:
        assert_disjoint_roots(Path(args.dataset_root), Path(args.result_root))
    result_root_resolved = Path(args.result_root).resolve()
    run_dir = (result_root_resolved / args.run_id).resolve()
    try:
        run_dir.relative_to(result_root_resolved)
    except ValueError as exc:
        raise ValueError("run-id must not contain path separators") from exc

    summary = WorkloadSummary(
        pod_name=args.pod_name,
        pod_index=args.pod_index,
        pod_count=args.pod_count,
        mode="verify-output",
        dataset_root=str(Path(args.dataset_root).resolve()) if args.dataset_root else "",
        result_root=str(run_dir),
    )
    started = time.monotonic()
    for json_path in sorted(run_dir.rglob("*.json")):
        ensure_within(run_dir, json_path)
        summary.files_attempted += 1
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            summary.files_failed += 1
            if len(summary.errors) < args.max_recorded_errors:
                summary.errors.append({"path": str(json_path), "error": str(exc)})
            continue
        if not isinstance(payload, dict) or "source_path" not in payload:
            summary.files_failed += 1
            if len(summary.errors) < args.max_recorded_errors:
                summary.errors.append({"path": str(json_path), "error": "missing source_path"})
            continue
        summary.files_succeeded += 1
    summary.elapsed_seconds = time.monotonic() - started
    _print_summary(summary)
    return summary


def _print_progress(summary: WorkloadSummary, elapsed: float) -> None:
    payload = {
        "event": "progress",
        "pod": summary.pod_name,
        "pod_index": summary.pod_index,
        "mode": summary.mode,
        "files_attempted": summary.files_attempted,
        "files_succeeded": summary.files_succeeded,
        "files_failed": summary.files_failed,
        "bytes_read": summary.bytes_read,
        "bytes_written": summary.bytes_written,
        "elapsed_seconds": round(elapsed, 3),
    }
    print(json.dumps(payload, sort_keys=True), flush=True)


def _print_summary(summary: WorkloadSummary) -> None:
    payload = {
        "event": "summary",
        "pod": summary.pod_name,
        "pod_index": summary.pod_index,
        "pod_count": summary.pod_count,
        "mode": summary.mode,
        "read_pattern": summary.read_pattern,
        "epochs_completed": summary.epochs_completed,
        "warmup_seconds": summary.warmup_seconds,
        "hotset_count": summary.hotset_count,
        "dataset_root": summary.dataset_root,
        "result_root": summary.result_root,
        "files_attempted": summary.files_attempted,
        "files_succeeded": summary.files_succeeded,
        "files_failed": summary.files_failed,
        "bytes_read": summary.bytes_read,
        "bytes_written": summary.bytes_written,
        "elapsed_seconds": round(summary.elapsed_seconds, 3),
        "throughput_mib_s": round(
            summary.bytes_read / (1024**2 * max(summary.elapsed_seconds, 0.001)),
            3,
        ),
        "per_bucket_counts": summary.per_bucket_counts,
        "per_bucket_bytes": summary.per_bucket_bytes,
        "per_file_latency_ms": summary.per_file_latency_ms,
        "warmup_latency_ms": summary.warmup_latency_ms,
        "hotset_latency_ms": summary.hotset_latency_ms,
        "slo": summary.slo,
        "errors": summary.errors,
    }
    print(json.dumps(payload, indent=2, sort_keys=True), flush=True)


def _env_default(name: str, fallback: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None or value == "":
        return fallback
    return value


def _resolve_pod_identity(args: argparse.Namespace) -> None:
    if not args.pod_name:
        args.pod_name = _env_default("POD_NAME") or socket.gethostname()
    if args.pod_index is None:
        idx = _env_default("JOB_COMPLETION_INDEX") or _env_default("POD_INDEX")
        args.pod_index = int(idx) if idx is not None else 0
    if args.pod_count is None:
        count = _env_default("POD_COUNT")
        args.pod_count = int(count) if count is not None else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Autonomous-vehicle dataset workload simulator for Lustre."
    )
    parser.add_argument(
        "--mode",
        choices=("discover", "read-only", "read-write-output", "verify-output"),
        default=_env_default("MODE", "discover"),
        help="Workload mode. Defaults to env MODE or 'discover'.",
    )
    parser.add_argument(
        "--dataset-root",
        default=_env_default("DATASET_ROOT", "/mnt/lustre"),
        help="Lustre directory containing the AV dataset.",
    )
    parser.add_argument(
        "--result-root",
        default=_env_default("RESULT_ROOT", "/mnt/lustre/pressure-tests/av-results"),
        help="Lustre directory under which per-run outputs are written.",
    )
    parser.add_argument(
        "--run-id",
        default=_env_default("RUN_ID"),
        help="Run identifier. Must be a single path segment.",
    )
    parser.add_argument(
        "--pod-name",
        default=_env_default("POD_NAME"),
        help="Pod name used for output isolation.",
    )
    parser.add_argument(
        "--pod-index",
        type=int,
        default=None,
        help="0-based pod index. Defaults to JOB_COMPLETION_INDEX env var.",
    )
    parser.add_argument(
        "--pod-count",
        type=int,
        default=None,
        help="Total number of pods in the Job. Defaults to POD_COUNT env var.",
    )
    parser.add_argument(
        "--files-per-pod",
        type=int,
        default=(int(_env_default("FILES_PER_POD", "0") or 0) or None),
        help="Maximum files this pod should process. 0/unset means no limit.",
    )
    parser.add_argument(
        "--max-bytes-per-pod",
        type=lambda v: parse_size(v) if v else None,
        default=parse_size(_env_default("MAX_BYTES_PER_POD", "0") or "0") or None,
        help="Maximum bytes this pod should read. 0/unset means no limit.",
    )
    parser.add_argument(
        "--chunk-size",
        type=parse_size,
        default=parse_size(_env_default("CHUNK_SIZE_BYTES", str(DEFAULT_CHUNK_SIZE_BYTES))),
        help="Chunk size used to stream large files.",
    )
    parser.add_argument(
        "--small-max",
        type=parse_size,
        default=parse_size(_env_default("SMALL_MAX_BYTES", str(DEFAULT_SMALL_MAX_BYTES))),
        help="Upper bound for the small size bucket.",
    )
    parser.add_argument(
        "--medium-max",
        type=parse_size,
        default=parse_size(_env_default("MEDIUM_MAX_BYTES", str(DEFAULT_MEDIUM_MAX_BYTES))),
        help="Upper bound for the medium size bucket.",
    )
    parser.add_argument(
        "--large-max",
        type=parse_size,
        default=parse_size(_env_default("LARGE_MAX_BYTES", str(DEFAULT_LARGE_MAX_BYTES))),
        help="Upper bound for the large size bucket.",
    )
    parser.add_argument(
        "--output-bytes-per-input",
        type=float,
        default=float(_env_default("OUTPUT_BYTES_PER_INPUT", str(DEFAULT_OUTPUT_BYTES_PER_INPUT))),
        help="Synthetic output bytes written per input byte (0 disables payloads).",
    )
    parser.add_argument(
        "--max-output-bytes-per-file",
        type=parse_size,
        default=parse_size(_env_default("MAX_OUTPUT_BYTES_PER_FILE", "1MiB")),
        help="Cap on synthetic output bytes per input file.",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=_split_env_list("EXCLUDE_PATHS"),
        help="Directory or file path to exclude during enumeration.",
    )
    parser.add_argument(
        "--subpath",
        default=_env_default("SUBPATH", ""),
        help=(
            "Relative path under DATASET_ROOT that becomes the walk root for "
            "this phase (e.g. 'training/camera_image'). Must not contain '..' "
            "or start with '/'. Empty means walk DATASET_ROOT directly."
        ),
    )
    parser.add_argument(
        "--split-filter",
        default=_env_default("SPLIT_FILTER", ""),
        help=(
            "Comma-separated list of first-level directory names under the walk "
            "root to descend into (e.g. 'training,validation'). Empty means no "
            "filter."
        ),
    )
    parser.add_argument(
        "--verify-reads",
        action=argparse.BooleanOptionalAction,
        default=_env_flag("VERIFY_READS", default=True),
        help="Compute SHA-256 for each file during read.",
    )
    parser.add_argument(
        "--fail-on-read-error",
        action=argparse.BooleanOptionalAction,
        default=_env_flag("FAIL_ON_READ_ERROR", default=True),
        help="Exit non-zero on the first read error.",
    )
    parser.add_argument(
        "--fail-on-write-error",
        action=argparse.BooleanOptionalAction,
        default=_env_flag("FAIL_ON_WRITE_ERROR", default=True),
        help="Exit non-zero on the first output write error.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=(int(_env_default("SEED", "0") or 0) or None),
        help="Deterministic shuffle seed within the pod's shard.",
    )
    parser.add_argument(
        "--stats-interval-seconds",
        type=int,
        default=int(_env_default("STATS_INTERVAL_SECONDS", "30") or "30"),
        help="Seconds between progress JSON lines.",
    )
    parser.add_argument(
        "--max-recorded-errors",
        type=int,
        default=int(_env_default("MAX_RECORDED_ERRORS", "20") or "20"),
        help="Maximum number of error entries kept in the summary.",
    )
    parser.add_argument(
        "--read-pattern",
        choices=READ_PATTERNS,
        default=_env_default("READ_PATTERN", "full"),
        help="How each file is read: full stream, random offsets, or head+tail.",
    )
    parser.add_argument(
        "--random-offset-reads",
        type=int,
        default=int(
            _env_default("RANDOM_OFFSET_READS", str(DEFAULT_RANDOM_OFFSET_READS))
            or DEFAULT_RANDOM_OFFSET_READS
        ),
        help="Random-offset chunk reads per file when --read-pattern=random-offset.",
    )
    parser.add_argument(
        "--epochs",
        type=int,
        default=int(_env_default("EPOCHS", str(DEFAULT_EPOCHS)) or DEFAULT_EPOCHS),
        help="Number of times to traverse the shard. Reshuffled deterministically per epoch.",
    )
    parser.add_argument(
        "--warmup-seconds",
        type=float,
        default=float(
            _env_default("WARMUP_SECONDS", str(DEFAULT_WARMUP_SECONDS))
            or DEFAULT_WARMUP_SECONDS
        ),
        help="Drop latency samples collected within this many seconds from the start.",
    )
    parser.add_argument(
        "--hotset-count",
        type=int,
        default=int(
            _env_default("HOTSET_COUNT", str(DEFAULT_HOTSET_COUNT))
            or DEFAULT_HOTSET_COUNT
        ),
        help="Number of top-K largest dataset files each pod re-reads per epoch.",
    )
    parser.add_argument(
        "--bucket-slo-p95-ms",
        default=_env_default("BUCKET_SLO_P95_MS"),
        help="Per-bucket p95 latency SLO in ms, e.g. 'small=20,medium=200,large=2000'.",
    )
    parser.add_argument(
        "--fail-on-slo",
        action=argparse.BooleanOptionalAction,
        default=_env_flag("FAIL_ON_SLO", default=False),
        help="Exit non-zero when any per-bucket p95 exceeds its threshold.",
    )
    return parser


def _split_env_list(name: str) -> list[str]:
    value = os.environ.get(name)
    if not value:
        return []
    return [item for item in value.split(":") if item]


def _env_flag(name: str, *, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _resolve_pod_identity(args)

    if args.mode == "discover":
        run_discover(args)
        return 0
    if args.mode in ("read-only", "read-write-output"):
        write_outputs = args.mode == "read-write-output"
        summary = run_read(args, write_outputs=write_outputs)
        if summary.files_failed and (
            args.fail_on_read_error
            or (write_outputs and args.fail_on_write_error)
        ):
            return 1
        if args.fail_on_slo and summary.slo and summary.slo.get("pass") is False:
            return 4
        return 0
    if args.mode == "verify-output":
        summary = run_verify(args)
        return 1 if summary.files_failed else 0
    raise SystemExit(f"unknown mode: {args.mode}")


if __name__ == "__main__":
    raise SystemExit(main())
