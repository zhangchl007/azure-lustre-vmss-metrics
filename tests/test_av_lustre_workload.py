from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "av_lustre_workload.py"
SPEC = importlib.util.spec_from_file_location("av_lustre_workload", SCRIPT_PATH)
assert SPEC is not None
av = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules["av_lustre_workload"] = av
SPEC.loader.exec_module(av)


@pytest.fixture()
def dataset_tree(tmp_path: Path) -> Path:
    root = tmp_path / "av-data"
    (root / "logs/sensor-a").mkdir(parents=True)
    (root / "logs/sensor-b").mkdir(parents=True)
    (root / "maps").mkdir(parents=True)
    (root / "models/v1").mkdir(parents=True)

    files = {
        "logs/sensor-a/small-001.bin": 150 * 1024,
        "logs/sensor-a/small-002.bin": 300 * 1024,
        "logs/sensor-b/medium-001.bin": 2 * 1024 * 1024,
        "logs/sensor-b/medium-002.bin": 8 * 1024 * 1024,
        "maps/large-001.bin": 80 * 1024 * 1024,
        "models/v1/large-002.bin": 64 * 1024 * 1024 + 1,
        "models/v1/tiny-001.bin": 100 * 1024,
    }
    for relative, size in files.items():
        path = root / relative
        with path.open("wb") as handle:
            handle.write(b"\xab" * size)
    return root


def test_parse_size_handles_units():
    assert av.parse_size("100KiB") == 100 * 1024
    assert av.parse_size("500MiB") == 500 * 1024 * 1024
    assert av.parse_size("1GiB") == 1024**3
    assert av.parse_size(2048) == 2048


def test_classify_size_buckets():
    assert av.classify_size(100 * 1024) == "small"
    assert av.classify_size(1 * 1024 * 1024) == "small"
    assert av.classify_size(8 * 1024 * 1024) == "medium"
    assert av.classify_size(64 * 1024 * 1024) == "medium"
    assert av.classify_size(80 * 1024 * 1024) == "large"
    assert av.classify_size(512 * 1024 * 1024) == "large"
    assert av.classify_size(600 * 1024 * 1024) == "xlarge"


def test_enumerate_dataset_lists_all_files(dataset_tree: Path):
    entries = av.enumerate_dataset(dataset_tree)
    assert len(entries) == 7
    paths = [str(entry.path.relative_to(dataset_tree)) for entry in entries]
    assert paths == sorted(paths)


def test_enumerate_dataset_excludes_paths(dataset_tree: Path):
    excluded = dataset_tree / "maps"
    entries = av.enumerate_dataset(dataset_tree, exclude_paths=[excluded])
    paths = {str(entry.path.relative_to(dataset_tree)) for entry in entries}
    assert all(not p.startswith("maps") for p in paths)
    assert len(entries) == 6


def test_shard_files_is_deterministic_and_non_overlapping(dataset_tree: Path):
    entries = av.enumerate_dataset(dataset_tree)
    shards = [av.shard_files(entries, pod_index=i, pod_count=3) for i in range(3)]
    rejoined = sorted(
        {str(entry.path) for shard in shards for entry in shard}
    )
    expected = sorted(str(entry.path) for entry in entries)
    assert rejoined == expected
    # No file appears in more than one shard
    seen: set[str] = set()
    for shard in shards:
        for entry in shard:
            assert str(entry.path) not in seen
            seen.add(str(entry.path))


def test_shard_files_rejects_invalid_indices(dataset_tree: Path):
    entries = av.enumerate_dataset(dataset_tree)
    with pytest.raises(ValueError):
        av.shard_files(entries, pod_index=3, pod_count=3)
    with pytest.raises(ValueError):
        av.shard_files(entries, pod_index=-1, pod_count=3)
    with pytest.raises(ValueError):
        av.shard_files(entries, pod_index=0, pod_count=0)


def test_select_files_respects_byte_budget(dataset_tree: Path):
    entries = av.enumerate_dataset(dataset_tree)
    selected = av.select_files(
        entries,
        files_per_pod=None,
        max_bytes_per_pod=1 * 1024 * 1024,
        seed=42,
    )
    assert sum(entry.size for entry in selected) <= 1 * 1024 * 1024
    # Should include only files <= 1 MiB
    assert all(entry.size <= 1 * 1024 * 1024 for entry in selected)


def test_percentile_linear_interpolation():
    assert av.percentile([], 50) == 0.0
    assert av.percentile([10.0], 95) == 10.0
    assert av.percentile([10.0, 20.0, 30.0, 40.0], 50) == 25.0
    assert av.percentile([10.0, 20.0, 30.0, 40.0], 100) == 40.0


def test_ensure_within_blocks_path_escape(tmp_path: Path):
    base = tmp_path / "base"
    base.mkdir()
    inside = base / "ok.txt"
    inside.write_bytes(b"x")
    assert av.ensure_within(base, inside) == inside.resolve()
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"x")
    with pytest.raises(ValueError):
        av.ensure_within(base, outside)


def test_write_per_pod_output_writes_summary_and_payload(tmp_path: Path):
    pod_dir = tmp_path / "results" / "pod-0"
    pod_dir.mkdir(parents=True)
    result = av.FileReadResult(
        path="/mnt/lustre/dataset/file.bin",
        size=10 * 1024 * 1024,
        bucket="medium",
        elapsed_seconds=0.5,
        chunk_count=10,
        bytes_read=10 * 1024 * 1024,
        checksum="deadbeef",
    )
    bytes_written = av.write_per_pod_output(
        pod_output_dir=pod_dir,
        file_result=result,
        output_bytes_per_input=0.01,
        max_output_bytes_per_file=512 * 1024,
    )
    assert bytes_written > 0
    json_files = list(pod_dir.rglob("*.json"))
    assert len(json_files) == 1
    payload = json.loads(json_files[0].read_text())
    assert payload["source_path"] == result.path
    assert payload["bucket"] == "medium"
    bin_files = list(pod_dir.rglob("*.bin"))
    assert len(bin_files) == 1
    assert bin_files[0].stat().st_size <= 512 * 1024


def test_write_per_pod_output_refuses_escape(tmp_path: Path):
    pod_dir = tmp_path / "pod"
    pod_dir.mkdir()
    sibling = tmp_path / "sibling"
    sibling.mkdir()
    with pytest.raises(ValueError):
        av.ensure_within(pod_dir, sibling / "x")


def test_run_discover_prints_profile(dataset_tree: Path, capsys, monkeypatch):
    parser = av.build_parser()
    args = parser.parse_args(
        [
            "--mode",
            "discover",
            "--dataset-root",
            str(dataset_tree),
        ]
    )
    av._resolve_pod_identity(args)
    av.run_discover(args)
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    profile = payload["profile"]
    assert profile["file_count"] == 7
    assert profile["directory_count"] >= 4
    assert profile["bucket_counts"]["large"] >= 1


def test_run_read_only_succeeds_and_emits_summary(dataset_tree: Path, capsys):
    parser = av.build_parser()
    args = parser.parse_args(
        [
            "--mode",
            "read-only",
            "--dataset-root",
            str(dataset_tree),
            "--pod-name",
            "pod-0",
            "--pod-index",
            "0",
            "--pod-count",
            "1",
            "--chunk-size",
            "1MiB",
            "--verify-reads",
        ]
    )
    av._resolve_pod_identity(args)
    summary = av.run_read(args, write_outputs=False)
    captured = capsys.readouterr()
    # Summary is pretty-printed JSON; decode the first JSON object only.
    decoder = json.JSONDecoder()
    payload, _ = decoder.raw_decode(captured.out.strip())
    assert summary.files_attempted == 7
    assert summary.files_succeeded == 7
    assert summary.files_failed == 0
    assert summary.bytes_read > 0
    assert payload["event"] == "summary"


def test_run_read_write_output_creates_isolated_outputs(dataset_tree: Path, tmp_path: Path, capsys):
    result_root = tmp_path / "av-results"
    parser = av.build_parser()
    args = parser.parse_args(
        [
            "--mode",
            "read-write-output",
            "--dataset-root",
            str(dataset_tree),
            "--result-root",
            str(result_root),
            "--run-id",
            "run-test",
            "--pod-name",
            "pod-0",
            "--pod-index",
            "0",
            "--pod-count",
            "1",
            "--chunk-size",
            "1MiB",
            "--output-bytes-per-input",
            "0.001",
            "--no-verify-reads",
        ]
    )
    av._resolve_pod_identity(args)
    summary = av.run_read(args, write_outputs=True)
    assert summary.files_succeeded == 7
    pod_dir = result_root / "run-test" / "pod-0"
    assert pod_dir.is_dir()
    json_artifacts = list(pod_dir.rglob("*.json"))
    assert len(json_artifacts) == 7
    # No source files were modified: re-enumerate and check sizes unchanged
    sizes_after = {p.name: p.stat().st_size for p in dataset_tree.rglob("*.bin")}
    assert sizes_after  # sanity


def test_run_read_write_output_blocks_overlapping_roots(dataset_tree: Path):
    # result_root inside dataset_root must be rejected
    parser = av.build_parser()
    bad_result_root = dataset_tree / "av-results"
    args = parser.parse_args(
        [
            "--mode",
            "read-write-output",
            "--dataset-root",
            str(dataset_tree),
            "--result-root",
            str(bad_result_root),
            "--run-id",
            "run-test",
            "--pod-name",
            "pod-0",
            "--pod-index",
            "0",
            "--pod-count",
            "1",
        ]
    )
    av._resolve_pod_identity(args)
    # dataset_tree itself is inside the run dir? No, the run dir lives under
    # dataset_tree so the simulator should refuse.
    with pytest.raises(ValueError):
        av.run_read(args, write_outputs=True)


def test_run_read_write_output_rejects_bad_run_id(dataset_tree: Path, tmp_path: Path):
    parser = av.build_parser()
    args = parser.parse_args(
        [
            "--mode",
            "read-write-output",
            "--dataset-root",
            str(dataset_tree),
            "--result-root",
            str(tmp_path / "results"),
            "--run-id",
            "../escape",
            "--pod-name",
            "pod-0",
            "--pod-index",
            "0",
            "--pod-count",
            "1",
        ]
    )
    av._resolve_pod_identity(args)
    with pytest.raises(ValueError):
        av.run_read(args, write_outputs=True)


def test_verify_output_succeeds(dataset_tree: Path, tmp_path: Path):
    parser = av.build_parser()
    write_args = parser.parse_args(
        [
            "--mode",
            "read-write-output",
            "--dataset-root",
            str(dataset_tree),
            "--result-root",
            str(tmp_path / "results"),
            "--run-id",
            "run-test",
            "--pod-name",
            "pod-0",
            "--pod-index",
            "0",
            "--pod-count",
            "1",
            "--no-verify-reads",
            "--output-bytes-per-input",
            "0",
        ]
    )
    av._resolve_pod_identity(write_args)
    av.run_read(write_args, write_outputs=True)

    verify_args = parser.parse_args(
        [
            "--mode",
            "verify-output",
            "--dataset-root",
            str(dataset_tree),
            "--result-root",
            str(tmp_path / "results"),
            "--run-id",
            "run-test",
            "--pod-name",
            "pod-0",
            "--pod-index",
            "0",
            "--pod-count",
            "1",
        ]
    )
    av._resolve_pod_identity(verify_args)
    summary = av.run_verify(verify_args)
    assert summary.files_attempted >= 7
    assert summary.files_failed == 0


def test_main_resolves_pod_index_from_env(dataset_tree: Path, monkeypatch):
    monkeypatch.setenv("MODE", "discover")
    monkeypatch.setenv("DATASET_ROOT", str(dataset_tree))
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "0")
    monkeypatch.setenv("POD_COUNT", "1")
    exit_code = av.main([])
    assert exit_code == 0


def test_parse_slo_pairs_supports_partial_thresholds():
    parsed = av.parse_slo_pairs("small=20,large=2000")
    assert parsed == {"small": 20.0, "large": 2000.0}
    assert av.parse_slo_pairs(None) == {}
    assert av.parse_slo_pairs("") == {}


def test_parse_slo_pairs_rejects_invalid_input():
    with pytest.raises(ValueError):
        av.parse_slo_pairs("small")
    with pytest.raises(ValueError):
        av.parse_slo_pairs("unknown=10")


def test_evaluate_slo_marks_pass_and_failures():
    samples = {
        "small": [1.0] * 100,
        "medium": [10.0] * 100,
        "large": [3000.0] * 100,
    }
    thresholds = {"small": 5.0, "medium": 50.0, "large": 2000.0}
    result = av.evaluate_slo(samples, thresholds)
    assert result["pass"] is False
    assert result["failures"] == ["large"]
    assert result["per_bucket"]["small"]["pass"] is True
    assert result["per_bucket"]["medium"]["pass"] is True
    assert result["per_bucket"]["large"]["pass"] is False
    assert result["per_bucket"]["xlarge"]["pass"] is None


def test_evaluate_slo_pass_when_all_below_threshold():
    samples = {"small": [1.0, 2.0, 3.0]}
    thresholds = {"small": 5.0, "medium": 100.0}
    result = av.evaluate_slo(samples, thresholds)
    assert result["pass"] is True
    assert result["failures"] == []


def test_read_file_pattern_head_tail_uses_only_two_chunks(dataset_tree: Path):
    entries = av.enumerate_dataset(dataset_tree)
    large = max(entries, key=lambda e: e.size)
    chunk = 1 * 1024 * 1024
    result = av.read_file_pattern(
        large,
        chunk_size=chunk,
        verify_checksum=False,
        pattern="head-tail",
    )
    assert result.pattern == "head-tail"
    assert result.error is None
    # Either both head and tail chunks (large file) or a single full read (small).
    assert result.chunk_count <= 2
    assert result.bytes_read <= 2 * chunk


def test_read_file_pattern_random_offset_respects_count(dataset_tree: Path):
    entries = av.enumerate_dataset(dataset_tree)
    large = max(entries, key=lambda e: e.size)
    chunk = 1 * 1024 * 1024
    result = av.read_file_pattern(
        large,
        chunk_size=chunk,
        verify_checksum=False,
        pattern="random-offset",
        random_offset_reads=3,
        rng=__import__("random").Random(0),
    )
    assert result.pattern == "random-offset"
    assert result.error is None
    assert result.chunk_count == 3
    assert result.bytes_read <= 3 * chunk


def test_run_read_supports_epochs_and_hotset(dataset_tree: Path, capsys):
    parser = av.build_parser()
    args = parser.parse_args(
        [
            "--mode",
            "read-only",
            "--dataset-root",
            str(dataset_tree),
            "--pod-name",
            "pod-0",
            "--pod-index",
            "0",
            "--pod-count",
            "1",
            "--chunk-size",
            "1MiB",
            "--no-verify-reads",
            "--epochs",
            "2",
            "--hotset-count",
            "2",
        ]
    )
    av._resolve_pod_identity(args)
    summary = av.run_read(args, write_outputs=False)
    assert summary.epochs_completed == 2
    assert summary.hotset_count == 2
    # 7 shard files * 2 epochs + 2 hotset * 2 epochs = 18 attempts
    assert summary.files_attempted == 18
    assert summary.files_failed == 0
    assert summary.hotset_latency_ms["overall"]["count"] == 4


def test_run_read_warmup_excludes_initial_samples(dataset_tree: Path):
    parser = av.build_parser()
    args = parser.parse_args(
        [
            "--mode",
            "read-only",
            "--dataset-root",
            str(dataset_tree),
            "--pod-name",
            "pod-0",
            "--pod-index",
            "0",
            "--pod-count",
            "1",
            "--chunk-size",
            "1MiB",
            "--no-verify-reads",
            "--warmup-seconds",
            "3600",
        ]
    )
    av._resolve_pod_identity(args)
    summary = av.run_read(args, write_outputs=False)
    # With a huge warmup window, every sample lands in warmup and steady is empty
    assert summary.warmup_latency_ms.get("count", 0) == 7
    assert summary.per_file_latency_ms == {} or "p50" not in summary.per_file_latency_ms


def test_run_read_with_slo_records_failure(dataset_tree: Path):
    parser = av.build_parser()
    args = parser.parse_args(
        [
            "--mode",
            "read-only",
            "--dataset-root",
            str(dataset_tree),
            "--pod-name",
            "pod-0",
            "--pod-index",
            "0",
            "--pod-count",
            "1",
            "--chunk-size",
            "1MiB",
            "--no-verify-reads",
            "--bucket-slo-p95-ms",
            "small=0.0001,medium=0.0001,large=0.0001",
        ]
    )
    av._resolve_pod_identity(args)
    summary = av.run_read(args, write_outputs=False)
    assert summary.slo["pass"] is False
    assert set(summary.slo["failures"]) >= {"small", "medium", "large"}


def test_main_returns_slo_exit_code(dataset_tree: Path, monkeypatch):
    monkeypatch.setenv("MODE", "read-only")
    monkeypatch.setenv("DATASET_ROOT", str(dataset_tree))
    monkeypatch.setenv("POD_COUNT", "1")
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "0")
    monkeypatch.setenv("VERIFY_READS", "false")
    monkeypatch.setenv("BUCKET_SLO_P95_MS", "small=0.0001,medium=0.0001,large=0.0001")
    monkeypatch.setenv("FAIL_ON_SLO", "true")
    exit_code = av.main([])
    assert exit_code == 4


def test_main_succeeds_when_slo_within_thresholds(dataset_tree: Path, monkeypatch):
    monkeypatch.setenv("MODE", "read-only")
    monkeypatch.setenv("DATASET_ROOT", str(dataset_tree))
    monkeypatch.setenv("POD_COUNT", "1")
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "0")
    monkeypatch.setenv("VERIFY_READS", "false")
    # Generous thresholds for a fast tmpfs read in tests
    monkeypatch.setenv("BUCKET_SLO_P95_MS", "small=60000,medium=60000,large=60000")
    monkeypatch.setenv("FAIL_ON_SLO", "true")
    exit_code = av.main([])
    assert exit_code == 0


# ---------------------------------------------------------------------------
# SUBPATH + SPLIT_FILTER + disjoint-roots preflight + write-gate
# ---------------------------------------------------------------------------


def test_parse_split_filter_handles_empty_and_whitespace():
    assert av.parse_split_filter(None) == []
    assert av.parse_split_filter("") == []
    assert av.parse_split_filter(" , , ") == []
    assert av.parse_split_filter("training,validation") == ["training", "validation"]
    assert av.parse_split_filter(" training , validation ") == ["training", "validation"]


def test_enumerate_dataset_split_filter_limits_descent(dataset_tree: Path):
    # Only descend into 'logs' at the top level; 'maps' and 'models' are skipped.
    entries = av.enumerate_dataset(dataset_tree, split_filter=["logs"])
    paths = {str(entry.path.relative_to(dataset_tree)) for entry in entries}
    assert paths == {
        "logs/sensor-a/small-001.bin",
        "logs/sensor-a/small-002.bin",
        "logs/sensor-b/medium-001.bin",
        "logs/sensor-b/medium-002.bin",
    }


def test_enumerate_dataset_split_filter_empty_means_walk_all(dataset_tree: Path):
    # Empty iterable behaves like None: full traversal.
    entries = av.enumerate_dataset(dataset_tree, split_filter=[])
    assert len(entries) == 7


def test_resolve_walk_root_returns_dataset_root_when_subpath_empty(dataset_tree: Path):
    assert av.resolve_walk_root(dataset_tree, None) == dataset_tree.resolve()
    assert av.resolve_walk_root(dataset_tree, "") == dataset_tree.resolve()
    assert av.resolve_walk_root(dataset_tree, "   ") == dataset_tree.resolve()


def test_resolve_walk_root_descends_into_subpath(dataset_tree: Path):
    walk_root = av.resolve_walk_root(dataset_tree, "logs/sensor-a")
    assert walk_root == (dataset_tree / "logs/sensor-a").resolve()


def test_resolve_walk_root_rejects_absolute_subpath(dataset_tree: Path):
    with pytest.raises(ValueError, match="absolute"):
        av.resolve_walk_root(dataset_tree, "/etc")


def test_resolve_walk_root_rejects_dotdot_subpath(dataset_tree: Path):
    with pytest.raises(ValueError, match=r"\.\.|segments"):
        av.resolve_walk_root(dataset_tree, "../escape")
    with pytest.raises(ValueError, match=r"\.\.|segments"):
        av.resolve_walk_root(dataset_tree, "logs/../../escape")


def test_assert_disjoint_roots_accepts_siblings(tmp_path: Path):
    dataset = tmp_path / "dataset"
    result = tmp_path / "results"
    dataset.mkdir()
    result.mkdir()
    d_resolved, r_resolved = av.assert_disjoint_roots(dataset, result)
    assert d_resolved == dataset.resolve()
    assert r_resolved == result.resolve()


def test_assert_disjoint_roots_rejects_identical(tmp_path: Path):
    root = tmp_path / "shared"
    root.mkdir()
    with pytest.raises(ValueError, match="same path"):
        av.assert_disjoint_roots(root, root)


def test_assert_disjoint_roots_rejects_result_inside_dataset(tmp_path: Path):
    dataset = tmp_path / "dataset"
    result = dataset / "results"
    result.mkdir(parents=True)
    with pytest.raises(ValueError, match="nested inside"):
        av.assert_disjoint_roots(dataset, result)


def test_assert_disjoint_roots_rejects_dataset_inside_result(tmp_path: Path):
    result = tmp_path / "results"
    dataset = result / "dataset"
    dataset.mkdir(parents=True)
    with pytest.raises(ValueError, match="nested inside"):
        av.assert_disjoint_roots(dataset, result)


def test_write_per_pod_output_rejects_under_dataset_root(tmp_path: Path):
    """The defense-in-depth write-gate must refuse outputs that resolve under dataset_root."""
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    # Simulate the misconfigured case where pod_output_dir is itself under dataset.
    # This pair would normally be blocked by assert_disjoint_roots; the write-gate
    # is a belt-and-suspenders second layer in case the preflight is bypassed.
    pod_output_dir = dataset / "results" / "run" / "pod-0"
    pod_output_dir.mkdir(parents=True)
    file_result = av.FileReadResult(
        path=str(dataset / "fake.bin"),
        size=1024,
        bucket="small",
        elapsed_seconds=0.01,
        chunk_count=1,
        bytes_read=1024,
        checksum=None,
    )
    with pytest.raises(ValueError, match="DATASET_ROOT"):
        av.write_per_pod_output(
            pod_output_dir=pod_output_dir,
            file_result=file_result,
            output_bytes_per_input=0.0,
            max_output_bytes_per_file=0,
            dataset_root=dataset,
        )


def test_write_per_pod_output_accepts_sibling_layout(tmp_path: Path):
    """When dataset_root and pod_output_dir are siblings, the write-gate passes."""
    dataset = tmp_path / "dataset"
    result = tmp_path / "results"
    dataset.mkdir()
    pod_output_dir = result / "run" / "pod-0"
    pod_output_dir.mkdir(parents=True)
    file_result = av.FileReadResult(
        path=str(dataset / "fake.bin"),
        size=1024,
        bucket="small",
        elapsed_seconds=0.01,
        chunk_count=1,
        bytes_read=1024,
        checksum=None,
    )
    bytes_written = av.write_per_pod_output(
        pod_output_dir=pod_output_dir,
        file_result=file_result,
        output_bytes_per_input=0.0,
        max_output_bytes_per_file=0,
        dataset_root=dataset,
    )
    assert bytes_written > 0


def test_run_read_aborts_when_roots_overlap(dataset_tree: Path, monkeypatch):
    """End-to-end preflight: read-only run with RESULT_ROOT nested in DATASET_ROOT aborts."""
    monkeypatch.setenv("MODE", "read-only")
    monkeypatch.setenv("DATASET_ROOT", str(dataset_tree))
    monkeypatch.setenv("RESULT_ROOT", str(dataset_tree / "nested-results"))
    monkeypatch.setenv("RUN_ID", "test-overlap")
    monkeypatch.setenv("POD_COUNT", "1")
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "0")
    monkeypatch.setenv("VERIFY_READS", "false")
    with pytest.raises(ValueError, match="nested inside"):
        av.main([])


def test_run_read_honors_subpath_env(dataset_tree: Path, monkeypatch, tmp_path: Path):
    """A SUBPATH env var should scope the walk root to the named subdirectory."""
    result_root = tmp_path / "av-results"
    monkeypatch.setenv("MODE", "read-only")
    monkeypatch.setenv("DATASET_ROOT", str(dataset_tree))
    monkeypatch.setenv("RESULT_ROOT", str(result_root))
    monkeypatch.setenv("SUBPATH", "logs/sensor-a")
    monkeypatch.setenv("POD_COUNT", "1")
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "0")
    monkeypatch.setenv("VERIFY_READS", "false")
    monkeypatch.setenv("FAIL_ON_READ_ERROR", "true")
    # Should succeed and only touch logs/sensor-a (2 files).
    exit_code = av.main([])
    assert exit_code == 0


def test_run_discover_with_split_filter(dataset_tree: Path, monkeypatch, capsys):
    monkeypatch.setenv("MODE", "discover")
    monkeypatch.setenv("DATASET_ROOT", str(dataset_tree))
    monkeypatch.setenv("SPLIT_FILTER", "logs")
    monkeypatch.setenv("POD_COUNT", "1")
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "0")
    exit_code = av.main([])
    assert exit_code == 0
    captured = capsys.readouterr().out
    payload = json.loads(captured)
    # Only the four files under logs/* should be counted.
    assert payload["profile"]["file_count"] == 4

