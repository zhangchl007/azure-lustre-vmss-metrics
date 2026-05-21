from __future__ import annotations

from prometheus_client import CollectorRegistry, generate_latest

from vmss_metrics_exporter.collector import VmssMetricsExporter
from vmss_metrics_exporter.models import (
    ManagedLustreCollectionResult,
    ManagedLustreFilesystem,
    ManagedLustreFilesystemAggregateMetric,
    ManagedLustreMdtMetric,
    ManagedLustreMdtOperationMetric,
    ManagedLustreOstMetric,
    ManagedLustreOstOperationMetric,
    VmssCount,
)


def test_collect_once_sets_metrics_and_removes_stale_series() -> None:
    registry = CollectorRegistry()
    first = [
        VmssCount(
            "sub-a", "rg-a", "vmss-a", "eastus", "Uniform", 3, 5,
            vm_size="Standard_D2s_v3", sku_tier="Standard",
        ),
        VmssCount(
            "sub-a", "rg-a", "vmss-b", "eastus", "Flexible", 1, 2,
            vm_size="Standard_D4s_v5", sku_tier="Standard",
        ),
    ]
    second = [
        VmssCount(
            "sub-a", "rg-a", "vmss-a", "eastus", "Uniform", 4, 6,
            vm_size="Standard_D8s_v5", sku_tier="Standard",
        ),
    ]
    calls = iter([first, second])
    exporter = VmssMetricsExporter(lambda: next(calls), registry=registry)

    exporter.collect_once()
    exporter.collect_once()

    metrics = generate_latest(registry).decode()
    expected_labels = (
        'location="eastus",orchestration_mode="Uniform",resource_group="rg-a",'
        'subscription_id="sub-a",vmss_name="vmss-a"'
    )
    assert f"azure_vmss_instance_count{{{expected_labels}}} 4.0" in metrics
    assert f"azure_vmss_capacity{{{expected_labels}}} 6.0" in metrics
    # New info metric reflects the latest sku.name and is stale-cleaned across reloads.
    assert (
        'azure_vmss_info{location="eastus",orchestration_mode="Uniform",'
        'resource_group="rg-a",sku_tier="Standard",subscription_id="sub-a",'
        'vm_size="Standard_D8s_v5",vmss_name="vmss-a"} 1.0'
    ) in metrics
    assert "vmss-b" not in metrics
    assert 'vm_size="Standard_D2s_v3"' not in metrics
    assert "azure_vmss_exporter_vmss_total 1.0" in metrics


def test_collect_once_sets_lustre_metrics_and_removes_stale_series() -> None:
    registry = CollectorRegistry()
    vmss_calls = iter([[], []])
    first_lustre = ManagedLustreCollectionResult(
        metrics=(
            ManagedLustreOstMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0", 100.0,
                bytes_used=900.0, bytes_total=1000.0,
                client_read_ops=200.0,
                client_read_throughput_bytes_per_second=300.0,
                client_write_ops=500.0,
                client_write_throughput_bytes_per_second=600.0,
                connected_clients=11.0,
            ),
            ManagedLustreOstMetric(
                "sub-a", "rg-a", "lustre-b", "westus3", "0", 200.0,
                bytes_used=800.0, bytes_total=1000.0,
            ),
        ),
        filesystem_count=2,
        operation_metrics=(
            ManagedLustreOstOperationMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0", "read",
                client_latency_milliseconds=12.5,
                client_ops=42.0,
            ),
            ManagedLustreOstOperationMetric(
                "sub-a", "rg-a", "lustre-b", "westus3", "0", "write",
                client_latency_milliseconds=25.0,
                client_ops=84.0,
            ),
        ),
        mdt_metrics=(
            ManagedLustreMdtMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0",
                bytes_available=700.0,
                bytes_used=300.0,
                bytes_total=1000.0,
                files_free=80.0,
                files_used=20.0,
                files_total=100.0,
                hsm_action_errors=2.0,
                hsm_current_requests=17.0,
                connected_clients=13.0,
            ),
            ManagedLustreMdtMetric(
                "sub-a", "rg-a", "lustre-b", "westus3", "0",
                bytes_available=600.0,
                bytes_used=400.0,
                bytes_total=1000.0,
                hsm_action_errors=7.0,
                hsm_current_requests=4.0,
            ),
        ),
        mdt_operation_metrics=(
            ManagedLustreMdtOperationMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0", "open",
                client_latency_milliseconds=1.5,
                client_ops=9.0,
            ),
            ManagedLustreMdtOperationMetric(
                "sub-a", "rg-a", "lustre-b", "westus3", "0", "close",
                client_latency_milliseconds=2.5,
                client_ops=19.0,
            ),
        ),
    )
    second_lustre = ManagedLustreCollectionResult(
        metrics=(
            ManagedLustreOstMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0", 150.0,
                bytes_used=850.0, bytes_total=1000.0,
                client_read_ops=210.0,
                client_read_throughput_bytes_per_second=310.0,
                client_write_ops=510.0,
                client_write_throughput_bytes_per_second=610.0,
                connected_clients=12.0,
            ),
        ),
        filesystem_count=1,
        operation_metrics=(
            ManagedLustreOstOperationMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0", "read",
                client_latency_milliseconds=15.0,
                client_ops=50.0,
            ),
        ),
        mdt_metrics=(
            ManagedLustreMdtMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0",
                bytes_available=750.0,
                bytes_used=250.0,
                bytes_total=1000.0,
                files_free=85.0,
                files_used=15.0,
                files_total=100.0,
                hsm_action_errors=1.0,
                hsm_current_requests=12.0,
                connected_clients=14.0,
            ),
        ),
        mdt_operation_metrics=(
            ManagedLustreMdtOperationMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0", "open",
                client_latency_milliseconds=1.0,
                client_ops=10.0,
            ),
        ),
        filesystem_aggregate_metrics=(
            ManagedLustreFilesystemAggregateMetric(
                "sub-a",
                "rg-a",
                "lustre-a",
                "westus3",
                connected_clients=14.0,
                metadata_amplification_ratio=10.0 / 720.0,
                sample_max_age_seconds=45.0,
            ),
        ),
    )
    lustre_calls = iter([first_lustre, second_lustre])
    exporter = VmssMetricsExporter(
        lambda: next(vmss_calls),
        collect_lustre_metrics=lambda: next(lustre_calls),
        registry=registry,
    )

    exporter.collect_once()
    exporter.collect_once()

    metrics = generate_latest(registry).decode()
    expected_labels = (
        'filesystem_name="lustre-a",location="westus3",ostnum="0",'
        'resource_group="rg-a",subscription_id="sub-a"'
    )
    assert f"azure_managed_lustre_ost_bytes_available{{{expected_labels}}} 150.0" in metrics
    assert f"azure_managed_lustre_ost_bytes_used{{{expected_labels}}} 850.0" in metrics
    assert f"azure_managed_lustre_ost_bytes_total{{{expected_labels}}} 1000.0" in metrics
    assert (
        f"azure_managed_lustre_ost_bytes_available_percent{{{expected_labels}}} 15.0"
        in metrics
    )
    assert f"azure_managed_lustre_ost_bytes_used_percent{{{expected_labels}}} 85.0" in metrics
    assert f"azure_managed_lustre_ost_connected_clients{{{expected_labels}}} 12.0" in metrics
    assert "azure_managed_lustre_ost_files_free" not in metrics
    assert "azure_managed_lustre_client_read_latency_total_milliseconds" not in metrics
    assert f"azure_managed_lustre_client_read_ops{{{expected_labels}}} 210.0" in metrics
    assert (
        f"azure_managed_lustre_client_read_throughput_bytes_per_second"
        f"{{{expected_labels}}} 310.0" in metrics
    )
    assert "azure_managed_lustre_client_write_latency_total_milliseconds" not in metrics
    assert f"azure_managed_lustre_client_write_ops{{{expected_labels}}} 510.0" in metrics
    assert (
        f"azure_managed_lustre_client_write_throughput_bytes_per_second"
        f"{{{expected_labels}}} 610.0" in metrics
    )
    operation_labels = (
        'filesystem_name="lustre-a",location="westus3",operation="read",ostnum="0",'
        'resource_group="rg-a",subscription_id="sub-a"'
    )
    assert (
        f"azure_managed_lustre_ost_client_latency_milliseconds{{{operation_labels}}} 15.0"
        in metrics
    )
    assert f"azure_managed_lustre_ost_client_ops{{{operation_labels}}} 50.0" in metrics
    mdt_labels = (
        'filesystem_name="lustre-a",location="westus3",mdtnum="0",'
        'resource_group="rg-a",subscription_id="sub-a"'
    )
    assert f"azure_managed_lustre_mdt_bytes_available{{{mdt_labels}}} 750.0" in metrics
    assert f"azure_managed_lustre_mdt_bytes_used{{{mdt_labels}}} 250.0" in metrics
    assert f"azure_managed_lustre_mdt_bytes_total{{{mdt_labels}}} 1000.0" in metrics
    assert f"azure_managed_lustre_mdt_bytes_available_percent{{{mdt_labels}}} 75.0" in metrics
    assert f"azure_managed_lustre_mdt_bytes_used_percent{{{mdt_labels}}} 25.0" in metrics
    assert f"azure_managed_lustre_mdt_connected_clients{{{mdt_labels}}} 14.0" in metrics
    assert f"azure_managed_lustre_mdt_files_free{{{mdt_labels}}} 85.0" in metrics
    assert f"azure_managed_lustre_mdt_files_used{{{mdt_labels}}} 15.0" in metrics
    assert f"azure_managed_lustre_mdt_files_total{{{mdt_labels}}} 100.0" in metrics
    assert f"azure_managed_lustre_mdt_files_free_percent{{{mdt_labels}}} 85.0" in metrics
    assert f"azure_managed_lustre_mdt_files_used_percent{{{mdt_labels}}} 15.0" in metrics
    assert f"azure_managed_lustre_hsm_action_errors{{{mdt_labels}}} 1.0" in metrics
    assert f"azure_managed_lustre_hsm_current_requests{{{mdt_labels}}} 12.0" in metrics
    mdt_operation_labels = (
        'filesystem_name="lustre-a",location="westus3",mdtnum="0",operation="open",'
        'resource_group="rg-a",subscription_id="sub-a"'
    )
    assert (
        f"azure_managed_lustre_mdt_client_latency_milliseconds"
        f"{{{mdt_operation_labels}}} 1.0" in metrics
    )
    assert f"azure_managed_lustre_mdt_client_ops{{{mdt_operation_labels}}} 10.0" in metrics
    filesystem_labels = (
        'filesystem_name="lustre-a",location="westus3",resource_group="rg-a",'
        'subscription_id="sub-a"'
    )
    assert (
        f"azure_managed_lustre_filesystem_connected_clients"
        f"{{{filesystem_labels}}} 14.0" in metrics
    )
    assert (
        f"azure_managed_lustre_metadata_amplification_ratio"
        f"{{{filesystem_labels}}} {10.0 / 720.0}" in metrics
    )
    assert (
        f"azure_managed_lustre_filesystem_sample_max_age_seconds"
        f"{{{filesystem_labels}}} 45.0" in metrics
    )
    assert "lustre-b" not in metrics
    assert "azure_managed_lustre_filesystem_total 1.0" in metrics
    assert "azure_managed_lustre_ost_sample_count 1.0" in metrics
    assert "azure_managed_lustre_last_success_timestamp_seconds" in metrics
    assert "azure_managed_lustre_ost_sample_timestamp_seconds" in metrics


def test_lustre_partial_failure_keeps_existing_series() -> None:
    registry = CollectorRegistry()
    first_lustre = ManagedLustreCollectionResult(
        metrics=(
            ManagedLustreOstMetric("sub-a", "rg-a", "lustre-a", "westus3", "0", 100.0),
            ManagedLustreOstMetric(
                "sub-a", "rg-a", "lustre-b", "westus3", "0", 200.0,
                bytes_used=800.0, bytes_total=1000.0,
            ),
        ),
        filesystem_count=2,
    )
    second_lustre = ManagedLustreCollectionResult(
        metrics=(
            ManagedLustreOstMetric("sub-a", "rg-a", "lustre-a", "westus3", "0", 150.0),
        ),
        filesystem_count=2,
        error_count=1,
    )
    lustre_calls = iter([first_lustre, second_lustre])
    exporter = VmssMetricsExporter(
        lambda: [],
        collect_lustre_metrics=lambda: next(lustre_calls),
        registry=registry,
    )

    exporter.collect_lustre_once()
    exporter.collect_lustre_once()

    metrics = generate_latest(registry).decode()
    assert "lustre-a" in metrics
    assert "lustre-b" in metrics
    assert "azure_managed_lustre_collection_errors_total 1.0" in metrics


def test_lustre_filesystem_inventory_is_exposed_without_samples() -> None:
    registry = CollectorRegistry()
    lustre = ManagedLustreCollectionResult(
        metrics=(),
        filesystem_count=1,
        filesystems=(
            ManagedLustreFilesystem(
                "sub-a",
                "rg-a",
                "lustre-a",
                "/subscriptions/sub-a/resourceGroups/rg-a/providers/"
                "Microsoft.StorageCache/amlFilesystems/lustre-a",
                "westus3",
                sku_tier="AMLFS-Durable-Premium-500",
                storage_capacity_tib=8.0,
            ),
        ),
    )
    exporter = VmssMetricsExporter(
        lambda: [],
        collect_lustre_metrics=lambda: lustre,
        registry=registry,
    )

    exporter.collect_lustre_once()

    metrics = generate_latest(registry).decode()
    info_labels = (
        'filesystem_name="lustre-a",location="westus3",resource_group="rg-a",'
        'sku_tier="AMLFS-Durable-Premium-500",subscription_id="sub-a"'
    )
    capacity_labels = (
        'filesystem_name="lustre-a",location="westus3",resource_group="rg-a",'
        'subscription_id="sub-a"'
    )
    assert f"azure_managed_lustre_filesystem_info{{{info_labels}}} 1.0" in metrics
    assert (
        f"azure_managed_lustre_filesystem_storage_capacity_tib"
        f"{{{capacity_labels}}} 8.0" in metrics
    )
    assert "azure_managed_lustre_filesystem_total 1.0" in metrics
    assert "azure_managed_lustre_ost_sample_count 0.0" in metrics
    assert "azure_managed_lustre_ost_bytes_available{" not in metrics


def test_lustre_filesystem_inventory_removes_stale_series() -> None:
    registry = CollectorRegistry()
    first = ManagedLustreCollectionResult(
        metrics=(),
        filesystem_count=2,
        filesystems=(
            ManagedLustreFilesystem("sub-a", "rg-a", "lustre-a", "id-a", "westus3"),
            ManagedLustreFilesystem("sub-a", "rg-a", "lustre-b", "id-b", "westus3"),
        ),
    )
    second = ManagedLustreCollectionResult(
        metrics=(),
        filesystem_count=1,
        filesystems=(
            ManagedLustreFilesystem("sub-a", "rg-a", "lustre-a", "id-a", "westus3"),
        ),
    )
    calls = iter([first, second])
    exporter = VmssMetricsExporter(
        lambda: [],
        collect_lustre_metrics=lambda: next(calls),
        registry=registry,
    )

    exporter.collect_lustre_once()
    exporter.collect_lustre_once()

    metrics = generate_latest(registry).decode()
    assert "lustre-a" in metrics
    assert "lustre-b" not in metrics
    assert "azure_managed_lustre_filesystem_total 1.0" in metrics


def test_collect_once_isolates_lustre_failures_from_vmss_success() -> None:
    registry = CollectorRegistry()
    counts = [
        VmssCount(
            "sub-a", "rg-a", "vmss-a", "eastus", "Uniform", 3, 5,
            vm_size="Standard_D2s_v3", sku_tier="Standard",
        ),
    ]

    def fail_lustre() -> ManagedLustreCollectionResult:
        raise RuntimeError("azure monitor temporarily unavailable")

    exporter = VmssMetricsExporter(
        lambda: counts,
        collect_lustre_metrics=fail_lustre,
        registry=registry,
    )

    assert exporter.collect_once() == tuple(counts)
    metrics = generate_latest(registry).decode()
    assert "vmss-a" in metrics
    assert "azure_vmss_exporter_vmss_total 1.0" in metrics
    assert "azure_managed_lustre_collection_errors_total 1.0" in metrics


def test_is_leader_gauge_defaults_to_1_when_election_disabled() -> None:
    registry = CollectorRegistry()
    VmssMetricsExporter(lambda: [], registry=registry)
    metrics = generate_latest(registry).decode()
    assert "azure_vmss_exporter_is_leader 1.0" in metrics


def test_is_leader_gauge_defaults_to_0_when_election_enabled() -> None:
    registry = CollectorRegistry()
    VmssMetricsExporter(lambda: [], registry=registry, leader_election_enabled=True)
    metrics = generate_latest(registry).decode()
    assert "azure_vmss_exporter_is_leader 0.0" in metrics


def test_set_leader_clears_resource_gauges_on_demotion() -> None:
    registry = CollectorRegistry()
    counts = [
        VmssCount(
            "sub-a", "rg-a", "vmss-a", "eastus", "Uniform", 3, 5,
            vm_size="Standard_D2s_v3", sku_tier="Standard",
        ),
    ]
    lustre = ManagedLustreCollectionResult(
        metrics=(
            ManagedLustreOstMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0", 100.0,
                bytes_used=900.0, bytes_total=1000.0,
            ),
        ),
        filesystem_count=1,
        filesystems=(
            ManagedLustreFilesystem("sub-a", "rg-a", "lustre-a", "id-a", "westus3"),
        ),
        mdt_metrics=(
            ManagedLustreMdtMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0",
                bytes_available=700.0,
                hsm_action_errors=3.0,
                hsm_current_requests=11.0,
            ),
        ),
    )
    exporter = VmssMetricsExporter(
        lambda: counts,
        collect_lustre_metrics=lambda: lustre,
        registry=registry,
        leader_election_enabled=True,
    )
    # Become leader and populate gauges, then demote.
    exporter.set_leader(True)
    exporter.collect_once()
    populated = generate_latest(registry).decode()
    assert "vmss-a" in populated
    assert "lustre-a" in populated
    assert "azure_managed_lustre_filesystem_info" in populated
    assert "azure_managed_lustre_hsm_action_errors{" in populated
    assert "azure_managed_lustre_hsm_current_requests{" in populated
    assert "azure_vmss_exporter_is_leader 1.0" in populated

    exporter.set_leader(False)
    cleared = generate_latest(registry).decode()
    assert "vmss-a" not in cleared
    assert "lustre-a" not in cleared
    assert "azure_managed_lustre_filesystem_info{" not in cleared
    assert "azure_managed_lustre_hsm_action_errors{" not in cleared
    assert "azure_managed_lustre_hsm_current_requests{" not in cleared
    assert "azure_vmss_exporter_is_leader 0.0" in cleared
    assert "azure_vmss_exporter_vmss_total 0.0" in cleared
    assert "azure_managed_lustre_filesystem_total 0.0" in cleared
    assert "azure_vmss_exporter_last_success_timestamp_seconds 0.0" in cleared
    assert "azure_vmss_exporter_collection_duration_seconds 0.0" in cleared
    assert "azure_managed_lustre_last_success_timestamp_seconds 0.0" in cleared
    assert "azure_managed_lustre_collection_duration_seconds 0.0" in cleared


def test_vmss_update_is_skipped_when_leadership_is_lost_mid_collection() -> None:
    registry = CollectorRegistry()
    counts = [
        VmssCount(
            "sub-a", "rg-a", "vmss-a", "eastus", "Uniform", 3, 5,
            vm_size="Standard_D2s_v3", sku_tier="Standard",
        ),
    ]
    exporter: VmssMetricsExporter

    def collect_after_demotion() -> list[VmssCount]:
        exporter.set_leader(False)
        return counts

    exporter = VmssMetricsExporter(
        collect_after_demotion,
        registry=registry,
        leader_election_enabled=True,
    )
    exporter.set_leader(True)

    assert exporter.collect_once() == tuple(counts)

    metrics = generate_latest(registry).decode()
    assert "vmss-a" not in metrics
    assert "azure_vmss_exporter_is_leader 0.0" in metrics
    assert "azure_vmss_exporter_vmss_total 0.0" in metrics
    assert "azure_vmss_exporter_last_success_timestamp_seconds 0.0" in metrics


def test_lustre_update_is_skipped_when_leadership_is_lost_mid_collection() -> None:
    registry = CollectorRegistry()
    result = ManagedLustreCollectionResult(
        metrics=(
            ManagedLustreOstMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0", 100.0,
                bytes_used=900.0, bytes_total=1000.0,
            ),
        ),
        filesystem_count=1,
    )
    exporter: VmssMetricsExporter

    def collect_lustre_after_demotion() -> ManagedLustreCollectionResult:
        exporter.set_leader(False)
        return result

    exporter = VmssMetricsExporter(
        lambda: [],
        collect_lustre_metrics=collect_lustre_after_demotion,
        registry=registry,
        leader_election_enabled=True,
    )
    exporter.set_leader(True)

    assert exporter.collect_lustre_once() == result

    metrics = generate_latest(registry).decode()
    assert "lustre-a" not in metrics
    assert "azure_vmss_exporter_is_leader 0.0" in metrics
    assert "azure_managed_lustre_filesystem_total 0.0" in metrics
    assert "azure_managed_lustre_ost_sample_count 0.0" in metrics
    assert "azure_managed_lustre_last_success_timestamp_seconds 0.0" in metrics


def test_set_leader_is_noop_when_election_disabled() -> None:
    registry = CollectorRegistry()
    counts = [
        VmssCount(
            "sub-a", "rg-a", "vmss-a", "eastus", "Uniform", 3, 5,
            vm_size="Standard_D2s_v3", sku_tier="Standard",
        ),
    ]
    exporter = VmssMetricsExporter(lambda: counts, registry=registry)
    exporter.collect_once()
    exporter.set_leader(False)  # must NOT wipe gauges when leader election is off
    metrics = generate_latest(registry).decode()
    assert "vmss-a" in metrics
    assert "azure_vmss_exporter_is_leader 1.0" in metrics


def test_follower_poll_loop_does_not_call_collect_counts() -> None:
    """When leadership is held by another replica, no Azure calls happen."""

    import threading

    calls: list[int] = []

    def collect() -> list[VmssCount]:
        calls.append(1)
        return []

    registry = CollectorRegistry()
    exporter = VmssMetricsExporter(
        collect,
        registry=registry,
        leader_election_enabled=True,
        poll_interval_seconds=300,
    )
    # Start polling threads without ever becoming leader.
    exporter.start()
    # Give the poll thread a moment to enter the leadership wait.
    threading.Event().wait(0.3)
    exporter.stop(timeout=2.0)
    assert calls == []
