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
    StandaloneVm,
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
                client_evictions=6.0,
                connected_clients=13.0,
            ),
            ManagedLustreMdtMetric(
                "sub-a", "rg-a", "lustre-b", "westus3", "0",
                bytes_available=600.0,
                bytes_used=400.0,
                bytes_total=1000.0,
                hsm_action_errors=7.0,
                hsm_current_requests=4.0,
                client_evictions=2.0,
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
                sample_timestamp_seconds=170000.0,
            ),
        ),
        filesystem_count=1,
        operation_metrics=(
            ManagedLustreOstOperationMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0", "read",
                client_latency_milliseconds=15.0,
                client_ops=50.0,
                sample_timestamp_seconds=170010.0,
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
                client_evictions=4.0,
                connected_clients=14.0,
                sample_timestamp_seconds=170020.0,
            ),
        ),
        mdt_operation_metrics=(
            ManagedLustreMdtOperationMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0", "open",
                client_latency_milliseconds=1.0,
                client_ops=10.0,
                sample_timestamp_seconds=170030.0,
            ),
        ),
        filesystem_aggregate_metrics=(
            ManagedLustreFilesystemAggregateMetric(
                "sub-a",
                "rg-a",
                "lustre-a",
                "westus3",
                connected_clients=14.0,
                client_evictions=4.0,
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
    assert f"azure_managed_lustre_mdt_client_evictions{{{mdt_labels}}} 4.0" in metrics
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
        f"azure_managed_lustre_filesystem_client_evictions"
        f"{{{filesystem_labels}}} 4.0" in metrics
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
    assert (
        f"azure_managed_lustre_ost_sample_timestamp_seconds{{{expected_labels}}} 170000.0"
        in metrics
    )
    assert (
        f"azure_managed_lustre_ost_operation_sample_timestamp_seconds{{{operation_labels}}}"
        " 170010.0" in metrics
    )
    assert (
        f"azure_managed_lustre_mdt_sample_timestamp_seconds{{{mdt_labels}}} 170020.0"
        in metrics
    )
    assert (
        f"azure_managed_lustre_mdt_operation_sample_timestamp_seconds"
        f"{{{mdt_operation_labels}}} 170030.0" in metrics
    )


def test_lustre_missing_sample_timestamps_do_not_reset_freshness_gauges() -> None:
    registry = CollectorRegistry()
    fresh_lustre = ManagedLustreCollectionResult(
        metrics=(
            ManagedLustreOstMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0", 100.0,
                sample_timestamp_seconds=170000.0,
            ),
        ),
        filesystem_count=1,
        operation_metrics=(
            ManagedLustreOstOperationMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0", "read",
                sample_timestamp_seconds=170000.0,
            ),
        ),
        mdt_metrics=(
            ManagedLustreMdtMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0",
                sample_timestamp_seconds=170000.0,
            ),
        ),
        mdt_operation_metrics=(
            ManagedLustreMdtOperationMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0", "open",
                sample_timestamp_seconds=170000.0,
            ),
        ),
    )
    stale_lustre = ManagedLustreCollectionResult(
        metrics=(
            ManagedLustreOstMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0", 110.0,
            ),
        ),
        filesystem_count=1,
        operation_metrics=(
            ManagedLustreOstOperationMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0", "read",
            ),
        ),
        mdt_metrics=(
            ManagedLustreMdtMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0",
            ),
        ),
        mdt_operation_metrics=(
            ManagedLustreMdtOperationMetric(
                "sub-a", "rg-a", "lustre-a", "westus3", "0", "open",
            ),
        ),
    )
    calls = iter([fresh_lustre, stale_lustre])
    exporter = VmssMetricsExporter(
        lambda: [],
        collect_lustre_metrics=lambda: next(calls),
        registry=registry,
    )

    exporter.collect_lustre_once()
    exporter.collect_lustre_once()

    metrics = generate_latest(registry).decode()
    ost_labels = (
        'filesystem_name="lustre-a",location="westus3",ostnum="0",'
        'resource_group="rg-a",subscription_id="sub-a"'
    )
    ost_operation_labels = (
        'filesystem_name="lustre-a",location="westus3",operation="read",ostnum="0",'
        'resource_group="rg-a",subscription_id="sub-a"'
    )
    mdt_labels = (
        'filesystem_name="lustre-a",location="westus3",mdtnum="0",'
        'resource_group="rg-a",subscription_id="sub-a"'
    )
    mdt_operation_labels = (
        'filesystem_name="lustre-a",location="westus3",mdtnum="0",operation="open",'
        'resource_group="rg-a",subscription_id="sub-a"'
    )
    # The second collection had no Azure Monitor timestamps. The previous
    # timestamp must remain in place so the staleness alert can age out
    # naturally instead of being silently reset by the exporter wall clock.
    assert (
        f"azure_managed_lustre_ost_sample_timestamp_seconds{{{ost_labels}}} 170000.0"
        in metrics
    )
    assert (
        f"azure_managed_lustre_ost_operation_sample_timestamp_seconds"
        f"{{{ost_operation_labels}}} 170000.0" in metrics
    )
    assert (
        f"azure_managed_lustre_mdt_sample_timestamp_seconds{{{mdt_labels}}} 170000.0"
        in metrics
    )
    assert (
        f"azure_managed_lustre_mdt_operation_sample_timestamp_seconds"
        f"{{{mdt_operation_labels}}} 170000.0" in metrics
    )


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
        f"azure_managed_lustre_discovered_filesystem_info{{{info_labels}}} 1.0"
        in metrics
    )
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
                client_evictions=5.0,
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
    assert "azure_managed_lustre_mdt_client_evictions{" in populated
    assert "azure_vmss_exporter_is_leader 1.0" in populated

    exporter.set_leader(False)
    cleared = generate_latest(registry).decode()
    assert "vmss-a" not in cleared
    assert "lustre-a" not in cleared
    assert "azure_managed_lustre_filesystem_info{" not in cleared
    assert "azure_managed_lustre_hsm_action_errors{" not in cleared
    assert "azure_managed_lustre_hsm_current_requests{" not in cleared
    assert "azure_managed_lustre_mdt_client_evictions{" not in cleared
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


def test_leader_reacquire_wakes_poller_immediately() -> None:
    """A brief leader-election bounce must trigger a fresh collect, not wait the full poll interval.

    Regression test for the production gap observed when ``POLL_INTERVAL_SECONDS=300``: every
    leader bounce cleared the per-resource gauges via ``_clear_resource_gauges`` but the
    re-acquired leader's polling thread stayed asleep on ``self._stop_event.wait(300)`` until
    its next natural cycle, leaving ``/metrics`` empty for up to 5 minutes.
    """

    import threading

    collect_event = threading.Event()
    collect_calls: list[int] = []

    def collect() -> list[VmssCount]:
        collect_calls.append(1)
        collect_event.set()
        return []

    registry = CollectorRegistry()
    # Use a deliberately long poll interval so a slow wake-up would make the
    # test hang well past its timeout instead of producing a flaky pass.
    exporter = VmssMetricsExporter(
        collect,
        registry=registry,
        leader_election_enabled=True,
        poll_interval_seconds=300,
    )
    exporter.start()
    try:
        exporter.set_leader(True)
        assert collect_event.wait(timeout=2.0), (
            "initial collect should happen after becoming leader"
        )
        collect_event.clear()
        baseline_calls = len(collect_calls)

        # Simulate a leader bounce: demote then promptly re-elect the same replica.
        exporter.set_leader(False)
        exporter.set_leader(True)

        # The re-acquired leader must collect again well before poll_interval_seconds.
        assert collect_event.wait(timeout=2.0), (
            "re-acquired leader did not collect within 2s; "
            "polling thread is still sleeping on the old poll interval"
        )
        assert len(collect_calls) > baseline_calls
    finally:
        exporter.stop(timeout=2.0)


def test_stop_interrupts_polling_sleep() -> None:
    """stop() must wake a poller that is mid-sleep, not block until the poll interval elapses."""

    import threading
    import time

    collect_event = threading.Event()

    def collect() -> list[VmssCount]:
        collect_event.set()
        return []

    registry = CollectorRegistry()
    exporter = VmssMetricsExporter(
        collect,
        registry=registry,
        poll_interval_seconds=300,
    )
    exporter.start()
    assert collect_event.wait(timeout=2.0), "initial collect should happen immediately"

    start = time.monotonic()
    exporter.stop(timeout=5.0)
    elapsed = time.monotonic() - start
    assert exporter._vmss_thread is None or not exporter._vmss_thread.is_alive()
    assert elapsed < 5.0, f"stop() took {elapsed:.2f}s; expected to interrupt the poll sleep"


# ---------------------------------------------------------------------------
# Standalone (non-VMSS) Azure VM inventory
# ---------------------------------------------------------------------------


def _make_standalone_vm(
    vm_name: str,
    *,
    subscription_id: str = "sub-a",
    resource_group: str = "rg-a",
    vm_size: str = "Standard_D2s_v3",
    power_state: str = "running",
    zone: str = "1",
    os_type: str = "Linux",
) -> StandaloneVm:
    return StandaloneVm(
        subscription_id=subscription_id,
        resource_group=resource_group,
        vm_name=vm_name,
        vm_id=f"id-{vm_name}",
        location="eastus",
        zone=zone,
        vm_size=vm_size,
        os_type=os_type,
        power_state=power_state,
    )


def test_standalone_vm_metrics_emit_info_power_state_and_count_by_size() -> None:
    registry = CollectorRegistry()
    vms = [
        _make_standalone_vm("vm-1", vm_size="Standard_D2s_v3", power_state="running"),
        _make_standalone_vm("vm-2", vm_size="Standard_D2s_v3", power_state="deallocated"),
        _make_standalone_vm("vm-3", vm_size="Standard_D4s_v5", power_state="stopped"),
    ]
    exporter = VmssMetricsExporter(
        lambda: [],
        collect_standalone_vms=lambda: vms,
        registry=registry,
    )

    exporter.collect_standalone_vms_once()

    metrics = generate_latest(registry).decode()
    info_vm1 = (
        'location="eastus",os_type="Linux",resource_group="rg-a",'
        'subscription_id="sub-a",vm_id="id-vm-1",vm_name="vm-1",'
        'vm_size="Standard_D2s_v3",zone="1"'
    )
    assert f"azure_vm_info{{{info_vm1}}} 1.0" in metrics

    # Each VM exposes one series per state; only the matching state is 1.
    assert (
        'azure_vm_power_state{resource_group="rg-a",state="running",'
        'subscription_id="sub-a",vm_name="vm-1"} 1.0' in metrics
    )
    assert (
        'azure_vm_power_state{resource_group="rg-a",state="stopped",'
        'subscription_id="sub-a",vm_name="vm-1"} 0.0' in metrics
    )
    assert (
        'azure_vm_power_state{resource_group="rg-a",state="deallocated",'
        'subscription_id="sub-a",vm_name="vm-2"} 1.0' in metrics
    )

    # Aggregate-by-size and total scalars.
    assert 'azure_vm_count_by_size{vm_size="Standard_D2s_v3"} 2.0' in metrics
    assert 'azure_vm_count_by_size{vm_size="Standard_D4s_v5"} 1.0' in metrics
    assert "azure_vm_exporter_vm_total 3.0" in metrics
    assert "azure_vm_exporter_last_success_timestamp_seconds" in metrics


def test_standalone_vm_metrics_remove_stale_series_across_runs() -> None:
    registry = CollectorRegistry()
    first = [
        _make_standalone_vm("vm-1", vm_size="Standard_D2s_v3", power_state="running"),
        _make_standalone_vm("vm-2", vm_size="Standard_D2s_v3", power_state="running"),
    ]
    second = [
        _make_standalone_vm("vm-1", vm_size="Standard_D4s_v5", power_state="stopped"),
    ]
    calls = iter([first, second])
    exporter = VmssMetricsExporter(
        lambda: [],
        collect_standalone_vms=lambda: next(calls),
        registry=registry,
    )

    exporter.collect_standalone_vms_once()
    exporter.collect_standalone_vms_once()

    metrics = generate_latest(registry).decode()
    # vm-2 is gone, including all of its power_state series, and its old
    # Standard_D2s_v3 vm_size bucket no longer appears.
    assert "vm-2" not in metrics
    # vm-1's old vm_size info series is replaced by the new one.
    assert 'vm_size="Standard_D2s_v3"' not in metrics or "vm-1" not in metrics.split(
        'vm_size="Standard_D2s_v3"'
    )[1].split("\n")[0]
    assert 'azure_vm_count_by_size{vm_size="Standard_D4s_v5"} 1.0' in metrics
    assert "azure_vm_exporter_vm_total 1.0" in metrics


def test_standalone_vm_cardinality_guardrail_suppresses_per_vm_series() -> None:
    registry = CollectorRegistry()
    # Two distinct sizes spread across many VMs so the by-size aggregate is
    # still meaningful when per-VM info is suppressed.
    vms = [
        _make_standalone_vm(
            f"vm-{i}",
            vm_size="Standard_D2s_v3" if i % 2 == 0 else "Standard_D4s_v5",
            power_state="running",
        )
        for i in range(12)
    ]
    exporter = VmssMetricsExporter(
        lambda: [],
        collect_standalone_vms=lambda: vms,
        standalone_vm_max_inventory=10,
        registry=registry,
    )

    exporter.collect_standalone_vms_once()

    metrics = generate_latest(registry).decode()
    # Per-VM series are suppressed entirely.
    assert "azure_vm_info{" not in metrics
    assert "azure_vm_power_state{" not in metrics
    # Aggregates still emit.
    assert 'azure_vm_count_by_size{vm_size="Standard_D2s_v3"} 6.0' in metrics
    assert 'azure_vm_count_by_size{vm_size="Standard_D4s_v5"} 6.0' in metrics
    assert "azure_vm_exporter_vm_total 12.0" in metrics
    # Guardrail tripping must not increment the error counter.
    assert "azure_vm_exporter_collection_errors_total 0.0" in metrics


def test_collect_once_isolates_standalone_vm_failures_from_vmss_success() -> None:
    registry = CollectorRegistry()
    counts = [
        VmssCount(
            "sub-a", "rg-a", "vmss-a", "eastus", "Uniform", 3, 5,
            vm_size="Standard_D2s_v3", sku_tier="Standard",
        ),
    ]

    def fail_standalone() -> list[StandaloneVm]:
        raise RuntimeError("resource graph temporarily unavailable")

    exporter = VmssMetricsExporter(
        lambda: counts,
        collect_standalone_vms=fail_standalone,
        registry=registry,
    )

    assert exporter.collect_once() == tuple(counts)
    metrics = generate_latest(registry).decode()
    assert "vmss-a" in metrics
    assert "azure_vmss_exporter_vmss_total 1.0" in metrics
    assert "azure_vm_exporter_collection_errors_total 1.0" in metrics
    # No standalone-VM inventory series were published because the call failed.
    assert "azure_vm_info{" not in metrics


def test_set_leader_clears_standalone_vm_gauges_on_demotion() -> None:
    registry = CollectorRegistry()
    vms = [
        _make_standalone_vm("vm-1", vm_size="Standard_D2s_v3", power_state="running"),
    ]
    exporter = VmssMetricsExporter(
        lambda: [],
        collect_standalone_vms=lambda: vms,
        registry=registry,
        leader_election_enabled=True,
    )
    exporter.set_leader(True)
    exporter.collect_standalone_vms_once()

    populated = generate_latest(registry).decode()
    assert "azure_vm_info{" in populated
    assert "azure_vm_power_state{" in populated
    assert "azure_vm_count_by_size{" in populated

    exporter.set_leader(False)
    cleared = generate_latest(registry).decode()
    assert "azure_vm_info{" not in cleared
    assert "azure_vm_power_state{" not in cleared
    assert "azure_vm_count_by_size{" not in cleared
    assert "azure_vm_exporter_vm_total 0.0" in cleared
    assert "azure_vm_exporter_last_success_timestamp_seconds 0.0" in cleared
    assert "azure_vm_exporter_collection_duration_seconds 0.0" in cleared
