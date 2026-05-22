from __future__ import annotations

import sys
import types
from dataclasses import dataclass

import pytest

from vmss_metrics_exporter.azure_managed_lustre import (
    AMLFS_FILESYSTEMS_QUERY,
    AZURE_MONITOR_METRIC_NAMES_PER_REQUEST,
    LUSTRE_METRICS,
    AzureManagedLustreCollector,
    _chunk_metric_names,
    create_metrics_query_client,
    normalize_filesystem_row,
    normalize_lustre_metrics_response,
    normalize_ost_bytes_available_response,
    normalize_ost_capacity_response,
    normalize_ost_metrics_response,
    parse_iso_duration,
    summarize_lustre_metrics,
)
from vmss_metrics_exporter.models import (
    ManagedLustreCollectionResult,
    ManagedLustreFilesystem,
    ManagedLustreMdtMetric,
    ManagedLustreMdtOperationMetric,
    ManagedLustreOstMetric,
    ManagedLustreOstOperationMetric,
    build_lustre_filesystem_aggregate_metrics,
)


@dataclass
class FakeResponse:
    data: list[dict[str, object]]
    skip_token: str | None = None


class FakeResourceGraphClient:
    def __init__(self) -> None:
        self.calls = 0

    def resources(self, _query: object) -> FakeResponse:
        self.calls += 1
        return FakeResponse(
            [
                {
                    "id": "/subscriptions/sub-a/resourceGroups/rg-a/providers/"
                    "Microsoft.StorageCache/amlFilesystems/lustre-a",
                    "subscriptionId": "sub-a",
                    "resourceGroup": "rg-a",
                    "filesystemName": "lustre-a",
                    "location": "westus3",
                    "skuTier": "AMLFS-Durable-Premium-500",
                    "storageCapacityTiB": "8",
                }
            ]
        )


class FakeMetricsClient:
    def __init__(self) -> None:
        self.resource_uris: list[str] = []
        self.metric_names: list[object] = []

    def query_resource(self, resource_uri: str, metric_names: object, **_kwargs: object) -> object:
        self.resource_uris.append(resource_uri)
        self.metric_names.append(metric_names)
        return {
            "metrics": [
                {
                    "name": "OSTBytesAvailable",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "0"},
                            ],
                            "data": [
                                {"average": None},
                                {"average": 123.0},
                            ],
                        },
                    ],
                },
                {
                    "name": "OSTBytesUsed",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "0"},
                            ],
                            "data": [
                                {"average": 877.0},
                            ],
                        },
                    ],
                },
                {
                    "name": "OSTBytesTotal",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "0"},
                            ],
                            "data": [
                                {"average": 1000.0},
                            ],
                        },
                    ],
                }
                ,
                {
                    "name": "OSTConnectedClients",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "0"},
                            ],
                            "data": [
                                {"average": 5.0},
                            ],
                        },
                    ],
                },
                {
                    "name": "OSTFilesFree",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "0"},
                            ],
                            "data": [
                                {"average": 9000.0},
                            ],
                        },
                    ],
                },
                {
                    "name": "OSTFilesUsed",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "0"},
                            ],
                            "data": [
                                {"average": 1000.0},
                            ],
                        },
                    ],
                },
                {
                    "name": "OSTFilesTotal",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "0"},
                            ],
                            "data": [
                                {"average": 10000.0},
                            ],
                        },
                    ],
                },
                {
                    "name": "OSTClientLatency",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "0"},
                                {"name": {"value": "operation"}, "value": "read"},
                            ],
                            "data": [
                                {"average": 12.5},
                            ],
                        },
                    ],
                },
                {
                    "name": "OSTClientOps",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "0"},
                                {"name": {"value": "operation"}, "value": "read"},
                            ],
                            "data": [
                                {"average": 42.0},
                            ],
                        },
                    ],
                },
                {
                    "name": "ClientReadLatencyTotal",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "0"},
                            ],
                            "data": [{"average": 100.0}],
                        },
                    ],
                },
                {
                    "name": "ClientReadOps",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "0"},
                            ],
                            "data": [{"average": 200.0}],
                        },
                    ],
                },
                {
                    "name": "ClientReadThroughput",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "0"},
                            ],
                            "data": [{"average": 300.0}],
                        },
                    ],
                },
                {
                    "name": "ClientWriteLatencyTotal",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "0"},
                            ],
                            "data": [{"average": 400.0}],
                        },
                    ],
                },
                {
                    "name": "ClientWriteOps",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "0"},
                            ],
                            "data": [{"average": 500.0}],
                        },
                    ],
                },
                {
                    "name": "ClientWriteThroughput",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "0"},
                            ],
                            "data": [{"average": 600.0}],
                        },
                    ],
                },
                {
                    "name": "MDTBytesAvailable",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "mdtnum"}, "value": "0"},
                            ],
                            "data": [{"average": 700.0}],
                        },
                    ],
                },
                {
                    "name": "MDTBytesUsed",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "mdtnum"}, "value": "0"},
                            ],
                            "data": [{"average": 300.0}],
                        },
                    ],
                },
                {
                    "name": "MDTBytesTotal",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "mdtnum"}, "value": "0"},
                            ],
                            "data": [{"average": 1000.0}],
                        },
                    ],
                },
                {
                    "name": "MDTConnectedClients",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "mdtnum"}, "value": "0"},
                            ],
                            "data": [{"average": 7.0}],
                        },
                    ],
                },
                {
                    "name": "MDTFilesFree",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "mdtnum"}, "value": "0"},
                            ],
                            "data": [{"average": 80.0}],
                        },
                    ],
                },
                {
                    "name": "MDTFilesUsed",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "mdtnum"}, "value": "0"},
                            ],
                            "data": [{"average": 20.0}],
                        },
                    ],
                },
                {
                    "name": "MDTFilesTotal",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "mdtnum"}, "value": "0"},
                            ],
                            "data": [{"average": 100.0}],
                        },
                    ],
                },
                {
                    "name": "MDTClientLatency",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "mdtnum"}, "value": "0"},
                                {"name": {"value": "operation"}, "value": "open"},
                            ],
                            "data": [{"average": 1.5}],
                        },
                    ],
                },
                {
                    "name": "MDTClientOps",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "mdtnum"}, "value": "0"},
                                {"name": {"value": "operation"}, "value": "open"},
                            ],
                            "data": [{"average": 9.0}],
                        },
                    ],
                },
                {
                    "name": "HSMActionErrors",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "mdtnum"}, "value": "0"},
                            ],
                            "data": [{"average": 2.0}],
                        },
                    ],
                },
                {
                    "name": "HSMCurrentRequests",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "mdtnum"}, "value": "0"},
                            ],
                            "data": [{"average": 17.0}],
                        },
                    ],
                },
            ]
        }


class PartiallyFailingMetricsClient(FakeMetricsClient):
    def query_resource(self, resource_uri: str, _metric_names: object, **_kwargs: object) -> object:
        if resource_uri.endswith("lustre-b"):
            raise RuntimeError("boom")
        return super().query_resource(resource_uri, _metric_names, **_kwargs)


def test_create_metrics_query_client_uses_provided_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credential = object()

    class _FakeMetricsQueryClient:
        def __init__(self, received_credential: object) -> None:
            self.credential = received_credential

    fake_module = types.ModuleType("azure.monitor.query")
    fake_module.MetricsQueryClient = _FakeMetricsQueryClient
    monkeypatch.setitem(sys.modules, "azure.monitor.query", fake_module)
    monkeypatch.setattr(
        "vmss_metrics_exporter.credentials.create_credential",
        lambda: pytest.fail("create_credential should not be called when credential is provided"),
    )

    client = create_metrics_query_client(credential)

    assert isinstance(client, _FakeMetricsQueryClient)
    assert client.credential is credential


def test_amlfs_query_discovers_all_filesystems() -> None:
    assert "microsoft.storagecache/amlfilesystems" in AMLFS_FILESYSTEMS_QUERY.lower()
    assert "filesystemName = name" in AMLFS_FILESYSTEMS_QUERY
    assert "storageCapacityTiB" in AMLFS_FILESYSTEMS_QUERY
    assert "let " not in AMLFS_FILESYSTEMS_QUERY.lower()


def test_lustre_metric_query_stays_within_azure_monitor_limit() -> None:
    # Azure Monitor accepts at most 20 metric names per query_resource request,
    # so the collector must chunk LUSTRE_METRICS into batches no larger than
    # AZURE_MONITOR_METRIC_NAMES_PER_REQUEST. Validate the chunking constant
    # honors the documented Azure Monitor cap and that every produced batch is
    # within the limit.
    assert AZURE_MONITOR_METRIC_NAMES_PER_REQUEST <= 20
    batches = _chunk_metric_names(LUSTRE_METRICS, AZURE_MONITOR_METRIC_NAMES_PER_REQUEST)
    assert batches, "expected at least one metric batch"
    assert all(len(batch) <= AZURE_MONITOR_METRIC_NAMES_PER_REQUEST for batch in batches)
    flattened = tuple(name for batch in batches for name in batch)
    assert flattened == tuple(LUSTRE_METRICS)


def test_normalize_filesystem_row() -> None:
    filesystem = normalize_filesystem_row(
        {
            "id": "/subscriptions/sub-a/resourceGroups/rg-a/providers/"
            "Microsoft.StorageCache/amlFilesystems/lustre-a",
            "subscriptionId": "sub-a",
            "resourceGroup": "rg-a",
            "filesystemName": "lustre-a",
            "location": "westus3",
            "skuTier": "AMLFS-Durable-Premium-500",
            "storageCapacityTiB": "8",
        }
    )

    assert filesystem.subscription_id == "sub-a"
    assert filesystem.filesystem_name == "lustre-a"
    assert filesystem.location == "westus3"
    assert filesystem.storage_capacity_tib == 8.0


def test_normalize_ost_bytes_available_response_uses_latest_non_null_average() -> None:
    filesystem = ManagedLustreFilesystem(
        "sub-a",
        "rg-a",
        "lustre-a",
        "/subscriptions/sub-a/resourceGroups/rg-a/providers/"
        "Microsoft.StorageCache/amlFilesystems/lustre-a",
        "westus3",
    )

    metrics = normalize_ost_bytes_available_response(
        filesystem,
        {
            "metrics": [
                {
                    "name": "OSTBytesAvailable",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "1"},
                            ],
                            "data": [
                                {"average": 10.0},
                                {"average": None},
                                {"average": 42.0},
                            ],
                        },
                        {
                            "metadata_values": [
                                {"name": {"value": "other"}, "value": "ignored"},
                            ],
                            "data": [{"average": 99.0}],
                        },
                    ],
                }
            ]
        },
    )

    assert len(metrics) == 1
    assert metrics[0].label_values == ("sub-a", "rg-a", "lustre-a", "westus3", "1")
    assert metrics[0].bytes_available == 42.0


def test_normalize_ost_capacity_response_groups_metric_names_by_ostnum() -> None:
    filesystem = ManagedLustreFilesystem(
        "sub-a",
        "rg-a",
        "lustre-a",
        "/subscriptions/sub-a/resourceGroups/rg-a/providers/"
        "Microsoft.StorageCache/amlFilesystems/lustre-a",
        "westus3",
    )

    metrics = normalize_ost_capacity_response(
        filesystem,
        {
            "metrics": [
                {
                    "name": "OSTBytesTotal",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "1"},
                            ],
                            "data": [{"average": 1000.0}],
                        }
                    ],
                },
                {
                    "name": "OSTBytesAvailable",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "1"},
                            ],
                            "data": [{"average": 250.0}],
                        }
                    ],
                },
                {
                    "name": "OSTBytesUsed",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "1"},
                            ],
                            "data": [{"average": 750.0}],
                        }
                    ],
                },
                {
                    "name": "OSTFilesFree",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "1"},
                            ],
                            "data": [{"average": 80.0}],
                        }
                    ],
                },
                {
                    "name": "OSTFilesUsed",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "1"},
                            ],
                            "data": [{"average": 20.0}],
                        }
                    ],
                },
                {
                    "name": "OSTFilesTotal",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "1"},
                            ],
                            "data": [{"average": 100.0}],
                        }
                    ],
                },
            ]
        },
    )

    assert len(metrics) == 1
    assert metrics[0].bytes_available == 250.0
    assert metrics[0].bytes_used == 750.0
    assert metrics[0].bytes_total == 1000.0
    assert metrics[0].bytes_available_percent == 25.0
    assert metrics[0].bytes_used_percent == 75.0
    assert not hasattr(metrics[0], "files_free")


def test_normalize_ost_metrics_response_preserves_operation_dimension() -> None:
    filesystem = ManagedLustreFilesystem(
        "sub-a",
        "rg-a",
        "lustre-a",
        "/subscriptions/sub-a/resourceGroups/rg-a/providers/"
        "Microsoft.StorageCache/amlFilesystems/lustre-a",
        "westus3",
    )

    ost_metrics, operation_metrics = normalize_ost_metrics_response(
        filesystem,
        {
            "metrics": [
                {
                    "name": "OSTClientLatency",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "1"},
                                {"name": {"value": "operation"}, "value": "read"},
                            ],
                            "data": [{"average": 10.0}],
                        },
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "1"},
                                {"name": {"value": "operation"}, "value": "write"},
                            ],
                            "data": [{"average": 20.0}],
                        },
                    ],
                },
                {
                    "name": "OSTClientOps",
                    "timeseries": [
                        {
                            "metadata_values": [
                                {"name": {"value": "ostnum"}, "value": "1"},
                                {"name": {"value": "operation"}, "value": "read"},
                            ],
                            "data": [{"average": 30.0}],
                        },
                    ],
                },
            ]
        },
    )

    assert ost_metrics == []
    assert len(operation_metrics) == 2
    read_metric = next(metric for metric in operation_metrics if metric.operation == "read")
    write_metric = next(metric for metric in operation_metrics if metric.operation == "write")
    assert read_metric.client_latency_milliseconds == 10.0
    assert read_metric.client_ops == 30.0
    assert write_metric.client_latency_milliseconds == 20.0
    assert write_metric.client_ops is None


def test_normalize_lustre_metrics_response_groups_ost_client_and_mdt_metrics() -> None:
    filesystem = ManagedLustreFilesystem(
        "sub-a",
        "rg-a",
        "lustre-a",
        "/subscriptions/sub-a/resourceGroups/rg-a/providers/"
        "Microsoft.StorageCache/amlFilesystems/lustre-a",
        "westus3",
    )

    ost_metrics, _ost_operations, mdt_metrics, mdt_operations = (
        normalize_lustre_metrics_response(filesystem, FakeMetricsClient().query_resource("", []))
    )

    assert len(ost_metrics) == 1
    assert ost_metrics[0].client_read_ops == 200.0
    assert ost_metrics[0].client_read_throughput_bytes_per_second == 300.0
    assert ost_metrics[0].client_write_ops == 500.0
    assert ost_metrics[0].client_write_throughput_bytes_per_second == 600.0
    assert ost_metrics[0].connected_clients == 5.0
    assert len(mdt_metrics) == 1
    assert mdt_metrics[0].label_values == ("sub-a", "rg-a", "lustre-a", "westus3", "0")
    assert mdt_metrics[0].bytes_available == 700.0
    assert mdt_metrics[0].bytes_used == 300.0
    assert mdt_metrics[0].bytes_total == 1000.0
    assert mdt_metrics[0].bytes_available_percent == 70.0
    assert mdt_metrics[0].connected_clients == 7.0
    assert mdt_metrics[0].files_free == 80.0
    assert mdt_metrics[0].files_used == 20.0
    assert mdt_metrics[0].files_total == 100.0
    assert mdt_metrics[0].hsm_action_errors == 2.0
    assert mdt_metrics[0].hsm_current_requests == 17.0
    assert len(mdt_operations) == 1
    assert mdt_operations[0].operation == "open"
    assert mdt_operations[0].client_latency_milliseconds == 1.5
    assert mdt_operations[0].client_ops == 9.0


def test_build_lustre_filesystem_aggregate_metrics() -> None:
    filesystems = (
        ManagedLustreFilesystem("sub-a", "rg-a", "lustre-a", "id-a", "westus3"),
        ManagedLustreFilesystem("sub-a", "rg-a", "lustre-idle", "id-idle", "westus3"),
    )

    aggregate_metrics = build_lustre_filesystem_aggregate_metrics(
        filesystems=filesystems,
        metrics=(
            ManagedLustreOstMetric(
                "sub-a",
                "rg-a",
                "lustre-a",
                "westus3",
                "0",
                client_read_ops=200.0,
                client_write_ops=500.0,
                sample_timestamp_seconds=900.0,
            ),
        ),
        operation_metrics=(),
        mdt_metrics=(
            ManagedLustreMdtMetric(
                "sub-a",
                "rg-a",
                "lustre-a",
                "westus3",
                "0",
                connected_clients=7.0,
                sample_timestamp_seconds=950.0,
            ),
            ManagedLustreMdtMetric(
                "sub-a",
                "rg-a",
                "lustre-a",
                "westus3",
                "1",
                connected_clients=13.0,
            ),
        ),
        mdt_operation_metrics=(
            ManagedLustreMdtOperationMetric(
                "sub-a",
                "rg-a",
                "lustre-a",
                "westus3",
                "0",
                "open",
                client_ops=21.0,
            ),
        ),
        now_seconds=1000.0,
    )

    assert len(aggregate_metrics) == 1
    aggregate = aggregate_metrics[0]
    assert aggregate.label_values == ("sub-a", "rg-a", "lustre-a", "westus3")
    assert aggregate.connected_clients == 13.0
    assert aggregate.metadata_amplification_ratio == 21.0 / 700.0
    assert aggregate.sample_max_age_seconds == 100.0


def test_build_lustre_filesystem_aggregate_metrics_clamps_zero_data_ops() -> None:
    aggregate_metrics = build_lustre_filesystem_aggregate_metrics(
        filesystems=(),
        metrics=(
            ManagedLustreOstMetric(
                "sub-a",
                "rg-a",
                "lustre-a",
                "westus3",
                "all",
                client_read_ops=0.0,
                client_write_ops=0.0,
            ),
        ),
        operation_metrics=(),
        mdt_metrics=(),
        mdt_operation_metrics=(
            ManagedLustreMdtOperationMetric(
                "sub-a",
                "rg-a",
                "lustre-a",
                "westus3",
                "all",
                "all",
                client_ops=9.0,
            ),
        ),
        now_seconds=1000.0,
    )

    assert aggregate_metrics[0].metadata_amplification_ratio == 9.0


def test_normalize_lustre_metrics_response_accepts_dimensionless_aggregate_series() -> None:
    filesystem = ManagedLustreFilesystem(
        "sub-a",
        "rg-a",
        "lustre-a",
        "/subscriptions/sub-a/resourceGroups/rg-a/providers/"
        "Microsoft.StorageCache/amlFilesystems/lustre-a",
        "westus3",
    )

    ost_metrics, ost_operations, mdt_metrics, mdt_operations = normalize_lustre_metrics_response(
        filesystem,
        {
            "metrics": [
                {"name": "OSTBytesAvailable", "timeseries": [{"data": [{"average": 123.0}]}]},
                {"name": "OSTClientOps", "timeseries": [{"data": [{"average": 42.0}]}]},
                {"name": "MDTBytesAvailable", "timeseries": [{"data": [{"average": 456.0}]}]},
                {"name": "MDTClientOps", "timeseries": [{"data": [{"average": 84.0}]}]},
                {"name": "HSMActionErrors", "timeseries": [{"data": [{"average": 3.0}]}]},
                {"name": "HSMCurrentRequests", "timeseries": [{"data": [{"average": 5.0}]}]},
            ]
        },
    )

    assert ost_metrics[0].ostnum == "all"
    assert ost_metrics[0].bytes_available == 123.0
    assert ost_operations[0].ostnum == "all"
    assert ost_operations[0].operation == "all"
    assert ost_operations[0].client_ops == 42.0
    assert mdt_metrics[0].mdtnum == "all"
    assert mdt_metrics[0].bytes_available == 456.0
    assert mdt_metrics[0].hsm_action_errors == 3.0
    assert mdt_metrics[0].hsm_current_requests == 5.0
    assert mdt_operations[0].mdtnum == "all"
    assert mdt_operations[0].operation == "all"
    assert mdt_operations[0].client_ops == 84.0


def test_collector_discovers_and_collects_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "vmss_metrics_exporter.azure_managed_lustre.build_query_request",
        lambda **kwargs: kwargs,
    )
    rg_client = FakeResourceGraphClient()
    metrics_client = FakeMetricsClient()
    collector = AzureManagedLustreCollector(
        rg_client,
        metrics_client,
        ["sub-a"],
        request_jitter_seconds=0,
        retry_base_delay_seconds=0,
    )

    result = collector.collect()

    assert rg_client.calls == 1
    assert result.filesystem_count == 1
    assert len(result.filesystems) == 1
    assert result.filesystems[0].filesystem_name == "lustre-a"
    assert result.filesystems[0].storage_capacity_tib == 8.0
    assert result.error_count == 0
    assert len(result.metrics) == 1
    assert result.metrics[0].ostnum == "0"
    assert result.metrics[0].bytes_available == 123.0
    assert result.metrics[0].bytes_used == 877.0
    assert result.metrics[0].bytes_total == 1000.0
    assert result.metrics[0].client_read_ops == 200.0
    assert result.metrics[0].client_write_throughput_bytes_per_second == 600.0
    assert result.metrics[0].connected_clients == 5.0
    assert len(result.operation_metrics) == 1
    assert result.operation_metrics[0].operation == "read"
    assert result.operation_metrics[0].client_latency_milliseconds == 12.5
    assert result.operation_metrics[0].client_ops == 42.0
    assert len(result.mdt_metrics) == 1
    assert result.mdt_metrics[0].bytes_available == 700.0
    assert result.mdt_metrics[0].files_total == 100.0
    assert result.mdt_metrics[0].hsm_action_errors == 2.0
    assert result.mdt_metrics[0].hsm_current_requests == 17.0
    assert result.mdt_metrics[0].connected_clients == 7.0
    assert len(result.mdt_operation_metrics) == 1
    assert result.mdt_operation_metrics[0].operation == "open"
    assert result.mdt_operation_metrics[0].client_ops == 9.0
    assert len(result.filesystem_aggregate_metrics) == 1
    assert result.filesystem_aggregate_metrics[0].connected_clients == 7.0
    assert result.filesystem_aggregate_metrics[0].metadata_amplification_ratio == 9.0 / 700.0
    expected_batches = [
        list(batch)
        for batch in _chunk_metric_names(
            LUSTRE_METRICS, AZURE_MONITOR_METRIC_NAMES_PER_REQUEST
        )
    ]
    assert metrics_client.metric_names == expected_batches


def test_collector_isolates_per_filesystem_metric_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "vmss_metrics_exporter.azure_managed_lustre.build_query_request",
        lambda **kwargs: kwargs,
    )

    class TwoFilesystemResourceGraphClient:
        def resources(self, _query: object) -> FakeResponse:
            return FakeResponse(
                [
                    {
                        "id": "/subscriptions/sub-a/resourceGroups/rg-a/providers/"
                        "Microsoft.StorageCache/amlFilesystems/lustre-a",
                        "subscriptionId": "sub-a",
                        "resourceGroup": "rg-a",
                        "filesystemName": "lustre-a",
                        "location": "westus3",
                    },
                    {
                        "id": "/subscriptions/sub-a/resourceGroups/rg-a/providers/"
                        "Microsoft.StorageCache/amlFilesystems/lustre-b",
                        "subscriptionId": "sub-a",
                        "resourceGroup": "rg-a",
                        "filesystemName": "lustre-b",
                        "location": "westus3",
                    },
                ]
            )

    collector = AzureManagedLustreCollector(
        TwoFilesystemResourceGraphClient(),
        PartiallyFailingMetricsClient(),
        ["sub-a"],
        request_jitter_seconds=0,
        retry_base_delay_seconds=0,
    )

    result = collector.collect()

    assert result.filesystem_count == 2
    assert {filesystem.filesystem_name for filesystem in result.filesystems} == {
        "lustre-a",
        "lustre-b",
    }
    assert result.error_count == 1
    assert len(result.metrics) == 1
    assert result.metrics[0].filesystem_name == "lustre-a"


def test_collector_applies_per_filesystem_request_jitter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep_calls: list[float] = []
    uniform_calls: list[tuple[float, float]] = []

    def fake_uniform(lower: float, upper: float) -> float:
        uniform_calls.append((lower, upper))
        return 0.125

    monkeypatch.setattr("vmss_metrics_exporter.azure_managed_lustre.random.uniform", fake_uniform)
    monkeypatch.setattr(
        "vmss_metrics_exporter.azure_managed_lustre.time.sleep",
        lambda delay: sleep_calls.append(delay),
    )
    collector = AzureManagedLustreCollector(
        FakeResourceGraphClient(),
        FakeMetricsClient(),
        ["sub-a"],
        request_jitter_seconds=0.4,
        retry_base_delay_seconds=0,
    )

    collector._collect_filesystem_metrics(
        ManagedLustreFilesystem(
            "sub-a",
            "rg-a",
            "lustre-a",
            "/subscriptions/sub-a/resourceGroups/rg-a/providers/"
            "Microsoft.StorageCache/amlFilesystems/lustre-a",
            "westus3",
        )
    )

    assert uniform_calls == [(0, 0.4)]
    assert sleep_calls == [0.125]


def test_collector_skips_request_jitter_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep_calls: list[float] = []
    uniform_calls: list[tuple[float, float]] = []

    monkeypatch.setattr(
        "vmss_metrics_exporter.azure_managed_lustre.random.uniform",
        lambda lower, upper: uniform_calls.append((lower, upper)) or 0.125,
    )
    monkeypatch.setattr(
        "vmss_metrics_exporter.azure_managed_lustre.time.sleep",
        lambda delay: sleep_calls.append(delay),
    )
    collector = AzureManagedLustreCollector(
        FakeResourceGraphClient(),
        FakeMetricsClient(),
        ["sub-a"],
        request_jitter_seconds=0,
        retry_base_delay_seconds=0,
    )

    collector._collect_filesystem_metrics(
        ManagedLustreFilesystem(
            "sub-a",
            "rg-a",
            "lustre-a",
            "/subscriptions/sub-a/resourceGroups/rg-a/providers/"
            "Microsoft.StorageCache/amlFilesystems/lustre-a",
            "westus3",
        )
    )

    assert uniform_calls == []
    assert sleep_calls == []


def test_parse_iso_duration_accepts_monitor_intervals() -> None:
    assert parse_iso_duration("PT1M").total_seconds() == 60
    assert parse_iso_duration("PT1H").total_seconds() == 3600


@pytest.mark.parametrize("value", ["", "P1D", "PT0M", "5m"])
def test_parse_iso_duration_rejects_invalid_values(value: str) -> None:
    with pytest.raises(ValueError):
        parse_iso_duration(value)


def test_summarize_lustre_metrics_empty() -> None:
    summary = summarize_lustre_metrics(
        ManagedLustreCollectionResult(
            metrics=(
                ManagedLustreOstMetric("sub-a", "rg-a", "lustre-a", "westus3", "0", 123.0),
            ),
            filesystem_count=1,
        )
    )

    assert "filesystem_name" in summary
    assert "bytes_available_percent" in summary
    assert "lustre-a" in summary


def test_summarize_lustre_metrics_with_filesystem_but_no_samples() -> None:
    """Mirrors the production state where the AMLFS is discovered but not yet mounted."""

    summary = summarize_lustre_metrics(
        ManagedLustreCollectionResult(metrics=(), filesystem_count=1)
    )

    assert "# no Lustre metric samples returned" in summary
    assert "OSTBytesAvailable" not in summary


def test_summarize_lustre_metrics_includes_operation_metrics() -> None:
    summary = summarize_lustre_metrics(
        ManagedLustreCollectionResult(
            metrics=(),
            filesystem_count=1,
            operation_metrics=(
                ManagedLustreOstOperationMetric(
                    "sub-a",
                    "rg-a",
                    "lustre-a",
                    "westus3",
                    "0",
                    "read",
                    client_latency_milliseconds=12.5,
                    client_ops=42.0,
                ),
            ),
        )
    )

    assert "client_latency_milliseconds" in summary
    assert "client_ops" in summary
    assert "read" in summary
    assert "# no Lustre metric samples returned" not in summary


def test_summarize_lustre_metrics_includes_mdt_metrics() -> None:
    summary = summarize_lustre_metrics(
        ManagedLustreCollectionResult(
            metrics=(),
            filesystem_count=1,
            mdt_metrics=(
                ManagedLustreMdtMetric(
                    "sub-a",
                    "rg-a",
                    "lustre-a",
                    "westus3",
                    "0",
                    bytes_available=700.0,
                    bytes_used=300.0,
                    bytes_total=1000.0,
                    files_free=80.0,
                    files_used=20.0,
                    files_total=100.0,
                    hsm_action_errors=3.0,
                    hsm_current_requests=5.0,
                ),
            ),
            mdt_operation_metrics=(
                ManagedLustreMdtOperationMetric(
                    "sub-a",
                    "rg-a",
                    "lustre-a",
                    "westus3",
                    "0",
                    "open",
                    client_latency_milliseconds=1.5,
                    client_ops=9.0,
                ),
            ),
        )
    )

    assert "mdtnum" in summary
    assert "700.0" in summary
    assert "open" in summary
    assert "hsm_action_errors" in summary
    assert "hsm_current_requests" in summary
    assert "# no Lustre metric samples returned" not in summary


def test_summarize_lustre_metrics_with_no_filesystems() -> None:
    summary = summarize_lustre_metrics(
        ManagedLustreCollectionResult(metrics=(), filesystem_count=0)
    )

    assert "# no Azure Managed Lustre filesystems discovered" in summary


def test_normalize_ost_capacity_response_handles_unmounted_filesystem() -> None:
    """Azure Monitor returns metric entries with empty timeseries when the FS has no OSTs in use."""

    filesystem = ManagedLustreFilesystem(
        "sub-a",
        "rg-a",
        "lustre-a",
        "/subscriptions/sub-a/resourceGroups/rg-a/providers/"
        "Microsoft.StorageCache/amlFilesystems/lustre-a",
        "westus3",
    )

    metrics = normalize_ost_capacity_response(
        filesystem,
        {
            "metrics": [
                {"name": "OSTBytesAvailable", "timeseries": []},
                {"name": "OSTBytesUsed", "timeseries": []},
                {"name": "OSTBytesTotal", "timeseries": []},
            ]
        },
    )

    assert list(metrics) == []


def test_normalize_ost_capacity_response_rejects_scalar_metrics_field() -> None:
    """Defensive: never iterate a string as a sequence of characters."""

    filesystem = ManagedLustreFilesystem(
        "sub-a",
        "rg-a",
        "lustre-a",
        "/subscriptions/sub-a/resourceGroups/rg-a/providers/"
        "Microsoft.StorageCache/amlFilesystems/lustre-a",
        "westus3",
    )

    metrics = normalize_ost_capacity_response(filesystem, {"metrics": "OSTBytesAvailable"})

    assert list(metrics) == []
