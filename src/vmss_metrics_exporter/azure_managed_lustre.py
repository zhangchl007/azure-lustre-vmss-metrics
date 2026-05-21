"""Azure Managed Lustre discovery and Azure Monitor metric collection."""

from __future__ import annotations

import logging
import random
import re
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from .azure_resource_graph import build_query_request
from .models import (
    ManagedLustreCollectionResult,
    ManagedLustreFilesystem,
    ManagedLustreMdtMetric,
    ManagedLustreMdtOperationMetric,
    ManagedLustreOstMetric,
    ManagedLustreOstOperationMetric,
    build_lustre_filesystem_aggregate_metrics,
)

LOGGER = logging.getLogger(__name__)

AGGREGATE_DIMENSION_VALUE = "all"

AMLFS_FILESYSTEMS_QUERY = """
Resources
| where type =~ 'microsoft.storagecache/amlfilesystems'
| project
    id,
    subscriptionId,
    resourceGroup,
    filesystemName = name,
    location,
    skuTier = tostring(sku.name),
    storageCapacityTiB = todouble(properties.storageCapacityTiB)
| order by subscriptionId asc, resourceGroup asc, filesystemName asc
""".strip()

LUSTRE_METRIC_NAMESPACE = "Microsoft.StorageCache/amlFilesystems"
# Azure Monitor's `query_resource` endpoint accepts at most 20 metric names per
# request. The exporter chunks `LUSTRE_METRICS` into batches of this size so we
# can keep growing the metric set without hitting the limit.
AZURE_MONITOR_METRIC_NAMES_PER_REQUEST = 20
OST_BYTES_AVAILABLE_METRIC = "OSTBytesAvailable"
OST_BYTES_USED_METRIC = "OSTBytesUsed"
OST_BYTES_TOTAL_METRIC = "OSTBytesTotal"
OST_CONNECTED_CLIENTS_METRIC = "OSTConnectedClients"
OST_CLIENT_LATENCY_METRIC = "OSTClientLatency"
OST_CLIENT_OPS_METRIC = "OSTClientOps"
CLIENT_READ_OPS_METRIC = "ClientReadOps"
CLIENT_READ_THROUGHPUT_METRIC = "ClientReadThroughput"
CLIENT_WRITE_OPS_METRIC = "ClientWriteOps"
CLIENT_WRITE_THROUGHPUT_METRIC = "ClientWriteThroughput"
MDT_BYTES_AVAILABLE_METRIC = "MDTBytesAvailable"
MDT_BYTES_USED_METRIC = "MDTBytesUsed"
MDT_BYTES_TOTAL_METRIC = "MDTBytesTotal"
MDT_CONNECTED_CLIENTS_METRIC = "MDTConnectedClients"
MDT_FILES_FREE_METRIC = "MDTFilesFree"
MDT_FILES_USED_METRIC = "MDTFilesUsed"
MDT_FILES_TOTAL_METRIC = "MDTFilesTotal"
MDT_CLIENT_LATENCY_METRIC = "MDTClientLatency"
MDT_CLIENT_OPS_METRIC = "MDTClientOps"
HSM_ACTION_ERRORS_METRIC = "HSMActionErrors"
HSM_CURRENT_REQUESTS_METRIC = "HSMCurrentRequests"
OST_CAPACITY_METRICS = (
    OST_BYTES_AVAILABLE_METRIC,
    OST_BYTES_USED_METRIC,
    OST_BYTES_TOTAL_METRIC,
)
OST_SIMPLE_METRICS = (
    OST_BYTES_AVAILABLE_METRIC,
    OST_BYTES_USED_METRIC,
    OST_BYTES_TOTAL_METRIC,
    OST_CONNECTED_CLIENTS_METRIC,
    CLIENT_READ_OPS_METRIC,
    CLIENT_READ_THROUGHPUT_METRIC,
    CLIENT_WRITE_OPS_METRIC,
    CLIENT_WRITE_THROUGHPUT_METRIC,
)
OST_OPERATION_METRICS = (
    OST_CLIENT_LATENCY_METRIC,
    OST_CLIENT_OPS_METRIC,
)
OST_METRICS = OST_SIMPLE_METRICS + OST_OPERATION_METRICS
MDT_SIMPLE_METRICS = (
    MDT_BYTES_AVAILABLE_METRIC,
    MDT_BYTES_USED_METRIC,
    MDT_BYTES_TOTAL_METRIC,
    MDT_CONNECTED_CLIENTS_METRIC,
    MDT_FILES_FREE_METRIC,
    MDT_FILES_USED_METRIC,
    MDT_FILES_TOTAL_METRIC,
    HSM_ACTION_ERRORS_METRIC,
    HSM_CURRENT_REQUESTS_METRIC,
)
MDT_OPERATION_METRICS = (
    MDT_CLIENT_LATENCY_METRIC,
    MDT_CLIENT_OPS_METRIC,
)
MDT_METRICS = MDT_SIMPLE_METRICS + MDT_OPERATION_METRICS
LUSTRE_METRICS = OST_METRICS + MDT_METRICS

_ISO_DURATION_PATTERN = re.compile(r"^PT(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?$", re.IGNORECASE)


class ResourceGraphClientProtocol(Protocol):
    """Protocol for the Resource Graph client subset used by this module."""

    def resources(self, query: object) -> object:
        """Execute a Resource Graph query request."""


class MetricsQueryClientProtocol(Protocol):
    """Protocol for the Azure Monitor metrics client subset used by this module."""

    def query_resource(
        self,
        resource_uri: str,
        metric_names: Sequence[str],
        **kwargs: Any,
    ) -> object:
        """Query metrics for one Azure resource."""


class AzureManagedLustreCollector:
    """Collect Azure Managed Lustre OST metrics for all configured subscriptions."""

    def __init__(
        self,
        resource_graph_client: ResourceGraphClientProtocol,
        metrics_client: MetricsQueryClientProtocol,
        subscription_ids: Sequence[str],
        *,
        page_size: int = 1000,
        lookback_minutes: int = 15,
        interval: str = "PT1M",
        max_workers: int = 4,
        request_jitter_seconds: float = 0.5,
        max_retries: int = 3,
        retry_base_delay_seconds: float = 1.0,
    ) -> None:
        if not subscription_ids:
            raise ValueError("subscription_ids cannot be empty")
        self._resource_graph_client = resource_graph_client
        self._metrics_client = metrics_client
        self._subscription_ids = tuple(subscription_ids)
        self._page_size = page_size
        self._lookback = timedelta(minutes=lookback_minutes)
        self._granularity = parse_iso_duration(interval)
        self._max_workers = max_workers
        self._request_jitter_seconds = request_jitter_seconds
        self._max_retries = max_retries
        self._retry_base_delay_seconds = retry_base_delay_seconds

    def collect(self) -> ManagedLustreCollectionResult:
        """Return OST metric samples for all discovered AMLFS resources."""

        filesystems = tuple(self.discover_filesystems())
        if not filesystems:
            return ManagedLustreCollectionResult(
                metrics=(),
                filesystem_count=0,
                filesystems=(),
            )

        metrics: list[ManagedLustreOstMetric] = []
        operation_metrics: list[ManagedLustreOstOperationMetric] = []
        mdt_metrics: list[ManagedLustreMdtMetric] = []
        mdt_operation_metrics: list[ManagedLustreMdtOperationMetric] = []
        error_count = 0
        worker_count = min(self._max_workers, len(filesystems))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(self._collect_filesystem_metrics, filesystem): filesystem
                for filesystem in filesystems
            }
            for future in as_completed(futures):
                filesystem = futures[future]
                try:
                    (
                        filesystem_metrics,
                        filesystem_operation_metrics,
                        filesystem_mdt_metrics,
                        filesystem_mdt_operation_metrics,
                    ) = future.result()
                    metrics.extend(filesystem_metrics)
                    operation_metrics.extend(filesystem_operation_metrics)
                    mdt_metrics.extend(filesystem_mdt_metrics)
                    mdt_operation_metrics.extend(filesystem_mdt_operation_metrics)
                except Exception:  # noqa: BLE001 - isolate per-resource Azure Monitor failures.
                    error_count += 1
                    LOGGER.exception(
                        "Failed to collect Managed Lustre metrics for %s",
                        filesystem.resource_id,
                    )

        metrics.sort(
            key=lambda item: (
                item.subscription_id,
                item.resource_group,
                item.filesystem_name,
                item.ostnum,
            )
        )
        operation_metrics.sort(
            key=lambda item: (
                item.subscription_id,
                item.resource_group,
                item.filesystem_name,
                item.ostnum,
                item.operation,
            )
        )
        mdt_metrics.sort(
            key=lambda item: (
                item.subscription_id,
                item.resource_group,
                item.filesystem_name,
                item.mdtnum,
            )
        )
        mdt_operation_metrics.sort(
            key=lambda item: (
                item.subscription_id,
                item.resource_group,
                item.filesystem_name,
                item.mdtnum,
                item.operation,
            )
        )
        filesystem_aggregate_metrics = build_lustre_filesystem_aggregate_metrics(
            filesystems=filesystems,
            metrics=metrics,
            operation_metrics=operation_metrics,
            mdt_metrics=mdt_metrics,
            mdt_operation_metrics=mdt_operation_metrics,
            now_seconds=time.time(),
        )
        return ManagedLustreCollectionResult(
            metrics=tuple(metrics),
            filesystem_count=len(filesystems),
            error_count=error_count,
            filesystems=filesystems,
            operation_metrics=tuple(operation_metrics),
            mdt_metrics=tuple(mdt_metrics),
            mdt_operation_metrics=tuple(mdt_operation_metrics),
            filesystem_aggregate_metrics=filesystem_aggregate_metrics,
        )

    def discover_filesystems(self) -> list[ManagedLustreFilesystem]:
        """Discover Azure Managed Lustre filesystems across configured subscriptions."""

        rows = self._run_paged_query(AMLFS_FILESYSTEMS_QUERY)
        return [normalize_filesystem_row(row) for row in rows]

    def _run_paged_query(self, query: str) -> list[Mapping[str, Any]]:
        all_rows: list[Mapping[str, Any]] = []
        skip_token: str | None = None

        while True:
            request = build_query_request(
                subscriptions=self._subscription_ids,
                query=query,
                top=self._page_size,
                skip_token=skip_token,
            )
            response = self._execute_with_retry(
                lambda request=request: self._resource_graph_client.resources(request)
            )
            data = getattr(response, "data", None) or []
            if not isinstance(data, list):
                raise TypeError(f"Unexpected Resource Graph response data type: {type(data)!r}")
            all_rows.extend(data)

            skip_token = getattr(response, "skip_token", None)
            if not skip_token:
                return all_rows

    def _collect_filesystem_metrics(
        self, filesystem: ManagedLustreFilesystem
    ) -> tuple[
        list[ManagedLustreOstMetric],
        list[ManagedLustreOstOperationMetric],
        list[ManagedLustreMdtMetric],
        list[ManagedLustreMdtOperationMetric],
    ]:
        # Azure Monitor caps each query_resource call at 20 metric names, so we
        # split LUSTRE_METRICS into batches and merge the per-batch responses
        # into a single synthetic response for normalization.
        self._apply_request_jitter()
        merged_metrics: list[Any] = []
        for batch in _chunk_metric_names(
            LUSTRE_METRICS, AZURE_MONITOR_METRIC_NAMES_PER_REQUEST
        ):
            response = self._execute_with_retry(
                lambda batch=batch: self._metrics_client.query_resource(
                    filesystem.resource_id,
                    list(batch),
                    metric_namespace=LUSTRE_METRIC_NAMESPACE,
                    timespan=self._lookback,
                    granularity=self._granularity,
                    aggregations=["Average"],
                )
            )
            merged_metrics.extend(_iter_sequence_attr(response, "metrics"))
        return normalize_lustre_metrics_response(filesystem, {"metrics": merged_metrics})

    def _apply_request_jitter(self) -> None:
        if self._request_jitter_seconds <= 0:
            return
        time.sleep(random.uniform(0, self._request_jitter_seconds))

    def _execute_with_retry(self, operation: Any) -> object:
        attempt = 0
        while True:
            try:
                return operation()
            except Exception as exc:  # noqa: BLE001 - Azure SDK raises multiple transient types.
                if attempt >= self._max_retries or not _is_retryable_exception(exc):
                    raise
                delay = self._retry_delay(attempt)
                LOGGER.warning(
                    "Azure Managed Lustre collection call failed on attempt %s/%s; "
                    "retrying in %.2fs: %s",
                    attempt + 1,
                    self._max_retries + 1,
                    delay,
                    exc,
                )
                time.sleep(delay)
                attempt += 1

    def _retry_delay(self, attempt: int) -> float:
        jitter = random.uniform(0, self._retry_base_delay_seconds)
        return (self._retry_base_delay_seconds * (2**attempt)) + jitter


def create_metrics_query_client() -> MetricsQueryClientProtocol:
    """Create an authenticated Azure Monitor metrics query client."""

    try:
        from azure.monitor.query import MetricsQueryClient

        from .credentials import create_credential
    except ImportError as exc:  # pragma: no cover - exercised only when dependencies are missing.
        raise RuntimeError(
            "azure-monitor-query is not installed. Install the project dependencies first."
        ) from exc

    return MetricsQueryClient(create_credential())


def normalize_filesystem_row(row: Mapping[str, Any]) -> ManagedLustreFilesystem:
    """Normalize one Resource Graph AMLFS row into a filesystem model."""

    return ManagedLustreFilesystem(
        subscription_id=_required_str(row, "subscriptionId"),
        resource_group=_required_str(row, "resourceGroup"),
        filesystem_name=_required_str(row, "filesystemName"),
        resource_id=_required_str(row, "id"),
        location=_optional_str(row, "location", default="unknown"),
        sku_tier=_optional_str(row, "skuTier", default="unknown"),
        storage_capacity_tib=_float_value(row.get("storageCapacityTiB"), default=0.0),
    )


def normalize_ost_bytes_available_response(
    filesystem: ManagedLustreFilesystem,
    response: object,
) -> list[ManagedLustreOstMetric]:
    """Normalize an Azure Monitor response to latest non-null OST samples.

    Kept as a compatibility wrapper for older tests/callers. It now accepts
    responses containing any of the supported OST metrics.
    """

    return normalize_ost_capacity_response(filesystem, response)


def normalize_ost_capacity_response(
    filesystem: ManagedLustreFilesystem,
    response: object,
) -> list[ManagedLustreOstMetric]:
    """Normalize Azure Monitor OST metrics into one sample per OST.

    Kept as a compatibility wrapper for older callers. Operation-dimension
    metrics such as `OSTClientLatency` are ignored by this wrapper; use
    :func:`normalize_ost_metrics_response` for the complete result.
    """

    metrics, _operation_metrics = normalize_ost_metrics_response(filesystem, response)
    return metrics


def normalize_ost_metrics_response(
    filesystem: ManagedLustreFilesystem,
    response: object,
) -> tuple[list[ManagedLustreOstMetric], list[ManagedLustreOstOperationMetric]]:
    """Normalize Azure Monitor OST metrics into Prometheus-ready samples."""

    ost_metrics, ost_operation_metrics, _mdt_metrics, _mdt_operation_metrics = (
        normalize_lustre_metrics_response(filesystem, response)
    )
    return ost_metrics, ost_operation_metrics


def normalize_lustre_metrics_response(
    filesystem: ManagedLustreFilesystem,
    response: object,
) -> tuple[
    list[ManagedLustreOstMetric],
    list[ManagedLustreOstOperationMetric],
    list[ManagedLustreMdtMetric],
    list[ManagedLustreMdtOperationMetric],
]:
    """Normalize Azure Monitor Lustre metrics into Prometheus-ready samples."""

    ost_values: dict[str, dict[str, float | None]] = {}
    ost_timestamps: dict[str, float] = {}
    operation_values: dict[tuple[str, str], dict[str, float | None]] = {}
    operation_timestamps: dict[tuple[str, str], float] = {}
    mdt_values: dict[str, dict[str, float | None]] = {}
    mdt_timestamps: dict[str, float] = {}
    mdt_operation_values: dict[tuple[str, str], dict[str, float | None]] = {}
    mdt_operation_timestamps: dict[tuple[str, str], float] = {}

    for metric in _iter_sequence_attr(response, "metrics"):
        metric_name = _metric_name(metric)
        if metric_name not in LUSTRE_METRICS:
            continue
        for time_series in _iter_sequence_attr(metric, "timeseries", fallback="time_series"):
            latest = _latest_average(time_series)
            if latest is None:
                continue
            value, sample_timestamp_seconds = latest
            ostnum: str | None = None
            mdtnum: str | None = None
            if metric_name in OST_METRICS:
                ostnum = _dimension_value_or_aggregate(time_series, "ostnum")
                if not ostnum:
                    continue
            elif metric_name in MDT_METRICS:
                mdtnum = _dimension_value_or_aggregate(time_series, "mdtnum")
                if not mdtnum:
                    continue
            else:
                continue

            if metric_name in OST_OPERATION_METRICS:
                operation = _dimension_value(time_series, "operation") or AGGREGATE_DIMENSION_VALUE
                key = (ostnum, operation)
                operation_values.setdefault(key, {})[metric_name] = value
                if sample_timestamp_seconds is not None:
                    operation_timestamps[key] = max(
                        sample_timestamp_seconds,
                        operation_timestamps.get(key, 0),
                    )
            elif metric_name in OST_SIMPLE_METRICS:
                ost_values.setdefault(ostnum, {})[metric_name] = value
                if sample_timestamp_seconds is not None:
                    ost_timestamps[ostnum] = max(
                        sample_timestamp_seconds,
                        ost_timestamps.get(ostnum, 0),
                    )
            elif metric_name in MDT_OPERATION_METRICS:
                operation = _dimension_value(time_series, "operation") or AGGREGATE_DIMENSION_VALUE
                key = (mdtnum, operation)
                mdt_operation_values.setdefault(key, {})[metric_name] = value
                if sample_timestamp_seconds is not None:
                    mdt_operation_timestamps[key] = max(
                        sample_timestamp_seconds,
                        mdt_operation_timestamps.get(key, 0),
                    )
            elif metric_name in MDT_SIMPLE_METRICS:
                mdt_values.setdefault(mdtnum, {})[metric_name] = value
                if sample_timestamp_seconds is not None:
                    mdt_timestamps[mdtnum] = max(
                        sample_timestamp_seconds,
                        mdt_timestamps.get(mdtnum, 0),
                    )

    results: list[ManagedLustreOstMetric] = []
    for ostnum, values in ost_values.items():
        results.append(
            ManagedLustreOstMetric(
                subscription_id=filesystem.subscription_id,
                resource_group=filesystem.resource_group,
                filesystem_name=filesystem.filesystem_name,
                location=filesystem.location,
                ostnum=ostnum,
                bytes_available=values.get(OST_BYTES_AVAILABLE_METRIC),
                bytes_used=values.get(OST_BYTES_USED_METRIC),
                bytes_total=values.get(OST_BYTES_TOTAL_METRIC),
                client_read_ops=values.get(CLIENT_READ_OPS_METRIC),
                client_read_throughput_bytes_per_second=values.get(
                    CLIENT_READ_THROUGHPUT_METRIC
                ),
                client_write_ops=values.get(CLIENT_WRITE_OPS_METRIC),
                client_write_throughput_bytes_per_second=values.get(
                    CLIENT_WRITE_THROUGHPUT_METRIC
                ),
                connected_clients=values.get(OST_CONNECTED_CLIENTS_METRIC),
                sample_timestamp_seconds=ost_timestamps.get(ostnum),
            )
        )

    operation_results: list[ManagedLustreOstOperationMetric] = []
    for (ostnum, operation), values in operation_values.items():
        operation_results.append(
            ManagedLustreOstOperationMetric(
                subscription_id=filesystem.subscription_id,
                resource_group=filesystem.resource_group,
                filesystem_name=filesystem.filesystem_name,
                location=filesystem.location,
                ostnum=ostnum,
                operation=operation,
                client_latency_milliseconds=values.get(OST_CLIENT_LATENCY_METRIC),
                client_ops=values.get(OST_CLIENT_OPS_METRIC),
                sample_timestamp_seconds=operation_timestamps.get((ostnum, operation)),
            )
        )

    mdt_results: list[ManagedLustreMdtMetric] = []
    for mdtnum, values in mdt_values.items():
        mdt_results.append(
            ManagedLustreMdtMetric(
                subscription_id=filesystem.subscription_id,
                resource_group=filesystem.resource_group,
                filesystem_name=filesystem.filesystem_name,
                location=filesystem.location,
                mdtnum=mdtnum,
                bytes_available=values.get(MDT_BYTES_AVAILABLE_METRIC),
                bytes_used=values.get(MDT_BYTES_USED_METRIC),
                bytes_total=values.get(MDT_BYTES_TOTAL_METRIC),
                files_free=values.get(MDT_FILES_FREE_METRIC),
                files_used=values.get(MDT_FILES_USED_METRIC),
                files_total=values.get(MDT_FILES_TOTAL_METRIC),
                hsm_action_errors=values.get(HSM_ACTION_ERRORS_METRIC),
                hsm_current_requests=values.get(HSM_CURRENT_REQUESTS_METRIC),
                connected_clients=values.get(MDT_CONNECTED_CLIENTS_METRIC),
                sample_timestamp_seconds=mdt_timestamps.get(mdtnum),
            )
        )

    mdt_operation_results: list[ManagedLustreMdtOperationMetric] = []
    for (mdtnum, operation), values in mdt_operation_values.items():
        mdt_operation_results.append(
            ManagedLustreMdtOperationMetric(
                subscription_id=filesystem.subscription_id,
                resource_group=filesystem.resource_group,
                filesystem_name=filesystem.filesystem_name,
                location=filesystem.location,
                mdtnum=mdtnum,
                operation=operation,
                client_latency_milliseconds=values.get(MDT_CLIENT_LATENCY_METRIC),
                client_ops=values.get(MDT_CLIENT_OPS_METRIC),
                sample_timestamp_seconds=mdt_operation_timestamps.get((mdtnum, operation)),
            )
        )
    return results, operation_results, mdt_results, mdt_operation_results


def parse_iso_duration(value: str) -> timedelta:
    """Parse the small ISO-8601 duration subset Azure Monitor intervals use."""

    match = _ISO_DURATION_PATTERN.match(value.strip())
    if not match:
        raise ValueError(f"Lustre metrics interval must look like PT1M or PT1H, got {value!r}")
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    duration = timedelta(hours=hours, minutes=minutes)
    if duration <= timedelta(0):
        raise ValueError(f"Lustre metrics interval must be greater than zero, got {value!r}")
    return duration


def summarize_lustre_metrics(result: ManagedLustreCollectionResult) -> str:
    """Create a compact human-readable summary for one-shot runs."""

    lines = [
        "subscription_id\tresource_group\tfilesystem_name\tlocation\tostnum\t"
        "bytes_available\tbytes_used\tbytes_total\tbytes_available_percent\t"
        "connected_clients\t"
        "client_read_ops\tclient_read_throughput_bytes_per_second\tclient_write_ops\t"
        "client_write_throughput_bytes_per_second"
    ]
    for item in result.metrics:
        lines.append(
            "\t".join(
                [
                    item.subscription_id,
                    item.resource_group,
                    item.filesystem_name,
                    item.location,
                    item.ostnum,
                    _optional_float_str(item.bytes_available),
                    _optional_float_str(item.bytes_used),
                    _optional_float_str(item.bytes_total),
                    _optional_float_str(item.bytes_available_percent),
                    _optional_float_str(item.connected_clients),
                    _optional_float_str(item.client_read_ops),
                    _optional_float_str(item.client_read_throughput_bytes_per_second),
                    _optional_float_str(item.client_write_ops),
                    _optional_float_str(item.client_write_throughput_bytes_per_second),
                ]
            )
        )
    if result.operation_metrics:
        lines.append("")
        lines.append(
            "subscription_id\tresource_group\tfilesystem_name\tlocation\tostnum\toperation\t"
            "client_latency_milliseconds\tclient_ops"
        )
        for item in result.operation_metrics:
            lines.append(
                "\t".join(
                    [
                        item.subscription_id,
                        item.resource_group,
                        item.filesystem_name,
                        item.location,
                        item.ostnum,
                        item.operation,
                        _optional_float_str(item.client_latency_milliseconds),
                        _optional_float_str(item.client_ops),
                    ]
                )
            )
    if result.mdt_metrics:
        lines.append("")
        lines.append(
            "subscription_id\tresource_group\tfilesystem_name\tlocation\tmdtnum\t"
            "bytes_available\tbytes_used\tbytes_total\tbytes_available_percent\t"
            "connected_clients\t"
            "files_free\tfiles_used\tfiles_total\tfiles_free_percent\t"
            "hsm_action_errors\thsm_current_requests"
        )
        for item in result.mdt_metrics:
            lines.append(
                "\t".join(
                    [
                        item.subscription_id,
                        item.resource_group,
                        item.filesystem_name,
                        item.location,
                        item.mdtnum,
                        _optional_float_str(item.bytes_available),
                        _optional_float_str(item.bytes_used),
                        _optional_float_str(item.bytes_total),
                        _optional_float_str(item.bytes_available_percent),
                        _optional_float_str(item.connected_clients),
                        _optional_float_str(item.files_free),
                        _optional_float_str(item.files_used),
                        _optional_float_str(item.files_total),
                        _optional_float_str(item.files_free_percent),
                        _optional_float_str(item.hsm_action_errors),
                        _optional_float_str(item.hsm_current_requests),
                    ]
                )
            )
    if result.mdt_operation_metrics:
        lines.append("")
        lines.append(
            "subscription_id\tresource_group\tfilesystem_name\tlocation\tmdtnum\toperation\t"
            "client_latency_milliseconds\tclient_ops"
        )
        for item in result.mdt_operation_metrics:
            lines.append(
                "\t".join(
                    [
                        item.subscription_id,
                        item.resource_group,
                        item.filesystem_name,
                        item.location,
                        item.mdtnum,
                        item.operation,
                        _optional_float_str(item.client_latency_milliseconds),
                        _optional_float_str(item.client_ops),
                    ]
                )
            )
    has_samples = any(
        (
            result.metrics,
            result.operation_metrics,
            result.mdt_metrics,
            result.mdt_operation_metrics,
        )
    )
    if not has_samples and result.filesystem_count == 0:
        lines.append("# no Azure Managed Lustre filesystems discovered")
    elif not has_samples:
        lines.append("# no Lustre metric samples returned")
    return "\n".join(lines)


def _optional_float_str(value: float | None) -> str:
    if value is None:
        return ""
    return str(value)


def _chunk_metric_names(
    metric_names: Sequence[str], chunk_size: int
) -> list[tuple[str, ...]]:
    """Split a metric-name list into chunks no larger than ``chunk_size``.

    Azure Monitor's ``query_resource`` endpoint accepts at most 20 metric names
    per call. The collector batches ``LUSTRE_METRICS`` through this helper and
    merges the resulting per-batch responses.
    """

    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    items = list(metric_names)
    return [tuple(items[i : i + chunk_size]) for i in range(0, len(items), chunk_size)]


def _is_retryable_exception(exc: Exception) -> bool:
    try:
        from azure.core.exceptions import ClientAuthenticationError
        from azure.identity import CredentialUnavailableError
    except ImportError:  # pragma: no cover - azure deps required at runtime.
        auth_error_types: tuple[type[BaseException], ...] = ()
    else:
        auth_error_types = (ClientAuthenticationError, CredentialUnavailableError)
    if auth_error_types and isinstance(exc, auth_error_types):
        return False

    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None) or getattr(exc, "status_code", None)
    if status_code is None:
        return True
    return status_code in {408, 409, 429, 500, 502, 503, 504}


def _required_str(row: Mapping[str, Any], key: str) -> str:
    value = row.get(key)
    if value is None or str(value).strip() == "":
        raise ValueError(f"Resource Graph row is missing required field {key!r}: {row!r}")
    return str(value)


def _optional_str(row: Mapping[str, Any], key: str, *, default: str) -> str:
    value = row.get(key)
    if value is None or str(value).strip() == "":
        return default
    return str(value)


def _float_value(value: Any, *, default: float) -> float:
    if value is None or value == "":
        return default
    return float(value)


def _iter_sequence_attr(item: object, name: str, *, fallback: str | None = None) -> list[Any]:
    value = _attr_or_mapping(item, name)
    if value is None and fallback is not None:
        value = _attr_or_mapping(item, fallback)
    if value is None:
        return []
    # Defensively reject scalar str/bytes so we never iterate them as character lists.
    if isinstance(value, (str, bytes)):
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    try:
        return list(value)
    except TypeError:
        return []


def _attr_or_mapping(item: object, name: str) -> Any:
    if isinstance(item, Mapping):
        return item.get(name)
    return getattr(item, name, None)


def _metric_name(metric: object) -> str | None:
    name = _attr_or_mapping(metric, "name")
    value = _attr_or_mapping(name, "value") if name is not None else None
    return str(value if value is not None else name) if name is not None else None


def _dimension_value(time_series: object, dimension_name: str) -> str | None:
    for metadata in _iter_sequence_attr(time_series, "metadata_values"):
        name = _attr_or_mapping(metadata, "name")
        raw_name = _attr_or_mapping(name, "value") if name is not None else None
        raw_name = raw_name if raw_name is not None else name
        if raw_name is not None and str(raw_name).lower() == dimension_name.lower():
            value = _attr_or_mapping(metadata, "value")
            if value is not None and str(value).strip():
                return str(value)
    return None


def _dimension_value_or_aggregate(time_series: object, dimension_name: str) -> str | None:
    metadata_values = _iter_sequence_attr(time_series, "metadata_values")
    if not metadata_values:
        return AGGREGATE_DIMENSION_VALUE
    return _dimension_value(time_series, dimension_name)


def _latest_average(time_series: object) -> tuple[float, float | None] | None:
    for point in reversed(_iter_sequence_attr(time_series, "data")):
        average = _attr_or_mapping(point, "average")
        if average is not None:
            return float(average), _timestamp_seconds(point)
    return None


def _timestamp_seconds(point: object) -> float | None:
    value = _attr_or_mapping(point, "timestamp") or _attr_or_mapping(point, "time_stamp")
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).timestamp()
    return None
