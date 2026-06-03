"""Shared data models for Azure metric collection."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class VmssCount:
    """Normalized VM Scale Set count data ready for metric exposition."""

    subscription_id: str
    resource_group: str
    vmss_name: str
    location: str
    orchestration_mode: str
    actual_instance_count: int
    capacity: int
    vm_size: str = "unknown"
    sku_tier: str = "unknown"

    @property
    def label_values(self) -> tuple[str, str, str, str, str]:
        """Return labels for `azure_vmss_instance_count` and `azure_vmss_capacity`."""

        return (
            self.subscription_id,
            self.resource_group,
            self.vmss_name,
            self.location,
            self.orchestration_mode,
        )

    @property
    def info_label_values(self) -> tuple[str, str, str, str, str, str, str]:
        """Return labels for the `azure_vmss_info` metadata metric."""

        return (
            self.subscription_id,
            self.resource_group,
            self.vmss_name,
            self.location,
            self.orchestration_mode,
            self.vm_size,
            self.sku_tier,
        )


@dataclass(frozen=True, slots=True)
class ManagedLustreFilesystem:
    """Discovered Azure Managed Lustre filesystem resource."""

    subscription_id: str
    resource_group: str
    filesystem_name: str
    resource_id: str
    location: str
    sku_tier: str = "unknown"
    storage_capacity_tib: float = 0.0

    @property
    def info_label_values(self) -> tuple[str, str, str, str, str]:
        """Return labels for the `azure_managed_lustre_filesystem_info` metric."""

        return (
            self.subscription_id,
            self.resource_group,
            self.filesystem_name,
            self.location,
            self.sku_tier,
        )

    @property
    def capacity_label_values(self) -> tuple[str, str, str, str]:
        """Return labels for the filesystem storage-capacity metric."""

        return (
            self.subscription_id,
            self.resource_group,
            self.filesystem_name,
            self.location,
        )


@dataclass(frozen=True, slots=True)
class ManagedLustreOstMetric:
    """Normalized Azure Managed Lustre OST metric sample ready for Prometheus."""

    subscription_id: str
    resource_group: str
    filesystem_name: str
    location: str
    ostnum: str
    bytes_available: float | None = None
    bytes_used: float | None = None
    bytes_total: float | None = None
    client_read_ops: float | None = None
    client_read_throughput_bytes_per_second: float | None = None
    client_write_ops: float | None = None
    client_write_throughput_bytes_per_second: float | None = None
    connected_clients: float | None = None
    sample_timestamp_seconds: float | None = None

    @property
    def label_values(self) -> tuple[str, str, str, str, str]:
        """Return labels for `azure_managed_lustre_ost_bytes_available`."""

        return (
            self.subscription_id,
            self.resource_group,
            self.filesystem_name,
            self.location,
            self.ostnum,
        )

    @property
    def bytes_available_percent(self) -> float | None:
        """Return available capacity percentage when total bytes are known."""

        if self.bytes_available is None or self.bytes_total is None or self.bytes_total <= 0:
            return None
        return (self.bytes_available / self.bytes_total) * 100

    @property
    def bytes_used_percent(self) -> float | None:
        """Return used capacity percentage when used and total bytes are known."""

        if self.bytes_used is None or self.bytes_total is None or self.bytes_total <= 0:
            return None
        return (self.bytes_used / self.bytes_total) * 100


@dataclass(frozen=True, slots=True)
class ManagedLustreOstOperationMetric:
    """Normalized Azure Managed Lustre OST operation metric sample."""

    subscription_id: str
    resource_group: str
    filesystem_name: str
    location: str
    ostnum: str
    operation: str
    client_latency_milliseconds: float | None = None
    client_ops: float | None = None
    sample_timestamp_seconds: float | None = None

    @property
    def label_values(self) -> tuple[str, str, str, str, str, str]:
        """Return labels for operation-dimension Lustre OST metrics."""

        return (
            self.subscription_id,
            self.resource_group,
            self.filesystem_name,
            self.location,
            self.ostnum,
            self.operation,
        )


@dataclass(frozen=True, slots=True)
class ManagedLustreMdtMetric:
    """Normalized Azure Managed Lustre MDT metric sample ready for Prometheus."""

    subscription_id: str
    resource_group: str
    filesystem_name: str
    location: str
    mdtnum: str
    bytes_available: float | None = None
    bytes_used: float | None = None
    bytes_total: float | None = None
    files_free: float | None = None
    files_used: float | None = None
    files_total: float | None = None
    hsm_action_errors: float | None = None
    hsm_current_requests: float | None = None
    connected_clients: float | None = None
    sample_timestamp_seconds: float | None = None

    @property
    def label_values(self) -> tuple[str, str, str, str, str]:
        """Return labels for per-MDT Lustre metrics."""

        return (
            self.subscription_id,
            self.resource_group,
            self.filesystem_name,
            self.location,
            self.mdtnum,
        )

    @property
    def bytes_available_percent(self) -> float | None:
        """Return available MDT capacity percentage when total bytes are known."""

        if self.bytes_available is None or self.bytes_total is None or self.bytes_total <= 0:
            return None
        return (self.bytes_available / self.bytes_total) * 100

    @property
    def bytes_used_percent(self) -> float | None:
        """Return used MDT capacity percentage when used and total bytes are known."""

        if self.bytes_used is None or self.bytes_total is None or self.bytes_total <= 0:
            return None
        return (self.bytes_used / self.bytes_total) * 100

    @property
    def files_free_percent(self) -> float | None:
        """Return free MDT inode/file percentage when total files are known."""

        if self.files_free is None or self.files_total is None or self.files_total <= 0:
            return None
        return (self.files_free / self.files_total) * 100

    @property
    def files_used_percent(self) -> float | None:
        """Return used MDT inode/file percentage when used and total files are known."""

        if self.files_used is None or self.files_total is None or self.files_total <= 0:
            return None
        return (self.files_used / self.files_total) * 100


@dataclass(frozen=True, slots=True)
class ManagedLustreMdtOperationMetric:
    """Normalized Azure Managed Lustre MDT operation metric sample."""

    subscription_id: str
    resource_group: str
    filesystem_name: str
    location: str
    mdtnum: str
    operation: str
    client_latency_milliseconds: float | None = None
    client_ops: float | None = None
    sample_timestamp_seconds: float | None = None

    @property
    def label_values(self) -> tuple[str, str, str, str, str, str]:
        """Return labels for operation-dimension Lustre MDT metrics."""

        return (
            self.subscription_id,
            self.resource_group,
            self.filesystem_name,
            self.location,
            self.mdtnum,
            self.operation,
        )


@dataclass(frozen=True, slots=True)
class ManagedLustreFilesystemAggregateMetric:
    """Derived per-filesystem Azure Managed Lustre metric sample."""

    subscription_id: str
    resource_group: str
    filesystem_name: str
    location: str
    connected_clients: float | None = None
    metadata_amplification_ratio: float | None = None
    sample_max_age_seconds: float | None = None

    @property
    def label_values(self) -> tuple[str, str, str, str]:
        """Return labels for per-filesystem derived Lustre metrics."""

        return (
            self.subscription_id,
            self.resource_group,
            self.filesystem_name,
            self.location,
        )


def build_lustre_filesystem_aggregate_metrics(
    *,
    filesystems: Sequence[ManagedLustreFilesystem],
    metrics: Sequence[ManagedLustreOstMetric],
    operation_metrics: Sequence[ManagedLustreOstOperationMetric],
    mdt_metrics: Sequence[ManagedLustreMdtMetric],
    mdt_operation_metrics: Sequence[ManagedLustreMdtOperationMetric],
    now_seconds: float,
) -> tuple[ManagedLustreFilesystemAggregateMetric, ...]:
    """Build derived per-filesystem metrics from normalized OST/MDT samples.

    Filesystems without any Azure Monitor samples are intentionally omitted so
    dashboards can distinguish discovered-but-idle resources via
    ``azure_managed_lustre_filesystem_info`` while derived gauges represent only
    filesystems with data backing the computed values.
    """

    keys = {filesystem.capacity_label_values for filesystem in filesystems}
    keys.update(_filesystem_metric_key(metric) for metric in metrics)
    keys.update(_filesystem_metric_key(metric) for metric in operation_metrics)
    keys.update(_filesystem_metric_key(metric) for metric in mdt_metrics)
    keys.update(_filesystem_metric_key(metric) for metric in mdt_operation_metrics)

    results: list[ManagedLustreFilesystemAggregateMetric] = []
    for key in sorted(keys):
        filesystem_metrics = [
            metric for metric in metrics if _filesystem_metric_key(metric) == key
        ]
        filesystem_operation_metrics = [
            metric for metric in operation_metrics if _filesystem_metric_key(metric) == key
        ]
        filesystem_mdt_metrics = [
            metric for metric in mdt_metrics if _filesystem_metric_key(metric) == key
        ]
        filesystem_mdt_operation_metrics = [
            metric for metric in mdt_operation_metrics if _filesystem_metric_key(metric) == key
        ]
        if not any(
            (
                filesystem_metrics,
                filesystem_operation_metrics,
                filesystem_mdt_metrics,
                filesystem_mdt_operation_metrics,
            )
        ):
            continue

        connected_clients = _max_optional(
            metric.connected_clients for metric in filesystem_mdt_metrics
        )
        metadata_ops = sum(
            metric.client_ops or 0.0 for metric in filesystem_mdt_operation_metrics
        )
        client_data_ops = sum(
            (metric.client_read_ops or 0.0) + (metric.client_write_ops or 0.0)
            for metric in filesystem_metrics
        )
        metadata_amplification_ratio = metadata_ops / max(client_data_ops, 1.0)
        timestamps = [
            timestamp
            for timestamp in (
                *(metric.sample_timestamp_seconds for metric in filesystem_metrics),
                *(
                    metric.sample_timestamp_seconds
                    for metric in filesystem_operation_metrics
                ),
                *(metric.sample_timestamp_seconds for metric in filesystem_mdt_metrics),
                *(
                    metric.sample_timestamp_seconds
                    for metric in filesystem_mdt_operation_metrics
                ),
            )
            if timestamp is not None
        ]
        sample_max_age_seconds = (
            max(0.0, now_seconds - min(timestamps)) if timestamps else None
        )
        results.append(
            ManagedLustreFilesystemAggregateMetric(
                subscription_id=key[0],
                resource_group=key[1],
                filesystem_name=key[2],
                location=key[3],
                connected_clients=connected_clients,
                metadata_amplification_ratio=metadata_amplification_ratio,
                sample_max_age_seconds=sample_max_age_seconds,
            )
        )
    return tuple(results)


def _filesystem_metric_key(
    metric: ManagedLustreOstMetric
    | ManagedLustreOstOperationMetric
    | ManagedLustreMdtMetric
    | ManagedLustreMdtOperationMetric,
) -> tuple[str, str, str, str]:
    return (
        metric.subscription_id,
        metric.resource_group,
        metric.filesystem_name,
        metric.location,
    )


def _max_optional(values: Iterable[float | None]) -> float | None:
    numeric_values = [value for value in values if value is not None]
    if not numeric_values:
        return None
    return max(numeric_values)


@dataclass(frozen=True, slots=True)
class ManagedLustreCollectionResult:
    """Result of one Azure Managed Lustre collection pass."""

    metrics: tuple[ManagedLustreOstMetric, ...]
    filesystem_count: int
    error_count: int = 0
    filesystems: tuple[ManagedLustreFilesystem, ...] = ()
    operation_metrics: tuple[ManagedLustreOstOperationMetric, ...] = ()
    mdt_metrics: tuple[ManagedLustreMdtMetric, ...] = ()
    mdt_operation_metrics: tuple[ManagedLustreMdtOperationMetric, ...] = ()
    filesystem_aggregate_metrics: tuple[ManagedLustreFilesystemAggregateMetric, ...] = ()
