"""Prometheus metric exposition and polling loop for VMSS counts."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Sequence
from contextlib import suppress

from prometheus_client import REGISTRY, Counter, Gauge
from prometheus_client.registry import CollectorRegistry

from .models import (
    ManagedLustreCollectionResult,
    ManagedLustreFilesystem,
    ManagedLustreMdtMetric,
    ManagedLustreMdtOperationMetric,
    ManagedLustreOstMetric,
    ManagedLustreOstOperationMetric,
    VmssCount,
)

LOGGER = logging.getLogger(__name__)

VMSS_LABELS = (
    "subscription_id",
    "resource_group",
    "vmss_name",
    "location",
    "orchestration_mode",
)

VMSS_INFO_LABELS = (
    "subscription_id",
    "resource_group",
    "vmss_name",
    "location",
    "orchestration_mode",
    "vm_size",
    "sku_tier",
)

LUSTRE_OST_LABELS = (
    "subscription_id",
    "resource_group",
    "filesystem_name",
    "location",
    "ostnum",
)

LUSTRE_OST_OPERATION_LABELS = (*LUSTRE_OST_LABELS, "operation")

LUSTRE_MDT_LABELS = (
    "subscription_id",
    "resource_group",
    "filesystem_name",
    "location",
    "mdtnum",
)

LUSTRE_MDT_OPERATION_LABELS = (*LUSTRE_MDT_LABELS, "operation")

LUSTRE_FILESYSTEM_INFO_LABELS = (
    "subscription_id",
    "resource_group",
    "filesystem_name",
    "location",
    "sku_tier",
)

LUSTRE_FILESYSTEM_CAPACITY_LABELS = (
    "subscription_id",
    "resource_group",
    "filesystem_name",
    "location",
)


class VmssMetricsExporter:
    """Poll Azure and update Prometheus gauges with cached VMSS counts."""

    def __init__(
        self,
        collect_counts: Callable[[], Sequence[VmssCount]],
        *,
        collect_lustre_metrics: Callable[[], ManagedLustreCollectionResult] | None = None,
        poll_interval_seconds: int = 300,
        lustre_poll_interval_seconds: int = 60,
        registry: CollectorRegistry | None = None,
        leader_election_enabled: bool = False,
    ) -> None:
        self._collect_counts = collect_counts
        self._collect_lustre_metrics = collect_lustre_metrics
        self._poll_interval_seconds = poll_interval_seconds
        self._lustre_poll_interval_seconds = lustre_poll_interval_seconds
        self._leader_election_enabled = leader_election_enabled
        self._stop_event = threading.Event()
        # When leader election is disabled the event is permanently set so the
        # polling loops behave exactly as they did before this feature landed.
        self._is_leader_event = threading.Event()
        if not leader_election_enabled:
            self._is_leader_event.set()
        self._vmss_thread: threading.Thread | None = None
        self._lustre_thread: threading.Thread | None = None
        self._metric_lock = threading.Lock()
        self._active_labelsets: set[tuple[str, str, str, str, str]] = set()
        self._active_info_labelsets: set[tuple[str, str, str, str, str, str, str]] = set()
        self._active_lustre_ost_labelsets: set[tuple[str, str, str, str, str]] = set()
        self._active_lustre_ost_operation_labelsets: set[
            tuple[str, str, str, str, str, str]
        ] = set()
        self._active_lustre_mdt_labelsets: set[tuple[str, str, str, str, str]] = set()
        self._active_lustre_mdt_operation_labelsets: set[
            tuple[str, str, str, str, str, str]
        ] = set()
        self._active_lustre_filesystem_info_labelsets: set[
            tuple[str, str, str, str, str]
        ] = set()
        self._active_lustre_filesystem_capacity_labelsets: set[
            tuple[str, str, str, str]
        ] = set()

        effective_registry = registry if registry is not None else REGISTRY

        self.instance_count = Gauge(
            "azure_vmss_instance_count",
            "Actual Azure VM Scale Set virtual machine instance count from Azure Resource Graph.",
            VMSS_LABELS,
            registry=effective_registry,
        )
        self.capacity = Gauge(
            "azure_vmss_capacity",
            "Desired Azure VM Scale Set capacity from the parent VMSS resource sku.capacity.",
            VMSS_LABELS,
            registry=effective_registry,
        )
        self.info = Gauge(
            "azure_vmss_info",
            (
                "Static metadata about each VM Scale Set (vm_size from sku.name, "
                "sku_tier from sku.tier). The value is always 1; join via "
                "`* on (subscription_id, resource_group, vmss_name) group_left(vm_size, sku_tier)`."
            ),
            VMSS_INFO_LABELS,
            registry=effective_registry,
        )
        self.last_success_timestamp = Gauge(
            "azure_vmss_exporter_last_success_timestamp_seconds",
            "Unix timestamp of the last successful Azure Resource Graph collection.",
            registry=effective_registry,
        )
        self.collection_duration = Gauge(
            "azure_vmss_exporter_collection_duration_seconds",
            "Duration in seconds of the most recent Azure Resource Graph collection attempt.",
            registry=effective_registry,
        )
        self.collection_errors = Counter(
            "azure_vmss_exporter_collection_errors",
            "Total Azure Resource Graph collection errors observed by this exporter process.",
            registry=effective_registry,
        )
        self.vmss_total = Gauge(
            "azure_vmss_exporter_vmss_total",
            "Number of Azure VM Scale Sets observed in the latest successful collection.",
            registry=effective_registry,
        )
        self.lustre_ost_bytes_available = Gauge(
            "azure_managed_lustre_ost_bytes_available",
            "Azure Managed Lustre OST bytes available from Azure Monitor OSTBytesAvailable.",
            LUSTRE_OST_LABELS,
            registry=effective_registry,
        )
        self.lustre_ost_bytes_used = Gauge(
            "azure_managed_lustre_ost_bytes_used",
            "Azure Managed Lustre OST bytes used from Azure Monitor OSTBytesUsed.",
            LUSTRE_OST_LABELS,
            registry=effective_registry,
        )
        self.lustre_ost_bytes_total = Gauge(
            "azure_managed_lustre_ost_bytes_total",
            "Azure Managed Lustre OST total bytes from Azure Monitor OSTBytesTotal.",
            LUSTRE_OST_LABELS,
            registry=effective_registry,
        )
        self.lustre_ost_bytes_available_percent = Gauge(
            "azure_managed_lustre_ost_bytes_available_percent",
            (
                "Azure Managed Lustre OST available capacity percentage derived from "
                "bytes available / total."
            ),
            LUSTRE_OST_LABELS,
            registry=effective_registry,
        )
        self.lustre_ost_bytes_used_percent = Gauge(
            "azure_managed_lustre_ost_bytes_used_percent",
            "Azure Managed Lustre OST used capacity percentage derived from bytes used / total.",
            LUSTRE_OST_LABELS,
            registry=effective_registry,
        )
        self.lustre_client_read_ops = Gauge(
            "azure_managed_lustre_client_read_ops",
            "Azure Managed Lustre client read operations from Azure Monitor ClientReadOps.",
            LUSTRE_OST_LABELS,
            registry=effective_registry,
        )
        self.lustre_client_read_throughput = Gauge(
            "azure_managed_lustre_client_read_throughput_bytes_per_second",
            "Azure Managed Lustre client read throughput from Azure Monitor ClientReadThroughput.",
            LUSTRE_OST_LABELS,
            registry=effective_registry,
        )
        self.lustre_client_write_ops = Gauge(
            "azure_managed_lustre_client_write_ops",
            "Azure Managed Lustre client write operations from Azure Monitor ClientWriteOps.",
            LUSTRE_OST_LABELS,
            registry=effective_registry,
        )
        self.lustre_client_write_throughput = Gauge(
            "azure_managed_lustre_client_write_throughput_bytes_per_second",
            "Azure Managed Lustre client write throughput from Azure Monitor "
            "ClientWriteThroughput.",
            LUSTRE_OST_LABELS,
            registry=effective_registry,
        )
        self.lustre_ost_connected_clients = Gauge(
            "azure_managed_lustre_ost_connected_clients",
            (
                "Azure Managed Lustre OST connected client count (exports) from Azure "
                "Monitor OSTConnectedClients. Approximates the number of mounted Lustre "
                "clients seen by each OST; per-OST variance can indicate failover or "
                "eviction in progress."
            ),
            LUSTRE_OST_LABELS,
            registry=effective_registry,
        )
        self.lustre_ost_sample_timestamp = Gauge(
            "azure_managed_lustre_ost_sample_timestamp_seconds",
            "Unix timestamp of the Azure Monitor sample backing each OST metric series.",
            LUSTRE_OST_LABELS,
            registry=effective_registry,
        )
        self.lustre_ost_client_latency = Gauge(
            "azure_managed_lustre_ost_client_latency_milliseconds",
            "Azure Managed Lustre OST client latency from Azure Monitor OSTClientLatency.",
            LUSTRE_OST_OPERATION_LABELS,
            registry=effective_registry,
        )
        self.lustre_ost_client_ops = Gauge(
            "azure_managed_lustre_ost_client_ops",
            "Azure Managed Lustre OST client operations from Azure Monitor OSTClientOps.",
            LUSTRE_OST_OPERATION_LABELS,
            registry=effective_registry,
        )
        self.lustre_ost_operation_sample_timestamp = Gauge(
            "azure_managed_lustre_ost_operation_sample_timestamp_seconds",
            "Unix timestamp of the Azure Monitor sample backing each OST operation metric series.",
            LUSTRE_OST_OPERATION_LABELS,
            registry=effective_registry,
        )
        self.lustre_mdt_bytes_available = Gauge(
            "azure_managed_lustre_mdt_bytes_available",
            "Azure Managed Lustre MDT bytes available from Azure Monitor MDTBytesAvailable.",
            LUSTRE_MDT_LABELS,
            registry=effective_registry,
        )
        self.lustre_mdt_bytes_used = Gauge(
            "azure_managed_lustre_mdt_bytes_used",
            "Azure Managed Lustre MDT bytes used from Azure Monitor MDTBytesUsed.",
            LUSTRE_MDT_LABELS,
            registry=effective_registry,
        )
        self.lustre_mdt_bytes_total = Gauge(
            "azure_managed_lustre_mdt_bytes_total",
            "Azure Managed Lustre MDT total bytes from Azure Monitor MDTBytesTotal.",
            LUSTRE_MDT_LABELS,
            registry=effective_registry,
        )
        self.lustre_mdt_bytes_available_percent = Gauge(
            "azure_managed_lustre_mdt_bytes_available_percent",
            "Azure Managed Lustre MDT available bytes percentage derived from bytes "
            "available / total.",
            LUSTRE_MDT_LABELS,
            registry=effective_registry,
        )
        self.lustre_mdt_bytes_used_percent = Gauge(
            "azure_managed_lustre_mdt_bytes_used_percent",
            "Azure Managed Lustre MDT used bytes percentage derived from bytes used / total.",
            LUSTRE_MDT_LABELS,
            registry=effective_registry,
        )
        self.lustre_mdt_files_free = Gauge(
            "azure_managed_lustre_mdt_files_free",
            "Azure Managed Lustre MDT free inode/file count from Azure Monitor MDTFilesFree.",
            LUSTRE_MDT_LABELS,
            registry=effective_registry,
        )
        self.lustre_mdt_files_used = Gauge(
            "azure_managed_lustre_mdt_files_used",
            "Azure Managed Lustre MDT used inode/file count from Azure Monitor MDTFilesUsed.",
            LUSTRE_MDT_LABELS,
            registry=effective_registry,
        )
        self.lustre_mdt_files_total = Gauge(
            "azure_managed_lustre_mdt_files_total",
            "Azure Managed Lustre MDT total inode/file count from Azure Monitor MDTFilesTotal.",
            LUSTRE_MDT_LABELS,
            registry=effective_registry,
        )
        self.lustre_mdt_files_free_percent = Gauge(
            "azure_managed_lustre_mdt_files_free_percent",
            "Azure Managed Lustre MDT free inode/file percentage derived from files free / total.",
            LUSTRE_MDT_LABELS,
            registry=effective_registry,
        )
        self.lustre_mdt_files_used_percent = Gauge(
            "azure_managed_lustre_mdt_files_used_percent",
            "Azure Managed Lustre MDT used inode/file percentage derived from files used / total.",
            LUSTRE_MDT_LABELS,
            registry=effective_registry,
        )
        self.lustre_mdt_sample_timestamp = Gauge(
            "azure_managed_lustre_mdt_sample_timestamp_seconds",
            "Unix timestamp of the Azure Monitor sample backing each MDT metric series.",
            LUSTRE_MDT_LABELS,
            registry=effective_registry,
        )
        self.lustre_hsm_action_errors = Gauge(
            "azure_managed_lustre_hsm_action_errors",
            "Azure Managed Lustre HSM action errors from Azure Monitor HSMActionErrors.",
            LUSTRE_MDT_LABELS,
            registry=effective_registry,
        )
        self.lustre_hsm_current_requests = Gauge(
            "azure_managed_lustre_hsm_current_requests",
            "Azure Managed Lustre HSM in-flight requests from Azure Monitor HSMCurrentRequests.",
            LUSTRE_MDT_LABELS,
            registry=effective_registry,
        )
        self.lustre_mdt_connected_clients = Gauge(
            "azure_managed_lustre_mdt_connected_clients",
            (
                "Azure Managed Lustre MDT connected client count (exports) from Azure "
                "Monitor MDTConnectedClients. Each mounted Lustre client opens one export "
                "per MDT, so ``max by (filesystem_name) (...)`` approximates the total "
                "number of clients connected to the filesystem."
            ),
            LUSTRE_MDT_LABELS,
            registry=effective_registry,
        )
        self.lustre_mdt_client_latency = Gauge(
            "azure_managed_lustre_mdt_client_latency_milliseconds",
            "Azure Managed Lustre MDT client latency from Azure Monitor MDTClientLatency.",
            LUSTRE_MDT_OPERATION_LABELS,
            registry=effective_registry,
        )
        self.lustre_mdt_client_ops = Gauge(
            "azure_managed_lustre_mdt_client_ops",
            "Azure Managed Lustre MDT client operations from Azure Monitor MDTClientOps.",
            LUSTRE_MDT_OPERATION_LABELS,
            registry=effective_registry,
        )
        self.lustre_mdt_operation_sample_timestamp = Gauge(
            "azure_managed_lustre_mdt_operation_sample_timestamp_seconds",
            "Unix timestamp of the Azure Monitor sample backing each MDT operation metric series.",
            LUSTRE_MDT_OPERATION_LABELS,
            registry=effective_registry,
        )
        self.lustre_filesystem_total = Gauge(
            "azure_managed_lustre_filesystem_total",
            "Number of Azure Managed Lustre filesystems discovered in the latest collection.",
            registry=effective_registry,
        )
        self.lustre_filesystem_info = Gauge(
            "azure_managed_lustre_filesystem_info",
            (
                "Static metadata about each discovered Azure Managed Lustre filesystem. "
                "The value is always 1."
            ),
            LUSTRE_FILESYSTEM_INFO_LABELS,
            registry=effective_registry,
        )
        self.lustre_filesystem_storage_capacity_tib = Gauge(
            "azure_managed_lustre_filesystem_storage_capacity_tib",
            "Configured Azure Managed Lustre filesystem storage capacity from Resource Graph.",
            LUSTRE_FILESYSTEM_CAPACITY_LABELS,
            registry=effective_registry,
        )
        self.lustre_ost_sample_count = Gauge(
            "azure_managed_lustre_ost_sample_count",
            (
                "Number of Azure Managed Lustre OST sample series produced by the latest "
                "collection. One series per OST per filesystem that returned data \u2014 "
                "this approximates total OST count only when every OST reported at least "
                "one metric."
            ),
            registry=effective_registry,
        )
        self.lustre_last_success_timestamp = Gauge(
            "azure_managed_lustre_last_success_timestamp_seconds",
            "Unix timestamp of the last successful Azure Managed Lustre collection.",
            registry=effective_registry,
        )
        self.lustre_collection_duration = Gauge(
            "azure_managed_lustre_collection_duration_seconds",
            "Duration in seconds of the most recent Azure Managed Lustre collection attempt.",
            registry=effective_registry,
        )
        self.lustre_collection_errors = Counter(
            "azure_managed_lustre_collection_errors",
            "Total Azure Managed Lustre collection errors observed by this exporter process.",
            registry=effective_registry,
        )
        self.is_leader = Gauge(
            "azure_vmss_exporter_is_leader",
            (
                "Whether this exporter replica currently holds the leader-election lock. "
                "1 = leader (actively collecting); 0 = follower (idle). When leader election "
                "is disabled this is always 1."
            ),
            registry=effective_registry,
        )
        self.is_leader.set(0.0 if leader_election_enabled else 1.0)

    def collect_once(self) -> Sequence[VmssCount]:
        """Collect once and immediately update gauges."""

        start = time.monotonic()
        try:
            counts = tuple(self._collect_counts())
        except Exception:  # noqa: BLE001 - keep exporter alive for transient Azure failures.
            self.collection_errors.inc()
            self.collection_duration.set(time.monotonic() - start)
            LOGGER.exception("VMSS metric collection failed")
            raise

        if not self._update_metrics(counts):
            LOGGER.info("Skipped VMSS metric update because leadership was lost mid-collection")
            return counts
        if self._collect_lustre_metrics is not None:
            with suppress(Exception):
                self.collect_lustre_once()
        with self._metric_lock:
            if not self._can_write_metrics_locked():
                LOGGER.info(
                    "Skipped VMSS collection summary update because leadership was lost "
                    "mid-collection"
                )
                return counts
            self.last_success_timestamp.set(time.time())
            self.collection_duration.set(time.monotonic() - start)
            self.vmss_total.set(len(counts))
        LOGGER.info("Collected metrics for %s VM Scale Sets", len(counts))
        return counts

    def collect_lustre_once(self) -> ManagedLustreCollectionResult | None:
        """Collect Azure Managed Lustre once and immediately update gauges."""

        if self._collect_lustre_metrics is None:
            return None
        start = time.monotonic()
        try:
            result = self._collect_lustre_metrics()
        except Exception:  # noqa: BLE001 - keep VMSS metrics alive if Azure Monitor fails.
            self.lustre_collection_errors.inc()
            self.lustre_collection_duration.set(time.monotonic() - start)
            LOGGER.exception("Azure Managed Lustre metric collection failed")
            raise

        remove_stale = result.error_count == 0
        if not self._update_lustre_metrics(
            result.filesystems,
            result.metrics,
            result.operation_metrics,
            result.mdt_metrics,
            result.mdt_operation_metrics,
            remove_stale=remove_stale,
        ):
            LOGGER.info(
                "Skipped Azure Managed Lustre metric update because leadership was lost "
                "mid-collection"
            )
            return result
        with self._metric_lock:
            if not self._can_write_metrics_locked():
                LOGGER.info(
                    "Skipped Azure Managed Lustre collection summary update because leadership "
                    "was lost mid-collection"
                )
                return result
            self.lustre_filesystem_total.set(result.filesystem_count)
            self.lustre_ost_sample_count.set(len(result.metrics))
            if result.error_count:
                self.lustre_collection_errors.inc(result.error_count)
            self.lustre_last_success_timestamp.set(time.time())
            self.lustre_collection_duration.set(time.monotonic() - start)
        LOGGER.info(
            "Collected metrics for %s Managed Lustre filesystems and %s OST series%s",
            result.filesystem_count,
            len(result.metrics),
            " with partial errors" if result.error_count else "",
        )
        return result

    def start(self) -> None:
        """Start the background polling thread."""

        if self._vmss_thread and self._vmss_thread.is_alive():
            return
        self._vmss_thread = threading.Thread(
            target=self._poll_vmss_forever,
            name="vmss-metrics-poller",
            daemon=True,
        )
        self._vmss_thread.start()
        if self._collect_lustre_metrics is not None:
            self._lustre_thread = threading.Thread(
                target=self._poll_lustre_forever,
                name="managed-lustre-metrics-poller",
                daemon=True,
            )
            self._lustre_thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        """Stop the background polling thread."""

        self._stop_event.set()
        # Wake up any poller blocked in _wait_for_leadership_or_stop().
        self._is_leader_event.set()
        if self._vmss_thread:
            self._vmss_thread.join(timeout=timeout)
        if self._lustre_thread:
            self._lustre_thread.join(timeout=timeout)

    def set_leader(self, is_leader: bool) -> None:
        """Update leadership state from a leader-election callback.

        Called from a leader-election supervisor thread; safe to invoke
        repeatedly and idempotent for the same target state. On the
        leader→follower transition every per-resource gauge is cleared so
        ``/metrics`` from a follower exposes only the leadership beacon and
        process-level Prometheus defaults.
        """

        if not self._leader_election_enabled:
            return
        if is_leader:
            self.is_leader.set(1.0)
            self._is_leader_event.set()
            LOGGER.info("Acquired leader-election lock; resuming Azure polling")
        else:
            self.is_leader.set(0.0)
            self._is_leader_event.clear()
            self._clear_resource_gauges()
            LOGGER.info(
                "Released leader-election lock; cleared resource gauges and paused polling"
            )

    def _clear_resource_gauges(self) -> None:
        """Drop every per-resource label set so a follower exposes nothing."""

        with self._metric_lock:
            for gauge in (
                self.instance_count,
                self.capacity,
                self.info,
                self.lustre_ost_bytes_available,
                self.lustre_ost_bytes_used,
                self.lustre_ost_bytes_total,
                self.lustre_ost_bytes_available_percent,
                self.lustre_ost_bytes_used_percent,
                self.lustre_client_read_ops,
                self.lustre_client_read_throughput,
                self.lustre_client_write_ops,
                self.lustre_client_write_throughput,
                self.lustre_ost_connected_clients,
                self.lustre_ost_sample_timestamp,
                self.lustre_ost_client_latency,
                self.lustre_ost_client_ops,
                self.lustre_ost_operation_sample_timestamp,
                self.lustre_mdt_bytes_available,
                self.lustre_mdt_bytes_used,
                self.lustre_mdt_bytes_total,
                self.lustre_mdt_bytes_available_percent,
                self.lustre_mdt_bytes_used_percent,
                self.lustre_mdt_files_free,
                self.lustre_mdt_files_used,
                self.lustre_mdt_files_total,
                self.lustre_mdt_files_free_percent,
                self.lustre_mdt_files_used_percent,
                self.lustre_mdt_sample_timestamp,
                self.lustre_hsm_action_errors,
                self.lustre_hsm_current_requests,
                self.lustre_mdt_connected_clients,
                self.lustre_mdt_client_latency,
                self.lustre_mdt_client_ops,
                self.lustre_mdt_operation_sample_timestamp,
                self.lustre_filesystem_info,
                self.lustre_filesystem_storage_capacity_tib,
            ):
                gauge.clear()
            self._active_labelsets = set()
            self._active_info_labelsets = set()
            self._active_lustre_ost_labelsets = set()
            self._active_lustre_ost_operation_labelsets = set()
            self._active_lustre_mdt_labelsets = set()
            self._active_lustre_mdt_operation_labelsets = set()
            self._active_lustre_filesystem_info_labelsets = set()
            self._active_lustre_filesystem_capacity_labelsets = set()
            # Reset summary scalars so dashboards don't show stale totals on the follower.
            self.vmss_total.set(0)
            self.lustre_filesystem_total.set(0)
            self.lustre_ost_sample_count.set(0)
            self.last_success_timestamp.set(0)
            self.collection_duration.set(0)
            self.lustre_last_success_timestamp.set(0)
            self.lustre_collection_duration.set(0)

    def _poll_vmss_forever(self) -> None:
        while not self._stop_event.is_set():
            if not self._is_leader_event.is_set():
                # Follower: wait until either we become leader or the process stops.
                self._wait_for_leadership_or_stop()
                continue
            with suppress(Exception):
                self._collect_vmss_once()
            self._stop_event.wait(self._poll_interval_seconds)

    def _poll_lustre_forever(self) -> None:
        while not self._stop_event.is_set():
            if not self._is_leader_event.is_set():
                self._wait_for_leadership_or_stop()
                continue
            with suppress(Exception):
                self.collect_lustre_once()
            self._stop_event.wait(self._lustre_poll_interval_seconds)

    def _wait_for_leadership_or_stop(self) -> None:
        """Block until leadership is acquired or shutdown is requested."""

        # Poll the stop event so we don't sleep forever after shutdown when we
        # never become the leader.
        while not self._stop_event.is_set():
            if self._is_leader_event.wait(timeout=1.0):
                return

    def _collect_vmss_once(self) -> Sequence[VmssCount]:
        start = time.monotonic()
        try:
            counts = tuple(self._collect_counts())
        except Exception:  # noqa: BLE001 - keep exporter alive for transient Azure failures.
            self.collection_errors.inc()
            self.collection_duration.set(time.monotonic() - start)
            LOGGER.exception("VMSS metric collection failed")
            raise

        if not self._update_metrics(counts):
            LOGGER.info("Skipped VMSS metric update because leadership was lost mid-collection")
            return counts
        with self._metric_lock:
            if not self._can_write_metrics_locked():
                LOGGER.info(
                    "Skipped VMSS collection summary update because leadership was lost "
                    "mid-collection"
                )
                return counts
            self.last_success_timestamp.set(time.time())
            self.collection_duration.set(time.monotonic() - start)
            self.vmss_total.set(len(counts))
        LOGGER.info("Collected metrics for %s VM Scale Sets", len(counts))
        return counts

    def _can_write_metrics_locked(self) -> bool:
        """Return whether this process may publish resource metrics.

        Must be called while holding ``_metric_lock`` so it is ordered with
        ``_clear_resource_gauges`` during leader→follower transitions.
        """

        return not self._leader_election_enabled or self._is_leader_event.is_set()

    def _update_metrics(self, counts: Sequence[VmssCount]) -> bool:
        new_labelsets = {count.label_values for count in counts}
        new_info_labelsets = {count.info_label_values for count in counts}
        with self._metric_lock:
            if not self._can_write_metrics_locked():
                return False
            for stale in self._active_labelsets - new_labelsets:
                with suppress(KeyError):
                    self.instance_count.remove(*stale)
                with suppress(KeyError):
                    self.capacity.remove(*stale)
            for stale_info in self._active_info_labelsets - new_info_labelsets:
                with suppress(KeyError):
                    self.info.remove(*stale_info)

            for count in counts:
                labels = count.label_values
                self.instance_count.labels(*labels).set(count.actual_instance_count)
                self.capacity.labels(*labels).set(count.capacity)
                self.info.labels(*count.info_label_values).set(1)

            self._active_labelsets = new_labelsets
            self._active_info_labelsets = new_info_labelsets
            return True

    def _update_lustre_metrics(
        self,
        filesystems: Sequence[ManagedLustreFilesystem],
        metrics: Sequence[ManagedLustreOstMetric],
        operation_metrics: Sequence[ManagedLustreOstOperationMetric],
        mdt_metrics: Sequence[ManagedLustreMdtMetric],
        mdt_operation_metrics: Sequence[ManagedLustreMdtOperationMetric],
        *,
        remove_stale: bool,
    ) -> bool:
        new_filesystem_info_labelsets = {
            filesystem.info_label_values for filesystem in filesystems
        }
        new_filesystem_capacity_labelsets = {
            filesystem.capacity_label_values for filesystem in filesystems
        }
        new_labelsets = {metric.label_values for metric in metrics}
        new_operation_labelsets = {metric.label_values for metric in operation_metrics}
        new_mdt_labelsets = {metric.label_values for metric in mdt_metrics}
        new_mdt_operation_labelsets = {
            metric.label_values for metric in mdt_operation_metrics
        }
        with self._metric_lock:
            if not self._can_write_metrics_locked():
                return False
            if remove_stale:
                for stale in (
                    self._active_lustre_filesystem_info_labelsets
                    - new_filesystem_info_labelsets
                ):
                    with suppress(KeyError):
                        self.lustre_filesystem_info.remove(*stale)
                for stale in (
                    self._active_lustre_filesystem_capacity_labelsets
                    - new_filesystem_capacity_labelsets
                ):
                    with suppress(KeyError):
                        self.lustre_filesystem_storage_capacity_tib.remove(*stale)
                for stale in self._active_lustre_ost_labelsets - new_labelsets:
                    for gauge in (
                        self.lustre_ost_bytes_available,
                        self.lustre_ost_bytes_used,
                        self.lustre_ost_bytes_total,
                        self.lustre_ost_bytes_available_percent,
                        self.lustre_ost_bytes_used_percent,
                        self.lustre_client_read_ops,
                        self.lustre_client_read_throughput,
                        self.lustre_client_write_ops,
                        self.lustre_client_write_throughput,
                        self.lustre_ost_connected_clients,
                        self.lustre_ost_sample_timestamp,
                    ):
                        with suppress(KeyError):
                            gauge.remove(*stale)
                for stale in self._active_lustre_ost_operation_labelsets - new_operation_labelsets:
                    for gauge in (
                        self.lustre_ost_client_latency,
                        self.lustre_ost_client_ops,
                        self.lustre_ost_operation_sample_timestamp,
                    ):
                        with suppress(KeyError):
                            gauge.remove(*stale)
                for stale in self._active_lustre_mdt_labelsets - new_mdt_labelsets:
                    for gauge in (
                        self.lustre_mdt_bytes_available,
                        self.lustre_mdt_bytes_used,
                        self.lustre_mdt_bytes_total,
                        self.lustre_mdt_bytes_available_percent,
                        self.lustre_mdt_bytes_used_percent,
                        self.lustre_mdt_files_free,
                        self.lustre_mdt_files_used,
                        self.lustre_mdt_files_total,
                        self.lustre_mdt_files_free_percent,
                        self.lustre_mdt_files_used_percent,
                        self.lustre_mdt_sample_timestamp,
                        self.lustre_hsm_action_errors,
                        self.lustre_hsm_current_requests,
                        self.lustre_mdt_connected_clients,
                    ):
                        with suppress(KeyError):
                            gauge.remove(*stale)
                for stale in (
                    self._active_lustre_mdt_operation_labelsets
                    - new_mdt_operation_labelsets
                ):
                    for gauge in (
                        self.lustre_mdt_client_latency,
                        self.lustre_mdt_client_ops,
                        self.lustre_mdt_operation_sample_timestamp,
                    ):
                        with suppress(KeyError):
                            gauge.remove(*stale)

            for filesystem in filesystems:
                self.lustre_filesystem_info.labels(*filesystem.info_label_values).set(1)
                self.lustre_filesystem_storage_capacity_tib.labels(
                    *filesystem.capacity_label_values
                ).set(filesystem.storage_capacity_tib)

            for metric in metrics:
                self._set_or_remove_lustre_gauge(
                    self.lustre_ost_bytes_available,
                    metric.label_values,
                    metric.bytes_available,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_ost_bytes_used,
                    metric.label_values,
                    metric.bytes_used,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_ost_bytes_total,
                    metric.label_values,
                    metric.bytes_total,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_ost_bytes_available_percent,
                    metric.label_values,
                    metric.bytes_available_percent,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_ost_bytes_used_percent,
                    metric.label_values,
                    metric.bytes_used_percent,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_client_read_ops,
                    metric.label_values,
                    metric.client_read_ops,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_client_read_throughput,
                    metric.label_values,
                    metric.client_read_throughput_bytes_per_second,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_client_write_ops,
                    metric.label_values,
                    metric.client_write_ops,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_client_write_throughput,
                    metric.label_values,
                    metric.client_write_throughput_bytes_per_second,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_ost_connected_clients,
                    metric.label_values,
                    metric.connected_clients,
                )
                self.lustre_ost_sample_timestamp.labels(*metric.label_values).set(
                    metric.sample_timestamp_seconds or time.time()
                )

            for metric in operation_metrics:
                self._set_or_remove_lustre_gauge(
                    self.lustre_ost_client_latency,
                    metric.label_values,
                    metric.client_latency_milliseconds,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_ost_client_ops,
                    metric.label_values,
                    metric.client_ops,
                )
                self.lustre_ost_operation_sample_timestamp.labels(*metric.label_values).set(
                    metric.sample_timestamp_seconds or time.time()
                )

            for metric in mdt_metrics:
                self._set_or_remove_lustre_gauge(
                    self.lustre_mdt_bytes_available,
                    metric.label_values,
                    metric.bytes_available,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_mdt_bytes_used,
                    metric.label_values,
                    metric.bytes_used,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_mdt_bytes_total,
                    metric.label_values,
                    metric.bytes_total,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_mdt_bytes_available_percent,
                    metric.label_values,
                    metric.bytes_available_percent,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_mdt_bytes_used_percent,
                    metric.label_values,
                    metric.bytes_used_percent,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_mdt_files_free,
                    metric.label_values,
                    metric.files_free,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_mdt_files_used,
                    metric.label_values,
                    metric.files_used,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_mdt_files_total,
                    metric.label_values,
                    metric.files_total,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_mdt_files_free_percent,
                    metric.label_values,
                    metric.files_free_percent,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_mdt_files_used_percent,
                    metric.label_values,
                    metric.files_used_percent,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_hsm_action_errors,
                    metric.label_values,
                    metric.hsm_action_errors,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_hsm_current_requests,
                    metric.label_values,
                    metric.hsm_current_requests,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_mdt_connected_clients,
                    metric.label_values,
                    metric.connected_clients,
                )
                self.lustre_mdt_sample_timestamp.labels(*metric.label_values).set(
                    metric.sample_timestamp_seconds or time.time()
                )

            for metric in mdt_operation_metrics:
                self._set_or_remove_lustre_gauge(
                    self.lustre_mdt_client_latency,
                    metric.label_values,
                    metric.client_latency_milliseconds,
                )
                self._set_or_remove_lustre_gauge(
                    self.lustre_mdt_client_ops,
                    metric.label_values,
                    metric.client_ops,
                )
                self.lustre_mdt_operation_sample_timestamp.labels(*metric.label_values).set(
                    metric.sample_timestamp_seconds or time.time()
                )

            if remove_stale:
                self._active_lustre_filesystem_info_labelsets = new_filesystem_info_labelsets
                self._active_lustre_filesystem_capacity_labelsets = (
                    new_filesystem_capacity_labelsets
                )
                self._active_lustre_ost_labelsets = new_labelsets
                self._active_lustre_ost_operation_labelsets = new_operation_labelsets
                self._active_lustre_mdt_labelsets = new_mdt_labelsets
                self._active_lustre_mdt_operation_labelsets = new_mdt_operation_labelsets
            else:
                self._active_lustre_filesystem_info_labelsets |= new_filesystem_info_labelsets
                self._active_lustre_filesystem_capacity_labelsets |= (
                    new_filesystem_capacity_labelsets
                )
                self._active_lustre_ost_labelsets |= new_labelsets
                self._active_lustre_ost_operation_labelsets |= new_operation_labelsets
                self._active_lustre_mdt_labelsets |= new_mdt_labelsets
                self._active_lustre_mdt_operation_labelsets |= new_mdt_operation_labelsets
            return True

    @staticmethod
    def _set_or_remove_lustre_gauge(
        gauge: Gauge,
        label_values: tuple[str, ...],
        value: float | None,
    ) -> None:
        if value is not None:
            gauge.labels(*label_values).set(value)
        else:
            with suppress(KeyError):
                gauge.remove(*label_values)
