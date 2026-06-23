"""Azure Resource Graph queries for VM Scale Set counts."""

from __future__ import annotations

import logging
import random
import re
import time
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Protocol

from .models import STANDALONE_VM_POWER_STATES, StandaloneVm, VmssCount

LOGGER = logging.getLogger(__name__)

# Upper bound on the exponential backoff term so a high retry count or large
# base delay cannot produce multi-minute sleeps that stall the collection loop.
_MAX_RETRY_BACKOFF_SECONDS = 30.0

VMSS_COUNTS_QUERY = """
Resources
| where type =~ 'microsoft.compute/virtualmachinescalesets'
| extend rgKey = tolower(resourceGroup), vmssKey = tolower(name)
| project
    subscriptionId,
    resourceGroup,
    vmssName = name,
    location,
    orchestrationMode = tostring(properties.orchestrationMode),
    capacity = toint(sku.capacity),
    vmSize = tostring(sku.name),
    skuTier = tostring(sku.tier),
    rgKey,
    vmssKey
| join kind=leftouter (
    ComputeResources
    | where type =~ 'microsoft.compute/virtualmachinescalesets/virtualmachines'
        or type =~ 'microsoft.compute/virtualmachines'
    | extend parentId = iff(
        type =~ 'microsoft.compute/virtualmachinescalesets/virtualmachines',
        id,
        tostring(properties.virtualMachineScaleSet.id)
    )
    | where isnotempty(parentId)
    | extend parentIdParts = split(parentId, '/')
    | extend powerState = tostring(properties.extended.instanceView.powerState.displayStatus)
    | extend provisioningState = tostring(properties.provisioningState)
    | extend memberState = case(
        provisioningState =~ 'Failed', 'failed',
        powerState =~ 'VM running', 'running',
        powerState =~ 'VM stopped', 'stopped',
        powerState =~ 'VM deallocated', 'deallocated',
        'unknown'
    )
    | project
        subscriptionId,
        rgKey = tolower(tostring(parentIdParts[4])),
        vmssKey = tolower(tostring(parentIdParts[8])),
        memberState
    | summarize
        actualInstanceCount = count(),
        runningInstanceCount = countif(memberState == 'running'),
        stoppedInstanceCount = countif(memberState == 'stopped'),
        deallocatedInstanceCount = countif(memberState == 'deallocated'),
        failedInstanceCount = countif(memberState == 'failed'),
        unknownInstanceCount = countif(memberState == 'unknown')
      by subscriptionId, rgKey, vmssKey
) on subscriptionId, rgKey, vmssKey
| extend actualInstanceCount = toint(coalesce(actualInstanceCount, 0))
| extend runningInstanceCount = toint(coalesce(runningInstanceCount, 0))
| extend stoppedInstanceCount = toint(coalesce(stoppedInstanceCount, 0))
| extend deallocatedInstanceCount = toint(coalesce(deallocatedInstanceCount, 0))
| extend failedInstanceCount = toint(coalesce(failedInstanceCount, 0))
| extend unknownInstanceCount = toint(coalesce(unknownInstanceCount, 0))
| extend capacity = toint(coalesce(capacity, 0))
| project
    subscriptionId,
    resourceGroup,
    vmssName,
    location,
    orchestrationMode,
    vmSize,
    skuTier,
    actualInstanceCount,
    runningInstanceCount,
    stoppedInstanceCount,
    deallocatedInstanceCount,
    failedInstanceCount,
    unknownInstanceCount,
    capacity
| order by subscriptionId asc, resourceGroup asc, vmssName asc
""".strip()

_VMSS_CHILD_ID_PATTERN = re.compile(
    r"/subscriptions/(?P<subscription_id>[^/]+)/resourceGroups/(?P<resource_group>[^/]+)/"
    r"providers/Microsoft\.Compute/virtualMachineScaleSets/(?P<vmss_name>[^/]+)/virtualMachines/[^/]+",
    re.IGNORECASE,
)


# Resource Graph query for *standalone* Azure VMs only. The
# ``properties.virtualMachineScaleSet.id`` filter excludes any VM that is a
# member of a VMSS (those are already covered by ``VMSS_COUNTS_QUERY``), so the
# two result sets are disjoint and the ``azure_vm_*`` metrics never double-count
# against ``azure_vmss_instance_count``. Azure Arc / hybrid machines live under
# ``microsoft.hybridcompute/machines`` and are intentionally out of scope.
STANDALONE_VMS_QUERY = """
Resources
| where type =~ 'microsoft.compute/virtualmachines'
| where isempty(tostring(properties.virtualMachineScaleSet.id))
| project
    subscriptionId,
    resourceGroup,
    vmName       = name,
    vmId         = tostring(properties.vmId),
    location,
    zone         = tostring(zones[0]),
    vmSize       = tostring(properties.hardwareProfile.vmSize),
    osType       = tostring(properties.storageProfile.osDisk.osType),
    powerState   = tostring(properties.extended.instanceView.powerState.displayStatus)
| order by subscriptionId asc, resourceGroup asc, vmName asc
""".strip()


# Map raw Azure Resource Graph ``displayStatus`` strings to the bounded enum
# emitted on the ``state`` label of ``azure_vm_power_state``. ARG returns
# human-readable strings like "VM running" / "VM deallocated"; normalizing in
# the collector lets us guarantee a fixed label set even if Azure adds new
# transitional states later (they fall through to ``unknown``).
_POWER_STATE_MAPPING: dict[str, str] = {
    "vm running": "running",
    "vm stopped": "stopped",
    "vm deallocated": "deallocated",
    "vm starting": "starting",
    "vm stopping": "stopping",
    "vm deallocating": "stopping",
}


class ResourceGraphClientProtocol(Protocol):
    """Protocol for the subset of ResourceGraphClient used by this exporter."""

    def resources(self, query: object) -> object:
        """Execute a Resource Graph query request."""


class _ResourceGraphPagedCollector:
    """Shared paging + retry behavior for Resource Graph KQL collectors.

    Subclasses implement :meth:`collect` and call :meth:`_run_paged_query` with
    their own KQL. Extracting this base class avoids duplicating ~40 lines of
    paging / backoff code between the VMSS and standalone-VM collectors and
    keeps retry behavior consistent.
    """

    def __init__(
        self,
        client: ResourceGraphClientProtocol,
        subscription_ids: Sequence[str],
        *,
        page_size: int = 1000,
        max_retries: int = 3,
        retry_base_delay_seconds: float = 1.0,
    ) -> None:
        if not subscription_ids:
            raise ValueError("subscription_ids cannot be empty")
        self._client = client
        self._subscription_ids = tuple(subscription_ids)
        self._page_size = page_size
        self._max_retries = max_retries
        self._retry_base_delay_seconds = retry_base_delay_seconds

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
            response = self._execute_with_retry(request)
            data = getattr(response, "data", None) or []
            if not isinstance(data, list):
                raise TypeError(f"Unexpected Resource Graph response data type: {type(data)!r}")
            all_rows.extend(data)

            skip_token = getattr(response, "skip_token", None)
            if not skip_token:
                return all_rows

    def _execute_with_retry(self, request: object) -> object:
        attempt = 0
        while True:
            try:
                return self._client.resources(request)
            except Exception as exc:  # noqa: BLE001 - Azure SDK raises multiple transient types.
                if attempt >= self._max_retries or not _is_retryable_exception(exc):
                    raise
                delay = self._retry_delay(attempt)
                LOGGER.warning(
                    "Resource Graph query failed on attempt %s/%s; retrying in %.2fs: %s",
                    attempt + 1,
                    self._max_retries + 1,
                    delay,
                    exc,
                )
                time.sleep(delay)
                attempt += 1

    def _retry_delay(self, attempt: int) -> float:
        backoff = min(
            self._retry_base_delay_seconds * (2**attempt),
            _MAX_RETRY_BACKOFF_SECONDS,
        )
        jitter = random.uniform(0, self._retry_base_delay_seconds)
        return backoff + jitter


class AzureResourceGraphVmssCollector(_ResourceGraphPagedCollector):
    """Collect VMSS counts from Azure Resource Graph."""

    def collect(self) -> list[VmssCount]:
        """Return normalized VMSS counts for all configured subscriptions."""

        rows = self._run_paged_query(VMSS_COUNTS_QUERY)
        return [normalize_vmss_count_row(row) for row in rows]


class AzureResourceGraphStandaloneVmCollector(_ResourceGraphPagedCollector):
    """Collect standalone (non-VMSS) Azure VM inventory from Resource Graph."""

    def collect(self) -> list[StandaloneVm]:
        """Return normalized standalone-VM inventory for all configured subscriptions."""

        rows = self._run_paged_query(STANDALONE_VMS_QUERY)
        return [normalize_standalone_vm_row(row) for row in rows]


def _is_retryable_exception(exc: Exception) -> bool:
    """Return whether an Azure SDK exception is likely transient."""

    # Authentication failures from our credential layer are not transient. Retrying
    # them just wastes time and floods logs.
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


def create_resource_graph_client(credential: object | None = None) -> ResourceGraphClientProtocol:
    """Create an authenticated Azure Resource Graph client.

    Uses :class:`~vmss_metrics_exporter.credentials.ResilientAzureCredential`, which
    falls back from Workload Identity to Managed Identity (and finally to
    `DefaultAzureCredential`) on hard authentication failures. The stock
    `DefaultAzureCredential` does not fall back through real auth errors, so this
    wrapper is required to keep the exporter alive when Workload Identity is
    misconfigured.
    """

    try:
        from azure.mgmt.resourcegraph import ResourceGraphClient
    except ImportError as exc:  # pragma: no cover - exercised only when dependencies are missing.
        raise RuntimeError(
            "Azure SDK packages are not installed. Install the project dependencies first."
        ) from exc

    if credential is None:
        from .credentials import create_credential

        credential = create_credential()
    return ResourceGraphClient(credential)


def build_query_request(
    *,
    subscriptions: Sequence[str],
    query: str,
    top: int,
    skip_token: str | None = None,
) -> object:
    """Build a Resource Graph query request while isolating Azure SDK model imports."""

    try:
        from azure.mgmt.resourcegraph.models import QueryRequest, QueryRequestOptions
    except ImportError as exc:  # pragma: no cover - exercised only when dependencies are missing.
        raise RuntimeError(
            "azure-mgmt-resourcegraph is not installed. Install the project dependencies first."
        ) from exc

    options_kwargs: dict[str, Any] = {"result_format": "objectArray", "top": top}
    if skip_token:
        options_kwargs["skip_token"] = skip_token

    return QueryRequest(
        subscriptions=list(subscriptions),
        query=query,
        options=QueryRequestOptions(**options_kwargs),
    )


def normalize_vmss_count_row(row: Mapping[str, Any]) -> VmssCount:
    """Normalize one Resource Graph row into a stable `VmssCount` model."""

    return VmssCount(
        subscription_id=_required_str(row, "subscriptionId"),
        resource_group=_required_str(row, "resourceGroup"),
        vmss_name=_required_str(row, "vmssName"),
        location=_optional_str(row, "location", default="unknown"),
        orchestration_mode=_optional_str(row, "orchestrationMode", default="unknown"),
        vm_size=_optional_str(row, "vmSize", default="unknown"),
        sku_tier=_optional_str(row, "skuTier", default="unknown"),
        actual_instance_count=_int_value(row.get("actualInstanceCount"), default=0),
        capacity=_int_value(row.get("capacity"), default=0),
        running_instance_count=_int_value(row.get("runningInstanceCount"), default=0),
        stopped_instance_count=_int_value(row.get("stoppedInstanceCount"), default=0),
        deallocated_instance_count=_int_value(row.get("deallocatedInstanceCount"), default=0),
        failed_instance_count=_int_value(row.get("failedInstanceCount"), default=0),
        unknown_instance_count=_int_value(row.get("unknownInstanceCount"), default=0),
    )


def normalize_standalone_vm_row(row: Mapping[str, Any]) -> StandaloneVm:
    """Normalize one Resource Graph row into a stable `StandaloneVm` model."""

    return StandaloneVm(
        subscription_id=_required_str(row, "subscriptionId"),
        resource_group=_required_str(row, "resourceGroup"),
        vm_name=_required_str(row, "vmName"),
        vm_id=_optional_str(row, "vmId", default="unknown"),
        location=_optional_str(row, "location", default="unknown"),
        # ``zone`` legitimately can be empty for non-zonal VMs; preserve that as
        # an empty string rather than the synthetic ``"unknown"`` sentinel so a
        # missing zone is distinguishable from an unparseable one in dashboards.
        zone=_optional_str(row, "zone", default=""),
        vm_size=_optional_str(row, "vmSize", default="unknown"),
        os_type=_optional_str(row, "osType", default="unknown"),
        power_state=_normalize_power_state(row.get("powerState")),
    )


def _normalize_power_state(value: Any) -> str:
    """Return the bounded ``state`` label for ``azure_vm_power_state``."""

    if value is None:
        return "unknown"
    text = str(value).strip().lower()
    if not text:
        return "unknown"
    return _POWER_STATE_MAPPING.get(text, "unknown")


def parse_vmss_parent_from_child_id(resource_id: str) -> tuple[str, str, str]:
    """Return `(subscription_id, resource_group, vmss_name)` from a VMSS VM resource ID."""

    match = _VMSS_CHILD_ID_PATTERN.search(resource_id)
    if not match:
        raise ValueError(f"Not a VMSS virtual machine resource ID: {resource_id!r}")
    return (
        match.group("subscription_id"),
        match.group("resource_group"),
        match.group("vmss_name"),
    )


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


def _int_value(value: Any, *, default: int) -> int:
    if value is None or value == "":
        return default
    return int(value)


def summarize_counts(counts: Iterable[VmssCount]) -> str:
    """Create a compact human-readable summary for one-shot runs."""

    lines = [
        "subscription_id\tresource_group\tvmss_name\tlocation\torchestration_mode\tvm_size\tsku_tier\tactual\tcapacity"
    ]
    for item in counts:
        lines.append(
            "\t".join(
                [
                    item.subscription_id,
                    item.resource_group,
                    item.vmss_name,
                    item.location,
                    item.orchestration_mode,
                    item.vm_size,
                    item.sku_tier,
                    str(item.actual_instance_count),
                    str(item.capacity),
                ]
            )
        )
    return "\n".join(lines)


def summarize_standalone_vms(vms: Iterable[StandaloneVm]) -> str:
    """Create a compact human-readable summary for one-shot runs."""

    lines = [
        "subscription_id\tresource_group\tvm_name\tlocation\tzone\tvm_size\tos_type\tpower_state"
    ]
    for item in vms:
        lines.append(
            "\t".join(
                [
                    item.subscription_id,
                    item.resource_group,
                    item.vm_name,
                    item.location,
                    item.zone,
                    item.vm_size,
                    item.os_type,
                    item.power_state,
                ]
            )
        )
    return "\n".join(lines)


# Re-export bounded state set for callers that need to pre-seed gauges.
__all__ = [
    "STANDALONE_VMS_QUERY",
    "STANDALONE_VM_POWER_STATES",
    "VMSS_COUNTS_QUERY",
    "AzureResourceGraphStandaloneVmCollector",
    "AzureResourceGraphVmssCollector",
    "ResourceGraphClientProtocol",
    "build_query_request",
    "create_resource_graph_client",
    "normalize_standalone_vm_row",
    "normalize_vmss_count_row",
    "parse_vmss_parent_from_child_id",
    "summarize_counts",
    "summarize_standalone_vms",
]
