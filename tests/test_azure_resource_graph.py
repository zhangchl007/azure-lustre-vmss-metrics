from __future__ import annotations

import sys
import types
from dataclasses import dataclass

import pytest

from vmss_metrics_exporter.azure_resource_graph import (
    STANDALONE_VMS_QUERY,
    VMSS_COUNTS_QUERY,
    AzureResourceGraphStandaloneVmCollector,
    AzureResourceGraphVmssCollector,
    _normalize_power_state,
    create_resource_graph_client,
    normalize_standalone_vm_row,
    normalize_vmss_count_row,
    parse_vmss_parent_from_child_id,
    summarize_counts,
    summarize_standalone_vms,
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
                    "subscriptionId": "sub-a",
                    "resourceGroup": "rg-a",
                    "vmssName": "vmss-a",
                    "location": "eastus",
                    "orchestrationMode": "Uniform",
                    "vmSize": "Standard_D2s_v3",
                    "skuTier": "Standard",
                    "actualInstanceCount": 3,
                    "capacity": 5,
                }
            ]
        )


class FakeBadRequestClient:
    def __init__(self) -> None:
        self.calls = 0

    def resources(self, _query: object) -> object:
        self.calls += 1
        response = type("Response", (), {"status_code": 400})()
        raise FakeAzureError(response)


class FakeAzureError(Exception):
    def __init__(self, response: object) -> None:
        super().__init__("bad request")
        self.response = response


def test_create_resource_graph_client_uses_provided_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credential = object()

    class _FakeResourceGraphClient:
        def __init__(self, received_credential: object) -> None:
            self.credential = received_credential

    fake_module = types.ModuleType("azure.mgmt.resourcegraph")
    fake_module.ResourceGraphClient = _FakeResourceGraphClient
    monkeypatch.setitem(sys.modules, "azure.mgmt.resourcegraph", fake_module)
    monkeypatch.setattr(
        "vmss_metrics_exporter.credentials.create_credential",
        lambda: pytest.fail("create_credential should not be called when credential is provided"),
    )

    client = create_resource_graph_client(credential)

    assert isinstance(client, _FakeResourceGraphClient)
    assert client.credential is credential


def test_normalize_vmss_count_row() -> None:
    count = normalize_vmss_count_row(
        {
            "subscriptionId": "sub-a",
            "resourceGroup": "rg-a",
            "vmssName": "vmss-a",
            "location": "eastus",
            "orchestrationMode": "Flexible",
            "vmSize": "Standard_D4s_v5",
            "skuTier": "Standard",
            "actualInstanceCount": "2",
            "runningInstanceCount": "1",
            "stoppedInstanceCount": "0",
            "deallocatedInstanceCount": "1",
            "failedInstanceCount": "0",
            "unknownInstanceCount": "0",
            "capacity": "4",
        }
    )

    assert count.subscription_id == "sub-a"
    assert count.actual_instance_count == 2
    assert count.running_instance_count == 1
    assert count.deallocated_instance_count == 1
    assert count.capacity == 4
    assert count.vm_size == "Standard_D4s_v5"
    assert count.sku_tier == "Standard"
    assert count.label_values == ("sub-a", "rg-a", "vmss-a", "eastus", "Flexible")
    assert count.info_label_values == (
        "sub-a",
        "rg-a",
        "vmss-a",
        "eastus",
        "Flexible",
        "Standard_D4s_v5",
        "Standard",
    )


def test_normalize_vmss_count_row_defaults_when_sku_missing() -> None:
    count = normalize_vmss_count_row(
        {
            "subscriptionId": "sub-a",
            "resourceGroup": "rg-a",
            "vmssName": "vmss-a",
        }
    )

    assert count.vm_size == "unknown"
    assert count.sku_tier == "unknown"


def test_vmss_counts_query_avoids_unsupported_let_statements() -> None:
    assert "let " not in VMSS_COUNTS_QUERY.lower()
    assert "ComputeResources" in VMSS_COUNTS_QUERY
    assert "microsoft.compute/virtualmachinescalesets/virtualmachines" in VMSS_COUNTS_QUERY
    assert "microsoft.compute/virtualmachines'" in VMSS_COUNTS_QUERY
    assert "vmSize = tostring(sku.name)" in VMSS_COUNTS_QUERY
    assert "skuTier = tostring(sku.tier)" in VMSS_COUNTS_QUERY


def test_parse_vmss_parent_from_child_id() -> None:
    resource_id = (
        "/subscriptions/sub-a/resourceGroups/rg-a/providers/Microsoft.Compute/"
        "virtualMachineScaleSets/vmss-a/virtualMachines/12"
    )

    assert parse_vmss_parent_from_child_id(resource_id) == ("sub-a", "rg-a", "vmss-a")


def test_parse_vmss_parent_rejects_non_child_id() -> None:
    with pytest.raises(ValueError):
        parse_vmss_parent_from_child_id("/subscriptions/sub-a/resourceGroups/rg-a")


def test_collector_normalizes_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "vmss_metrics_exporter.azure_resource_graph.build_query_request",
        lambda **kwargs: kwargs,
    )
    client = FakeResourceGraphClient()
    collector = AzureResourceGraphVmssCollector(client, ["sub-a"], retry_base_delay_seconds=0)

    counts = collector.collect()

    assert client.calls == 1
    assert len(counts) == 1
    assert counts[0].vmss_name == "vmss-a"
    assert counts[0].actual_instance_count == 3


def test_collector_does_not_retry_bad_request(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "vmss_metrics_exporter.azure_resource_graph.build_query_request",
        lambda **kwargs: kwargs,
    )
    client = FakeBadRequestClient()
    collector = AzureResourceGraphVmssCollector(
        client,
        ["sub-a"],
        max_retries=3,
        retry_base_delay_seconds=0,
    )

    with pytest.raises(FakeAzureError):
        collector.collect()

    assert client.calls == 1


class _FakeAuthFailureClient:
    def __init__(self) -> None:
        self.calls = 0

    def resources(self, _query: object) -> object:
        from azure.core.exceptions import ClientAuthenticationError

        self.calls += 1
        raise ClientAuthenticationError("AADSTS700211: no federated identity match")


def test_collector_does_not_retry_authentication_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Auth failures from the credential layer must not be retried."""

    from azure.core.exceptions import ClientAuthenticationError

    monkeypatch.setattr(
        "vmss_metrics_exporter.azure_resource_graph.build_query_request",
        lambda **kwargs: kwargs,
    )
    client = _FakeAuthFailureClient()
    collector = AzureResourceGraphVmssCollector(
        client,
        ["sub-a"],
        max_retries=3,
        retry_base_delay_seconds=0,
    )

    with pytest.raises(ClientAuthenticationError):
        collector.collect()

    assert client.calls == 1


def test_summarize_counts_contains_tabular_output() -> None:
    row = normalize_vmss_count_row(
        {
            "subscriptionId": "sub-a",
            "resourceGroup": "rg-a",
            "vmssName": "vmss-a",
            "location": "eastus",
            "orchestrationMode": "Uniform",
            "vmSize": "Standard_DS2_v2",
            "skuTier": "Standard",
            "actualInstanceCount": 1,
            "capacity": 1,
        }
    )

    summary = summarize_counts([row])

    assert "subscription_id" in summary
    assert "vm_size" in summary
    assert "sku_tier" in summary
    assert "vmss-a" in summary
    assert "Standard_DS2_v2" in summary


# ---------------------------------------------------------------------------
# Standalone (non-VMSS) VM inventory collector
# ---------------------------------------------------------------------------


class FakeStandaloneVmClient:
    """Two-page Resource Graph stub for the standalone-VM collector tests."""

    def __init__(self) -> None:
        self.calls = 0

    def resources(self, query: object) -> FakeResponse:
        self.calls += 1
        if self.calls == 1:
            return FakeResponse(
                data=[
                    {
                        "subscriptionId": "sub-a",
                        "resourceGroup": "rg-a",
                        "vmName": "vm-1",
                        "vmId": "id-1",
                        "location": "eastus",
                        "zone": "1",
                        "vmSize": "Standard_D2s_v3",
                        "osType": "Linux",
                        "powerState": "VM running",
                    },
                ],
                skip_token="page-2",
            )
        return FakeResponse(
            data=[
                {
                    "subscriptionId": "sub-a",
                    "resourceGroup": "rg-a",
                    "vmName": "vm-2",
                    "vmId": "id-2",
                    "location": "eastus",
                    "zone": "",
                    "vmSize": "Standard_D4s_v5",
                    "osType": "Windows",
                    "powerState": "VM deallocated",
                },
            ],
            skip_token=None,
        )


def test_standalone_vms_query_filters_out_vmss_members() -> None:
    # The query MUST scope to standalone VMs only so the two metric families
    # (azure_vmss_instance_count and azure_vm_info) are guaranteed disjoint.
    lowered = STANDALONE_VMS_QUERY.lower()
    assert "microsoft.compute/virtualmachines" in lowered
    assert "isempty(tostring(properties.virtualmachinescaleset.id))" in lowered


def test_normalize_standalone_vm_row_happy_path() -> None:
    vm = normalize_standalone_vm_row(
        {
            "subscriptionId": "sub-a",
            "resourceGroup": "rg-a",
            "vmName": "vm-1",
            "vmId": "vmid-1",
            "location": "eastus",
            "zone": "2",
            "vmSize": "Standard_D2s_v3",
            "osType": "Linux",
            "powerState": "VM running",
        }
    )

    assert vm.subscription_id == "sub-a"
    assert vm.vm_name == "vm-1"
    assert vm.vm_id == "vmid-1"
    assert vm.zone == "2"
    assert vm.power_state == "running"
    assert vm.info_label_values == (
        "sub-a",
        "rg-a",
        "vm-1",
        "vmid-1",
        "eastus",
        "2",
        "Standard_D2s_v3",
        "Linux",
    )
    assert vm.identity_label_values == ("sub-a", "rg-a", "vm-1")


def test_normalize_standalone_vm_row_defaults_for_optional_fields() -> None:
    vm = normalize_standalone_vm_row(
        {
            "subscriptionId": "sub-a",
            "resourceGroup": "rg-a",
            "vmName": "vm-1",
        }
    )

    # Missing zone stays empty (legitimate for non-zonal VMs); other optional
    # fields fall back to the synthetic "unknown" sentinel.
    assert vm.zone == ""
    assert vm.vm_id == "unknown"
    assert vm.location == "unknown"
    assert vm.vm_size == "unknown"
    assert vm.os_type == "unknown"
    assert vm.power_state == "unknown"


def test_normalize_standalone_vm_row_requires_identity_fields() -> None:
    with pytest.raises(ValueError):
        normalize_standalone_vm_row({"subscriptionId": "sub-a", "resourceGroup": "rg-a"})


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("VM running", "running"),
        ("vm RUNNING", "running"),
        ("VM stopped", "stopped"),
        ("VM deallocated", "deallocated"),
        ("VM starting", "starting"),
        ("VM stopping", "stopping"),
        ("VM deallocating", "stopping"),
        ("PowerState/unknown", "unknown"),
        ("", "unknown"),
        (None, "unknown"),
    ],
)
def test_normalize_power_state_mapping(raw: object, expected: str) -> None:
    assert _normalize_power_state(raw) == expected


def test_standalone_collector_pages_and_normalizes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "vmss_metrics_exporter.azure_resource_graph.build_query_request",
        lambda **kwargs: kwargs,
    )
    client = FakeStandaloneVmClient()
    collector = AzureResourceGraphStandaloneVmCollector(
        client, ["sub-a"], retry_base_delay_seconds=0
    )

    vms = collector.collect()

    assert client.calls == 2
    assert [vm.vm_name for vm in vms] == ["vm-1", "vm-2"]
    assert vms[0].power_state == "running"
    assert vms[1].power_state == "deallocated"
    assert vms[1].zone == ""


def test_summarize_standalone_vms_contains_tabular_output() -> None:
    vm = normalize_standalone_vm_row(
        {
            "subscriptionId": "sub-a",
            "resourceGroup": "rg-a",
            "vmName": "vm-1",
            "vmId": "vmid-1",
            "location": "eastus",
            "zone": "1",
            "vmSize": "Standard_D2s_v3",
            "osType": "Linux",
            "powerState": "VM running",
        }
    )

    summary = summarize_standalone_vms([vm])

    assert "vm_name" in summary
    assert "power_state" in summary
    assert "vm-1" in summary
    assert "running" in summary
