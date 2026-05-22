from __future__ import annotations

from vmss_metrics_exporter import main as main_module
from vmss_metrics_exporter.config import Settings
from vmss_metrics_exporter.models import ManagedLustreCollectionResult


class _Closeable:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


class _FakeCredential(_Closeable):
    pass


class _FakeResourceGraphClient(_Closeable):
    pass


class _FakeMetricsQueryClient(_Closeable):
    pass


class _FakeVmssCollector:
    instances: list[_FakeVmssCollector] = []

    def __init__(
        self,
        resource_graph_client: object,
        subscription_ids: object,
        **kwargs: object,
    ) -> None:
        self.resource_graph_client = resource_graph_client
        self.subscription_ids = subscription_ids
        self.kwargs = kwargs
        self.collect_calls = 0
        self.instances.append(self)

    def collect(self) -> list[object]:
        self.collect_calls += 1
        return []


class _FakeLustreCollector:
    instances: list[_FakeLustreCollector] = []

    def __init__(
        self,
        resource_graph_client: object,
        metrics_query_client: object,
        subscription_ids: object,
        **kwargs: object,
    ) -> None:
        self.resource_graph_client = resource_graph_client
        self.metrics_query_client = metrics_query_client
        self.subscription_ids = subscription_ids
        self.kwargs = kwargs
        self.collect_calls = 0
        self.instances.append(self)

    def collect(self) -> ManagedLustreCollectionResult:
        self.collect_calls += 1
        return ManagedLustreCollectionResult(metrics=(), filesystem_count=0)


def _install_common_main_fakes(monkeypatch) -> tuple[
    _FakeCredential,
    _FakeResourceGraphClient,
    _FakeMetricsQueryClient,
    list[object],
    list[object],
]:
    credential = _FakeCredential()
    resource_graph_client = _FakeResourceGraphClient()
    metrics_query_client = _FakeMetricsQueryClient()
    rg_factory_credentials: list[object] = []
    metrics_factory_credentials: list[object] = []

    _FakeVmssCollector.instances = []
    _FakeLustreCollector.instances = []

    monkeypatch.setattr(
        main_module,
        "load_settings",
        lambda **_kwargs: Settings(subscription_ids=("sub-a",)),
    )
    monkeypatch.setattr(main_module, "create_credential", lambda: credential)

    def fake_create_resource_graph_client(received_credential: object | None = None) -> object:
        rg_factory_credentials.append(received_credential)
        return resource_graph_client

    def fake_create_metrics_query_client(received_credential: object | None = None) -> object:
        metrics_factory_credentials.append(received_credential)
        return metrics_query_client

    monkeypatch.setattr(
        main_module,
        "create_resource_graph_client",
        fake_create_resource_graph_client,
    )
    monkeypatch.setattr(
        main_module,
        "create_metrics_query_client",
        fake_create_metrics_query_client,
    )
    monkeypatch.setattr(main_module, "AzureResourceGraphVmssCollector", _FakeVmssCollector)
    monkeypatch.setattr(main_module, "AzureManagedLustreCollector", _FakeLustreCollector)

    return (
        credential,
        resource_graph_client,
        metrics_query_client,
        rg_factory_credentials,
        metrics_factory_credentials,
    )


def test_main_once_reuses_shared_credential_and_closes_resources(monkeypatch, capsys) -> None:
    (
        credential,
        resource_graph_client,
        metrics_query_client,
        rg_factory_credentials,
        metrics_factory_credentials,
    ) = _install_common_main_fakes(monkeypatch)

    exit_code = main_module.main(["--once"])

    assert exit_code == 0
    assert rg_factory_credentials == [credential]
    assert metrics_factory_credentials == [credential]
    assert len(_FakeVmssCollector.instances) == 1
    assert _FakeVmssCollector.instances[0].resource_graph_client is resource_graph_client
    assert _FakeVmssCollector.instances[0].collect_calls == 1
    assert len(_FakeLustreCollector.instances) == 1
    assert _FakeLustreCollector.instances[0].resource_graph_client is resource_graph_client
    assert _FakeLustreCollector.instances[0].metrics_query_client is metrics_query_client
    assert _FakeLustreCollector.instances[0].collect_calls == 1
    assert metrics_query_client.close_calls == 1
    assert resource_graph_client.close_calls == 1
    assert credential.close_calls == 1
    assert "subscription_id" in capsys.readouterr().out


def test_main_closes_shared_credential_when_startup_fails(monkeypatch) -> None:
    (
        credential,
        resource_graph_client,
        metrics_query_client,
        rg_factory_credentials,
        metrics_factory_credentials,
    ) = _install_common_main_fakes(monkeypatch)

    def fail_metrics_client(received_credential: object | None = None) -> object:
        metrics_factory_credentials.append(received_credential)
        raise RuntimeError("metrics client boom")

    monkeypatch.setattr(main_module, "create_metrics_query_client", fail_metrics_client)

    exit_code = main_module.main(["--once"])

    assert exit_code == 1
    assert rg_factory_credentials == [credential]
    assert metrics_factory_credentials == [credential]
    assert resource_graph_client.close_calls == 1
    assert metrics_query_client.close_calls == 0
    assert credential.close_calls == 1
