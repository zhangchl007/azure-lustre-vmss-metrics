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


class _FakeExporterCallbacks:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def set_leader(self, is_leader: bool) -> None:
        self.calls.append(f"set_leader:{is_leader}")

    def collect_once(self) -> list[object]:
        self.calls.append("collect_once")
        return []


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


def test_started_leading_populates_metrics_before_labeling_service_endpoint(monkeypatch) -> None:
    exporter = _FakeExporterCallbacks()
    label_calls: list[bool] = []
    monkeypatch.setattr(
        main_module,
        "_patch_pod_leader_label",
        lambda _settings, *, is_leader: label_calls.append(is_leader),
    )

    main_module._on_started_leading(
        Settings(
            subscription_ids=("sub-a",),
            leader_election_identity="pod-a",
            leader_election_namespace="default",
        ),
        exporter,  # type: ignore[arg-type]
    )

    assert exporter.calls == ["set_leader:True", "collect_once"]
    assert label_calls == [True]


def test_stopped_leading_removes_service_endpoint_before_clearing_metrics(monkeypatch) -> None:
    exporter = _FakeExporterCallbacks()
    calls: list[str] = []

    def fake_patch(_settings: object, *, is_leader: bool) -> None:
        calls.append(f"label:{is_leader}")

    def fake_set_leader(is_leader: bool) -> None:
        calls.append(f"set_leader:{is_leader}")

    exporter.set_leader = fake_set_leader  # type: ignore[method-assign]
    monkeypatch.setattr(main_module, "_patch_pod_leader_label", fake_patch)

    main_module._on_stopped_leading(
        Settings(
            subscription_ids=("sub-a",),
            leader_election_identity="pod-a",
            leader_election_namespace="default",
        ),
        exporter,  # type: ignore[arg-type]
    )

    assert calls == ["label:False", "set_leader:False"]


def test_shutdown_drain_sleeps_for_configured_window(monkeypatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr(main_module.time, "sleep", lambda seconds: sleeps.append(seconds))

    main_module._drain_after_leader_release(
        Settings(subscription_ids=("sub-a",), shutdown_drain_seconds=12.5)
    )

    assert sleeps == [12.5]


def test_shutdown_drain_is_skipped_when_disabled(monkeypatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr(main_module.time, "sleep", lambda seconds: sleeps.append(seconds))

    main_module._drain_after_leader_release(
        Settings(subscription_ids=("sub-a",), shutdown_drain_seconds=0.0)
    )

    assert sleeps == []
