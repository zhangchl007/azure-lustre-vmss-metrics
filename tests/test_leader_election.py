from __future__ import annotations

import copy
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import pytest
from kubernetes import client

from vmss_metrics_exporter.leader_election import (
    LeaderElectionConfig,
    LeaderElectionRunner,
    _build_real_election,
    _LeaseElection,
    _normalize_bearer_token_scheme,
    _refresh_service_account_token_from_file,
    _wrap_refresh_api_key_hook,
)


class ApiStatusError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"Kubernetes API status {status}")
        self.status = status


class FakeCoordinationV1Api:
    def __init__(self, lease: object | None = None) -> None:
        self.lease = copy.deepcopy(lease)
        self.next_resource_version = 1
        self.read_failures: list[int] = []
        self.create_failures: list[int] = []
        self.replace_failures: list[int] = []
        self.replace_conflicts_remaining = 0
        self.create_calls = 0
        self.replace_calls = 0
        self.request_timeouts: list[tuple[float, float] | None] = []
        if self.lease is not None:
            metadata = getattr(self.lease, "metadata", None)
            if getattr(metadata, "resource_version", None) is None:
                metadata.resource_version = str(self.next_resource_version)
                self.next_resource_version += 1

    def read_namespaced_lease(
        self,
        *,
        name: str,
        namespace: str,
        _request_timeout: tuple[float, float] | None = None,
    ) -> object:
        self.request_timeouts.append(_request_timeout)
        if self.read_failures:
            raise ApiStatusError(self.read_failures.pop(0))
        if self.lease is None:
            raise ApiStatusError(404)
        assert self.lease.metadata.name == name
        assert self.lease.metadata.namespace == namespace
        return copy.deepcopy(self.lease)

    def create_namespaced_lease(
        self,
        *,
        namespace: str,
        body: object,
        _request_timeout: tuple[float, float] | None = None,
    ) -> object:
        self.request_timeouts.append(_request_timeout)
        self.create_calls += 1
        if self.create_failures:
            raise ApiStatusError(self.create_failures.pop(0))
        if self.lease is not None:
            raise ApiStatusError(409)
        assert body.metadata.namespace == namespace
        self.lease = copy.deepcopy(body)
        self.lease.metadata.resource_version = str(self.next_resource_version)
        self.next_resource_version += 1
        return copy.deepcopy(self.lease)

    def replace_namespaced_lease(
        self,
        *,
        name: str,
        namespace: str,
        body: object,
        _request_timeout: tuple[float, float] | None = None,
    ) -> object:
        self.request_timeouts.append(_request_timeout)
        self.replace_calls += 1
        if self.replace_failures:
            raise ApiStatusError(self.replace_failures.pop(0))
        if self.lease is None:
            raise ApiStatusError(404)
        if self.replace_conflicts_remaining > 0:
            self.replace_conflicts_remaining -= 1
            raise ApiStatusError(409)
        assert body.metadata.name == name
        assert body.metadata.namespace == namespace
        if body.metadata.resource_version != self.lease.metadata.resource_version:
            raise ApiStatusError(409)
        self.lease = copy.deepcopy(body)
        self.lease.metadata.resource_version = str(self.next_resource_version)
        self.next_resource_version += 1
        return copy.deepcopy(self.lease)


@dataclass
class StubElection:
    """Test double for the runner supervisor loop."""

    on_started_leading: Callable[[], None]
    on_stopped_leading: Callable[[], None]
    behaviour: list[str] = field(default_factory=list)
    release_calls: int = 0
    release_notify_values: list[bool] = field(default_factory=list)

    def run(self) -> None:
        action = self.behaviour.pop(0) if self.behaviour else "lead-then-stop"
        if action == "raise":
            raise RuntimeError("simulated transient API error")
        if action == "lead-then-stop":
            self.on_started_leading()
            self.on_stopped_leading()
            return
        if action == "stop-only":
            self.on_stopped_leading()
            return
        if action == "wait":
            while self.release_calls == 0:
                time.sleep(0.01)
            return
        raise AssertionError(f"unknown behaviour: {action!r}")

    def release(self, *, notify_stopped: bool = True) -> None:
        self.release_notify_values.append(notify_stopped)
        self.release_calls += 1


def _make_config(identity: str = "test-pod-0") -> LeaderElectionConfig:
    return LeaderElectionConfig(
        lock_name="test-lock",
        lock_namespace="default",
        identity=identity,
        lease_duration_seconds=5,
        renew_deadline_seconds=2,
        retry_period_seconds=1,
    )


def _make_lease(
    *,
    holder_identity: str | None,
    renew_age_seconds: int = 0,
    lease_duration_seconds: int = 5,
    lease_transitions: int = 0,
    resource_version: str = "1",
) -> object:
    now = datetime.now(timezone.utc) - timedelta(seconds=renew_age_seconds)
    return client.V1Lease(
        api_version="coordination.k8s.io/v1",
        kind="Lease",
        metadata=client.V1ObjectMeta(
            name="test-lock",
            namespace="default",
            resource_version=resource_version,
        ),
        spec=client.V1LeaseSpec(
            acquire_time=now,
            holder_identity=holder_identity,
            lease_duration_seconds=lease_duration_seconds,
            lease_transitions=lease_transitions,
            renew_time=now,
        ),
    )


def _make_election(
    api: FakeCoordinationV1Api,
    *,
    config: LeaderElectionConfig | None = None,
    started: Callable[[], None] | None = None,
    stopped: Callable[[], None] | None = None,
    stop_event: threading.Event | None = None,
    coordination_api_factory: Callable[[], object] | None = None,
) -> _LeaseElection:
    return _LeaseElection(
        config=config or _make_config(),
        on_started_leading=started or (lambda: None),
        on_stopped_leading=stopped or (lambda: None),
        coordination_api=api,
        coordination_api_factory=coordination_api_factory,
        stop_event=stop_event,
    )


def test_leader_election_config_validates_durations() -> None:
    with pytest.raises(ValueError):
        LeaderElectionConfig(
            lock_name="x", lock_namespace="default", identity="x",
            lease_duration_seconds=10, renew_deadline_seconds=10, retry_period_seconds=2,
        )
    with pytest.raises(ValueError):
        LeaderElectionConfig(
            lock_name="x", lock_namespace="default", identity="x",
            lease_duration_seconds=3, renew_deadline_seconds=2, retry_period_seconds=1,
        )
    with pytest.raises(ValueError):
        LeaderElectionConfig(
            lock_name="", lock_namespace="default", identity="x",
        )
    with pytest.raises(ValueError):
        LeaderElectionConfig(
            lock_name="x", lock_namespace="default", identity="x",
            lease_duration_seconds=15, renew_deadline_seconds=5, retry_period_seconds=5,
        )
    with pytest.raises(ValueError):
        LeaderElectionConfig(
            lock_name="x", lock_namespace="default", identity="x",
            lease_duration_seconds=15, renew_deadline_seconds=6, retry_period_seconds=5,
        )


def test_build_real_election_uses_kubernetes_lease() -> None:
    election = _build_real_election(
        config=_make_config(),
        on_started_leading=lambda: None,
        on_stopped_leading=lambda: None,
    )

    assert isinstance(election, _LeaseElection)


def test_lease_election_creates_missing_lease() -> None:
    api = FakeCoordinationV1Api()
    election = _make_election(api)

    assert election._try_acquire_once() is True

    assert api.create_calls == 1
    assert api.lease.spec.holder_identity == "test-pod-0"
    assert api.lease.spec.lease_duration_seconds == 5
    assert api.lease.spec.lease_transitions == 0


def test_lease_election_recovers_from_unauthorized_read_during_acquire() -> None:
    stale_api = FakeCoordinationV1Api()
    stale_api.read_failures.append(401)
    refreshed_api = FakeCoordinationV1Api()
    refresh_calls = 0

    def refresh_api() -> object:
        nonlocal refresh_calls
        refresh_calls += 1
        return refreshed_api

    election = _make_election(stale_api, coordination_api_factory=refresh_api)

    assert election._try_acquire_once() is True

    assert refresh_calls == 1
    assert stale_api.create_calls == 0
    assert refreshed_api.create_calls == 1
    assert refreshed_api.lease.spec.holder_identity == "test-pod-0"


def test_lease_election_recovers_from_unauthorized_read_during_renewal() -> None:
    lease = _make_lease(holder_identity="test-pod-0")
    stale_api = FakeCoordinationV1Api(lease)
    stale_api.read_failures.append(401)
    refreshed_api = FakeCoordinationV1Api(lease)
    refresh_calls = 0

    def refresh_api() -> object:
        nonlocal refresh_calls
        refresh_calls += 1
        return refreshed_api

    election = _make_election(stale_api, coordination_api_factory=refresh_api)

    assert election._renew_once() == "renewed"

    assert refresh_calls == 1
    assert stale_api.replace_calls == 0
    assert refreshed_api.replace_calls == 1
    assert refreshed_api.lease.spec.holder_identity == "test-pod-0"


def test_lease_election_retries_replace_after_unauthorized() -> None:
    lease = _make_lease(holder_identity="test-pod-0")
    stale_api = FakeCoordinationV1Api(lease)
    stale_api.replace_failures.append(401)
    refreshed_api = FakeCoordinationV1Api(lease)
    refresh_calls = 0

    def refresh_api() -> object:
        nonlocal refresh_calls
        refresh_calls += 1
        return refreshed_api

    election = _make_election(stale_api, coordination_api_factory=refresh_api)

    assert election._renew_once() == "renewed"

    assert refresh_calls == 1
    assert stale_api.replace_calls == 1
    assert refreshed_api.replace_calls == 1
    assert refreshed_api.lease.spec.holder_identity == "test-pod-0"


def test_lease_election_bounds_kubernetes_api_request_timeout() -> None:
    api = FakeCoordinationV1Api()
    election = _make_election(
        api,
        config=LeaderElectionConfig(
            lock_name="test-lock",
            lock_namespace="default",
            identity="test-pod-0",
            lease_duration_seconds=15,
            renew_deadline_seconds=5,
            retry_period_seconds=1,
        ),
    )

    assert election._try_acquire_once() is True

    assert api.request_timeouts == [(2.5, 2.5), (2.5, 2.5)]


def test_lease_election_acquires_empty_released_lease() -> None:
    api = FakeCoordinationV1Api(_make_lease(holder_identity="", lease_transitions=3))
    election = _make_election(api)

    assert election._try_acquire_once() is True

    assert api.create_calls == 0
    assert api.replace_calls == 1
    assert api.lease.spec.holder_identity == "test-pod-0"
    assert api.lease.spec.lease_transitions == 4


def test_lease_election_does_not_steal_unexpired_lease() -> None:
    api = FakeCoordinationV1Api(_make_lease(holder_identity="other-pod", renew_age_seconds=1))
    election = _make_election(api)

    assert election._try_acquire_once() is False

    assert api.replace_calls == 0
    assert api.lease.spec.holder_identity == "other-pod"


def test_lease_election_does_not_trust_remote_time_on_first_observation() -> None:
    api = FakeCoordinationV1Api(_make_lease(holder_identity="other-pod", renew_age_seconds=600))
    election = _make_election(api)

    assert election._try_acquire_once() is False

    assert api.replace_calls == 0
    assert api.lease.spec.holder_identity == "other-pod"


def test_lease_election_acquires_expired_lease_and_bumps_transition() -> None:
    api = FakeCoordinationV1Api(
        _make_lease(holder_identity="other-pod", renew_age_seconds=10, lease_transitions=7)
    )
    election = _make_election(api)
    election._observe_lease(api.lease, time.monotonic() - 10)

    assert election._try_acquire_once() is True

    assert api.replace_calls == 1
    assert api.lease.spec.holder_identity == "test-pod-0"
    assert api.lease.spec.lease_transitions == 8


def test_lease_election_retries_after_conflict() -> None:
    api = FakeCoordinationV1Api(
        _make_lease(holder_identity="other-pod", renew_age_seconds=10, lease_transitions=1)
    )
    api.replace_conflicts_remaining = 1
    election = _make_election(api)
    election._observe_lease(api.lease, time.monotonic() - 10)

    assert election._try_acquire_once() is False
    assert election._try_acquire_once() is True

    assert api.replace_calls == 2
    assert api.lease.spec.holder_identity == "test-pod-0"
    assert api.lease.spec.lease_transitions == 2


def test_lease_election_renews_self_held_lease() -> None:
    old_lease = _make_lease(holder_identity="test-pod-0", renew_age_seconds=2, lease_transitions=2)
    old_renew_time = old_lease.spec.renew_time
    api = FakeCoordinationV1Api(old_lease)
    election = _make_election(api)

    assert election._renew_once() == "renewed"

    assert api.replace_calls == 1
    assert api.lease.spec.holder_identity == "test-pod-0"
    assert api.lease.spec.lease_transitions == 2
    assert api.lease.spec.renew_time > old_renew_time


def test_lease_election_release_clears_holder_and_stops_once() -> None:
    api = FakeCoordinationV1Api(_make_lease(holder_identity="test-pod-0", lease_transitions=5))
    stopped: list[int] = []
    election = _make_election(api, stopped=lambda: stopped.append(1))
    election._become_leader()

    election.release()
    election.release()

    assert api.lease.spec.holder_identity == ""
    assert api.lease.spec.lease_duration_seconds == 1
    assert api.lease.spec.lease_transitions == 5
    assert stopped == [1]


def test_lease_election_release_can_skip_stopped_callback_on_shutdown() -> None:
    api = FakeCoordinationV1Api(_make_lease(holder_identity="test-pod-0"))
    stopped: list[int] = []
    election = _make_election(api, stopped=lambda: stopped.append(1))
    election._become_leader()

    election.release(notify_stopped=False)
    election.release()

    assert api.lease.spec.holder_identity == ""
    assert stopped == []


def test_lease_election_run_releases_on_shutdown_without_waiting_retry_period() -> None:
    api = FakeCoordinationV1Api()
    started = threading.Event()
    stopped = threading.Event()
    election = _make_election(
        api,
        started=started.set,
        stopped=stopped.set,
    )
    thread = threading.Thread(target=election.run)

    thread.start()
    assert started.wait(1)
    release_started = time.monotonic()
    election.release()
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert stopped.is_set()
    assert api.lease.spec.holder_identity == ""
    assert time.monotonic() - release_started < 1


def test_normalize_bearer_token_scheme_capitalizes_lowercase_bearer() -> None:
    class Config:
        api_key = {"authorization": "bearer token-value"}

    _normalize_bearer_token_scheme(Config())

    assert Config.api_key["authorization"] == "Bearer token-value"
    assert Config.api_key["BearerToken"] == "Bearer token-value"


def test_normalize_bearer_token_scheme_populates_kubernetes_36_key() -> None:
    class Config:
        api_key = {"authorization": "token-value"}

    _normalize_bearer_token_scheme(Config())

    assert Config.api_key["authorization"] == "Bearer token-value"
    assert Config.api_key["BearerToken"] == "Bearer token-value"


def test_refresh_api_key_hook_keeps_bearer_token_key_in_sync() -> None:
    class Config:
        api_key = {"authorization": "Bearer old-token", "BearerToken": "Bearer old-token"}

        def refresh_api_key_hook(self, config: object) -> None:
            self.api_key["authorization"] = "bearer refreshed-token"

    config = Config()
    _wrap_refresh_api_key_hook(config)

    config.refresh_api_key_hook(config)

    assert config.api_key["authorization"] == "Bearer refreshed-token"
    assert config.api_key["BearerToken"] == "Bearer refreshed-token"


def test_service_account_token_file_refresh_updates_both_kubernetes_auth_keys(
    tmp_path: pytest.TempPathFactory,
) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("projected-token\n")

    class Config:
        api_key = {"authorization": "Bearer old-token", "BearerToken": "Bearer old-token"}

    config = Config()

    assert _refresh_service_account_token_from_file(config, str(token_file)) is True

    assert config.api_key["authorization"] == "Bearer projected-token"
    assert config.api_key["BearerToken"] == "Bearer projected-token"


def test_refresh_api_key_hook_uses_latest_projected_token_file(
    tmp_path: pytest.TempPathFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("first-token\n")
    monkeypatch.setenv("KUBERNETES_SERVICEACCOUNT_TOKEN_FILE", str(token_file))

    class Config:
        api_key = {"authorization": "Bearer old-token", "BearerToken": "Bearer old-token"}

        def refresh_api_key_hook(self, config: object) -> None:
            self.api_key["authorization"] = "bearer stale-hook-token"

    config = Config()
    _wrap_refresh_api_key_hook(config)

    config.refresh_api_key_hook(config)
    token_file.write_text("second-token\n")
    config.refresh_api_key_hook(config)

    assert config.api_key["authorization"] == "Bearer second-token"
    assert config.api_key["BearerToken"] == "Bearer second-token"


def test_refresh_api_key_hook_reinstalls_wrapper_after_incluster_refresh(
    tmp_path: pytest.TempPathFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("projected-token\n")
    monkeypatch.setenv("KUBERNETES_SERVICEACCOUNT_TOKEN_FILE", str(token_file))

    class Config:
        api_key = {"authorization": "Bearer old-token", "BearerToken": "Bearer old-token"}

        def _raw_refresh(self, config: object) -> None:
            self.api_key["authorization"] = "bearer refreshed-token"
            config.refresh_api_key_hook = self._raw_refresh

        refresh_api_key_hook = _raw_refresh

    config = Config()
    _wrap_refresh_api_key_hook(config)

    config.refresh_api_key_hook(config)
    first_hook = config.refresh_api_key_hook
    config.api_key["authorization"] = "bearer second-token"
    config.refresh_api_key_hook(config)

    assert config.refresh_api_key_hook is first_hook
    assert config.api_key["authorization"] == "Bearer projected-token"
    assert config.api_key["BearerToken"] == "Bearer projected-token"


def test_runner_invokes_callbacks_on_leadership_change() -> None:
    started: list[int] = []
    stopped: list[int] = []
    behaviours = ["lead-then-stop"]

    runner = LeaderElectionRunner(
        _make_config(),
        on_started_leading=lambda: started.append(1),
        on_stopped_leading=lambda: stopped.append(1),
        election_factory=lambda **kwargs: StubElection(
            kwargs["on_started_leading"],
            kwargs["on_stopped_leading"],
            behaviour=behaviours,
        ),
    )

    threading.Timer(0.3, runner.release).start()
    runner.run_forever()

    assert started == [1]
    assert stopped == [1]


def test_runner_retries_on_transient_election_exception() -> None:
    attempts: list[float] = []

    def factory(**kwargs: object) -> StubElection:
        attempts.append(time.monotonic())
        # First call raises, second succeeds.
        behaviour = ["raise"] if len(attempts) == 1 else ["lead-then-stop"]
        return StubElection(
            kwargs["on_started_leading"],  # type: ignore[arg-type]
            kwargs["on_stopped_leading"],  # type: ignore[arg-type]
            behaviour=behaviour,
        )

    started: list[int] = []
    runner = LeaderElectionRunner(
        _make_config(),
        on_started_leading=lambda: started.append(1),
        on_stopped_leading=lambda: None,
        election_factory=factory,
    )
    threading.Timer(1.8, runner.release).start()
    runner.run_forever()

    assert len(attempts) >= 2
    assert started == [1]


def test_runner_swallows_callback_exceptions() -> None:
    """A buggy callback must not abort the supervisor loop."""

    iterations: list[int] = []

    def factory(**kwargs: object) -> StubElection:
        iterations.append(1)
        return StubElection(
            kwargs["on_started_leading"],  # type: ignore[arg-type]
            kwargs["on_stopped_leading"],  # type: ignore[arg-type]
            behaviour=["lead-then-stop"],
        )

    def angry_callback() -> None:
        raise RuntimeError("boom")

    runner = LeaderElectionRunner(
        _make_config(),
        on_started_leading=angry_callback,
        on_stopped_leading=angry_callback,
        election_factory=factory,
    )
    threading.Timer(0.5, runner.release).start()
    runner.run_forever()

    assert iterations  # supervisor kept running despite callback raising


def test_runner_release_calls_current_election_release_without_stopped_notification() -> None:
    stub: StubElection | None = None

    def factory(**kwargs: object) -> StubElection:
        nonlocal stub
        stub = StubElection(
            kwargs["on_started_leading"],  # type: ignore[arg-type]
            kwargs["on_stopped_leading"],  # type: ignore[arg-type]
            behaviour=["wait"],
        )
        return stub

    runner = LeaderElectionRunner(
        _make_config(),
        on_started_leading=lambda: None,
        on_stopped_leading=lambda: None,
        election_factory=factory,
    )
    thread = threading.Thread(target=runner.run_forever)

    thread.start()
    deadline = time.monotonic() + 1
    while stub is None and time.monotonic() < deadline:
        time.sleep(0.01)
    runner.release(notify_stopped=False)
    thread.join(timeout=1)

    assert stub is not None
    assert stub.release_calls == 1
    assert stub.release_notify_values == [False]
    assert not thread.is_alive()


def test_runner_aborts_when_kube_config_loader_fails() -> None:
    """If we can't reach the API server, the supervisor exits cleanly."""

    def bad_loader() -> None:
        raise RuntimeError("no kubeconfig in test env")

    runner = LeaderElectionRunner(
        _make_config(),
        on_started_leading=lambda: None,
        on_stopped_leading=lambda: None,
        kube_config_loader=bad_loader,
        election_factory=lambda **_: pytest.fail("election factory should not be called"),
    )
    runner.run_forever()  # returns immediately
