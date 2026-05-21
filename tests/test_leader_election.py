from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import pytest

from vmss_metrics_exporter.leader_election import (
    LeaderElectionConfig,
    LeaderElectionRunner,
    _build_real_election,
    _normalize_bearer_token_scheme,
    _wrap_refresh_api_key_hook,
)


@dataclass
class StubElection:
    """Test double for ``kubernetes.leaderelection.LeaderElection``."""

    on_started_leading: Callable[[], None]
    on_stopped_leading: Callable[[], None]
    behaviour: list[str] = field(default_factory=list)

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
        raise AssertionError(f"unknown behaviour: {action!r}")


def _make_config() -> LeaderElectionConfig:
    return LeaderElectionConfig(
        lock_name="test-lock",
        lock_namespace="default",
        identity="test-pod-0",
        lease_duration_seconds=15,
        renew_deadline_seconds=10,
        retry_period_seconds=1,
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


def test_build_real_election_wires_configmaplock_fields() -> None:
    """Smoke-test the upstream ConfigMapLock constructor signature we rely on."""

    config = _make_config()
    election = _build_real_election(
        config=config,
        on_started_leading=lambda: None,
        on_stopped_leading=lambda: None,
    )

    lock = election.election_config.lock
    assert lock.name == config.lock_name
    assert lock.namespace == config.lock_namespace
    assert lock.identity == config.identity


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

    def _drive() -> None:
        # Schedule a release shortly so run_forever exits.
        threading.Timer(0.3, runner.release).start()

    _drive()
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
