"""Leader election wrapper around ``kubernetes.leaderelection``.

The official Python client ships ``ConfigMapLock`` (the only lock available as
of ``kubernetes`` 35.x). The upstream package is functional but coarse:

* :meth:`LeaderElection.run` blocks the calling thread inside an internal
  acquire→renew loop.
* When the renew loop fails, :meth:`run` returns and ``onstopped_leading``
  fires once; the candidate is no longer the leader. We supervise the call so
  the runner re-enters acquisition after a back-off period.
* ``onstarted_leading`` is invoked from a new daemon thread spawned by the
  upstream library; this module wraps both callbacks in a guard that swallows
  exceptions so a buggy callback cannot crash the supervisor loop.
* There is no cooperative interrupt for the upstream sleep loops, so SIGTERM
  handling relies on the supervisor thread being a ``daemon`` thread plus
  ``release()`` short-circuiting subsequent iterations.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class LeaderElectionConfig:
    """Runtime parameters for a leader election candidate."""

    lock_name: str
    lock_namespace: str
    identity: str
    lease_duration_seconds: int = 15
    renew_deadline_seconds: int = 10
    retry_period_seconds: int = 2

    def __post_init__(self) -> None:
        if self.lease_duration_seconds < 5:
            raise ValueError("lease_duration_seconds must be >= 5")
        if self.renew_deadline_seconds >= self.lease_duration_seconds:
            raise ValueError(
                "renew_deadline_seconds must be strictly less than "
                "lease_duration_seconds"
            )
        if self.retry_period_seconds < 1:
            raise ValueError("retry_period_seconds must be >= 1")
        if self.retry_period_seconds >= self.renew_deadline_seconds:
            raise ValueError(
                "retry_period_seconds must be strictly less than "
                "renew_deadline_seconds"
            )
        if not self.lock_name or not self.lock_namespace or not self.identity:
            raise ValueError("lock_name, lock_namespace, and identity are required")


class _RunnableElection(Protocol):
    """Anything with a blocking ``run()`` method satisfies this protocol."""

    def run(self) -> None: ...


ElectionFactory = Callable[..., _RunnableElection]
KubeConfigLoader = Callable[[], None]


class LeaderElectionRunner:
    """Supervise a ``kubernetes.leaderelection.LeaderElection`` instance.

    Intended to be executed in a daemon thread::

        runner = LeaderElectionRunner(cfg, on_started_leading=..., on_stopped_leading=...)
        threading.Thread(target=runner.run_forever, daemon=True).start()
        ...
        runner.release()  # at shutdown
    """

    def __init__(
        self,
        config: LeaderElectionConfig,
        *,
        on_started_leading: Callable[[], None],
        on_stopped_leading: Callable[[], None],
        kube_config_loader: KubeConfigLoader | None = None,
        election_factory: ElectionFactory | None = None,
    ) -> None:
        self._config = config
        self._on_started_leading = on_started_leading
        self._on_stopped_leading = on_stopped_leading
        self._kube_config_loader = kube_config_loader
        self._election_factory = election_factory or _build_real_election
        self._stop_event = threading.Event()

    def run_forever(self) -> None:
        """Block until :meth:`release` is called.

        Each iteration creates a fresh ``LeaderElection`` instance, runs it
        until leadership is lost, and then sleeps before retrying. Transient
        exceptions (network blips, API server hiccups) are logged and
        exponentially backed off, capped at 60 seconds.
        """

        if self._kube_config_loader is not None:
            try:
                self._kube_config_loader()
            except Exception:  # noqa: BLE001 - surface the failure but keep the runner alive
                LOGGER.exception("Failed to load Kubernetes config; leader election exiting")
                return

        backoff = float(self._config.retry_period_seconds)
        while not self._stop_event.is_set():
            try:
                election = self._election_factory(
                    config=self._config,
                    on_started_leading=self._safe(self._on_started_leading),
                    on_stopped_leading=self._safe(self._on_stopped_leading),
                )
                election.run()
                # A clean return means we lost leadership; reset backoff for the next attempt.
                backoff = float(self._config.retry_period_seconds)
            except Exception:  # noqa: BLE001 - upstream library raises bare RuntimeError
                LOGGER.exception(
                    "Leader election iteration failed; retrying in %.1fs", backoff
                )
            if self._stop_event.wait(backoff):
                return
            backoff = min(backoff * 2.0, 60.0)

    def release(self) -> None:
        """Signal the supervisor to exit after the current iteration."""

        self._stop_event.set()

    def _safe(self, callback: Callable[[], None]) -> Callable[[], None]:
        def wrapped() -> None:
            try:
                callback()
            except Exception:  # noqa: BLE001 - never propagate into the upstream library
                LOGGER.exception("Leader election callback raised; suppressed")

        return wrapped


def _build_real_election(
    *,
    config: LeaderElectionConfig,
    on_started_leading: Callable[[], None],
    on_stopped_leading: Callable[[], None],
) -> _RunnableElection:
    """Construct the production :class:`kubernetes.leaderelection.LeaderElection`.

    Imports are deferred so the unit tests do not need the ``kubernetes``
    package available at collection time and can inject a stub factory.
    """

    from kubernetes.leaderelection import electionconfig, leaderelection
    from kubernetes.leaderelection.resourcelock.configmaplock import ConfigMapLock

    lock = ConfigMapLock(
        name=config.lock_name,
        namespace=config.lock_namespace,
        identity=config.identity,
    )
    election_cfg = electionconfig.Config(
        lock=lock,
        lease_duration=config.lease_duration_seconds,
        renew_deadline=config.renew_deadline_seconds,
        retry_period=config.retry_period_seconds,
        onstarted_leading=on_started_leading,
        onstopped_leading=on_stopped_leading,
    )
    return leaderelection.LeaderElection(election_cfg)


def load_incluster_kube_config() -> None:
    """Load Kubernetes credentials from the in-cluster service account."""

    from kubernetes import client
    from kubernetes import config as k8s_config

    k8s_config.load_incluster_config()
    configuration = client.Configuration.get_default_copy()
    _normalize_bearer_token_scheme(configuration)
    _wrap_refresh_api_key_hook(configuration)
    client.Configuration.set_default(configuration)


def _normalize_bearer_token_scheme(configuration: object) -> None:
    """Normalize in-cluster auth from ``bearer`` to ``Bearer``.

    ``kubernetes`` 36.0.0 changed generated API calls to request auth setting
    ``BearerToken`` while the in-cluster loader still populates the older
    ``authorization`` key. It also refreshes tokens as lowercase ``bearer``.
    Populate both keys with a canonical ``Bearer ...`` value so both old and new
    clients send the service-account token.
    """

    api_key = getattr(configuration, "api_key", None)
    if not isinstance(api_key, dict):
        return
    value = api_key.get("authorization") or api_key.get("BearerToken")
    if not isinstance(value, str):
        return
    if value.startswith("bearer "):
        value = "Bearer " + value[len("bearer ") :]
    elif not value.startswith("Bearer "):
        value = "Bearer " + value
    api_key["authorization"] = value
    api_key["BearerToken"] = value


def _wrap_refresh_api_key_hook(configuration: object) -> None:
    """Keep refreshed in-cluster tokens compatible with Kubernetes client 36."""

    refresh_api_key_hook = getattr(configuration, "refresh_api_key_hook", None)
    if not callable(refresh_api_key_hook):
        return

    def wrapped_refresh_api_key_hook(config: object) -> None:
        refresh_api_key_hook(config)
        _normalize_bearer_token_scheme(config)

    setattr(configuration, "refresh_api_key_hook", wrapped_refresh_api_key_hook)
