"""Kubernetes Lease-based leader election with cooperative shutdown.

The upstream ``kubernetes.leaderelection`` helper only provides a
``ConfigMapLock`` in the Python client and does not actively release the lock
when the process receives SIGTERM. During rolling updates, that forces the next
pod to wait for the full lease duration before it can start collecting metrics.

This module keeps the public ``LeaderElectionRunner`` API used by ``main.py``
but implements the election directly with ``coordination.k8s.io/v1 Lease``:

* followers acquire a missing, empty, expired, or self-held Lease;
* leaders renew with Kubernetes resourceVersion optimistic concurrency;
* shutdown calls ``release()`` to clear ``holderIdentity`` best-effort so a
  standby pod can acquire on its next retry tick;
* all sleeps are interruptible via ``threading.Event.wait()``;
* callbacks are wrapped by the runner so user code cannot crash the election
  supervisor.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Protocol

LOGGER = logging.getLogger(__name__)

_DEFAULT_SERVICE_ACCOUNT_TOKEN_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/token"

_JITTER_FACTOR = 1.2

_RENEWED: Literal["renewed"] = "renewed"
_TRANSIENT: Literal["transient"] = "transient"
_LOST: Literal["lost"] = "lost"
_RenewResult = Literal["renewed", "transient", "lost"]


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
        if self.renew_deadline_seconds <= self.retry_period_seconds * _JITTER_FACTOR:
            raise ValueError(
                "renew_deadline_seconds must be greater than "
                "retry_period_seconds * 1.2"
            )
        if not self.lock_name or not self.lock_namespace or not self.identity:
            raise ValueError("lock_name, lock_namespace, and identity are required")


class _RunnableElection(Protocol):
    """Anything with a blocking ``run()`` method satisfies this protocol."""

    def run(self) -> None: ...


ElectionFactory = Callable[..., _RunnableElection]
KubeConfigLoader = Callable[[], None]


class LeaderElectionRunner:
    """Supervise one leader-election candidate.

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
        self._state_lock = threading.Lock()
        self._current_election: _RunnableElection | None = None

    def run_forever(self) -> None:
        """Block until :meth:`release` is called.

        Each iteration creates a fresh election instance and runs it until
        leadership is lost or shutdown is requested. Transient exceptions are
        logged and exponentially backed off, capped at 60 seconds.
        """

        if self._kube_config_loader is not None:
            try:
                self._kube_config_loader()
            except Exception:  # noqa: BLE001 - surface the failure but exit cleanly
                LOGGER.exception("Failed to load Kubernetes config; leader election exiting")
                return

        backoff = float(self._config.retry_period_seconds)
        while not self._stop_event.is_set():
            election: _RunnableElection | None = None
            try:
                election = self._election_factory(
                    config=self._config,
                    on_started_leading=self._safe(self._on_started_leading),
                    on_stopped_leading=self._safe(self._on_stopped_leading),
                    stop_event=self._stop_event,
                )
                with self._state_lock:
                    self._current_election = election
                election.run()
                # A clean return means shutdown or leadership loss; reset backoff.
                backoff = float(self._config.retry_period_seconds)
            except Exception:  # noqa: BLE001 - Kubernetes client may raise broad errors
                LOGGER.exception(
                    "Leader election iteration failed; retrying in %.1fs", backoff
                )
            finally:
                with self._state_lock:
                    if self._current_election is election:
                        self._current_election = None
            if self._stop_event.wait(backoff):
                return
            backoff = min(backoff * 2.0, 60.0)

    def release(self, *, notify_stopped: bool = True) -> None:
        """Stop the supervisor and actively release the current Lease if held.

        ``notify_stopped`` controls whether the active election invokes
        ``on_stopped_leading`` while releasing. Production shutdown uses
        ``False`` so the terminating pod does not clear metrics while its HTTP
        endpoint can still be scraped. Organic leadership loss still notifies
        normally and clears follower gauges.
        """

        self._stop_event.set()
        with self._state_lock:
            election = self._current_election
        release = getattr(election, "release", None)
        if callable(release):
            try:
                release(notify_stopped=notify_stopped)
            except Exception:  # noqa: BLE001 - shutdown must remain best-effort
                LOGGER.exception("Failed to release leader election Lease; suppressed")

    def _safe(self, callback: Callable[[], None]) -> Callable[[], None]:
        def wrapped() -> None:
            try:
                callback()
            except Exception:  # noqa: BLE001 - never propagate into election machinery
                LOGGER.exception("Leader election callback raised; suppressed")

        return wrapped


class _LeaseElection:
    """Blocking Kubernetes Lease election loop."""

    def __init__(
        self,
        *,
        config: LeaderElectionConfig,
        on_started_leading: Callable[[], None],
        on_stopped_leading: Callable[[], None],
        coordination_api: object,
        coordination_api_factory: Callable[[], object] | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        self._config = config
        self._on_started_leading = on_started_leading
        self._on_stopped_leading = on_stopped_leading
        self._coordination_api = coordination_api
        self._coordination_api_factory = coordination_api_factory
        self._stop_event = stop_event or threading.Event()
        self._state_lock = threading.Lock()
        self._api_lock = threading.Lock()
        self._observed_lock = threading.Lock()
        self._observed_key: tuple[object, ...] | None = None
        self._observed_time_monotonic = 0.0
        self._request_timeout = _api_request_timeout(config.renew_deadline_seconds)
        self._is_leader = False

    def run(self) -> None:
        """Acquire and renew leadership until stopped or leadership is lost."""

        try:
            while not self._stop_event.is_set():
                if self._try_acquire_once():
                    self._become_leader()
                    self._renew_loop()
                if self._stop_event.wait(self._config.retry_period_seconds):
                    break
        finally:
            if self._stop_event.is_set():
                self.release()
            else:
                self._stop_leading()

    def release(self, *, notify_stopped: bool = True) -> None:
        """Signal shutdown and clear ``holderIdentity`` if this candidate owns the Lease.

        This mirrors client-go's ``ReleaseOnCancel`` behavior: release is a
        best-effort Lease update that empties ``holderIdentity`` and writes a
        one-second lease duration so even older clients can recover quickly.
        ``notify_stopped=False`` is used only for process shutdown to avoid
        publishing blank metrics while the terminating pod is still scrapeable.
        """

        self._stop_event.set()
        if self._leading:
            self._release_lease_best_effort()
        self._stop_leading(notify=notify_stopped)

    @property
    def _leading(self) -> bool:
        with self._state_lock:
            return self._is_leader

    def _become_leader(self) -> None:
        with self._state_lock:
            if self._is_leader:
                return
            self._is_leader = True
        LOGGER.info(
            "Acquired Kubernetes Lease %s/%s as %s",
            self._config.lock_namespace,
            self._config.lock_name,
            self._config.identity,
        )
        self._on_started_leading()

    def _stop_leading(self, *, notify: bool = True) -> None:
        with self._state_lock:
            if not self._is_leader:
                return
            self._is_leader = False
        LOGGER.info(
            "Released Kubernetes Lease %s/%s as %s",
            self._config.lock_namespace,
            self._config.lock_name,
            self._config.identity,
        )
        if notify:
            self._on_stopped_leading()

    def _renew_loop(self) -> None:
        renew_deadline = time.monotonic() + self._config.renew_deadline_seconds
        while not self._stop_event.is_set():
            result = self._renew_once()
            if result == _RENEWED:
                renew_deadline = time.monotonic() + self._config.renew_deadline_seconds
            elif result == _LOST:
                LOGGER.warning(
                    "Lost Kubernetes Lease %s/%s to another holder",
                    self._config.lock_namespace,
                    self._config.lock_name,
                )
                self._stop_leading()
                return
            elif time.monotonic() >= renew_deadline:
                LOGGER.warning(
                    "Failed to renew Kubernetes Lease %s/%s before %.1fs deadline",
                    self._config.lock_namespace,
                    self._config.lock_name,
                    float(self._config.renew_deadline_seconds),
                )
                self._stop_leading()
                return

            if self._stop_event.wait(self._config.retry_period_seconds):
                return

    def _try_acquire_once(self) -> bool:
        now = _utcnow()
        now_monotonic = time.monotonic()
        with self._api_lock:
            try:
                lease = self._read_lease()
            except Exception as exc:  # noqa: BLE001 - status varies by client version
                if _api_status(exc) == 401:
                    raise
                if _api_status(exc) != 404:
                    LOGGER.warning("Failed to read Kubernetes Lease for acquisition: %s", exc)
                    return False
                return self._create_lease(now)

            self._observe_lease(lease, now_monotonic)
            holder_identity = self._holder_identity(lease)
            is_self = holder_identity == self._config.identity
            if (
                holder_identity not in (None, "")
                and not is_self
                and self._observed_lease_valid(lease, now_monotonic)
            ):
                return False

            return self._replace_lease(
                lease,
                holder_identity=self._config.identity,
                acquire_time=self._acquire_time(lease) if is_self else now,
                renew_time=now,
                lease_transitions=self._lease_transitions(lease) + int(not is_self),
            )

    def _renew_once(self) -> _RenewResult:
        now = _utcnow()
        now_monotonic = time.monotonic()
        with self._api_lock:
            try:
                lease = self._read_lease()
            except Exception as exc:  # noqa: BLE001
                status = _api_status(exc)
                if status == 401:
                    raise
                if status == 404:
                    return _TRANSIENT
                LOGGER.warning("Failed to read Kubernetes Lease for renewal: %s", exc)
                return _TRANSIENT

            self._observe_lease(lease, now_monotonic)
            holder_identity = self._holder_identity(lease)
            if holder_identity not in (None, "", self._config.identity):
                return _LOST

            if self._replace_lease(
                lease,
                holder_identity=self._config.identity,
                acquire_time=self._acquire_time(lease) or now,
                renew_time=now,
                lease_transitions=self._lease_transitions(lease),
            ):
                return _RENEWED
            return _TRANSIENT

    def _create_lease(self, now: datetime) -> bool:
        body = self._lease_body(
            holder_identity=self._config.identity,
            acquire_time=now,
            renew_time=now,
            lease_transitions=0,
        )
        try:
            created = self._coordination_api.create_namespaced_lease(
                namespace=self._config.lock_namespace,
                body=body,
                _request_timeout=self._request_timeout,
            )
            self._observe_lease(created, time.monotonic())
            return True
        except Exception as exc:  # noqa: BLE001
            if _api_status(exc) == 401 and self._refresh_coordination_api("create Lease"):
                try:
                    created = self._coordination_api.create_namespaced_lease(
                        namespace=self._config.lock_namespace,
                        body=body,
                        _request_timeout=self._request_timeout,
                    )
                    self._observe_lease(created, time.monotonic())
                    return True
                except Exception as retry_exc:  # noqa: BLE001
                    exc = retry_exc
            if _api_status(exc) != 409:
                LOGGER.warning("Failed to create Kubernetes Lease: %s", exc)
            return False

    def _replace_lease(
        self,
        lease: object,
        *,
        holder_identity: str,
        acquire_time: datetime | None,
        renew_time: datetime,
        lease_transitions: int,
        lease_duration_seconds: int | None = None,
    ) -> bool:
        body = self._lease_body(
            holder_identity=holder_identity,
            acquire_time=acquire_time,
            renew_time=renew_time,
            lease_transitions=lease_transitions,
            lease_duration_seconds=lease_duration_seconds,
            resource_version=self._resource_version(lease),
        )
        try:
            updated = self._coordination_api.replace_namespaced_lease(
                name=self._config.lock_name,
                namespace=self._config.lock_namespace,
                body=body,
                _request_timeout=self._request_timeout,
            )
            self._observe_lease(updated, time.monotonic())
            return True
        except Exception as exc:  # noqa: BLE001
            if _api_status(exc) == 401 and self._refresh_coordination_api("replace Lease"):
                try:
                    updated = self._coordination_api.replace_namespaced_lease(
                        name=self._config.lock_name,
                        namespace=self._config.lock_namespace,
                        body=body,
                        _request_timeout=self._request_timeout,
                    )
                    self._observe_lease(updated, time.monotonic())
                    return True
                except Exception as retry_exc:  # noqa: BLE001
                    exc = retry_exc
            if _api_status(exc) != 409:
                LOGGER.warning("Failed to replace Kubernetes Lease: %s", exc)
            return False

    def _release_lease_best_effort(self) -> None:
        deadline = time.monotonic() + self._config.renew_deadline_seconds
        while True:
            with self._api_lock:
                try:
                    lease = self._read_lease()
                except Exception as exc:  # noqa: BLE001
                    if _api_status(exc) != 404:
                        LOGGER.warning("Failed to read Kubernetes Lease for release: %s", exc)
                    return
                if self._holder_identity(lease) != self._config.identity:
                    return

                now = _utcnow()
                if self._replace_lease(
                    lease,
                    holder_identity="",
                    acquire_time=now,
                    renew_time=now,
                    lease_transitions=self._lease_transitions(lease),
                    lease_duration_seconds=1,
                ):
                    return
            if time.monotonic() >= deadline:
                LOGGER.warning(
                    "Timed out releasing Kubernetes Lease %s/%s",
                    self._config.lock_namespace,
                    self._config.lock_name,
                )
                return
            time.sleep(min(0.1, self._config.retry_period_seconds / 10))

    def _read_lease(self) -> object:
        try:
            return self._coordination_api.read_namespaced_lease(
                name=self._config.lock_name,
                namespace=self._config.lock_namespace,
                _request_timeout=self._request_timeout,
            )
        except Exception as exc:  # noqa: BLE001
            if _api_status(exc) == 401 and self._refresh_coordination_api("read Lease"):
                return self._coordination_api.read_namespaced_lease(
                    name=self._config.lock_name,
                    namespace=self._config.lock_namespace,
                    _request_timeout=self._request_timeout,
                )
            raise

    def _refresh_coordination_api(self, operation: str) -> bool:
        if self._coordination_api_factory is None:
            return False
        LOGGER.warning(
            "Kubernetes API returned 401 Unauthorized during %s; reloading "
            "in-cluster credentials and retrying once",
            operation,
        )
        try:
            self._coordination_api = self._coordination_api_factory()
        except Exception:  # noqa: BLE001 - caller will handle the original API failure
            LOGGER.exception("Failed to reload Kubernetes credentials after 401")
            return False
        return True

    def _lease_body(
        self,
        *,
        holder_identity: str,
        acquire_time: datetime | None,
        renew_time: datetime,
        lease_transitions: int,
        lease_duration_seconds: int | None = None,
        resource_version: str | None = None,
    ) -> object:
        from kubernetes import client

        return client.V1Lease(
            api_version="coordination.k8s.io/v1",
            kind="Lease",
            metadata=client.V1ObjectMeta(
                name=self._config.lock_name,
                namespace=self._config.lock_namespace,
                resource_version=resource_version,
                labels={"app.kubernetes.io/name": "vmss-metrics-exporter"},
            ),
            spec=client.V1LeaseSpec(
                acquire_time=acquire_time,
                holder_identity=holder_identity,
                lease_duration_seconds=lease_duration_seconds
                or self._config.lease_duration_seconds,
                lease_transitions=lease_transitions,
                renew_time=renew_time,
            ),
        )

    def _observe_lease(self, lease: object | None, now_monotonic: float) -> None:
        if lease is None:
            return
        lease_key = self._lease_key(lease)
        with self._observed_lock:
            if lease_key != self._observed_key:
                self._observed_key = lease_key
                self._observed_time_monotonic = now_monotonic

    def _observed_lease_valid(self, lease: object, now_monotonic: float) -> bool:
        with self._observed_lock:
            observed_time = self._observed_time_monotonic
        duration = self._lease_duration_seconds(lease)
        return observed_time + duration > now_monotonic

    def _lease_key(self, lease: object) -> tuple[object, ...]:
        return (
            self._resource_version(lease),
            self._holder_identity(lease),
            self._lease_duration_seconds(lease),
            self._lease_transitions(lease),
            self._acquire_time(lease),
            self._renew_time(lease),
        )

    def _holder_identity(self, lease: object) -> str | None:
        return _spec_attr(lease, "holder_identity")

    def _acquire_time(self, lease: object) -> datetime | None:
        return _as_utc_datetime(_spec_attr(lease, "acquire_time"))

    def _renew_time(self, lease: object) -> datetime | None:
        return _as_utc_datetime(_spec_attr(lease, "renew_time"))

    def _lease_duration_seconds(self, lease: object) -> int:
        value = _spec_attr(lease, "lease_duration_seconds")
        if isinstance(value, int) and value > 0:
            return value
        return self._config.lease_duration_seconds

    def _lease_transitions(self, lease: object) -> int:
        value = _spec_attr(lease, "lease_transitions")
        if isinstance(value, int) and value >= 0:
            return value
        return 0

    def _resource_version(self, lease: object) -> str | None:
        metadata = getattr(lease, "metadata", None)
        resource_version = getattr(metadata, "resource_version", None)
        return resource_version if isinstance(resource_version, str) else None


def _build_real_election(
    *,
    config: LeaderElectionConfig,
    on_started_leading: Callable[[], None],
    on_stopped_leading: Callable[[], None],
    stop_event: threading.Event | None = None,
) -> _RunnableElection:
    """Construct the production Kubernetes Lease election.

    Imports are deferred so unit tests can import this module without requiring
    Kubernetes config at collection time.
    """

    from kubernetes import client

    return _LeaseElection(
        config=config,
        on_started_leading=on_started_leading,
        on_stopped_leading=on_stopped_leading,
        coordination_api=client.CoordinationV1Api(),
        coordination_api_factory=_reload_incluster_coordination_api,
        stop_event=stop_event,
    )


def _reload_incluster_coordination_api() -> object:
    """Reload the projected ServiceAccount token and build a fresh Lease client."""

    from kubernetes import client

    load_incluster_kube_config()
    return client.CoordinationV1Api()


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
    """Keep projected in-cluster tokens compatible with Kubernetes client 36.

    Kubernetes ServiceAccount tokens are projected files and can rotate while a
    pod is running. The Python client's in-cluster refresh hook is version-
    sensitive, so this wrapper also rereads the token file whenever auth is
    refreshed. That keeps long-lived leader-election clients aligned with
    Kubernetes' bounded-token best practice instead of relying on one cached
    process-start token.
    """

    refresh_api_key_hook = getattr(configuration, "refresh_api_key_hook", None)
    if not callable(refresh_api_key_hook):
        return

    def wrapped_refresh_api_key_hook(config: object) -> None:
        refresh_api_key_hook(config)
        _refresh_service_account_token_from_file(config)
        _normalize_bearer_token_scheme(config)
        # The Kubernetes in-cluster refresh hook calls its private _set_config(),
        # which reassigns ``config.refresh_api_key_hook`` back to the original
        # raw hook. Reinstall this wrapper after every refresh so future service
        # account token rotations keep the generated ``BearerToken`` auth key in
        # sync with the refreshed ``authorization`` token.
        config.refresh_api_key_hook = wrapped_refresh_api_key_hook

    configuration.refresh_api_key_hook = wrapped_refresh_api_key_hook


def _refresh_service_account_token_from_file(
    configuration: object,
    token_file: str | None = None,
) -> bool:
    """Refresh Kubernetes auth keys from the projected ServiceAccount token file."""

    api_key = getattr(configuration, "api_key", None)
    if not isinstance(api_key, dict):
        return False
    path = token_file or os.getenv(
        "KUBERNETES_SERVICEACCOUNT_TOKEN_FILE",
        _DEFAULT_SERVICE_ACCOUNT_TOKEN_FILE,
    )
    try:
        with open(path, encoding="utf-8") as token_handle:
            token = token_handle.read().strip()
    except OSError:
        return False
    if not token:
        return False
    api_key["authorization"] = f"Bearer {token}"
    api_key["BearerToken"] = f"Bearer {token}"
    return True


def _spec_attr(lease: object, attr_name: str) -> Any:
    spec = getattr(lease, "spec", None)
    return getattr(spec, attr_name, None)


def _as_utc_datetime(value: object) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _api_status(exc: BaseException) -> int | None:
    status = getattr(exc, "status", None)
    return status if isinstance(status, int) else None


def _api_request_timeout(renew_deadline_seconds: int) -> tuple[float, float]:
    """Return Kubernetes client request timeout below the renew deadline.

    This mirrors client-go's ``NewFromKubeconfig`` heuristic: a single hung API
    call should not consume the whole renew deadline. Python's generated client
    accepts ``_request_timeout`` as either a float or ``(connect, read)`` tuple;
    using a tuple bounds both phases explicitly.
    """

    timeout = max(1.0, renew_deadline_seconds / 2.0)
    return (timeout, timeout)
