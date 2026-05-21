"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass

from dotenv import load_dotenv

_PLACEHOLDER_SUBSCRIPTION = "00000000-0000-0000-0000-000000000000"


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime settings for the VMSS metrics exporter."""

    subscription_ids: tuple[str, ...]
    poll_interval_seconds: int = 300
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"
    arg_page_size: int = 1000
    arg_max_retries: int = 3
    arg_retry_base_delay_seconds: float = 1.0
    enable_managed_lustre_metrics: bool = True
    lustre_poll_interval_seconds: int = 60
    lustre_metrics_lookback_minutes: int = 15
    lustre_metrics_interval: str = "PT1M"
    lustre_metrics_max_workers: int = 4
    lustre_metrics_request_jitter_seconds: float = 0.5
    leader_election_enabled: bool = False
    leader_election_lock_name: str = "vmss-metrics-exporter"
    leader_election_namespace: str = "default"
    leader_election_identity: str = ""
    leader_election_lease_duration_seconds: int = 15
    leader_election_renew_deadline_seconds: int = 10
    leader_election_retry_period_seconds: int = 2


def load_settings(*, require_subscription_ids: bool = True) -> Settings:
    """Load exporter settings from `.env` and process environment.

    `DefaultAzureCredential` reads Azure auth-related environment variables itself, so this module
    only parses exporter-specific settings.
    """

    load_dotenv()

    subscription_ids = _parse_subscription_ids(os.getenv("AZURE_SUBSCRIPTION_IDS", ""))
    if require_subscription_ids and not subscription_ids:
        raise ValueError(
            "AZURE_SUBSCRIPTION_IDS must contain at least one real Azure subscription ID. "
            "Use a comma-separated list for multiple subscriptions."
        )

    return Settings(
        subscription_ids=subscription_ids,
        poll_interval_seconds=_get_int("POLL_INTERVAL_SECONDS", default=300, minimum=30),
        host=os.getenv("HOST", "0.0.0.0"),
        port=_get_int("PORT", default=8000, minimum=1, maximum=65535),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        arg_page_size=_get_int("ARG_PAGE_SIZE", default=1000, minimum=1, maximum=1000),
        arg_max_retries=_get_int("ARG_MAX_RETRIES", default=3, minimum=0, maximum=10),
        arg_retry_base_delay_seconds=_get_float(
            "ARG_RETRY_BASE_DELAY_SECONDS", default=1.0, minimum=0.0, maximum=60.0
        ),
        enable_managed_lustre_metrics=_get_bool(
            "ENABLE_MANAGED_LUSTRE_METRICS", default=True
        ),
        lustre_poll_interval_seconds=_get_int(
            "LUSTRE_POLL_INTERVAL_SECONDS", default=60, minimum=15
        ),
        lustre_metrics_lookback_minutes=_get_int(
            "LUSTRE_METRICS_LOOKBACK_MINUTES", default=15, minimum=1, maximum=1440
        ),
        lustre_metrics_interval=os.getenv("LUSTRE_METRICS_INTERVAL", "PT1M"),
        lustre_metrics_max_workers=_get_int(
            "LUSTRE_METRICS_MAX_WORKERS", default=4, minimum=1, maximum=32
        ),
        lustre_metrics_request_jitter_seconds=_get_float(
            "LUSTRE_METRICS_REQUEST_JITTER_SECONDS", default=0.5, minimum=0.0, maximum=60.0
        ),
        **_load_leader_election_settings(),
    )


def _parse_subscription_ids(raw: str) -> tuple[str, ...]:
    values = tuple(
        item.strip()
        for item in raw.replace(";", ",").split(",")
        if item.strip() and item.strip() != _PLACEHOLDER_SUBSCRIPTION
    )
    return tuple(dict.fromkeys(values))


def _get_int(
    name: str,
    *,
    default: int,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be <= {maximum}, got {value}")
    return value


def _get_float(
    name: str,
    *,
    default: float,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be <= {maximum}, got {value}")
    return value


def _get_bool(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean, got {raw!r}")


def _load_leader_election_settings() -> dict[str, object]:
    """Parse leader-election environment variables with cross-field validation.

    Returns kwargs suitable for ``Settings(**kwargs)``. When leader election is
    disabled (the default), the lease parameters fall back to safe defaults.
    """

    enabled = _get_bool("LEADER_ELECTION_ENABLED", default=False)
    lock_name = os.getenv("LEADER_ELECTION_LOCK_NAME", "vmss-metrics-exporter")
    namespace = (
        os.getenv("LEADER_ELECTION_NAMESPACE")
        or os.getenv("POD_NAMESPACE")
        or "default"
    )
    identity = (
        os.getenv("LEADER_ELECTION_IDENTITY")
        or os.getenv("POD_NAME")
        or socket.gethostname()
    )
    lease_duration = _get_int(
        "LEADER_ELECTION_LEASE_DURATION_SECONDS", default=15, minimum=5, maximum=120
    )
    renew_deadline = _get_int(
        "LEADER_ELECTION_RENEW_DEADLINE_SECONDS", default=10, minimum=2, maximum=119
    )
    retry_period = _get_int(
        "LEADER_ELECTION_RETRY_PERIOD_SECONDS", default=2, minimum=1, maximum=60
    )
    if renew_deadline >= lease_duration:
        raise ValueError(
            "LEADER_ELECTION_RENEW_DEADLINE_SECONDS "
            f"({renew_deadline}) must be strictly less than "
            f"LEADER_ELECTION_LEASE_DURATION_SECONDS ({lease_duration})"
        )
    if retry_period >= renew_deadline:
        raise ValueError(
            "LEADER_ELECTION_RETRY_PERIOD_SECONDS "
            f"({retry_period}) must be strictly less than "
            f"LEADER_ELECTION_RENEW_DEADLINE_SECONDS ({renew_deadline})"
        )
    return {
        "leader_election_enabled": enabled,
        "leader_election_lock_name": lock_name,
        "leader_election_namespace": namespace,
        "leader_election_identity": identity,
        "leader_election_lease_duration_seconds": lease_duration,
        "leader_election_renew_deadline_seconds": renew_deadline,
        "leader_election_retry_period_seconds": retry_period,
    }
