from __future__ import annotations

import pytest

from vmss_metrics_exporter.config import load_settings


def test_load_settings_parses_subscriptions_and_deduplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AZURE_SUBSCRIPTION_IDS", "sub-a, sub-b;sub-a")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "300")
    monkeypatch.setenv("PORT", "9000")

    settings = load_settings()

    assert settings.subscription_ids == ("sub-a", "sub-b")
    assert settings.poll_interval_seconds == 300
    assert settings.port == 9000
    assert settings.enable_managed_lustre_metrics is True
    assert settings.lustre_poll_interval_seconds == 60
    assert settings.lustre_metrics_lookback_minutes == 15
    assert settings.lustre_metrics_interval == "PT1M"
    assert settings.lustre_metrics_max_workers == 4
    assert settings.lustre_metrics_request_jitter_seconds == 0.5
    assert settings.shutdown_drain_seconds == 0.0


def test_placeholder_subscription_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_SUBSCRIPTION_IDS", "00000000-0000-0000-0000-000000000000")

    with pytest.raises(ValueError, match="AZURE_SUBSCRIPTION_IDS"):
        load_settings()


def test_poll_interval_has_safe_minimum(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_SUBSCRIPTION_IDS", "sub-a")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "1")

    with pytest.raises(ValueError, match="POLL_INTERVAL_SECONDS"):
        load_settings()


def test_lustre_settings_can_be_overridden(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_SUBSCRIPTION_IDS", "sub-a")
    monkeypatch.setenv("ENABLE_MANAGED_LUSTRE_METRICS", "false")
    monkeypatch.setenv("LUSTRE_POLL_INTERVAL_SECONDS", "30")
    monkeypatch.setenv("LUSTRE_METRICS_LOOKBACK_MINUTES", "30")
    monkeypatch.setenv("LUSTRE_METRICS_INTERVAL", "PT5M")
    monkeypatch.setenv("LUSTRE_METRICS_MAX_WORKERS", "8")
    monkeypatch.setenv("LUSTRE_METRICS_REQUEST_JITTER_SECONDS", "0.25")

    settings = load_settings()

    assert settings.enable_managed_lustre_metrics is False
    assert settings.lustre_poll_interval_seconds == 30
    assert settings.lustre_metrics_lookback_minutes == 30
    assert settings.lustre_metrics_interval == "PT5M"
    assert settings.lustre_metrics_max_workers == 8
    assert settings.lustre_metrics_request_jitter_seconds == 0.25


def test_lustre_boolean_setting_rejects_invalid_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AZURE_SUBSCRIPTION_IDS", "sub-a")
    monkeypatch.setenv("ENABLE_MANAGED_LUSTRE_METRICS", "maybe")

    with pytest.raises(ValueError, match="ENABLE_MANAGED_LUSTRE_METRICS"):
        load_settings()


def test_shutdown_drain_seconds_can_be_overridden(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_SUBSCRIPTION_IDS", "sub-a")
    monkeypatch.setenv("SHUTDOWN_DRAIN_SECONDS", "12.5")

    settings = load_settings()

    assert settings.shutdown_drain_seconds == 12.5


def test_leader_election_defaults_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_SUBSCRIPTION_IDS", "sub-a")
    monkeypatch.delenv("LEADER_ELECTION_ENABLED", raising=False)
    monkeypatch.delenv("POD_NAME", raising=False)
    monkeypatch.delenv("POD_NAMESPACE", raising=False)

    settings = load_settings()

    assert settings.leader_election_enabled is False
    assert settings.leader_election_lock_name == "vmss-metrics-exporter"
    assert settings.leader_election_namespace == "default"
    assert settings.leader_election_identity  # falls back to hostname, must be non-empty
    assert settings.leader_election_lease_duration_seconds == 15
    assert settings.leader_election_renew_deadline_seconds == 10
    assert settings.leader_election_retry_period_seconds == 2


def test_leader_election_uses_downward_api_envs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_SUBSCRIPTION_IDS", "sub-a")
    monkeypatch.setenv("LEADER_ELECTION_ENABLED", "true")
    monkeypatch.setenv("POD_NAME", "vmss-metrics-exporter-abc-123")
    monkeypatch.setenv("POD_NAMESPACE", "observability")

    settings = load_settings()

    assert settings.leader_election_enabled is True
    assert settings.leader_election_namespace == "observability"
    assert settings.leader_election_identity == "vmss-metrics-exporter-abc-123"


def test_leader_election_rejects_renew_deadline_not_less_than_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AZURE_SUBSCRIPTION_IDS", "sub-a")
    monkeypatch.setenv("LEADER_ELECTION_LEASE_DURATION_SECONDS", "10")
    monkeypatch.setenv("LEADER_ELECTION_RENEW_DEADLINE_SECONDS", "10")

    with pytest.raises(ValueError, match="RENEW_DEADLINE"):
        load_settings()


def test_leader_election_rejects_retry_period_not_less_than_renew(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AZURE_SUBSCRIPTION_IDS", "sub-a")
    monkeypatch.setenv("LEADER_ELECTION_LEASE_DURATION_SECONDS", "15")
    monkeypatch.setenv("LEADER_ELECTION_RENEW_DEADLINE_SECONDS", "5")
    monkeypatch.setenv("LEADER_ELECTION_RETRY_PERIOD_SECONDS", "5")

    with pytest.raises(ValueError, match="RETRY_PERIOD"):
        load_settings()


def test_leader_election_rejects_renew_within_retry_jitter_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # renew (6) > retry (5) but <= retry * 1.2 (6.0): accepted by the old
    # settings check yet rejected by LeaderElectionConfig, which crashed the
    # exporter at startup. load_settings must now reject it up front so the
    # two validators agree.
    monkeypatch.setenv("AZURE_SUBSCRIPTION_IDS", "sub-a")
    monkeypatch.setenv("LEADER_ELECTION_LEASE_DURATION_SECONDS", "15")
    monkeypatch.setenv("LEADER_ELECTION_RENEW_DEADLINE_SECONDS", "6")
    monkeypatch.setenv("LEADER_ELECTION_RETRY_PERIOD_SECONDS", "5")

    with pytest.raises(ValueError, match="RETRY_PERIOD"):
        load_settings()
