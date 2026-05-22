"""Tests for the resilient credential fallback chain."""

from __future__ import annotations

import os
from collections.abc import Iterable

import pytest
from azure.core.credentials import AccessToken
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import CredentialUnavailableError

from vmss_metrics_exporter import credentials as creds_module
from vmss_metrics_exporter.credentials import ResilientAzureCredential


class _FakeCred:
    """Test double that mimics a TokenCredential."""

    def __init__(self, behavior: Iterable[object]) -> None:
        # behavior is an iterable of either AccessToken instances or Exception instances.
        # Each get_token call consumes the next item.
        self._iter = iter(list(behavior))
        self.calls = 0
        self.close_calls = 0

    def get_token(self, *_scopes: str, **_kwargs: object) -> AccessToken:
        self.calls += 1
        outcome = next(self._iter)
        if isinstance(outcome, BaseException):
            raise outcome
        assert isinstance(outcome, AccessToken)
        return outcome

    def close(self) -> None:  # pragma: no cover - close is best-effort.
        self.close_calls += 1


class _CloseRaisesCred:
    def get_token(self, *_scopes: str, **_kwargs: object) -> AccessToken:
        raise AssertionError("not used")

    def close(self) -> None:
        raise RuntimeError("close failed")


def _install_chain(monkeypatch: pytest.MonkeyPatch, chain: list[tuple[str, object]]) -> None:
    monkeypatch.setattr(creds_module, "_build_credential_chain", lambda: chain)


def test_workload_identity_success_short_circuits(monkeypatch: pytest.MonkeyPatch) -> None:
    """If WI returns a token, MI must never be called."""

    token = AccessToken("wi-token", expires_on=9_999_999_999)
    wi = _FakeCred([token])
    mi = _FakeCred([Exception("must not be called")])
    _install_chain(monkeypatch, [("workload-identity", wi), ("managed-identity", mi)])

    cred = ResilientAzureCredential()
    result = cred.get_token("https://management.azure.com/.default")

    assert result.token == "wi-token"
    assert wi.calls == 1
    assert mi.calls == 0


def test_falls_back_to_managed_identity_on_hard_auth_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`ClientAuthenticationError` from WI must NOT abort the chain — this is the bug fix."""

    wi = _FakeCred(
        [
            ClientAuthenticationError(
                message="AADSTS700211: No matching federated identity record found."
            )
        ]
    )
    mi_token = AccessToken("mi-token", expires_on=9_999_999_999)
    mi = _FakeCred([mi_token])
    _install_chain(monkeypatch, [("workload-identity", wi), ("managed-identity", mi)])

    cred = ResilientAzureCredential()
    result = cred.get_token("https://management.azure.com/.default")

    assert result.token == "mi-token"
    assert wi.calls == 1
    assert mi.calls == 1


def test_service_principal_success_short_circuits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A configured SP is first-class and does not fall through to WI/MI on success."""

    sp_token = AccessToken("sp-token", expires_on=9_999_999_999)
    sp = _FakeCred([sp_token])
    wi = _FakeCred([Exception("must not be called")])
    _install_chain(monkeypatch, [("service-principal", sp), ("workload-identity", wi)])

    cred = ResilientAzureCredential()
    result = cred.get_token("https://management.azure.com/.default")

    assert result.token == "sp-token"
    assert sp.calls == 1
    assert wi.calls == 0


def test_falls_back_on_credential_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    wi = _FakeCred([CredentialUnavailableError(message="WI env vars not present")])
    mi_token = AccessToken("mi-token", expires_on=9_999_999_999)
    mi = _FakeCred([mi_token])
    _install_chain(monkeypatch, [("workload-identity", wi), ("managed-identity", mi)])

    cred = ResilientAzureCredential()
    result = cred.get_token("https://management.azure.com/.default")

    assert result.token == "mi-token"


def test_all_failures_raise_combined_error(monkeypatch: pytest.MonkeyPatch) -> None:
    wi = _FakeCred([ClientAuthenticationError(message="AADSTS700211")])
    mi = _FakeCred([ClientAuthenticationError(message="IMDS unreachable")])
    _install_chain(monkeypatch, [("workload-identity", wi), ("managed-identity", mi)])

    cred = ResilientAzureCredential()
    with pytest.raises(ClientAuthenticationError) as excinfo:
        cred.get_token("https://management.azure.com/.default")

    message = str(excinfo.value)
    assert "workload-identity" in message
    assert "managed-identity" in message
    assert "AADSTS700211" in message
    assert "IMDS unreachable" in message


def test_active_credential_cached_then_invalidated_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once a credential succeeds it's reused; if it later fails the chain is re-run."""

    token1 = AccessToken("wi-token-1", expires_on=9_999_999_999)
    token2 = AccessToken("mi-token-2", expires_on=9_999_999_999)
    wi = _FakeCred(
        [
            token1,
            ClientAuthenticationError(message="WI suddenly broken"),
            ClientAuthenticationError(message="WI still broken"),
        ]
    )
    mi = _FakeCred([token2])
    _install_chain(monkeypatch, [("workload-identity", wi), ("managed-identity", mi)])

    cred = ResilientAzureCredential()

    first = cred.get_token("scope/.default")
    assert first.token == "wi-token-1"
    # Cached: a second call goes straight to wi without walking the chain. That call
    # fails, so the chain is re-walked: wi is tried again (and fails again), then mi.
    second = cred.get_token("scope/.default")
    assert second.token == "mi-token-2"
    assert wi.calls == 3  # 1 cached success + 1 cached-call failure + 1 re-walk failure
    assert mi.calls == 1


def test_construction_requires_at_least_one_credential(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_chain(monkeypatch, [])
    with pytest.raises(RuntimeError):
        ResilientAzureCredential()


def test_close_is_best_effort_and_repeatable(monkeypatch: pytest.MonkeyPatch) -> None:
    good = _FakeCred([])
    bad = _CloseRaisesCred()
    _install_chain(monkeypatch, [("good", good), ("bad", bad)])
    cred = ResilientAzureCredential()

    cred.close()
    cred.close()

    assert good.close_calls == 2


def test_chain_skips_workload_identity_when_env_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """WorkloadIdentityCredential must be omitted when WI env vars are not all present."""

    monkeypatch.delenv("AZURE_FEDERATED_TOKEN_FILE", raising=False)
    monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)
    monkeypatch.delenv("AZURE_TENANT_ID", raising=False)
    monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("VMSS_METRICS_AUTH_MODE", raising=False)

    chain = creds_module._build_credential_chain()
    names = [name for name, _ in chain]
    assert not any(name.startswith("workload-identity") for name in names)
    # DefaultAzureCredential is always present and now hosts Managed Identity internally.
    assert names == ["default-azure-credential"]


def test_service_principal_env_creates_sp_only_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """In auto mode, complete SP env vars override WI and create an env-only DAC."""

    captured_kwargs: dict[str, object] = {}

    class _FakeDefaultAzureCredential:
        def __init__(self, **kwargs: object) -> None:
            captured_kwargs.update(kwargs)

    monkeypatch.setattr(creds_module, "DefaultAzureCredential", _FakeDefaultAzureCredential)
    monkeypatch.setenv("AZURE_CLIENT_ID", "11111111-2222-3333-4444-555555555555")
    monkeypatch.setenv("AZURE_TENANT_ID", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "fake-secret")
    monkeypatch.setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/token")
    monkeypatch.delenv("VMSS_METRICS_AUTH_MODE", raising=False)

    chain = creds_module._build_credential_chain()

    assert [name for name, _ in chain] == ["service-principal"]
    assert captured_kwargs == creds_module._SP_ONLY_DAC_EXCLUDE_KWARGS
    assert "managed_identity_client_id" not in captured_kwargs


def test_service_principal_auth_mode_requires_complete_sp_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VMSS_METRICS_AUTH_MODE", "service_principal")
    monkeypatch.setenv("AZURE_CLIENT_ID", "11111111-2222-3333-4444-555555555555")
    monkeypatch.setenv("AZURE_TENANT_ID", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)

    with pytest.raises(RuntimeError, match="AZURE_CLIENT_SECRET"):
        creds_module._build_credential_chain()


def test_workload_identity_auth_mode_ignores_complete_sp_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit WI mode prevents a mounted SP secret from winning inside DAC."""

    captured_kwargs: dict[str, object] = {}

    class _FakeWorkloadIdentityCredential:
        pass

    class _FakeDefaultAzureCredential:
        def __init__(self, **kwargs: object) -> None:
            captured_kwargs.update(kwargs)

    monkeypatch.setattr(creds_module, "WorkloadIdentityCredential", _FakeWorkloadIdentityCredential)
    monkeypatch.setattr(creds_module, "DefaultAzureCredential", _FakeDefaultAzureCredential)
    monkeypatch.setenv("VMSS_METRICS_AUTH_MODE", "workload_identity")
    monkeypatch.setenv("AZURE_CLIENT_ID", "11111111-2222-3333-4444-555555555555")
    monkeypatch.setenv("AZURE_TENANT_ID", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "fake-secret")
    monkeypatch.setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/token")

    chain = creds_module._build_credential_chain()

    assert [name for name, _ in chain] == ["workload-identity", "default-azure-credential"]
    assert captured_kwargs == {
        "exclude_workload_identity_credential": True,
        "exclude_environment_credential": True,
        "managed_identity_client_id": "11111111-2222-3333-4444-555555555555",
    }


def test_workload_identity_auth_mode_requires_wi_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VMSS_METRICS_AUTH_MODE", "workload_identity")
    monkeypatch.setenv("AZURE_CLIENT_ID", "11111111-2222-3333-4444-555555555555")
    monkeypatch.setenv("AZURE_TENANT_ID", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    monkeypatch.delenv("AZURE_FEDERATED_TOKEN_FILE", raising=False)
    monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)

    with pytest.raises(RuntimeError, match="AZURE_FEDERATED_TOKEN_FILE"):
        creds_module._build_credential_chain()


def test_invalid_auth_mode_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VMSS_METRICS_AUTH_MODE", "totally_wrong")

    with pytest.raises(ValueError, match="Unsupported VMSS_METRICS_AUTH_MODE"):
        creds_module._build_credential_chain()


def test_service_principal_mode_surfaces_construction_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit SP mode must not swallow the real error and report 'no credentials'."""

    class _BoomCredential:
        def __init__(self, **_kwargs: object) -> None:
            raise RuntimeError("boom-sp")

    monkeypatch.setattr(creds_module, "DefaultAzureCredential", _BoomCredential)
    monkeypatch.setenv("VMSS_METRICS_AUTH_MODE", "service_principal")
    monkeypatch.setenv("AZURE_CLIENT_ID", "11111111-2222-3333-4444-555555555555")
    monkeypatch.setenv("AZURE_TENANT_ID", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "fake-secret")

    with pytest.raises(RuntimeError, match="boom-sp"):
        creds_module._build_credential_chain()


def test_workload_identity_mode_surfaces_construction_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit WI mode must not silently fall back to MI/CLI when WI ctor raises."""

    class _BoomWorkloadIdentity:
        def __init__(self, **_kwargs: object) -> None:
            raise RuntimeError("boom-wi")

    monkeypatch.setattr(creds_module, "WorkloadIdentityCredential", _BoomWorkloadIdentity)
    monkeypatch.setenv("VMSS_METRICS_AUTH_MODE", "workload_identity")
    monkeypatch.setenv("AZURE_CLIENT_ID", "11111111-2222-3333-4444-555555555555")
    monkeypatch.setenv("AZURE_TENANT_ID", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    monkeypatch.setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/token")
    monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)

    with pytest.raises(RuntimeError, match="boom-wi"):
        creds_module._build_credential_chain()


def test_auto_mode_workload_identity_construction_failure_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """In auto mode, WI construction failure may still log and fall back to DAC."""

    class _BoomWorkloadIdentity:
        def __init__(self, **_kwargs: object) -> None:
            raise RuntimeError("boom-wi-auto")

    class _FakeDefaultAzureCredential:
        def __init__(self, **_kwargs: object) -> None:
            pass

    monkeypatch.setattr(creds_module, "WorkloadIdentityCredential", _BoomWorkloadIdentity)
    monkeypatch.setattr(creds_module, "DefaultAzureCredential", _FakeDefaultAzureCredential)
    monkeypatch.setenv("AZURE_CLIENT_ID", "11111111-2222-3333-4444-555555555555")
    monkeypatch.setenv("AZURE_TENANT_ID", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    monkeypatch.setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/token")
    monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("VMSS_METRICS_AUTH_MODE", raising=False)

    chain = creds_module._build_credential_chain()

    assert [name for name, _ in chain] == ["default-azure-credential"]


def test_partial_service_principal_env_does_not_block_workload_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AZURE_CLIENT_SECRET is the signal that SP should override WI."""

    monkeypatch.setenv("AZURE_CLIENT_ID", "11111111-2222-3333-4444-555555555555")
    monkeypatch.setenv("AZURE_TENANT_ID", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    monkeypatch.setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/token")
    monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("VMSS_METRICS_AUTH_MODE", raising=False)

    chain = creds_module._build_credential_chain()

    assert [name for name, _ in chain] == ["workload-identity", "default-azure-credential"]


def test_default_azure_credential_fallback_keeps_managed_identity_client_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without SP, the fallback DAC still pins user-assigned MI to AZURE_CLIENT_ID."""

    captured_kwargs: dict[str, object] = {}

    class _FakeDefaultAzureCredential:
        def __init__(self, **kwargs: object) -> None:
            captured_kwargs.update(kwargs)

    monkeypatch.setattr(creds_module, "DefaultAzureCredential", _FakeDefaultAzureCredential)
    monkeypatch.setenv("AZURE_CLIENT_ID", "11111111-2222-3333-4444-555555555555")
    monkeypatch.setenv("AZURE_TENANT_ID", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("AZURE_FEDERATED_TOKEN_FILE", raising=False)
    monkeypatch.delenv("VMSS_METRICS_AUTH_MODE", raising=False)

    chain = creds_module._build_credential_chain()

    assert [name for name, _ in chain] == ["default-azure-credential"]
    assert captured_kwargs == {
        "exclude_workload_identity_credential": True,
        "managed_identity_client_id": "11111111-2222-3333-4444-555555555555",
    }


def test_default_azure_credential_construction_hides_wi_token_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DAC can host SP while its inner MI is forced away from the WI shortcut."""

    class _FakeWorkloadIdentityCredential:
        pass

    class _FakeDefaultAzureCredential:
        def __init__(self, **_kwargs: object) -> None:
            assert "AZURE_FEDERATED_TOKEN_FILE" not in os.environ

    monkeypatch.setattr(creds_module, "WorkloadIdentityCredential", _FakeWorkloadIdentityCredential)
    monkeypatch.setattr(creds_module, "DefaultAzureCredential", _FakeDefaultAzureCredential)
    monkeypatch.setenv("AZURE_CLIENT_ID", "11111111-2222-3333-4444-555555555555")
    monkeypatch.setenv("AZURE_TENANT_ID", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("VMSS_METRICS_AUTH_MODE", raising=False)
    monkeypatch.setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/token")

    chain = creds_module._build_credential_chain()

    assert [name for name, _ in chain] == ["workload-identity", "default-azure-credential"]
    assert os.environ["AZURE_FEDERATED_TOKEN_FILE"] == "/var/run/secrets/azure/tokens/token"


def test_chain_includes_workload_identity_when_env_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AZURE_CLIENT_ID", "11111111-2222-3333-4444-555555555555")
    monkeypatch.setenv("AZURE_TENANT_ID", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("VMSS_METRICS_AUTH_MODE", raising=False)
    monkeypatch.setenv(
        "AZURE_FEDERATED_TOKEN_FILE",
        "/var/run/secrets/azure/tokens/azure-identity-token",
    )

    chain = creds_module._build_credential_chain()
    names = [name for name, _ in chain]
    # Workload identity is explicit so the resilient wrapper can catch its hard auth errors.
    assert names == ["workload-identity", "default-azure-credential"]


def test_force_imds_managed_identity_clears_then_restores_token_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The MI construction context manager must not leak env-var changes."""

    monkeypatch.setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/token")

    with creds_module._force_imds_managed_identity():
        assert "AZURE_FEDERATED_TOKEN_FILE" not in os.environ

    # After the context exits the env var is back, so WorkloadIdentityCredential keeps working.
    assert os.environ["AZURE_FEDERATED_TOKEN_FILE"] == "/var/run/secrets/token"
