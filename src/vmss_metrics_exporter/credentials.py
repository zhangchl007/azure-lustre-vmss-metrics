"""Resilient Azure credential chain with explicit Kubernetes auth-mode selection.

Background
----------
`azure.identity.DefaultAzureCredential` chains several credentials but only proceeds to
the next one when the current one raises `CredentialUnavailableError`. A real
authentication failure (for example AADSTS70021 / AADSTS700211 federated-credential
mismatch, AADSTS53003 conditional-access block, or a malformed federated token file)
raises `ClientAuthenticationError`, which **stops the chain**.

In practice, on AKS the Workload Identity admission webhook injects WI environment
variables. `DefaultAzureCredential` then commits to `WorkloadIdentityCredential` and if
WI auth fails it never falls back to `ManagedIdentityCredential` — even though the
kubelet/user-assigned identity attached to the underlying VMSS could have served the
token via IMDS.

To make matters worse, modern `azure-identity`'s `ManagedIdentityCredential` itself
silently reuses the Workload Identity token-exchange flow whenever the WI env vars are
present. That means even if you swap in MI explicitly, it fails for the same reason WI
failed.

This module provides `ResilientAzureCredential`, which:

1. Honors `VMSS_METRICS_AUTH_MODE` so Kubernetes deployment YAML can explicitly choose
    `workload_identity`, `service_principal`, or `auto`.
2. Uses Service Principal auth first in `auto` mode when the full SP environment is
    present (`AZURE_CLIENT_ID` + `AZURE_TENANT_ID` + `AZURE_CLIENT_SECRET`). This makes
    a Kubernetes Secret-provided SP deterministic even when the AKS Workload Identity
    webhook also injects WI env vars.
3. Otherwise tries `WorkloadIdentityCredential` first (explicit) so a hard auth error from WI
   can be caught and the chain can continue.
4. Falls back to `DefaultAzureCredential` (with WI excluded and `managed_identity_client_id`
    pinned to `$AZURE_CLIENT_ID`). DAC contains Managed Identity, Azure CLI,
    PowerShell, etc., so a separate explicit `ManagedIdentityCredential` step is
    unnecessary. We construct DAC with the WI trigger env var temporarily cleared so
    its inner MI credential targets **IMDS**, not the WI shortcut.
5. Caches the first credential that succeeds so subsequent token requests reuse it
   without re-running the chain.
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from typing import Any

from azure.core.credentials import AccessToken
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import (
    CredentialUnavailableError,
    DefaultAzureCredential,
    WorkloadIdentityCredential,
)

LOGGER = logging.getLogger(__name__)

_AUTH_MODE_ENV_VAR = "VMSS_METRICS_AUTH_MODE"
_AUTH_MODE_AUTO = "auto"
_AUTH_MODE_SERVICE_PRINCIPAL = "service_principal"
_AUTH_MODE_WORKLOAD_IDENTITY = "workload_identity"
_SUPPORTED_AUTH_MODES = {
    _AUTH_MODE_AUTO,
    _AUTH_MODE_SERVICE_PRINCIPAL,
    _AUTH_MODE_WORKLOAD_IDENTITY,
}

_WORKLOAD_IDENTITY_ENV_VARS = (
    "AZURE_CLIENT_ID",
    "AZURE_TENANT_ID",
    "AZURE_FEDERATED_TOKEN_FILE",
)
_SERVICE_PRINCIPAL_ENV_VARS = (
    "AZURE_CLIENT_ID",
    "AZURE_TENANT_ID",
    "AZURE_CLIENT_SECRET",
)
_SP_ONLY_DAC_EXCLUDE_KWARGS = {
    "exclude_workload_identity_credential": True,
    "exclude_managed_identity_credential": True,
    "exclude_shared_token_cache_credential": True,
    "exclude_visual_studio_code_credential": True,
    "exclude_cli_credential": True,
    "exclude_developer_cli_credential": True,
    "exclude_powershell_credential": True,
    "exclude_interactive_browser_credential": True,
    "exclude_broker_credential": True,
}
# Env vars that make Managed Identity short-circuit into the Workload Identity
# token-exchange flow. We clear these while constructing DefaultAzureCredential so
# that DAC's inner ManagedIdentityCredential targets IMDS, not WI.
_MI_TO_WI_TRIGGER_ENV_VARS = ("AZURE_FEDERATED_TOKEN_FILE",)


class ResilientAzureCredential:
    """Token credential that prefers SP, then falls back through WI and DAC-backed auth.

    The standard `DefaultAzureCredential` / `ChainedTokenCredential` only falls through
    on `CredentialUnavailableError`. This implementation catches *any* exception from
    a credential and tries the next one, then surfaces a single combined error if every
    credential fails. A complete Service Principal environment intentionally overrides
    AKS Workload Identity. The fallback DAC step supports Managed Identity and developer
    credentials in the normal azure-identity order.
    """

    def __init__(self, *, scopes_for_probe: tuple[str, ...] | None = None) -> None:
        self._lock = threading.Lock()
        self._active: tuple[str, Any] | None = None
        self._credentials = _build_credential_chain()
        self._scopes_for_probe = scopes_for_probe
        if not self._credentials:
            raise RuntimeError(
                "No Azure credentials could be initialized. Ensure azure-identity is "
                "installed and at least one auth mechanism (workload identity, managed "
                "identity, environment, or Azure CLI) is configured."
            )

    # The signature mirrors `TokenCredential.get_token` so this object is a drop-in.
    def get_token(self, *scopes: str, **kwargs: Any) -> AccessToken:
        # Fast path: a credential has already proven itself.
        with self._lock:
            active = self._active
        if active is not None:
            name, cred = active
            try:
                return cred.get_token(*scopes, **kwargs)
            except Exception as exc:  # noqa: BLE001 - intentional: re-run the chain on any error.
                LOGGER.warning(
                    "Cached credential %s failed (%s); re-running fallback chain.",
                    name,
                    _summarize_error(exc),
                )
                with self._lock:
                    self._active = None

        errors: list[str] = []
        for name, cred in self._credentials:
            try:
                token = cred.get_token(*scopes, **kwargs)
            except CredentialUnavailableError as exc:
                LOGGER.debug("Credential %s unavailable: %s", name, exc)
                errors.append(f"{name}: unavailable ({_summarize_error(exc)})")
                continue
            except Exception as exc:  # noqa: BLE001 - keep walking the chain on auth errors.
                LOGGER.warning(
                    "Credential %s failed (%s); trying next credential in fallback chain.",
                    name,
                    _summarize_error(exc),
                )
                errors.append(f"{name}: {_summarize_error(exc)}")
                continue

            LOGGER.info("Acquired Azure token via %s", name)
            with self._lock:
                self._active = (name, cred)
            return token

        raise ClientAuthenticationError(
            "All Azure credentials in the fallback chain failed:\n  - "
            + "\n  - ".join(errors)
        )

    def close(self) -> None:
        for _, cred in self._credentials:
            with suppress(Exception):
                close = getattr(cred, "close", None)
                if callable(close):
                    close()


def _build_credential_chain() -> list[tuple[str, Any]]:
    """Build the ordered list of credentials to try.

    The chain is intentionally short and deterministic:

    * **service-principal** — explicit override when `AZURE_CLIENT_ID`,
      `AZURE_TENANT_ID`, and `AZURE_CLIENT_SECRET` are all present. This uses DAC
            with only `EnvironmentCredential` enabled, so AKS-injected WI env vars cannot win.
    * **workload-identity** — explicit, only when the WI env vars are present and SP
      is not complete. Having this step explicit (instead of letting
      `DefaultAzureCredential` host it) is what allows `ResilientAzureCredential` to
      catch its hard auth errors and continue.
    * **default-azure-credential** — handles everything else in the standard DAC order
      with workload identity excluded: Managed Identity via IMDS (with the WI shortcut
      suppressed), Azure CLI, PowerShell, VS Code, etc. The `managed_identity_client_id`
      is pinned to `$AZURE_CLIENT_ID` so MI targets the same identity that backs WI, if
      that identity is also attached to the node VMSS.
    """

    chain: list[tuple[str, Any]] = []
    auth_mode = _requested_auth_mode()
    client_id = os.getenv("AZURE_CLIENT_ID")
    has_service_principal_env = all(os.getenv(name) for name in _SERVICE_PRINCIPAL_ENV_VARS)
    has_workload_identity_env = all(os.getenv(name) for name in _WORKLOAD_IDENTITY_ENV_VARS)

    if auth_mode == _AUTH_MODE_SERVICE_PRINCIPAL or (
        auth_mode == _AUTH_MODE_AUTO and has_service_principal_env
    ):
        if not has_service_principal_env:
            missing = ", ".join(_missing_env_vars(_SERVICE_PRINCIPAL_ENV_VARS))
            raise RuntimeError(
                f"{_AUTH_MODE_ENV_VAR}=service_principal requires these env vars: {missing}"
            )
        try:
            chain.append(
                (
                    "service-principal",
                    DefaultAzureCredential(**_SP_ONLY_DAC_EXCLUDE_KWARGS),
                )
            )
        except Exception as exc:  # noqa: BLE001
            if auth_mode == _AUTH_MODE_SERVICE_PRINCIPAL:
                # User explicitly selected SP; do not silently mask the failure.
                raise
            LOGGER.warning(
                "Service Principal DefaultAzureCredential could not be constructed: %s",
                _summarize_error(exc),
            )
        return chain

    if auth_mode == _AUTH_MODE_WORKLOAD_IDENTITY and not has_workload_identity_env:
        missing = ", ".join(_missing_env_vars(_WORKLOAD_IDENTITY_ENV_VARS))
        raise RuntimeError(
            f"{_AUTH_MODE_ENV_VAR}=workload_identity requires these env vars: {missing}"
        )

    if has_workload_identity_env:
        try:
            chain.append(("workload-identity", WorkloadIdentityCredential()))
        except Exception as exc:  # noqa: BLE001 - construction itself may raise on bad config.
            if auth_mode == _AUTH_MODE_WORKLOAD_IDENTITY:
                # User explicitly selected WI; do not silently fall back.
                raise
            LOGGER.warning(
                "WorkloadIdentityCredential could not be constructed: %s",
                _summarize_error(exc),
            )
    else:
        LOGGER.debug(
            "Workload Identity env vars not all present; skipping WorkloadIdentityCredential."
        )

    dac_kwargs: dict[str, Any] = {"exclude_workload_identity_credential": True}
    if auth_mode == _AUTH_MODE_WORKLOAD_IDENTITY:
        # The user explicitly selected WI, so don't let a mounted SP secret win later
        # inside DAC if WI fails. DAC still provides MI/developer fallback.
        dac_kwargs["exclude_environment_credential"] = True
    if client_id:
        dac_kwargs["managed_identity_client_id"] = client_id
    try:
        with _force_imds_managed_identity():
            chain.append(("default-azure-credential", DefaultAzureCredential(**dac_kwargs)))
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning(
            "DefaultAzureCredential could not be constructed: %s", _summarize_error(exc)
        )

    return chain


def _requested_auth_mode() -> str:
    """Return the normalized auth mode requested by deployment configuration."""

    value = os.getenv(_AUTH_MODE_ENV_VAR, _AUTH_MODE_AUTO).strip().lower().replace("-", "_")
    if value not in _SUPPORTED_AUTH_MODES:
        supported = ", ".join(sorted(_SUPPORTED_AUTH_MODES))
        raise ValueError(
            f"Unsupported {_AUTH_MODE_ENV_VAR}={value!r}; expected one of: {supported}"
        )
    return value


def _missing_env_vars(names: tuple[str, ...]) -> list[str]:
    """Return names that are unset or empty."""

    return [name for name in names if not os.getenv(name)]


def _summarize_error(exc: BaseException) -> str:
    """Return a single-line summary of an exception suitable for logs."""

    message = str(exc).strip().splitlines()[0] if str(exc).strip() else exc.__class__.__name__
    return f"{exc.__class__.__name__}: {message[:240]}"


@contextmanager
def _force_imds_managed_identity() -> Iterator[None]:
    """Temporarily clear env vars that make `ManagedIdentityCredential` use Workload Identity.

    In modern `azure-identity`, `ManagedIdentityCredential` detects WI env vars at
    construction time and internally uses the WI token-exchange flow instead of IMDS.
    For our fallback chain to provide a *real* alternative when WI is broken, MI must
    target IMDS directly. We clear the trigger env vars only for the duration of MI
    construction, then restore them so `WorkloadIdentityCredential` still works.
    """

    saved: dict[str, str] = {}
    for key in _MI_TO_WI_TRIGGER_ENV_VARS:
        value = os.environ.pop(key, None)
        if value is not None:
            saved[key] = value
    try:
        yield
    finally:
        os.environ.update(saved)


def create_credential() -> ResilientAzureCredential:
    """Public entry point used by the rest of the package."""

    return ResilientAzureCredential()
