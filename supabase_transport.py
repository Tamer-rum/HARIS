"""Bounded server-only HTTP transport for the HARIS Supabase RPC contract.

This module performs no work at import time.  A transport is built only by the
backend persistence composition root, and an outbound request is possible only
after a dedicated persistence runtime gate authorizes the exact configured
project hostname.
"""
from __future__ import annotations

import os
import logging
from typing import Any, Callable
from urllib.parse import urlparse

import httpx

from durable_core import RepositoryUnavailable, ResourceAlreadyOwned, VersionConflict, _require_safe
from postgres_persistence import (
    PersistenceAuthenticationFailed,
    PersistenceHostNotAllowed,
    PersistenceNetworkBlocked,
    PersistenceNotConfigured,
    PersistenceResponseInvalid,
    PersistenceRpcContractFailed,
    PersistenceSchemaNotReady,
    PersistenceTransportUnavailable,
)


HARIS_RPC_ALLOWLIST = frozenset({
    "haris_ack_outbox",
    "haris_acquire_resource",
    "haris_append_domain_event",
    "haris_append_incident_transition",
    "haris_append_outbox",
    "haris_append_policy_cost",
    "haris_claim_inbox",
    "haris_claim_outbox",
    "haris_create_incident",
    "haris_event_sequence",
    "haris_fail_outbox",
    "haris_latest_checkpoint",
    "haris_pending_outbox",
    "haris_policy_cost_totals",
    "haris_process_inbound_event",
    "haris_read_domain",
    "haris_read_policy_cost",
    "haris_release_resource",
    "haris_save_action",
    "haris_save_checkpoint",
    "haris_save_recovery",
    "haris_save_verification",
    "haris_transition_incident",
    "haris_update_incident",
    "haris_write_network_state",
})

HARIS_RPC_RESPONSE_SHAPES = {
    "haris_ack_outbox": "null",
    "haris_acquire_resource": "object",
    "haris_append_domain_event": "object",
    "haris_append_incident_transition": "object",
    "haris_append_outbox": "object",
    "haris_append_policy_cost": "object",
    "haris_claim_inbox": "boolean",
    "haris_claim_outbox": "array",
    "haris_create_incident": "object",
    "haris_event_sequence": "object",
    "haris_fail_outbox": "null",
    "haris_latest_checkpoint": "null_or_object",
    "haris_pending_outbox": "array",
    "haris_policy_cost_totals": "object",
    "haris_process_inbound_event": "object",
    "haris_read_domain": "array",
    "haris_read_policy_cost": "array",
    "haris_release_resource": "object",
    "haris_save_action": "object",
    "haris_save_checkpoint": "object",
    "haris_save_recovery": "object",
    "haris_save_verification": "object",
    "haris_transition_incident": "object",
    "haris_update_incident": "object",
    "haris_write_network_state": "object",
}

_VERSION_CONFLICT_RPCS = frozenset({
    "haris_process_inbound_event",
    "haris_save_action", "haris_transition_incident",
    "haris_update_incident", "haris_write_network_state",
})
_RESOURCE_CONFLICT_RPCS = frozenset({
    "haris_ack_outbox", "haris_acquire_resource", "haris_fail_outbox",
    "haris_release_resource",
})
_MAX_RESPONSE_BYTES = 4 * 1024 * 1024


def _shape_type(value: Any) -> str:
    if value is None: return "null"
    if isinstance(value, bool): return "boolean"
    if isinstance(value, dict): return "object"
    if isinstance(value, list): return "array"
    if isinstance(value, int): return "integer"
    if isinstance(value, str): return "string"
    return "unknown"


def _shape_matches(expected: str, actual: str) -> bool:
    return actual == expected or expected == "null_or_object" and actual in {"null", "object"}


def _validated_project_origin(value: str) -> tuple[str, str]:
    parsed = urlparse(value)
    hostname = (parsed.hostname or "").lower()
    if (
        parsed.scheme.lower() != "https" or not hostname
        or parsed.username or parsed.password or parsed.query or parsed.fragment
        or parsed.path not in {"", "/"}
        or not hostname.endswith(".supabase.co")
        or hostname == "supabase.co"
    ):
        raise PersistenceNotConfigured("PERSISTENCE_NOT_CONFIGURED")
    return f"https://{hostname}", hostname


def authorize_persistence_network(hostname: str) -> None:
    """Authorize only the configured Supabase project for durable RPC traffic."""
    from runtime import (
        RuntimeEnvironment, external_access_policy, record_provider_access,
        runtime_environment,
    )

    active_runtime = runtime_environment()
    policy = external_access_policy()
    dedicated_runtime_gate = (
        active_runtime is RuntimeEnvironment.PRODUCTION
    ) or (
        active_runtime is RuntimeEnvironment.PERSISTENCE_INTEGRATION
        and os.getenv("HARIS_ALLOW_PERSISTENCE_INTEGRATION", "").lower() == "true"
    ) or (
        active_runtime is RuntimeEnvironment.REAL_QOD_VALIDATION
        and os.getenv("HARIS_ALLOW_REAL_QOD_VALIDATION", "").lower() == "true"
    )
    if (
        not dedicated_runtime_gate
        or not policy.allow_remote_database or not policy.allow_persistence_http
        or os.getenv("HARIS_PERSISTENCE_MODE", "").lower() != "postgres"
    ):
        raise PersistenceNetworkBlocked()
    configured_url = os.getenv("SUPABASE_URL", "")
    try:
        _origin, configured_hostname = _validated_project_origin(configured_url)
    except PersistenceNotConfigured as exc:
        raise PersistenceHostNotAllowed() from exc
    if configured_hostname != hostname:
        raise PersistenceHostNotAllowed()
    record_provider_access("supabase_rpc")


class SupabaseRpcTransport:
    """Synchronous PostgREST RPC transport with no arbitrary HTTP surface."""

    def __init__(
        self,
        project_url: str,
        api_key: str,
        *,
        expected_hostname: str,
        timeout_seconds: float = 5.0,
        client: httpx.Client | None = None,
        network_authorizer: Callable[[str], None] = authorize_persistence_network,
    ) -> None:
        origin, hostname = _validated_project_origin(project_url)
        if hostname != expected_hostname.lower() or not api_key:
            raise PersistenceNotConfigured("PERSISTENCE_NOT_CONFIGURED")
        if timeout_seconds <= 0 or timeout_seconds > 15:
            raise PersistenceNotConfigured("PERSISTENCE_NOT_CONFIGURED")
        self._origin = origin
        self._hostname = hostname
        self._api_key = api_key
        self._timeout = httpx.Timeout(timeout_seconds, connect=min(timeout_seconds, 3.0))
        self._client = client
        self._owns_client = client is None
        self._authorize = network_authorizer

    def __repr__(self) -> str:
        return f"<SupabaseRpcTransport hostname={self._hostname!r} credentials=redacted>"

    @property
    def hostname(self) -> str:
        return self._hostname

    def _http_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(follow_redirects=False)
        return self._client

    def rpc(self, name: str, arguments: dict[str, Any]) -> Any:
        if name not in HARIS_RPC_ALLOWLIST:
            raise PersistenceRpcContractFailed()
        expected_shape = HARIS_RPC_RESPONSE_SHAPES[name]
        if not isinstance(arguments, dict):
            raise PersistenceRpcContractFailed(rpc_name=name,expected_shape=expected_shape,actual_shape_type="unknown")
        try:
            _require_safe(arguments)
        except ValueError as exc:
            raise PersistenceRpcContractFailed(rpc_name=name,expected_shape=expected_shape,actual_shape_type="unknown") from exc
        self._authorize(self._hostname)
        # HTTPX/httpcore debug logging can include complete request targets or
        # headers. HARIS persistence diagnostics are deliberately status-only.
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        headers = {
            "apikey": self._api_key,
            "Authorization": f"Bearer {self._api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        try:
            response = self._http_client().request(
                "POST", f"{self._origin}/rest/v1/rpc/{name}",
                json=arguments, headers=headers, timeout=self._timeout,
                follow_redirects=False,
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise PersistenceTransportUnavailable("PERSISTENCE_UNAVAILABLE") from exc
        except httpx.HTTPError as exc:
            raise PersistenceTransportUnavailable("PERSISTENCE_UNAVAILABLE") from exc

        status = response.status_code
        if status in {401, 403}:
            raise PersistenceAuthenticationFailed(http_status=status)
        if status == 404:
            raise PersistenceSchemaNotReady(http_status=status)
        if status == 409:
            if name in _VERSION_CONFLICT_RPCS:
                raise VersionConflict("repository_version_conflict")
            if name in _RESOURCE_CONFLICT_RPCS:
                raise ResourceAlreadyOwned("repository_resource_conflict")
            raise RepositoryUnavailable("persistence_conflict")
        if 300 <= status < 400:
            raise PersistenceHostNotAllowed(http_status=status)
        if status == 429 or status >= 500:
            raise PersistenceTransportUnavailable(http_status=status)
        if status < 200 or status >= 300:
            raise PersistenceRpcContractFailed(http_status=status,rpc_name=name,expected_shape=expected_shape,actual_shape_type="unknown")
        if len(response.content) > _MAX_RESPONSE_BYTES:
            raise PersistenceResponseInvalid(http_status=status,rpc_name=name,expected_shape=expected_shape,actual_shape_type="unknown")
        if status == 204 or not response.content:
            result = None
            actual_shape = "null"
            if not _shape_matches(expected_shape, actual_shape):
                raise PersistenceResponseInvalid(http_status=status,rpc_name=name,expected_shape=expected_shape,actual_shape_type=actual_shape)
            return result
        try:
            result = response.json()
        except (ValueError, UnicodeError) as exc:
            raise PersistenceResponseInvalid(http_status=status,rpc_name=name,expected_shape=expected_shape,actual_shape_type="unknown") from exc
        actual_shape = _shape_type(result)
        if not _shape_matches(expected_shape, actual_shape):
            raise PersistenceResponseInvalid(http_status=status,rpc_name=name,expected_shape=expected_shape,actual_shape_type=actual_shape)
        return result

    def preflight(self) -> Any:
        """Perform the smallest migration-dependent, read-only RPC probe."""
        return self.rpc("haris_read_domain", {
            "p_kind": "network",
            "p_key": "PERSISTENCE-PREFLIGHT-NO-MATCH",
            "p_after": 0,
            "p_limit": 1,
        })

    def close(self) -> None:
        if self._owns_client and self._client is not None:
            self._client.close()
        self._client = None


def build_supabase_rpc_transport(settings: Any, hostname: str) -> SupabaseRpcTransport:
    """Factory used lazily by the backend repository composition root."""
    secret = getattr(settings, "supabase_key", None)
    key = secret.get_secret_value() if hasattr(secret, "get_secret_value") else str(secret or "")
    return SupabaseRpcTransport(
        str(getattr(settings, "supabase_url", "")), key,
        expected_hostname=hostname,
    )
