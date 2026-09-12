"""Central, dependency-light security boundary for the HARIS operational API."""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Deque, Optional

from pydantic import SecretStr


RATE_LIMITING_SCOPE = "PER_PROCESS_PROTOTYPE"
MULTI_INSTANCE_SHARED_RATE_LIMITING = "NOT_PROVEN"
RATE_LIMIT_WINDOW_SECONDS = 60
RATE_LIMIT_REQUESTS = 120
RATE_LIMIT_MAX_KEYS = 2048


class RouteClass(str, Enum):
    PUBLIC_HEALTH = "public_health"
    OPERATIONAL_READ = "operational_read"
    CONTROL_IDENTITY = "control_identity"
    PROVIDER_CALLBACK = "provider_callback"
    FIXTURE_MUTATION = "fixture_mutation"
    DOCUMENTATION = "documentation"
    OTHER = "other"


PUBLIC_HEALTH_ROUTES = {
    ("GET", "/api/nac/health"),
    ("GET", "/api/platform/health"),
}

OPERATIONAL_READ_ROUTES = {
    ("GET", "/api/v1/noc/snapshot"),
    ("GET", "/api/nac/network-state"),
    ("GET", "/api/nac/incidents/active"),
    ("GET", "/api/nac/mode"),
    ("GET", "/api/nac/capabilities"),
    ("GET", "/api/nac/incidents"),
    ("GET", "/api/nac/observations/status"),
    ("GET", "/api/nac/observations/latest"),
    ("GET", "/api/nac/observations/history"),
    ("GET", "/api/nac/autonomous/status"),
    ("GET", "/api/nac/congestion"),
    ("POST", "/api/nac/device-status"),
    ("POST", "/api/nac/location"),
}

CONTROL_IDENTITY_ROUTES = {
    ("POST", "/api/nac/admin/live-read-canary"),
    ("POST", "/api/nac/autonomous/run"),
    ("POST", "/api/nac/autonomous/field-intervention-demo"),
    ("POST", "/api/nac/autonomous/consent-action"),
    ("POST", "/api/nac/autonomous/consent-action-token"),
    ("POST", "/api/nac/auth/number-verification/start"),
    ("POST", "/api/nac/trusted-dispatch/evaluate"),
}

FIXTURE_MUTATION_ROUTES = {
    ("POST", "/api/nac/geofence"),
    ("POST", "/api/nac/qos"),
    ("POST", "/api/nac/slice/attach"),
    ("POST", "/api/nac/slice/detach"),
}

PROVIDER_CALLBACK_ROUTES = {
    ("GET", "/api/nac/auth/number-verification/callback"),
    ("POST", "/api/nac/callbacks/nokia/geofence"),
    ("POST", "/api/events/nokia/congestion"),
}

DOCUMENTATION_PATHS = {"/docs", "/redoc", "/openapi.json"}


def classify_route(method: str, path: str) -> RouteClass:
    key = (method.upper(), path)
    if key in PUBLIC_HEALTH_ROUTES:
        return RouteClass.PUBLIC_HEALTH
    if key in OPERATIONAL_READ_ROUTES:
        return RouteClass.OPERATIONAL_READ
    if key in CONTROL_IDENTITY_ROUTES:
        return RouteClass.CONTROL_IDENTITY
    if key in FIXTURE_MUTATION_ROUTES:
        return RouteClass.FIXTURE_MUTATION
    if key in PROVIDER_CALLBACK_ROUTES:
        return RouteClass.PROVIDER_CALLBACK
    if path in DOCUMENTATION_PATHS:
        return RouteClass.DOCUMENTATION
    if method.upper() == "GET" and path.startswith("/api/nac/incidents/"):
        return RouteClass.OPERATIONAL_READ
    if method.upper() == "DELETE" and (
        path.startswith("/api/nac/geofence/") or path.startswith("/api/nac/qos/")
    ):
        return RouteClass.FIXTURE_MUTATION
    return RouteClass.OTHER


PROTECTED_ROUTE_CLASSES = {
    RouteClass.OPERATIONAL_READ,
    RouteClass.CONTROL_IDENTITY,
    RouteClass.FIXTURE_MUTATION,
}


@dataclass(frozen=True)
class OperationalAuthError(Exception):
    status_code: int
    detail: str


def _secret_value(secret: Optional[SecretStr]) -> Optional[str]:
    if secret is None:
        return None
    value = secret.get_secret_value().strip()
    return value or None


def authenticate_bearer(expected_secret: Optional[SecretStr], authorization: Optional[str]) -> str:
    """Authenticate without returning, recording, or formatting the raw token."""
    expected = _secret_value(expected_secret)
    if expected is None:
        raise OperationalAuthError(503, "Operational API authentication is not configured.")
    if not authorization:
        raise OperationalAuthError(401, "Operational API authentication is required.")
    scheme, separator, submitted = authorization.partition(" ")
    if not separator or scheme.casefold() != "bearer" or not submitted.strip():
        raise OperationalAuthError(401, "Operational API authentication is required.")
    submitted = submitted.strip()
    if not hmac.compare_digest(submitted.encode("utf-8"), expected.encode("utf-8")):
        raise OperationalAuthError(403, "Operational API authorization denied.")
    # A high-entropy credential is reduced to a one-way, context-bound key.
    return hashlib.sha256(b"haris-operational-principal-v1\x00" + submitted.encode("utf-8")).hexdigest()


def authenticate_shared_secret(expected_secret: Optional[SecretStr], submitted: Optional[str]) -> str:
    expected = _secret_value(expected_secret)
    if expected is None or submitted is None:
        raise OperationalAuthError(401, "Unauthorized event notification.")
    if not hmac.compare_digest(submitted.encode("utf-8"), expected.encode("utf-8")):
        raise OperationalAuthError(401, "Unauthorized event notification.")
    return hashlib.sha256(b"haris-provider-callback-v1\x00" + submitted.encode("utf-8")).hexdigest()


class BoundedRateLimiter:
    """Thread-safe fixed-window limiter with deterministic expiry and bounded keys."""

    def __init__(self, *, limit: int = RATE_LIMIT_REQUESTS, window_seconds: int = RATE_LIMIT_WINDOW_SECONDS, max_keys: int = RATE_LIMIT_MAX_KEYS):
        if limit < 1 or window_seconds < 1 or max_keys < 1:
            raise ValueError("Rate-limit bounds must be positive.")
        self.limit = limit
        self.window_seconds = window_seconds
        self.max_keys = max_keys
        self._entries: "OrderedDict[str, Deque[float]]" = OrderedDict()
        self._lock = threading.Lock()

    def allow(self, principal_digest: str, route_class: RouteClass | str, client_address: str, *, now: Optional[float] = None) -> bool:
        instant = time.monotonic() if now is None else float(now)
        material = f"{principal_digest}|{str(route_class)}|{client_address}".encode("utf-8")
        key = hashlib.sha256(material).hexdigest()
        cutoff = instant - self.window_seconds
        with self._lock:
            expired = [entry_key for entry_key, values in self._entries.items() if not values or values[-1] <= cutoff]
            for entry_key in expired:
                self._entries.pop(entry_key, None)
            values = self._entries.get(key)
            if values is None:
                while len(self._entries) >= self.max_keys:
                    self._entries.popitem(last=False)
                values = deque()
                self._entries[key] = values
            while values and values[0] <= cutoff:
                values.popleft()
            self._entries.move_to_end(key)
            if len(values) >= self.limit:
                return False
            values.append(instant)
            return True

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


operational_rate_limiter = BoundedRateLimiter()


_PROVIDER_IDENTIFIER_KEYS = {
    "provider_resource_id",
    "provider_resource_ids",
    "provider_session_id",
    "provider_subscription_id",
    "provider_slice_id",
    "provider_identifier",
    "provider_identifiers",
    "session_id",
    "subscription_id",
    "event_subscription_id",
}

_SENSITIVE_RESPONSE_KEYS = {
    "token", "access_token", "refresh_token", "id_token", "api_token", "api_key",
    "apikey", "authorization", "client_secret", "oauth_state", "oauth_code",
    "authorization_code", "authorization_url", "consent_action_token", "workflow_session_token",
    "password", "phone_number", "msisdn", "login_hint",
}


def _redact_sensitive_text(value: str) -> str:
    value = re.sub(r"\bBearer\s+[A-Za-z0-9._~+/=-]+", "Bearer [REDACTED]", value, flags=re.I)
    value = re.sub(
        r"([?&\s](?:code|state|access_token|refresh_token|id_token|token|api_key|apikey|client_secret)=)[^&#\s]+",
        r"\1[REDACTED]", value, flags=re.I,
    )
    return re.sub(r"(?<!\*)\+\d{8,15}\b", "***[REDACTED]", value)


def redact_provider_identifiers(
    value: Any, *, allow_authorization_url: bool = False,
    allow_handoff_tokens: bool = False,
) -> Any:
    """Recursively sanitize operational payloads at the external boundary.

    ``authorization_url`` is allowed only for the two authenticated, bounded
    consent handoff responses. It remains forbidden in status/NOC/WebSocket
    payloads and is never logged by this function.
    """
    if isinstance(value, dict):
        redacted = {}
        for key, item in value.items():
            normalized = str(key).casefold()
            is_provider_identifier = normalized in _PROVIDER_IDENTIFIER_KEYS or (
                normalized.startswith("provider_")
                and normalized.endswith(("_id", "_ids", "_identifier", "_identifiers"))
            )
            is_secret = normalized in _SENSITIVE_RESPONSE_KEYS or normalized.endswith(
                ("_api_key", "_access_token", "_client_secret", "_oauth_state")
            )
            if normalized == "authorization_url" and allow_authorization_url:
                redacted[key] = item
            elif normalized in {"consent_action_token", "workflow_session_token"} and allow_handoff_tokens:
                redacted[key] = item
            elif (is_provider_identifier or is_secret) and item is not None:
                redacted[key] = "[REDACTED]"
            else:
                redacted[key] = redact_provider_identifiers(
                    item, allow_authorization_url=allow_authorization_url,
                    allow_handoff_tokens=allow_handoff_tokens,
                )
        return redacted
    if isinstance(value, list):
        return [redact_provider_identifiers(item, allow_authorization_url=allow_authorization_url, allow_handoff_tokens=allow_handoff_tokens) for item in value]
    if isinstance(value, tuple):
        return [redact_provider_identifiers(item, allow_authorization_url=allow_authorization_url, allow_handoff_tokens=allow_handoff_tokens) for item in value]
    return _redact_sensitive_text(value) if isinstance(value, str) else value


def redact_json_bytes(
    body: bytes, *, allow_authorization_url: bool = False,
    allow_handoff_tokens: bool = False,
) -> bytes:
    try:
        parsed = json.loads(body)
    except (TypeError, ValueError, UnicodeDecodeError):
        return body
    return json.dumps(
        redact_provider_identifiers(
            parsed, allow_authorization_url=allow_authorization_url,
            allow_handoff_tokens=allow_handoff_tokens,
        ),
        separators=(",", ":"),
    ).encode("utf-8")
