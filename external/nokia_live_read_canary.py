"""Guarded one-shot Nokia read canary; importing this module is inert.

The utility is deliberately not part of the FastAPI/runtime composition.  It
performs at most one congestion, one reachability and one location adapter
call, emits classifications only, and has no persistence or mutation hooks.
"""
from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Mapping
from typing import Any


TARGET_CELL = "T03"
TARGET_DEVICE = "ambulance-01"
MAX_EXPLICIT_READS = 3
_CAPABILITIES = ("CONGESTION", "REACHABILITY", "LOCATION")
_ALLOWED_RESULTS = {
    "SUCCESS", "UNAUTHORIZED", "UNSUPPORTED_IDENTITY", "RATE_LIMITED",
    "TIMEOUT", "UNAVAILABLE", "ERROR_SANITIZED",
}


def _guards_pass(environment: Mapping[str, str]) -> bool:
    """Require every activation flag explicitly; dotenv/defaults cannot opt in."""
    return (
        environment.get("HARIS_RUNTIME_ENV", "").strip().lower() == "external_integration"
        and environment.get("HARIS_ALLOW_EXTERNAL_TESTS", "").strip().lower() == "true"
        and environment.get("NAC_MODE", "").strip().lower() == "live_read_only"
        and environment.get("NOKIA_OBSERVATION_ENABLED", "").strip().lower() == "false"
        and environment.get("ENABLE_CONTINUOUS_LOOP", "").strip().lower() == "false"
        and environment.get("ENABLE_LIVE_WRITE_LOOP", "").strip().lower() == "false"
        and bool(environment.get("NAC_API_TOKEN", "").strip())
    )


def _status_code(exc: BaseException) -> int | None:
    for candidate in (exc, getattr(exc, "response", None)):
        value = getattr(candidate, "status_code", None)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return None


def classify_exception(exc: BaseException) -> str:
    """Map type/status metadata only; never inspect or serialize exception text."""
    name = type(exc).__name__.lower()
    status = _status_code(exc)
    if status in {401, 403} or "authentication" in name or "unauthorized" in name:
        return "UNAUTHORIZED"
    if status == 429 or "ratelimit" in name or "rate_limit" in name:
        return "RATE_LIMITED"
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)) or "timeout" in name:
        return "TIMEOUT"
    if status == 404 or "notfound" in name or "invalidparameter" in name:
        return "UNSUPPORTED_IDENTITY"
    if (
        status in {502, 503, 504}
        or "connection" in name
        or "gateway" in name
        or "serviceerror" in name
        or "unavailable" in name
    ):
        return "UNAVAILABLE"
    return "ERROR_SANITIZED"


async def _classified_read(operation: Callable[[], Any]) -> str:
    try:
        result = await operation()
    except Exception as exc:  # classification is intentionally value-blind
        return classify_exception(exc)
    return "SUCCESS" if isinstance(result, list) and bool(result) else "UNAVAILABLE"


def _safe_lines(results: Mapping[str, str], attempted: int) -> list[str]:
    lines: list[str] = []
    for capability in _CAPABILITIES:
        classification = results.get(capability, "UNAVAILABLE")
        if classification not in _ALLOWED_RESULTS:
            classification = "ERROR_SANITIZED"
        lines.append(f"{capability}={classification}")
        if classification == "SUCCESS":
            lines.append(f"{capability}_PROVENANCE=NOKIA_LIVE")
    lines.extend((
        f"EXPLICIT_READS_ATTEMPTED={max(0, min(int(attempted), MAX_EXPLICIT_READS))}",
        "PROVIDER_MUTATIONS=0",
        "PERSISTENCE_WRITES=0",
    ))
    return lines


async def run_canary(
    *,
    environment: Mapping[str, str] | None = None,
    settings_factory: Callable[[], Any] | None = None,
    client_factory: Callable[[Any], Any] | None = None,
) -> tuple[list[str], int]:
    """Return sanitized output lines and a process exit code."""
    environment = os.environ if environment is None else environment
    if not _guards_pass(environment):
        return _safe_lines({}, 0), 2

    # Import HARIS/SDK composition only after every raw process guard passes.
    if settings_factory is None:
        from config import AppSettings
        settings_factory = AppSettings
    if client_factory is None:
        from nokia_clients import LiveNokiaClient
        client_factory = LiveNokiaClient

    try:
        settings = settings_factory()
        if (
            str(settings.nac_mode).lower() != "live_read_only"
            or settings.nokia_observation_enabled
            or settings.enable_continuous_loop
            or settings.enable_live_write_loop
            or not settings.nac_api_token
        ):
            return _safe_lines({}, 0), 2
        client = client_factory(settings)
    except Exception:
        return _safe_lines({}, 0), 2

    attempted = 0
    results: dict[str, str] = {}
    operations = (
        ("CONGESTION", lambda: client.congestion_insights([TARGET_CELL])),
        ("REACHABILITY", lambda: client.device_status([TARGET_DEVICE])),
        ("LOCATION", lambda: client.location_retrieval([TARGET_DEVICE])),
    )
    for capability, operation in operations:
        if attempted >= MAX_EXPLICIT_READS:
            break
        attempted += 1
        results[capability] = await _classified_read(operation)
    return _safe_lines(results, attempted), 0


def main() -> int:
    lines, exit_code = asyncio.run(run_canary())
    for line in lines:
        print(line)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
