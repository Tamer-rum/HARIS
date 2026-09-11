"""Reconcile one existing Phase 7E QoD cleanup without CREATE or DELETE.

The artifact supplies opaque durable references internally.  Console output is
fixed and sanitized: provider/action/incident identifiers are never emitted.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

ALLOWED_RESULTS = {
    "CLEANUP_VERIFIED", "RESOURCE_STILL_ACTIVE",
    "RECONCILIATION_REQUIRED", "BLOCKED_SAFE",
}


class ReconciliationAbort(RuntimeError):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def _reference(path: Path) -> tuple[str, str]:
    root = (PROJECT_ROOT / "artifacts" / "validation").resolve()
    resolved = path.resolve()
    if root not in resolved.parents or not resolved.name.startswith("real_qod_REAL-QOD-TEST-"):
        raise ReconciliationAbort("ARTIFACT_NOT_ALLOWED")
    try:
        record = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        raise ReconciliationAbort("ARTIFACT_INVALID") from None
    run_id = str(record.get("run_id") or "")
    action_id = str(record.get("action_id") or "")
    if not run_id.startswith("REAL-QOD-TEST-") or not action_id:
        raise ReconciliationAbort("DURABLE_REFERENCE_MISSING")
    return run_id, action_id


def _bundle() -> tuple[Any, Any]:
    from config import AppSettings
    from postgres_persistence import build_repository_bundle
    from runtime import RuntimeEnvironment

    settings = AppSettings().model_copy(update={
        "enable_continuous_loop": False,
        "nokia_observation_enabled": False,
        "geofencing_monitoring_enabled": False,
        "gemini_api_key": None,
        "groq_api_key": None,
    })
    if settings.haris_persistence_mode != "postgres":
        raise ReconciliationAbort("POSTGRES_REQUIRED")
    bundle = build_repository_bundle(
        settings, runtime=RuntimeEnvironment.REAL_QOD_VALIDATION,
    )
    return settings, bundle


def _durable_view(bundle: Any, run_id: str, action_id: str) -> dict[str, Any]:
    original = bundle.actions.get(action_id)
    if original is None or original.incident_id != f"{run_id}-INCIDENT" or original.command_type != "QOD_PLAN":
        raise ReconciliationAbort("DURABLE_ACTION_MISMATCH")
    incident = bundle.incidents.get(original.incident_id)
    actions = bundle.actions.for_incident(original.incident_id)
    cleanup = next((
        row for row in actions
        if row.command_type == "QOD_RELEASE"
        and row.parameters_safe.get("original_command_id") == original.command_id
    ), None)
    owner = bundle.resource_ownership.get_active(original.resource_key)
    owner_state = "NONE" if owner is None else (
        "OWNED_BY_INCIDENT" if owner.get("owner_incident_id") == original.incident_id else "FOREIGN"
    )
    verifications = bundle.verification.for_incident(original.incident_id)
    recovery = bundle.recovery.for_incident(original.incident_id)
    reconciliation_attempts = [
        row for row in verifications
        if row.get("verification_type") == "ROLLBACK_RECONCILIATION"
        and (row.get("result") or {}).get("command_id") == cleanup.command_id
    ] if cleanup else []
    pending_outbox = [
        row for row in bundle.outbox.unsent()
        if row.get("incident_id") == original.incident_id
        or (row.get("payload") or {}).get("incident_id") == original.incident_id
    ]
    return {
        "status": "DURABLE_STATE_INSPECTED",
        "incident_state": str((incident or {}).get("state") or "UNAVAILABLE"),
        "action_count": len(actions),
        "original_action_state": original.state.value,
        "provider_resource_id_known": bool(original.provider_resource_id),
        "cleanup_action_present": cleanup is not None,
        "cleanup_action_state": cleanup.state.value if cleanup else "UNAVAILABLE",
        "ownership_state": owner_state,
        "verification_count": len(verifications),
        "reconciliation_attempt_count": len(reconciliation_attempts),
        "recovery_present": recovery is not None,
        "recovery_state": str((recovery or {}).get("state") or "UNAVAILABLE"),
        "pending_outbox_count": len(pending_outbox),
        "create_permitted": False,
        "delete_permitted": False,
    }


class _ReadOnlySessions:
    def __init__(self, sessions: Any):
        self._sessions = sessions
        self.reads = 0

    def get(self, resource_id: str) -> Any:
        if self.reads >= 1:
            raise ReconciliationAbort("SAFE_READ_BUDGET_EXCEEDED")
        self.reads += 1
        return self._sessions.get(resource_id)


class _ReadOnlyNokiaClient:
    """Expose one QoD GET and no mutation method to the provider adapter."""
    def __init__(self, raw: Any):
        sessions = _ReadOnlySessions(raw.client.sessions)
        self.client = SimpleNamespace(sessions=sessions)
        self.sessions = sessions

    async def request_qos(self, *_args: Any, **_kwargs: Any) -> Any:
        raise ReconciliationAbort("CREATE_PROHIBITED")

    async def release_qos(self, *_args: Any, **_kwargs: Any) -> Any:
        raise ReconciliationAbort("DELETE_PROHIBITED")


def _live_read_context(settings: Any) -> bool:
    return settings.nac_mode in {"live_read_only", "live_write"}


async def _reconcile(settings: Any, bundle: Any, run_id: str, action_id: str) -> dict[str, Any]:
    from durable_execution import DurableActionExecutionService, ExistingNokiaActuatorAdapter
    from durable_reconciliation import DurableActionReconciliationService
    from nokia_clients import build_nokia_client
    from runtime_events import RuntimeIngestionMetrics

    if not _live_read_context(settings):
        raise ReconciliationAbort("LIVE_READ_CONTEXT_REQUIRED")
    if not settings.nac_api_token:
        raise ReconciliationAbort("NOKIA_CONFIGURATION_REQUIRED")
    raw = build_nokia_client(settings)
    read_only = _ReadOnlyNokiaClient(raw)
    adapter = ExistingNokiaActuatorAdapter(read_only, settings)
    metrics = RuntimeIngestionMetrics()
    executor = DurableActionExecutionService(
        bundle=bundle, adapter=adapter, settings=settings,
        is_ready=lambda: True, metrics=metrics,
    )
    service = DurableActionReconciliationService(
        bundle=bundle, adapter=adapter, settings=settings,
        execution_service=executor, is_ready=lambda: True, metrics=metrics,
    )
    result = await service.reconcile_validation_cleanup(action_id, run_id)
    status = result.status if result.status in ALLOWED_RESULTS else "BLOCKED_SAFE"
    return {
        "RECONCILIATION_STATUS": status,
        "provider_read_performed": result.provider_reads == 1,
        "provider_mutation_performed": False,
        "create_performed": False,
        "delete_performed": False,
        "action_state": result.action_state or "UNAVAILABLE",
        "verification_state": result.verification_state or "UNAVAILABLE",
        "safe_reason": result.reason,
    }


def _emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="HARIS real-QoD cleanup reconciliation only")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--inspect", action="store_true")
    mode.add_argument("--reconcile", action="store_true")
    parser.add_argument("--artifact", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        run_id, action_id = _reference(args.artifact)
        settings, bundle = _bundle()
        view = _durable_view(bundle, run_id, action_id)
        if args.inspect:
            _emit(view)
            return 0
        _emit(asyncio.run(_reconcile(settings, bundle, run_id, action_id)))
        return 0
    except ReconciliationAbort as exc:
        _emit({
            "RECONCILIATION_STATUS": "BLOCKED_SAFE",
            "safe_reason": exc.reason,
            "provider_mutation_performed": False,
            "create_performed": False,
            "delete_performed": False,
        })
        return 2
    except Exception:
        _emit({
            "RECONCILIATION_STATUS": "BLOCKED_SAFE",
            "safe_reason": "RECONCILIATION_UNAVAILABLE",
            "provider_mutation_performed": False,
            "create_performed": False,
            "delete_performed": False,
        })
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
