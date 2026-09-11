"""Authoritative recovery writes and bounded incident projection repair."""
from __future__ import annotations
import copy
import time
from typing import Any
from durable_core import RecoveryState, VersionConflict

_TERMINAL = {RecoveryState.FAILED.value, RecoveryState.COMPLETE.value}
_FORWARD = {
    RecoveryState.PENDING.value: {RecoveryState.RELEASING.value, RecoveryState.VERIFYING_RELEASE.value, RecoveryState.PARTIAL.value, RecoveryState.FAILED.value, RecoveryState.COMPLETE.value},
    RecoveryState.RELEASING.value: {RecoveryState.VERIFYING_RELEASE.value, RecoveryState.PARTIAL.value, RecoveryState.FAILED.value, RecoveryState.COMPLETE.value},
    RecoveryState.VERIFYING_RELEASE.value: {RecoveryState.PARTIAL.value, RecoveryState.FAILED.value, RecoveryState.COMPLETE.value},
    RecoveryState.PARTIAL.value: {RecoveryState.COMPLETE.value},
}

def _recovery_repo(bundle: Any) -> Any:
    return getattr(bundle, "recovery", None) or bundle.recoveries

def _valid_forward(current: str | None, target: str) -> bool:
    if target not in {state.value for state in RecoveryState}: return False
    if current is None or current == target: return True
    if current in _TERMINAL: return False
    return target in _FORWARD.get(current, set())

def repair_incident_recovery_projection(bundle: Any, incident_id: str, *, retries: int = 3) -> bool:
    """Repair only the denormalized field; authority remains in recovery."""
    recovery = _recovery_repo(bundle).for_incident(incident_id)
    if not recovery: return True
    authoritative = recovery.get("state")
    for _ in range(max(1, retries)):
        incident = bundle.incidents.get(incident_id)
        if not incident: return False
        if incident.get("recovery_state") == authoritative: return True
        updated = copy.deepcopy(incident)
        updated["recovery_state"] = authoritative
        updated["updated_at"] = max(float(updated.get("updated_at") or 0), time.time())
        try:
            bundle.incidents.update(updated, expected_version=int(incident.get("version", 0)))
            return True
        except VersionConflict:
            continue
    return False

def save_recovery_and_repair(bundle: Any, record: dict[str, Any], *, retries: int = 3) -> dict[str, Any]:
    """CAS the authoritative record, then best-effort repair its projection."""
    repository = _recovery_repo(bundle)
    current = repository.for_incident(record["incident_id"])
    current_state = current.get("state") if current else None
    target = str(record.get("state") or "")
    if not _valid_forward(current_state, target):
        raise VersionConflict("recovery_transition_conflict")
    if current_state == target:
        stored = current
    else:
        expected = int(current.get("version", 0)) if current else -1
        stored = repository.save(record, expected_version=expected)
    repair_incident_recovery_projection(bundle, record["incident_id"], retries=retries)
    return stored

def overlay_authoritative_recovery(bundle: Any, incident: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(incident)
    recovery = _recovery_repo(bundle).for_incident(result.get("incident_id", ""))
    if recovery:
        result["recovery_state"] = recovery.get("state")
        result["recovery_authority"] = "RECOVERY_REPOSITORY"
    return result
