"""Offline-safe persistence startup lifecycle and bounded reconstruction.

This module owns no credentials and constructs no provider clients.  It turns
repository state into reconstructible runtime views before workers may start.
"""
from __future__ import annotations

import copy
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from durable_core import ActionState, ProjectionIntegrityError


class PlatformLifecycleState(str, Enum):
    STARTING = "STARTING"
    PERSISTENCE_CONFIGURING = "PERSISTENCE_CONFIGURING"
    PERSISTENCE_CONNECTING = "PERSISTENCE_CONNECTING"
    RECONSTRUCTING = "RECONSTRUCTING"
    READY = "READY"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


@dataclass
class ReadinessMetrics:
    reconstruction_duration_ms: float = 0.0
    reconstructed_active_incidents: int = 0
    reconstructed_pending_actions: int = 0
    reconciliation_required_actions: int = 0
    repository_ready: bool = False
    persistence_startup_failures: int = 0


@dataclass
class ReconstructionResult:
    checkpoint: dict[str, Any] | None
    network_state: dict[str, dict[str, Any]]
    active_incidents: list[dict[str, Any]]
    active_ownership: list[dict[str, Any]]
    pending_actions: list[Any]
    reconciliation_required: list[Any]
    pending_verifications: list[dict[str, Any]]
    pending_recoveries: list[dict[str, Any]]
    pending_outbox: list[dict[str, Any]]
    event_sequence: int
    duration_ms: float


@dataclass
class PlatformLifecycle:
    state: PlatformLifecycleState = PlatformLifecycleState.STARTING
    mode: str = "memory"
    configured: bool = False
    connected: bool = False
    reconstruction: str = "NOT_STARTED"
    reason: str | None = None
    metrics: ReadinessMetrics = field(default_factory=ReadinessMetrics)
    reconstruction_result: ReconstructionResult | None = None
    history: list[str] = field(default_factory=lambda: [PlatformLifecycleState.STARTING.value])

    def reset(self, mode: str) -> None:
        failures = self.metrics.persistence_startup_failures
        self.state = PlatformLifecycleState.STARTING
        self.mode = mode
        self.configured = False
        self.connected = False
        self.reconstruction = "NOT_STARTED"
        self.reason = None
        self.reconstruction_result = None
        self.history = [PlatformLifecycleState.STARTING.value]
        self.metrics = ReadinessMetrics(persistence_startup_failures=failures)

    def transition(self, state: PlatformLifecycleState) -> None:
        self.state = state
        self.history.append(state.value)

    def fail(self, reason: str, *, failed: bool = False) -> None:
        self.transition(PlatformLifecycleState.FAILED if failed else PlatformLifecycleState.DEGRADED)
        self.reconstruction = "FAILED" if reason in {"RECONSTRUCTION_FAILED", "PROJECTION_INTEGRITY_FAILED"} else self.reconstruction
        self.reason = reason
        self.metrics.repository_ready = False
        self.metrics.persistence_startup_failures += 1

    def ready(self, result: ReconstructionResult) -> None:
        self.transition(PlatformLifecycleState.READY)
        self.reconstruction = "COMPLETE"
        self.reason = None
        self.reconstruction_result = result
        self.metrics.reconstruction_duration_ms = result.duration_ms
        self.metrics.reconstructed_active_incidents = len(result.active_incidents)
        self.metrics.reconstructed_pending_actions = len(result.pending_actions)
        self.metrics.reconciliation_required_actions = len(result.reconciliation_required)
        self.metrics.repository_ready = True

    def public_status(self) -> dict[str, Any]:
        return {
            "status": "READY" if self.state is PlatformLifecycleState.READY else "NOT_READY",
            "lifecycle_state": self.state.value,
            "mode": self.mode,
            "configured": self.configured,
            "connected": self.connected,
            "reconstruction": self.reconstruction,
            "reason": self.reason,
            "repository_ready": self.metrics.repository_ready,
            "active_incidents_reconstructed": self.metrics.reconstructed_active_incidents,
            "pending_actions_reconstructed": self.metrics.reconstructed_pending_actions,
            "reconciliation_required": self.metrics.reconciliation_required_actions,
            "outbox_pending": len(self.reconstruction_result.pending_outbox) if self.reconstruction_result else 0,
            "reconstruction_duration_ms": round(self.metrics.reconstruction_duration_ms, 3),
            "persistence_startup_failures": self.metrics.persistence_startup_failures,
        }


def _visible_projection(projection: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value for key, value in projection.items()
        if not str(key).startswith("PERSISTENCE-TEST-")
        and not (isinstance(value, dict) and value.get("source_mode") == "PERSISTENCE_INTEGRATION_TEST")
    }


def _integration_record(value: Any) -> bool:
    if getattr(value, "incident_id", None) and str(value.incident_id).startswith("PERSISTENCE-TEST-"):
        return True
    if not isinstance(value, dict):
        return False
    if value.get("source_mode") == "PERSISTENCE_INTEGRATION_TEST":
        return True
    return any(
        str(value.get(key) or "").startswith("PERSISTENCE-TEST-")
        for key in ("incident_id", "owner_incident_id", "primary_entity", "entity_id", "event_id", "resource_key")
    )


def _validate_reconstruction(result: ReconstructionResult) -> None:
    incident_ids: set[str] = set()
    correlations: set[str] = set()
    for incident in result.active_incidents:
        incident_id = str(incident.get("incident_id") or "")
        correlation = str(incident.get("correlation_key") or "")
        if not incident_id or not correlation or incident_id in incident_ids or correlation in correlations:
            raise ProjectionIntegrityError("inconsistent active incident index")
        if int(incident.get("version", 0)) < 0:
            raise ProjectionIntegrityError("incident version regression")
        incident_ids.add(incident_id); correlations.add(correlation)

    resources: set[str] = set()
    for ownership in result.active_ownership:
        resource = str(ownership.get("resource_key") or "")
        if not resource or resource in resources:
            raise ProjectionIntegrityError("inconsistent active resource ownership")
        if ownership.get("owner_incident_id") not in incident_ids:
            raise ProjectionIntegrityError("resource ownership references missing active incident")
        if int(ownership.get("version", 0)) < 0:
            raise ProjectionIntegrityError("resource ownership version regression")
        resources.add(resource)

    for action in result.pending_actions:
        if action.incident_id not in incident_ids:
            raise ProjectionIntegrityError("non-terminal action references missing active incident")
        if action.state not in set(ActionState):
            raise ProjectionIntegrityError("impossible action state")
        if int(action.version) < 0:
            raise ProjectionIntegrityError("action version regression")

    for family in (result.pending_verifications, result.pending_recoveries):
        for record in family:
            if record.get("incident_id") not in incident_ids:
                raise ProjectionIntegrityError("pending record references missing active incident")

    for entity_id, entity in result.network_state.items():
        if str(entity.get("entity_id") or "") != str(entity_id):
            raise ProjectionIntegrityError("network projection key mismatch")
        if int(entity.get("version", 0)) < 0:
            raise ProjectionIntegrityError("network projection version regression")

    checkpoint = result.checkpoint or {}
    checkpoint_sequence = int(checkpoint.get("last_event_sequence", checkpoint.get("sequence", 0)) or 0)
    if checkpoint_sequence > result.event_sequence:
        raise ProjectionIntegrityError("checkpoint sequence exceeds event sequence")
    checkpoint_projection = checkpoint.get("projection")
    if checkpoint_projection is not None and checkpoint_sequence == result.event_sequence:
        if _visible_projection(checkpoint_projection) != result.network_state:
            raise ProjectionIntegrityError("latest checkpoint projection mismatch")


def reconstruct_platform_state(bundle: Any) -> ReconstructionResult:
    """Reconstruct only bounded current-state views in deterministic order."""
    started = time.monotonic()
    checkpoint = bundle.checkpoints.latest()
    network_state = bundle.network_state.load_all()
    from recovery_consistency import overlay_authoritative_recovery, repair_incident_recovery_projection
    active_incidents = [row for row in bundle.incidents.active() if not _integration_record(row)]
    for row in active_incidents:
        repair_incident_recovery_projection(bundle, row["incident_id"])
    active_incidents = [overlay_authoritative_recovery(bundle, row) for row in active_incidents]
    active_ownership = [row for row in bundle.resource_ownership.active() if not _integration_record(row)]
    bundle.actions.pending_or_unknown()
    bundle.actions.mark_restart_unknown()
    pending_actions = [row for row in bundle.actions.pending_or_unknown() if not _integration_record(row)]
    reconciliation = [row for row in bundle.actions.reconciliation_required() if not _integration_record(row)]
    pending_verifications = [row for row in bundle.verification.pending() if not _integration_record(row)]
    pending_recoveries = [row for row in bundle.recovery.pending() if not _integration_record(row)]
    pending_outbox = [row for row in bundle.outbox.unsent() if not _integration_record(row)]
    event_sequence = bundle.events.sequence()
    result = ReconstructionResult(
        checkpoint=copy.deepcopy(checkpoint),
        network_state=copy.deepcopy(_visible_projection(network_state)),
        active_incidents=copy.deepcopy(active_incidents),
        active_ownership=copy.deepcopy(active_ownership),
        pending_actions=copy.deepcopy(pending_actions),
        reconciliation_required=copy.deepcopy(reconciliation),
        pending_verifications=copy.deepcopy(pending_verifications),
        pending_recoveries=copy.deepcopy(pending_recoveries),
        pending_outbox=copy.deepcopy(pending_outbox),
        event_sequence=int(event_sequence),
        duration_ms=(time.monotonic() - started) * 1000,
    )
    _validate_reconstruction(result)
    return result
