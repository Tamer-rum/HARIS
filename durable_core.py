"""Offline-first durable domain core for HARIS event-sourced operations.

The event bus is deliberately not the system of record.  This module defines
repository boundaries and in-memory implementations used by the normal test
suite; a Postgres deployment can implement the same boundaries from the SQL
migration without exposing domain tables to a browser.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Iterable, Optional, Protocol

from platform_events import EventType, HarisEvent


class RepositoryUnavailable(RuntimeError): pass
class VersionConflict(RuntimeError): pass
class DuplicateEvent(RuntimeError): pass
class InvalidTransition(RuntimeError): pass
class ResourceAlreadyOwned(RuntimeError): pass
class UnknownActionOutcome(RuntimeError): pass
class ProjectionIntegrityError(RuntimeError): pass


def _is_persistence_integration_record(value: Any) -> bool:
    if getattr(value, "incident_id", None) and str(value.incident_id).startswith("PERSISTENCE-TEST-"):
        return True
    if not isinstance(value, dict): return False
    if value.get("source_mode") == "PERSISTENCE_INTEGRATION_TEST": return True
    return any(str(value.get(key) or "").startswith("PERSISTENCE-TEST-") for key in ("incident_id","owner_incident_id","primary_entity","entity_id","event_id","resource_key"))


_FORBIDDEN_DURABLE_KEYS = {
    "token", "access_token", "refresh_token", "api_key", "authorization",
    "authorization_url", "client_secret", "oauth_state",
    "consent_action_token", "oauth_code", "phone_number", "msisdn",
}


def _require_safe(value: Any) -> None:
    """Reject secret/identity transport fields before any durable write."""
    if isinstance(value, dict):
        for key, nested in value.items():
            if str(key).lower() in _FORBIDDEN_DURABLE_KEYS:
                raise ValueError("durable domain record contains a sensitive field")
            _require_safe(nested)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            _require_safe(nested)
    elif isinstance(value, str):
        if re.search(r"(?:[?&](?:code|state|token|access_token|client_secret)=|\bbearer\s+)", value, re.I):
            raise ValueError("durable domain record contains a sensitive value")
        if re.search(
            r"\b(?:access_token|refresh_token|client_secret|oauth_state|authorization_url|"
            r"consent_action_token|oauth_code|api_key|token)\s*[:=]",
            value,
            re.I,
        ):
            raise ValueError("durable domain record contains a sensitive value")
        if re.search(r"\+\d{8,15}\b", value):
            raise ValueError("durable domain record contains an unmasked phone number")


class Clock(Protocol):
    def now(self) -> float: ...


class SystemClock:
    def now(self) -> float:
        return time.time()


class IncidentState(str, Enum):
    DETECTED = "DETECTED"; EVALUATING = "EVALUATING"; PLANNED = "PLANNED"
    WARDEN_REVIEW = "WARDEN_REVIEW"; APPROVED = "APPROVED"; MITIGATING = "MITIGATING"
    VERIFYING = "VERIFYING"; RECOVERING = "RECOVERING"; RESOLVED = "RESOLVED"
    BLOCKED = "BLOCKED"; FAILED = "FAILED"; ESCALATED = "ESCALATED"; CANCELLED = "CANCELLED"


ACTIVE_INCIDENT_STATES = {
    IncidentState.DETECTED, IncidentState.EVALUATING, IncidentState.PLANNED,
    IncidentState.WARDEN_REVIEW, IncidentState.APPROVED, IncidentState.MITIGATING,
    IncidentState.VERIFYING, IncidentState.RECOVERING, IncidentState.ESCALATED,
}
_TRANSITIONS = {
    IncidentState.DETECTED: {IncidentState.EVALUATING, IncidentState.BLOCKED, IncidentState.CANCELLED},
    IncidentState.EVALUATING: {IncidentState.PLANNED, IncidentState.BLOCKED, IncidentState.FAILED, IncidentState.ESCALATED},
    IncidentState.PLANNED: {IncidentState.WARDEN_REVIEW, IncidentState.BLOCKED, IncidentState.CANCELLED},
    IncidentState.WARDEN_REVIEW: {IncidentState.APPROVED, IncidentState.BLOCKED, IncidentState.ESCALATED},
    IncidentState.APPROVED: {IncidentState.MITIGATING, IncidentState.BLOCKED, IncidentState.FAILED},
    IncidentState.MITIGATING: {IncidentState.VERIFYING, IncidentState.FAILED, IncidentState.BLOCKED},
    IncidentState.VERIFYING: {IncidentState.RECOVERING, IncidentState.RESOLVED, IncidentState.FAILED, IncidentState.ESCALATED},
    IncidentState.RECOVERING: {IncidentState.RESOLVED, IncidentState.FAILED, IncidentState.BLOCKED},
    IncidentState.ESCALATED: {IncidentState.RECOVERING, IncidentState.CANCELLED, IncidentState.BLOCKED},
}


class ActionState(str, Enum):
    PENDING = "PENDING"; READY = "READY"; SENT = "SENT"; ACKNOWLEDGED = "ACKNOWLEDGED"
    AVAILABLE = "AVAILABLE"; SUCCESS = "SUCCESS"; FAILED = "FAILED"; TIMED_OUT = "TIMED_OUT"
    ROLLBACK_REQUIRED = "ROLLBACK_REQUIRED"; ROLLED_BACK = "ROLLED_BACK"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"; RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"


class VerificationState(str, Enum):
    PENDING = "PENDING"; IMPROVED = "IMPROVED"; UNCHANGED = "UNCHANGED"
    DEGRADED = "DEGRADED"; FAILED = "FAILED"; INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


class RecoveryState(str, Enum):
    PENDING = "PENDING"; RELEASING = "RELEASING"; VERIFYING_RELEASE = "VERIFYING_RELEASE"
    COMPLETE = "COMPLETE"; PARTIAL = "PARTIAL"; FAILED = "FAILED"


@dataclass
class ActionCommand:
    incident_id: str; command_type: str; resource_key: str; device_id: Optional[str]
    plan_version: int; requested_at: float; parameters_safe: Dict[str, Any] = field(default_factory=dict)
    preconditions: Dict[str, Any] = field(default_factory=dict); command_id: str = field(default_factory=lambda: f"cmd-{uuid.uuid4().hex}")
    state: ActionState = ActionState.PENDING; provider_resource_id: Optional[str] = None
    attempt_count: int = 0; last_attempt_at: Optional[float] = None; completed_at: Optional[float] = None
    failure_reason: Optional[str] = None; version: int = 0

    def __post_init__(self) -> None:
        _require_safe(self.parameters_safe)
        _require_safe(self.preconditions)

    @property
    def idempotency_key(self) -> str:
        raw = f"{self.incident_id}|{self.command_type}|{self.resource_key}|{self.plan_version}"
        return hashlib.sha256(raw.encode()).hexdigest()


class EventRepository(Protocol):
    def append(self, event: HarisEvent) -> bool: ...
    def get(self, event_id: str) -> Optional[HarisEvent]: ...
    def lookup(self, idempotency_key: str) -> Optional[HarisEvent]: ...
    def events_after(self, sequence: int = 0) -> list[tuple[int, HarisEvent]]: ...
    def sequence(self) -> int: ...


class NetworkStateRepository(Protocol):
    def load_all(self) -> Dict[str, Dict[str, Any]]: ...
    def get(self, entity_id: str) -> Optional[Dict[str, Any]]: ...
    def save(self, entity: Dict[str, Any], expected_version: int) -> Dict[str, Any]: ...
    def save_snapshot(self, projection: Dict[str, Any], sequence: int, created_at: float) -> None: ...
    def latest_snapshot(self) -> Optional[Dict[str, Any]]: ...


class CheckpointRepository(Protocol):
    def save(self, projection: Dict[str, Any], sequence: int, created_at: float) -> Dict[str, Any]: ...
    def latest(self) -> Optional[Dict[str, Any]]: ...


class IncidentRepository(Protocol):
    def create_or_get_active(self, incident: Dict[str, Any]) -> tuple[Dict[str, Any], bool]: ...
    def get(self, incident_id: str) -> Optional[Dict[str, Any]]: ...
    def active(self) -> list[Dict[str, Any]]: ...
    def recent(self, limit: int = 50) -> list[Dict[str, Any]]: ...
    def find_active(self, correlation_key: str) -> Optional[Dict[str, Any]]: ...
    def update(self, incident: Dict[str, Any], expected_version: int) -> Dict[str, Any]: ...
    def transition(self, incident_id: str, to_state: IncidentState, *, actor: str, reason_code: str, trace_id: str, at: float) -> Dict[str, Any]: ...
    def transitions(self, incident_id: str) -> list[Dict[str, Any]]: ...


class ActionRepository(Protocol):
    def create_or_get(self, command: ActionCommand) -> tuple[ActionCommand, bool]: ...
    def get(self, command_id: str) -> Optional[ActionCommand]: ...
    def get_by_idempotency_key(self, key: str) -> Optional[ActionCommand]: ...
    def update(self, command: ActionCommand, expected_version: int) -> ActionCommand: ...
    def pending_or_unknown(self) -> list[ActionCommand]: ...
    def reconciliation_required(self) -> list[ActionCommand]: ...
    def mark_restart_unknown(self) -> list[ActionCommand]: ...
    def for_incident(self, incident_id: str) -> list[ActionCommand]: ...


class ResourceOwnershipRepository(Protocol):
    def acquire(self, ownership: Dict[str, Any]) -> Dict[str, Any]: ...
    def renew(self, resource_key: str, owner_incident_id: str, expected_version: int, lease_seconds: int) -> Dict[str, Any]: ...
    def release(self, resource_key: str, owner_incident_id: str, at: float, expected_version: int) -> Dict[str, Any]: ...
    def owned_by(self, incident_id: str) -> list[Dict[str, Any]]: ...
    def get_active(self, resource_key: str) -> Optional[Dict[str, Any]]: ...
    def active(self) -> list[Dict[str, Any]]: ...


class VerificationRepository(Protocol):
    def save(self, record: Dict[str, Any]) -> Dict[str, Any]: ...
    def get(self, verification_id: str) -> Optional[Dict[str, Any]]: ...
    def for_incident(self, incident_id: str) -> list[Dict[str, Any]]: ...
    def pending(self) -> list[Dict[str, Any]]: ...


class RecoveryRepository(Protocol):
    def save(self, record: Dict[str, Any], expected_version: Optional[int] = None) -> Dict[str, Any]: ...
    def get(self, recovery_id: str) -> Optional[Dict[str, Any]]: ...
    def for_incident(self, incident_id: str) -> Optional[Dict[str, Any]]: ...
    def pending(self) -> list[Dict[str, Any]]: ...


class OutboxRepository(Protocol):
    def append(self, event: Dict[str, Any]) -> Dict[str, Any]: ...
    def unsent(self) -> list[Dict[str, Any]]: ...
    def mark_sent(self, outbox_id: str, at: float) -> None: ...
    def claim(self, owner: str, limit: int = 25, lease_seconds: int = 30) -> list[Dict[str, Any]]: ...
    def renew(self, outbox_id: str, owner: str, claim_generation: int, lease_seconds: int) -> Dict[str, Any]: ...
    def ack(self, outbox_id: str, owner: str, claim_generation: Optional[int] = None) -> None: ...
    def fail(self, outbox_id: str, owner: str, error_safe: str, retry: bool, claim_generation: Optional[int] = None) -> None: ...


class InboxRepository(Protocol):
    def claim(self, idempotency_key: str, event_id: str, claimed_at: float) -> bool: ...


class InboundEventRepository(Protocol):
    """Atomic durability boundary for normalized inbound evidence."""

    def process(
        self,
        event: HarisEvent,
        *,
        projection: Optional[Dict[str, Any]],
        incident: Optional[Dict[str, Any]],
        transition: Optional[Dict[str, Any]],
        outbox: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]: ...


class CostLedgerRepository(Protocol):
    """Policy-cost ledger only; this is explicitly not Nokia billing."""
    def append(self, entry: Dict[str, Any]) -> Dict[str, Any]: ...
    def for_incident(self, incident_id: str) -> list[Dict[str, Any]]: ...
    def incident_total(self, incident_id: str) -> float: ...
    def day_total(self, day_bucket: str) -> float: ...


class ResourceLockManager(Protocol):
    def acquire(self, owner: str, resource_keys: Iterable[str]) -> list[str]: ...
    def release(self, owner: str, resource_keys: Iterable[str]) -> None: ...


class InMemoryEventRepository:
    def __init__(self) -> None:
        self._events: list[HarisEvent] = []; self._keys: set[str] = set()
    def append(self, event: HarisEvent) -> bool:
        _require_safe(event.payload)
        if event.event_id in {item.event_id for item in self._events} or event.idempotency_key in self._keys:
            return False
        self._events.append(copy.deepcopy(event)); self._keys.add(event.idempotency_key); return True
    def events_after(self, sequence: int = 0) -> list[tuple[int, HarisEvent]]:
        return [(index, copy.deepcopy(event)) for index, event in enumerate(self._events, 1) if index > sequence]
    def sequence(self) -> int: return len(self._events)
    def get(self, event_id: str) -> Optional[HarisEvent]:
        return next((copy.deepcopy(item) for item in self._events if item.event_id == event_id), None)
    def lookup(self, idempotency_key: str) -> Optional[HarisEvent]:
        return next((copy.deepcopy(item) for item in self._events if item.idempotency_key == idempotency_key), None)


class InMemoryNetworkStateRepository:
    def __init__(self) -> None: self._items: Dict[str, Dict[str, Any]] = {}; self._snapshot: Optional[Dict[str, Any]] = None
    def load_all(self) -> Dict[str, Dict[str, Any]]: return copy.deepcopy(self._items)
    def save(self, entity: Dict[str, Any], expected_version: int) -> Dict[str, Any]:
        current = self._items.get(entity["entity_id"]); current_version = int(current.get("version", 0)) if current else 0
        if current_version != expected_version: raise VersionConflict(f"network entity {entity['entity_id']} advanced")
        stored = copy.deepcopy(entity); stored["version"] = current_version + 1; self._items[stored["entity_id"]] = stored
        return copy.deepcopy(stored)
    def save_snapshot(self, projection: Dict[str, Any], sequence: int, created_at: float) -> None:
        self._snapshot = {"projection": copy.deepcopy(projection), "sequence": sequence, "created_at": created_at, "snapshot_version": 1}
    def latest_snapshot(self) -> Optional[Dict[str, Any]]: return copy.deepcopy(self._snapshot)
    def get(self, entity_id: str) -> Optional[Dict[str, Any]]: return copy.deepcopy(self._items.get(entity_id))


class InMemoryIncidentRepository:
    def __init__(self) -> None: self._items: Dict[str, Dict[str, Any]] = {}; self._transitions: Dict[str, list[Dict[str, Any]]] = {}
    def create_or_get_active(self, incident: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
        correlation = incident["correlation_key"]
        for existing in self._items.values():
            if existing["correlation_key"] == correlation and IncidentState(existing["state"]) in ACTIVE_INCIDENT_STATES:
                return copy.deepcopy(existing), False
        stored = copy.deepcopy(incident); stored.setdefault("version", 0); self._items[stored["incident_id"]] = stored
        self._transitions[stored["incident_id"]] = [{"incident_id": stored["incident_id"], "from_state": None, "to_state": IncidentState.DETECTED.value, "timestamp": stored["opened_at"], "actor": "SENTINEL", "reason_code": "TRIGGER_EVENT", "trace_id": stored.get("trace_id")}]
        return copy.deepcopy(stored), True
    def get(self, incident_id: str) -> Optional[Dict[str, Any]]: return copy.deepcopy(self._items.get(incident_id))
    def active(self) -> list[Dict[str, Any]]: return [copy.deepcopy(item) for item in self._items.values() if IncidentState(item["state"]) in ACTIVE_INCIDENT_STATES]
    def recent(self, limit: int = 50) -> list[Dict[str, Any]]:
        rows = sorted(
            self._items.values(),
            key=lambda item: float(item.get("updated_at") or item.get("opened_at") or 0),
            reverse=True,
        )
        return [copy.deepcopy(item) for item in rows[:max(0, int(limit))]]
    def update(self, incident: Dict[str, Any], expected_version: int) -> Dict[str, Any]:
        current = self._items.get(incident["incident_id"])
        if not current or int(current.get("version", 0)) != expected_version: raise VersionConflict(incident["incident_id"])
        if IncidentState(incident["state"]) != IncidentState(current["state"]): raise InvalidTransition("incident state changes require transition()")
        stored = copy.deepcopy(incident); stored["version"] = expected_version + 1; self._items[stored["incident_id"]] = stored; return copy.deepcopy(stored)
    def transition(self, incident_id: str, to_state: IncidentState, *, actor: str, reason_code: str, trace_id: str, at: float) -> Dict[str, Any]:
        item = self._items.get(incident_id)
        if not item: raise KeyError(incident_id)
        from_state = IncidentState(item["state"])
        if to_state not in _TRANSITIONS.get(from_state, set()): raise InvalidTransition(f"{from_state.value} -> {to_state.value}")
        item.update({"state": to_state.value, "updated_at": at, "version": int(item.get("version", 0)) + 1})
        if to_state in {IncidentState.RESOLVED, IncidentState.BLOCKED, IncidentState.FAILED, IncidentState.CANCELLED}: item["closed_at"] = at
        self._transitions[incident_id].append({"incident_id": incident_id, "from_state": from_state.value, "to_state": to_state.value, "timestamp": at, "actor": actor, "reason_code": reason_code, "trace_id": trace_id})
        return copy.deepcopy(item)
    def transitions(self, incident_id: str) -> list[Dict[str, Any]]: return copy.deepcopy(self._transitions.get(incident_id, []))
    def find_active(self, correlation_key: str) -> Optional[Dict[str, Any]]:
        return next((copy.deepcopy(item) for item in self._items.values() if item["correlation_key"] == correlation_key and IncidentState(item["state"]) in ACTIVE_INCIDENT_STATES), None)


class InMemoryActionRepository:
    def __init__(self) -> None: self._by_id: Dict[str, ActionCommand] = {}; self._by_key: Dict[str, str] = {}
    def create_or_get(self, command: ActionCommand) -> tuple[ActionCommand, bool]:
        existing_id = self._by_key.get(command.idempotency_key)
        if existing_id: return copy.deepcopy(self._by_id[existing_id]), False
        self._by_id[command.command_id] = copy.deepcopy(command); self._by_key[command.idempotency_key] = command.command_id
        return copy.deepcopy(command), True
    def pending_or_unknown(self) -> list[ActionCommand]: return [copy.deepcopy(item) for item in self._by_id.values() if item.state in {ActionState.PENDING, ActionState.READY, ActionState.SENT, ActionState.OUTCOME_UNKNOWN, ActionState.RECONCILIATION_REQUIRED}]
    def mark_restart_unknown(self) -> list[ActionCommand]:
        changed = []
        for item in self._by_id.values():
            if item.state == ActionState.SENT:
                item.state = ActionState.OUTCOME_UNKNOWN; item.failure_reason = "process_restart_before_provider_outcome"; item.version += 1; changed.append(copy.deepcopy(item))
        return changed
    def get(self, command_id: str) -> Optional[ActionCommand]: return copy.deepcopy(self._by_id.get(command_id))
    def get_by_idempotency_key(self, key: str) -> Optional[ActionCommand]:
        return copy.deepcopy(self._by_id.get(self._by_key.get(key, "")))
    def update(self, command: ActionCommand, expected_version: int) -> ActionCommand:
        current = self._by_id.get(command.command_id)
        if not current or current.version != expected_version: raise VersionConflict(command.command_id)
        stored = copy.deepcopy(command); stored.version = expected_version + 1; self._by_id[stored.command_id] = stored; return copy.deepcopy(stored)
    def reconciliation_required(self) -> list[ActionCommand]:
        return [copy.deepcopy(item) for item in self._by_id.values() if item.state in {ActionState.SENT, ActionState.OUTCOME_UNKNOWN, ActionState.RECONCILIATION_REQUIRED}]
    def for_incident(self, incident_id: str) -> list[ActionCommand]:
        return [copy.deepcopy(item) for item in self._by_id.values() if item.incident_id == incident_id]


class InMemoryResourceOwnershipRepository:
    def __init__(self) -> None: self._items: Dict[str, Dict[str, Any]] = {}
    def acquire(self, ownership: Dict[str, Any]) -> Dict[str, Any]:
        current = self._items.get(ownership["resource_key"])
        now = float(ownership.get("lease_started_at") or ownership.get("acquired_at") or time.time())
        if current and current.get("ownership_state") == "OWNED":
            if float(current.get("lease_expires_at") or float("inf")) > now:
                if current.get("owner_incident_id") != ownership["owner_incident_id"]: raise ResourceAlreadyOwned(ownership["resource_key"])
                return copy.deepcopy(current)
            stored = copy.deepcopy(ownership); stored["version"] = int(current.get("version", 0)) + 1; stored["ownership_state"] = "OWNED"; self._items[stored["resource_key"]] = stored; return copy.deepcopy(stored)
        stored = copy.deepcopy(ownership); stored.setdefault("version", 0); stored.setdefault("ownership_state", "OWNED"); self._items[stored["resource_key"]] = stored; return copy.deepcopy(stored)
    def renew(self, resource_key: str, owner_incident_id: str, expected_version: int, lease_seconds: int) -> Dict[str, Any]:
        current = self._items.get(resource_key); now = time.time()
        if not current or current.get("ownership_state") != "OWNED" or current.get("owner_incident_id") != owner_incident_id or int(current.get("version", 0)) != expected_version or float(current.get("lease_expires_at") or 0) <= now: raise ResourceAlreadyOwned(resource_key)
        current.update({"lease_expires_at": now + lease_seconds, "version": expected_version + 1}); return copy.deepcopy(current)
    def release(self, resource_key: str, owner_incident_id: str, at: float, expected_version: int) -> Dict[str, Any]:
        current = self._items.get(resource_key)
        if not current or current.get("owner_incident_id") != owner_incident_id or int(current.get("version", 0)) != expected_version: raise ResourceAlreadyOwned(resource_key)
        current.update({"ownership_state": "RELEASED", "released_at": at, "version": int(current.get("version", 0)) + 1}); return copy.deepcopy(current)
    def owned_by(self, incident_id: str) -> list[Dict[str, Any]]: return [copy.deepcopy(item) for item in self._items.values() if item.get("owner_incident_id") == incident_id and item.get("ownership_state") == "OWNED"]
    def get_active(self, resource_key: str) -> Optional[Dict[str, Any]]:
        item = self._items.get(resource_key); return copy.deepcopy(item) if item and item.get("ownership_state") == "OWNED" else None
    def active(self) -> list[Dict[str, Any]]: return [copy.deepcopy(item) for item in self._items.values() if item.get("ownership_state") == "OWNED"]


class InMemoryVerificationRepository:
    def __init__(self) -> None: self._items: Dict[str, Dict[str, Any]] = {}
    def save(self, record: Dict[str, Any]) -> Dict[str, Any]: self._items[record["verification_id"]] = copy.deepcopy(record); return copy.deepcopy(record)
    def for_incident(self, incident_id: str) -> list[Dict[str, Any]]: return [copy.deepcopy(item) for item in self._items.values() if item["incident_id"] == incident_id]
    def get(self, verification_id: str) -> Optional[Dict[str, Any]]: return copy.deepcopy(self._items.get(verification_id))
    def pending(self) -> list[Dict[str, Any]]: return [copy.deepcopy(item) for item in self._items.values() if item.get("state") == VerificationState.PENDING.value]


class InMemoryRecoveryRepository:
    def __init__(self) -> None: self._items: Dict[str, Dict[str, Any]] = {}
    def save(self, record: Dict[str, Any], expected_version: Optional[int] = None) -> Dict[str, Any]:
        current = self._items.get(record["incident_id"])
        if expected_version is None:
            expected_version = int(current.get("version", 0)) if current else -1
        if current is None:
            if expected_version not in {-1, 0}: raise VersionConflict("recovery_version_conflict")
            stored = copy.deepcopy(record); stored["version"] = 0
        else:
            if expected_version != int(current.get("version", 0)):
                raise VersionConflict("recovery_version_conflict")
            if current.get("state") == record.get("state"):
                return copy.deepcopy(current)
            stored = copy.deepcopy(record); stored["version"] = expected_version + 1
        self._items[stored["incident_id"]] = stored
        return copy.deepcopy(stored)
    def for_incident(self, incident_id: str) -> Optional[Dict[str, Any]]: return copy.deepcopy(self._items.get(incident_id))
    def get(self, recovery_id: str) -> Optional[Dict[str, Any]]:
        return next((copy.deepcopy(item) for item in self._items.values() if item.get("recovery_id") == recovery_id), None)
    def pending(self) -> list[Dict[str, Any]]: return [copy.deepcopy(item) for item in self._items.values() if item.get("state") in {RecoveryState.PENDING.value, RecoveryState.RELEASING.value, RecoveryState.VERIFYING_RELEASE.value}]


class InMemoryOutboxRepository:
    def __init__(self) -> None: self._items: Dict[str, Dict[str, Any]] = {}
    def append(self, event: Dict[str, Any]) -> Dict[str, Any]:
        _require_safe(event)
        stored = copy.deepcopy(event); stored.setdefault("outbox_id", f"out-{uuid.uuid4().hex}")
        existing = self._items.get(stored["outbox_id"])
        if existing is not None:
            return copy.deepcopy(existing)
        stored.setdefault("sent_at", None); self._items[stored["outbox_id"]] = stored; return copy.deepcopy(stored)
    def unsent(self) -> list[Dict[str, Any]]: return [copy.deepcopy(item) for item in self._items.values() if item.get("sent_at") is None]
    def mark_sent(self, outbox_id: str, at: float) -> None: self._items[outbox_id]["sent_at"] = at
    def ack(self, outbox_id: str, owner: str, claim_generation: Optional[int] = None) -> None:
        item = self._items[outbox_id]
        generation = int(item.get("claim_generation", 0)) if claim_generation is None else claim_generation
        if item.get("claim_owner") != owner or int(item.get("claim_generation", 0)) != generation: raise ResourceAlreadyOwned(outbox_id)
        item.update({"state": "SENT", "sent_at": time.time(), "claim_expires_at": None})
    def claim(self, owner: str, limit: int = 25, lease_seconds: int = 30) -> list[Dict[str, Any]]:
        now = time.time(); rows = []
        for item in sorted(self._items.values(), key=lambda row: row.get("created_at", 0)):
            if len(rows) >= limit: break
            if item.get("state", "PENDING") in {"PENDING", "FAILED"} or (item.get("state") == "CLAIMED" and float(item.get("claim_expires_at") or 0) < now):
                item.update({"state": "CLAIMED", "claim_owner": owner, "claimed_at": now, "claim_expires_at": now + lease_seconds, "claim_generation": int(item.get("claim_generation", 0)) + 1, "attempt_count": int(item.get("attempt_count", 0)) + 1, "last_attempt_at": now}); rows.append(copy.deepcopy(item))
        return rows
    def renew(self, outbox_id: str, owner: str, claim_generation: int, lease_seconds: int) -> Dict[str, Any]:
        item = self._items[outbox_id]; now = time.time()
        if item.get("state") != "CLAIMED" or item.get("claim_owner") != owner or int(item.get("claim_generation", 0)) != claim_generation or float(item.get("claim_expires_at") or 0) <= now: raise ResourceAlreadyOwned(outbox_id)
        item["claim_expires_at"] = now + lease_seconds; return copy.deepcopy(item)
    def fail(self, outbox_id: str, owner: str, error_safe: str, retry: bool, claim_generation: Optional[int] = None) -> None:
        _require_safe(error_safe)
        item = self._items[outbox_id]
        generation = int(item.get("claim_generation", 0)) if claim_generation is None else claim_generation
        if item.get("claim_owner") != owner or int(item.get("claim_generation", 0)) != generation: raise ResourceAlreadyOwned(outbox_id)
        item.update({"state": "PENDING" if retry else "FAILED", "last_error_safe": error_safe[:256], "claim_owner": None, "claim_expires_at": None})


class InMemoryInboxRepository:
    def __init__(self) -> None:
        self._claimed: Dict[str, Dict[str, Any]] = {}

    def claim(self, idempotency_key: str, event_id: str, claimed_at: float) -> bool:
        if idempotency_key in self._claimed:
            return False
        self._claimed[idempotency_key] = {"event_id": event_id, "claimed_at": claimed_at}
        return True


class InMemoryCostLedgerRepository:
    def __init__(self) -> None:
        self._entries: list[Dict[str, Any]] = []

    def append(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        _require_safe(entry)
        stored = copy.deepcopy(entry)
        stored.setdefault("ledger_id", f"cost-{uuid.uuid4().hex}")
        stored.setdefault("cost_basis", "HARIS_POLICY_COST_MODEL")
        existing = next((item for item in self._entries if item.get("ledger_id") == stored["ledger_id"]), None)
        if existing is not None:
            if existing != stored:
                raise VersionConflict("policy cost ledger identity conflict")
            return copy.deepcopy(existing)
        self._entries.append(stored)
        return copy.deepcopy(stored)

    def for_incident(self, incident_id: str) -> list[Dict[str, Any]]:
        return [copy.deepcopy(item) for item in self._entries if item.get("incident_id") == incident_id]
    def incident_total(self, incident_id: str) -> float: return sum(float(item.get("estimated_policy_cost") or 0) for item in self._entries if item.get("incident_id") == incident_id)
    def day_total(self, day_bucket: str) -> float: return sum(float(item.get("estimated_policy_cost") or 0) for item in self._entries if item.get("day_bucket") == day_bucket)


class InMemoryCheckpointRepository:
    def __init__(self) -> None: self._latest: Optional[Dict[str, Any]] = None
    def save(self, projection: Dict[str, Any], sequence: int, created_at: float) -> Dict[str, Any]:
        _require_safe(projection); self._latest = {"snapshot_version": 1, "last_event_sequence": sequence, "projection": copy.deepcopy(projection), "created_at": created_at}; return copy.deepcopy(self._latest)
    def latest(self) -> Optional[Dict[str, Any]]: return copy.deepcopy(self._latest)


class InMemoryInboundEventRepository:
    """Fixture/test equivalent of the production atomic inbound RPC.

    Production correctness is database-enforced.  The lock and rollback
    snapshot here exist only so the in-memory implementation has equivalent
    transactional behavior during deterministic offline tests.
    """

    def __init__(self, bundle: "InMemoryRepositoryBundle") -> None:
        self.bundle = bundle
        self._lock = threading.RLock()

    def process(
        self,
        event: HarisEvent,
        *,
        projection: Optional[Dict[str, Any]],
        incident: Optional[Dict[str, Any]],
        transition: Optional[Dict[str, Any]],
        outbox: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        values = (event.model_dump(mode="json"), projection, incident, transition, outbox)
        _require_safe(values)
        repositories = (
            self.bundle.events,
            self.bundle.network_state,
            self.bundle.incidents,
            self.bundle.inbox,
            self.bundle.outbox,
        )
        with self._lock:
            snapshots = [copy.deepcopy(repository.__dict__) for repository in repositories]
            try:
                if not self.bundle.inbox.claim(event.idempotency_key, event.event_id, time.time()):
                    return {"status": "duplicate", "event_id": event.event_id}
                if not self.bundle.events.append(event):
                    return {"status": "duplicate", "event_id": event.event_id}
                stored_projection = None
                if projection is not None:
                    value = copy.deepcopy(projection)
                    expected_version = int(value.pop("expected_version", 0))
                    stored_projection = self.bundle.network_state.save(value, expected_version)
                stored_incident = None
                if incident is not None:
                    stored_incident, created = self.bundle.incidents.create_or_get_active(incident)
                    stored_incident["_created"] = created
                if transition is not None:
                    self.bundle.incidents.transition(
                        transition["incident_id"],
                        IncidentState(transition["to_state"]),
                        actor=transition["actor"],
                        reason_code=transition["reason_code"],
                        trace_id=transition["trace_id"],
                        at=float(transition["occurred_at"]),
                    )
                if outbox is not None:
                    self.bundle.outbox.append(outbox)
                return {
                    "status": "accepted",
                    "event": {"inserted": True, "event_id": event.event_id},
                    "projection": stored_projection,
                    "incident": stored_incident,
                }
            except Exception:
                for repository, snapshot in zip(repositories, snapshots):
                    repository.__dict__.clear()
                    repository.__dict__.update(snapshot)
                raise


class InMemoryRepositoryBundle:
    def __init__(self) -> None:
        self.events=InMemoryEventRepository(); self.network_state=InMemoryNetworkStateRepository(); self.incidents=InMemoryIncidentRepository(); self.actions=InMemoryActionRepository(); self.resource_ownership=InMemoryResourceOwnershipRepository(); self.verification=InMemoryVerificationRepository(); self.recovery=InMemoryRecoveryRepository(); self.inbox=InMemoryInboxRepository(); self.outbox=InMemoryOutboxRepository(); self.checkpoints=InMemoryCheckpointRepository(); self.cost_ledger=InMemoryCostLedgerRepository(); self.resource_locks=InMemoryResourceLockManager(); self.inbound=InMemoryInboundEventRepository(self)


class InMemoryResourceLockManager:
    def __init__(self) -> None: self._owners: Dict[str, str] = {}
    def acquire(self, owner: str, resource_keys: Iterable[str]) -> list[str]:
        keys = sorted(set(resource_keys))
        conflict = next((key for key in keys if key in self._owners and self._owners[key] != owner), None)
        if conflict: raise ResourceAlreadyOwned(conflict)
        for key in keys: self._owners[key] = owner
        return keys
    def release(self, owner: str, resource_keys: Iterable[str]) -> None:
        for key in sorted(set(resource_keys)):
            if self._owners.get(key) == owner: self._owners.pop(key, None)


class PostgresAdvisoryLockManager:
    """Postgres-compatible lock adapter with injected execution.

    The application supplies a transaction-bound executor in production.  The
    adapter itself opens no connection and is therefore safe in offline tests.
    """
    def __init__(self, execute: Callable[[str, tuple[Any, ...]], Any]) -> None:
        self._execute = execute

    @staticmethod
    def _lock_id(resource_key: str) -> int:
        return int(hashlib.sha256(resource_key.encode()).hexdigest()[:15], 16)

    def acquire(self, owner: str, resource_keys: Iterable[str]) -> list[str]:
        del owner  # PostgreSQL transaction scope owns the advisory lock.
        keys = sorted(set(resource_keys))
        for key in keys:
            self._execute("SELECT pg_advisory_xact_lock(%s)", (self._lock_id(key),))
        return keys

    def release(self, owner: str, resource_keys: Iterable[str]) -> None:
        # xact locks release on commit/rollback; explicit release is unsafe.
        del owner, resource_keys


class PostgresRepositoryAdapter:
    """Offline-safe contract shell for the SQL migration's server-side adapter.

    It deliberately takes an injected transactional executor.  No Supabase SDK,
    key, connection, or remote call is constructed at import time.  Production
    wiring belongs behind this interface and must use backend service-role
    credentials only.
    """
    def __init__(self, execute: Callable[[str, tuple[Any, ...]], Any]) -> None:
        self._execute = execute

    def append_event(self, event: HarisEvent) -> Any:
        _require_safe(event.payload)
        payload = event.model_dump(mode="json")
        payload["idempotency_key"] = event.idempotency_key
        return self._execute(
            "SELECT haris_append_domain_event(%s::jsonb)",
            (json.dumps(payload, sort_keys=True),),
        )


class DurablePlatformCore:
    """Application service that reconstructs only persisted, safe facts."""
    def __init__(self, *, events: Optional[EventRepository] = None, network: Optional[NetworkStateRepository] = None, incidents: Optional[IncidentRepository] = None, actions: Optional[ActionRepository] = None, ownership: Optional[ResourceOwnershipRepository] = None, verifications: Optional[VerificationRepository] = None, recoveries: Optional[RecoveryRepository] = None, outbox: Optional[OutboxRepository] = None, inbox: Optional[InboxRepository] = None, checkpoints: Optional[CheckpointRepository] = None, cost_ledger: Optional[CostLedgerRepository] = None, clock: Optional[Clock] = None) -> None:
        self.events = events or InMemoryEventRepository(); self.network = network or InMemoryNetworkStateRepository(); self.incidents = incidents or InMemoryIncidentRepository(); self.actions = actions or InMemoryActionRepository(); self.ownership = ownership or InMemoryResourceOwnershipRepository(); self.verifications = verifications or InMemoryVerificationRepository(); self.recoveries = recoveries or InMemoryRecoveryRepository(); self.outbox = outbox or InMemoryOutboxRepository(); self.inbox = inbox or InMemoryInboxRepository(); self.checkpoints = checkpoints; self.cost_ledger = cost_ledger or InMemoryCostLedgerRepository(); self.clock = clock or SystemClock(); self.readiness = "STARTING"; self._projection: Dict[str, Dict[str, Any]] = {}; self._reconstructed_sequence = 0; self._reconstruction = None

    def reconstruct(self) -> None:
        """Restore durable indexes before workers are permitted to operate."""
        self.readiness = "RECONSTRUCTING"
        self._projection = self.network.load_all()
        snapshot = self.network.latest_snapshot()
        self._reconstructed_sequence = int(snapshot.get("sequence", 0)) if snapshot else 0
        # A command in SENT is deliberately *not* retried: provider outcome is
        # unknowable after a crash until a later reconciliation step.
        self.actions.mark_restart_unknown()
        self.readiness = "READY"

    def restore_reconstruction(self, result: Any) -> None:
        """Install already-validated startup views without rereading storage."""
        self.readiness = "RECONSTRUCTING"
        self._projection = copy.deepcopy(result.network_state)
        self._reconstructed_sequence = int(result.event_sequence)
        self._reconstruction = result
        self.readiness = "READY"

    def apply_committed_projection(self, projection: Optional[Dict[str, Any]]) -> None:
        """Refresh the process-local view only after a durable commit."""
        if projection is None:
            return
        entity_id = str(projection.get("entity_id") or "")
        if not entity_id:
            raise ProjectionIntegrityError("committed projection has no entity identity")
        self._projection[entity_id] = copy.deepcopy(projection)

    def append_event(self, event: HarisEvent) -> bool:
        if not self.inbox.claim(event.idempotency_key, event.event_id, self.clock.now()):
            return False
        accepted = self.events.append(event)
        if not accepted: return False
        self.outbox.append({"event_id": event.event_id, "event_type": event.event_type.value, "trace_id": event.trace_id, "created_at": self.clock.now()})
        self._project_event(event)
        return True

    def _project_event(self, event: HarisEvent) -> bool:
        current = self._projection.get(event.entity_id)
        event_type = event.event_type
        observed_key = {
            EventType.NETWORK_CONGESTION_CHANGED: "raw_congestion_observed_at",
            EventType.DEVICE_REACHABILITY_CHANGED: "reachability_observed_at",
            EventType.DEVICE_LOCATION_UPDATED: "location_observed_at",
        }.get(event_type)
        if observed_key and current and float(event.source_timestamp) < float(current.get(observed_key) or 0):
            # The event stays durable, even though it cannot roll back the
            # latest projection for that evidence channel.
            return False
        # Projection timestamps follow the durable event, not wall-clock replay
        # time, so rebuilding later is deterministic.
        now = float(event.created_at); expected = int(current.get("version", 0)) if current else 0
        item = copy.deepcopy(current or {"entity_id": event.entity_id, "entity_type": event.entity_type, "mapping_source": "HARIS_CONFIGURED_LOGICAL_CELL_MAPPING", "active_incident_ids": []})
        update = {"provenance": event.provenance.value, "freshness": "FRESH", "last_source_change_at": now, "updated_at": now}
        if event_type == EventType.NETWORK_CONGESTION_CHANGED:
            update.update({"raw_congestion": event.payload.get("congestion_level"), "raw_congestion_observed_at": event.source_timestamp, "haris_operational_state": "INCIDENT_OPEN" if event.payload.get("congestion_level") == "High" else "WATCHING" if event.payload.get("congestion_level") == "Medium" else "STABLE", "last_operational_change_at": now})
        elif event_type == EventType.DEVICE_REACHABILITY_CHANGED:
            update.update({"reachability_summary": copy.deepcopy(event.payload), "reachability_observed_at": event.source_timestamp})
        elif event_type == EventType.DEVICE_LOCATION_UPDATED:
            update.update({"location_summary": copy.deepcopy(event.payload), "location_observed_at": event.source_timestamp})
        item.update(update)
        stored = self.network.save(item, expected); self._projection[event.entity_id] = stored
        if event_type == EventType.NETWORK_CONGESTION_CHANGED and event.payload.get("congestion_level") == "High": self._open_incident(event, stored)
        return True

    def _open_incident(self, event: HarisEvent, entity: Dict[str, Any]) -> Dict[str, Any]:
        now = float(event.created_at)
        # Incident id is deterministic for a trigger, so event replay produces
        # the same projection instead of fabricating a second incident.
        stable = hashlib.sha256(f"{event.correlation_key}|{event.event_id}".encode()).hexdigest()[:12]
        incident = {"incident_id": f"inc-{stable}", "correlation_key": event.correlation_key, "primary_entity": event.entity_id, "affected_entities": [event.entity_id], "affected_devices": [], "trigger_event_id": event.event_id, "trigger_provenance": event.provenance.value, "trigger_source_timestamp": event.source_timestamp, "opened_at": now, "updated_at": now, "severity": "critical", "priority": "P1", "state": IncidentState.DETECTED.value, "plan_version": 0, "warden_decision": None, "verification_state": VerificationState.PENDING.value, "recovery_state": RecoveryState.PENDING.value, "outcome": None, "closed_at": None, "version": 0, "trace_id": event.trace_id}
        stored, created = self.incidents.create_or_get_active(incident)
        if created:
            entity = copy.deepcopy(entity); entity["active_incident_ids"] = sorted(set(entity.get("active_incident_ids", []) + [stored["incident_id"]])); self._projection[event.entity_id] = self.network.save(entity, int(entity["version"]))
        return stored

    def queue_action(self, command: ActionCommand) -> ActionCommand: return self.actions.create_or_get(command)[0]
    def acquire_resource(self, ownership: Dict[str, Any]) -> Dict[str, Any]: return self.ownership.acquire(ownership)
    def record_verification(self, record: Dict[str, Any]) -> Dict[str, Any]: return self.verifications.save(record)
    def record_recovery(self, record: Dict[str, Any]) -> Dict[str, Any]:
        from recovery_consistency import save_recovery_and_repair
        return save_recovery_and_repair(self, record)
    def record_policy_cost(self, entry: Dict[str, Any]) -> Dict[str, Any]: return self.cost_ledger.append(entry)
    def snapshot(self) -> Dict[str, Any]:
        incidents = [item for item in self.incidents.active() if not _is_persistence_integration_record(item)]
        actions = [item for item in self.actions.pending_or_unknown() if not _is_persistence_integration_record(item)]
        projection = {key:value for key,value in self._projection.items() if not _is_persistence_integration_record(value) and not str(key).startswith("PERSISTENCE-TEST-")}
        return {"platform_status": self.readiness, "state_version": self.events.sequence(), "network_state": copy.deepcopy(projection), "active_incidents": incidents, "actions": [vars(item) for item in actions], "source": "DURABLE_REPOSITORY", "generated_at": self.clock.now()}
    def checkpoint(self) -> None:
        sequence = self.events.sequence(); created_at = self.clock.now()
        if self.checkpoints is not None: self.checkpoints.save(self._projection, sequence, created_at)
        else: self.network.save_snapshot(self._projection, sequence, created_at)
    def replay_projection(self) -> Dict[str, Dict[str, Any]]:
        replay = DurablePlatformCore(events=InMemoryEventRepository(), network=InMemoryNetworkStateRepository(), clock=self.clock)
        replay.reconstruct()
        for _, event in self.events.events_after(0): replay.append_event(event)
        return replay._projection
    def verify_projection_integrity(self) -> bool:
        # Compare replay with the repository projection, not a stale in-process
        # cache.  A database-side/manual corruption must be detected too.
        if self.replay_projection() != self.network.load_all():
            raise ProjectionIntegrityError("durable projection does not match replay")
        return True
