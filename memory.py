from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol

from pydantic import BaseModel, Field

from config import AppSettings, DevicePolicy, get_settings

logger = logging.getLogger("haris.memory")


class IncidentMemory(BaseModel):
    incident_id: str
    summary: str
    storm_type: str
    peak_congestion_level: str
    peak_confidence_level: int
    affected_cells: List[str]
    affected_devices: List[str]
    actions: List[str]
    executed_actions: List[str]
    outcome: str
    cycle_id: Optional[str] = None
    mode: Optional[str] = None
    checkpoint_id: Optional[str] = None
    checkpoint_type: str = "incident_checkpoint"
    checkpoint_ordinal: int = 0
    audit: Dict[str, Any] = Field(default_factory=dict)
    previous_hash: Optional[str] = None
    record_hash: Optional[str] = None
    verification: Dict[str, Any] = Field(default_factory=dict)
    rollback: Dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    completed_at: Optional[str] = None


class HistoryRepository(Protocol):
    """Append-only durable history boundary; implementations never update rows."""

    def load_records(self) -> List[Dict[str, Any]]: ...

    def append_record(self, record: Dict[str, Any]) -> Dict[str, Any]: ...


class HistoryTailConflict(RuntimeError):
    """A concurrent durable append advanced the chain tail before this write."""


class HistoryCheckpointConflict(RuntimeError):
    """A checkpoint id exists but does not match the requested evidence."""


class SupabaseHistoryRepository:
    """Lazy server-only RPC repository for append-only audit history."""

    table_name = "haris_audit_records"

    def __init__(self, url: str, key: str):
        self._url = url
        self._key = key
        self._client: Any = None

    def _get_client(self) -> Any:
        if self._client is None:
            from supabase import create_client
            self._client = create_client(self._url, self._key)
        return self._client

    def load_records(self) -> List[Dict[str, Any]]:
        response = self._get_client().rpc("read_haris_audit_records").execute()
        return [item["record"] for item in (response.data or []) if isinstance(item.get("record"), dict)]

    def append_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        # The SECURITY DEFINER RPC serializes the tail check and INSERT. It
        # never performs an update/upsert and verifies duplicate checkpoints.
        try:
            response = self._get_client().rpc("append_haris_audit_record", {
                "p_checkpoint_id": record["checkpoint_id"],
                "p_cycle_id": record.get("cycle_id"),
                "p_incident_id": record.get("incident_id"),
                "p_created_at": record.get("created_at"),
                "p_previous_hash": record.get("previous_hash"),
                "p_record_hash": record["record_hash"],
                "p_record": record,
            }).execute()
        except Exception as exc:
            message = str(exc)
            if "haris_audit_tail_conflict" in message:
                raise HistoryTailConflict(message) from exc
            if "haris_audit_checkpoint_conflict" in message:
                raise HistoryCheckpointConflict(message) from exc
            raise
        data = response.data
        if isinstance(data, list):
            data = data[0] if data else None
        if not isinstance(data, dict) or not isinstance(data.get("record"), dict):
            raise RuntimeError("durable_history_rpc_invalid_response")
        return data["record"]


class SqliteHistoryRepository:
    """Append-only adapter for tests and local development, with no network I/O."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)

    def _ensure_schema(self) -> None:
        connection = self._connect()
        try:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS haris_audit_records ("
                "sequence INTEGER PRIMARY KEY AUTOINCREMENT, checkpoint_id TEXT NOT NULL UNIQUE, cycle_id TEXT, incident_id TEXT, "
                "created_at TEXT NOT NULL, previous_hash TEXT, record_hash TEXT UNIQUE, record TEXT NOT NULL)"
            )
            connection.execute("CREATE INDEX IF NOT EXISTS idx_haris_audit_cycle ON haris_audit_records(cycle_id)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_haris_audit_incident ON haris_audit_records(incident_id)")
            connection.commit()
        finally:
            connection.close()

    def load_records(self) -> List[Dict[str, Any]]:
        connection = self._connect()
        try:
            rows = connection.execute("SELECT record FROM haris_audit_records ORDER BY sequence ASC").fetchall()
        finally:
            connection.close()
        return [json.loads(row[0]) for row in rows]

    def append_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT record FROM haris_audit_records WHERE checkpoint_id = ?", (record["checkpoint_id"],)
            ).fetchone()
            if existing:
                stored = json.loads(existing[0])
                if stored != record:
                    raise HistoryCheckpointConflict("haris_audit_checkpoint_conflict")
                connection.commit()
                return stored
            tail = connection.execute(
                "SELECT record_hash FROM haris_audit_records ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            if (tail[0] if tail else None) != record.get("previous_hash"):
                raise HistoryTailConflict("haris_audit_tail_conflict")
            connection.execute(
                "INSERT INTO haris_audit_records "
                "(checkpoint_id, cycle_id, incident_id, created_at, previous_hash, record_hash, record) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (record["checkpoint_id"], record.get("cycle_id"), record.get("incident_id"), record.get("created_at"),
                 record.get("previous_hash"), record.get("record_hash"),
                 json.dumps(record, sort_keys=True, separators=(",", ":"), default=str)),
            )
            connection.commit()
            return record
        finally:
            connection.close()


_SECRET_KEY_PARTS = (
    "oauth_state", "authorization_code", "authorization_url", "consent_action", "workflow_session",
    "access_token", "refresh_token", "id_token", "api_token", "api_key", "apikey", "x_api_key",
    "client_secret", "notification_auth_token", "bearer", "authorization", "secret", "password",
)
_PHONE_KEY_PARTS = ("phone_number", "phone", "msisdn", "login_hint")
_OAUTH_CONTEXT_PARTS = ("oauth", "authorization", "callback", "consent", "verification")


def _mask_phone(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value))
    return f"***{digits[-4:]}" if len(digits) >= 4 else "***"


def _sanitize_free_text(value: str) -> str:
    """Redact credentials/phone data embedded in useful trace prose."""
    value = re.sub(r"\bBearer\s+[A-Za-z0-9._~+/=-]+", "Bearer [REDACTED]", value, flags=re.I)
    value = re.sub(
        r"([?&\s](?:code|state|access_token|refresh_token|id_token|token|api_key|apikey|client_secret|login_hint)=)[^&#\s]+",
        r"\1[REDACTED]", value, flags=re.I,
    )
    # Telephone-like values require a leading +, except for the documented
    # Nokia simulator MSISDN range. This preserves normal cell/tower metrics.
    value = re.sub(r"(?<!\*)\+\d{8,15}\b", lambda match: _mask_phone(match.group(0)), value)
    return re.sub(r"(?<![\d*])9999999\d{4}(?!\d)", lambda match: _mask_phone(match.group(0)), value)


def _safe_history_value(value: Any, key: str = "", context: tuple[str, ...] = ()) -> Any:
    """Context-aware, recursive sanitization before durable persistence."""
    lowered = key.lower()
    next_context = context + (lowered,)
    oauth_context = any(any(part in item for part in _OAUTH_CONTEXT_PARTS) for item in context)
    if any(part in lowered for part in _SECRET_KEY_PARTS) or lowered in {"token", "authorization"}:
        return "[REDACTED]"
    if lowered in {"state", "code"} and oauth_context:
        return "[REDACTED]"
    if any(part in lowered for part in _PHONE_KEY_PARTS):
        return _mask_phone(value)
    if isinstance(value, dict):
        return {str(name): _safe_history_value(item, str(name), next_context) for name, item in value.items()}
    if isinstance(value, list):
        return [_safe_history_value(item, key, context) for item in value]
    if isinstance(value, str):
        return _sanitize_free_text(value)
    return value


class MemoryStore:
    """Process-local active memory plus lazily loaded durable audit history.

    Active graph/dispatch state remains process-local. Completed incidents and
    callback checkpoints use Supabase/Postgres when configured.
    """

    def __init__(self, settings: Optional[AppSettings] = None, history_repository: Optional[HistoryRepository] = None):
        self.settings = settings or get_settings()
        self.local_file = Path(".haris_memory.json")
        self._incidents: List[IncidentMemory] = []
        self._policies: Dict[str, DevicePolicy] = {}
        self._history_loaded = False
        self._persistence_error: Optional[str] = None
        self._history_repository = history_repository or self._make_history_repository()
        self._load_local_policies()

    def _make_history_repository(self) -> Optional[HistoryRepository]:
        if not self.settings.has_durable_history:
            return None
        # Do not construct an SDK client yet: web-server startup stays local.
        return SupabaseHistoryRepository(self.settings.supabase_url, self.settings.supabase_key.get_secret_value())

    def _load_local_policies(self) -> None:
        if not self.local_file.exists():
            return
        try:
            raw = json.loads(self.local_file.read_text(encoding="utf-8"))
            self._policies = {key: DevicePolicy(**value) for key, value in raw.get("policies", {}).items()}
            if self._history_repository is None:
                self._incidents = [IncidentMemory(**item) for item in raw.get("incidents", [])]
                self._history_loaded = True
        except Exception:
            logger.warning("Local fallback history could not be loaded")

    def _save_local_policies(self) -> None:
        payload: Dict[str, Any] = {"policies": {key: policy.model_dump() for key, policy in self._policies.items()}}
        if self._history_repository is None:
            payload["incidents"] = [item.model_dump() for item in self._incidents[-200:]]
        self.local_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _ensure_history_loaded(self) -> None:
        if self._history_loaded:
            return
        self._history_loaded = True
        if self._history_repository is None:
            return
        try:
            self._incidents = [IncidentMemory(**record) for record in self._history_repository.load_records()]
            self._persistence_error = None
        except Exception:
            self._persistence_error = "history_load_failed"
            logger.warning("Durable history load failed; active safety controls remain unchanged")

    @property
    def persistence_status(self) -> Dict[str, Any]:
        if self._history_repository is None:
            return {"backend": "memory", "durable": False, "available": True, "status": "PROCESS_LOCAL"}
        backend = "supabase" if isinstance(self._history_repository, SupabaseHistoryRepository) else "sqlite"
        if self._persistence_error:
            return {
                "backend": backend, "durable": False, "available": False,
                "status": "UNAVAILABLE", "reason": self._persistence_error,
            }
        return {"backend": backend, "durable": True, "available": True, "status": "DURABLE"}

    @staticmethod
    def _canonical_hash(record: IncidentMemory, previous: Optional[str]) -> str:
        payload = record.model_dump(exclude={"previous_hash", "record_hash"})
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256((canonical + (previous or "")).encode("utf-8")).hexdigest()

    @staticmethod
    def _checkpoint_id(record: IncidentMemory) -> str:
        """Stable non-secret identity for one logical audit checkpoint."""
        material = "|".join((
            record.cycle_id or record.incident_id,
            record.incident_id,
            record.checkpoint_type,
            str(record.checkpoint_ordinal),
        ))
        return f"checkpoint-{hashlib.sha256(material.encode('utf-8')).hexdigest()[:32]}"

    @staticmethod
    def _terminal(outcome: str) -> bool:
        return outcome.lower() not in {"identity_verification_pending", "waiting_for_identity_verification", "pending"}

    async def remember_incident(self, incident: IncidentMemory) -> None:
        self._ensure_history_loaded()
        # Freeze and sanitize before hashing. Later graph changes cannot mutate
        # durable evidence or introduce credentials into it.
        safe_incident = IncidentMemory(**_safe_history_value(incident.model_dump()))
        checkpoint_id = safe_incident.checkpoint_id or self._checkpoint_id(safe_incident)
        safe_incident = safe_incident.model_copy(update={"checkpoint_id": checkpoint_id})
        existing = next((item for item in self._incidents if item.checkpoint_id == checkpoint_id), None)
        if existing:
            return
        for attempt in range(3):
            previous = self._incidents[-1].record_hash if self._incidents else None
            chained = safe_incident.model_copy(update={"previous_hash": previous})
            chained = chained.model_copy(update={"record_hash": self._canonical_hash(chained, previous)})
            if self._history_repository is None:
                self._incidents.append(chained)
                self._save_local_policies()
                return
            try:
                stored = IncidentMemory(**self._history_repository.append_record(chained.model_dump()))
                if not any(item.checkpoint_id == stored.checkpoint_id for item in self._incidents):
                    self._incidents.append(stored)
                self._persistence_error = None
                return
            except HistoryTailConflict:
                # A second writer won the serialized database append. Reload the
                # immutable tail then recompute this same checkpoint's hash.
                self._incidents = [IncidentMemory(**record) for record in self._history_repository.load_records()]
                if any(item.checkpoint_id == checkpoint_id for item in self._incidents):
                    return
            except HistoryCheckpointConflict:
                self._persistence_error = "checkpoint_conflict"
                logger.warning("Durable history checkpoint conflict; audit was not marked as saved")
                return
            except Exception:
                self._persistence_error = "history_write_failed"
                logger.warning("Durable history write failed; audit was not marked as saved")
                return
        self._persistence_error = "history_tail_conflict"
        logger.warning("Durable history append could not acquire a stable chain tail")

    async def set_policy(self, policy: DevicePolicy) -> None:
        self._policies[policy.device_id] = policy
        self._save_local_policies()

    async def get_policy(self, device_id: str) -> DevicePolicy:
        if device_id in self._policies:
            return self._policies[device_id]
        default_tier = 1 if device_id in {"ambulance-01", "scada-01", "pipeline-01", "dispatch-01"} else 3
        policy = DevicePolicy(device_id=device_id, mission_tier=default_tier)
        await self.set_policy(policy)
        return policy

    async def search_incidents(self, query: str, limit: int = 5) -> List[IncidentMemory]:
        self._ensure_history_loaded()
        terms = {term.lower() for term in query.split() if len(term) > 2}
        scored = []
        for incident in self._incidents:
            text = f"{incident.summary} {incident.storm_type} {incident.outcome}".lower()
            scored.append((sum(term in text for term in terms), incident))
        scored.sort(key=lambda item: item[0], reverse=True)
        return [item for score, item in scored[:limit] if score > 0]

    def count(self) -> int:
        self._ensure_history_loaded()
        return len(self._incidents)

    def recent_incidents(self, limit: int = 50) -> List[IncidentMemory]:
        self._ensure_history_loaded()
        return list(reversed(self._incidents[-limit:]))

    @staticmethod
    def normalized_view(record: IncidentMemory | Dict[str, Any]) -> Dict[str, Any]:
        raw = record.model_dump() if isinstance(record, IncidentMemory) else dict(record)
        audit = raw.get("audit") if isinstance(raw.get("audit"), dict) else {}
        return {
            "cycle_id": raw.get("cycle_id") or "N/A", "created_at": raw.get("created_at") or "N/A", "completed_at": raw.get("completed_at"),
            "mode": raw.get("mode") or "N/A", "outcome": raw.get("outcome") or "N/A",
            "affected_cells": raw.get("affected_cells") or [], "affected_devices": raw.get("affected_devices") or [],
            "prediction": audit.get("prediction") or None, "environment": audit.get("environment") or None,
            "plan": audit.get("plan") or None, "warden": audit.get("warden") or None,
            "execution": audit.get("execution") or None, "trusted_dispatch": audit.get("trusted_dispatch") or None,
            "dispatch_history": audit.get("dispatch_history") or [], "verification": audit.get("verification") or raw.get("verification") or None,
            "rollback": audit.get("rollback") or raw.get("rollback") or None, "trace": audit.get("trace") or None,
            "events": audit.get("events") or [], "previous_hash": raw.get("previous_hash"), "record_hash": raw.get("record_hash"),
        }

    def get_incident(self, cycle_or_incident_id: str) -> Optional[IncidentMemory]:
        self._ensure_history_loaded()
        return next((item for item in reversed(self._incidents) if item.cycle_id == cycle_or_incident_id or item.incident_id == cycle_or_incident_id), None)

    def verify_audit_chain(self) -> Dict[str, Any]:
        self._ensure_history_loaded()
        persistence = {"persistence": self.persistence_status} if self._history_repository is not None or self._persistence_error else {}
        previous = None
        for index, record in enumerate(self._incidents):
            if not record.record_hash:
                return {"valid": False, "reason": "legacy_record_without_hash", "index": index, **persistence}
            expected = self._canonical_hash(record, previous)
            if record.previous_hash != previous or record.record_hash != expected:
                return {"valid": False, "reason": "hash_mismatch", "index": index, **persistence}
            previous = record.record_hash
        return {"valid": True, "records": len(self._incidents), **persistence}
