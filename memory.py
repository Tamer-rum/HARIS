from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    audit: Dict[str, Any] = Field(default_factory=dict)
    previous_hash: Optional[str] = None
    record_hash: Optional[str] = None

    verification: Dict[str, Any] = Field(default_factory=dict)
    rollback: Dict[str, Any] = Field(default_factory=dict)

    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

class MemoryStore:
    def __init__(self, settings: Optional[AppSettings] = None):
        self.settings = settings or get_settings()
        self.local_file = Path(".haris_memory.json")
        self._incidents: List[IncidentMemory] = []
        self._policies: Dict[str, DevicePolicy] = {}
        self.supabase = None
        self.mem0 = None
        self._init_backends()

    def _init_backends(self) -> None:
        if self.settings.has_supabase:
            try:
                from supabase import create_client
                self.supabase = create_client(
                    self.settings.supabase_url,
                    self.settings.supabase_key.get_secret_value(),
                )
            except Exception as exc:
                logger.warning("Supabase initialization failed; continuing with local memory: %s", exc)
        if self.settings.has_mem0:
            try:
                from mem0 import MemoryClient
                self.mem0 = MemoryClient(api_key=self.settings.mem0_api_key.get_secret_value())
            except Exception as exc:
                logger.warning("Mem0 initialization failed; continuing without remote semantic memory: %s", exc)
        self._load_local()

    def _load_local(self) -> None:
        if not self.local_file.exists():
            return
        try:
            raw = json.loads(self.local_file.read_text(encoding="utf-8"))
            self._incidents = [IncidentMemory(**x) for x in raw.get("incidents", [])]
            self._policies = {k: DevicePolicy(**v) for k, v in raw.get("policies", {}).items()}
        except Exception as exc:
            logger.warning("Local memory file could not be loaded: %s", exc)

    def _save_local(self) -> None:
        payload = {
            "incidents": [x.model_dump() for x in self._incidents[-200:]],
            "policies": {k: v.model_dump() for k, v in self._policies.items()},
        }
        self.local_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    async def remember_incident(self, incident: IncidentMemory) -> None:
        # Hash chain makes local append-only history tamper-evident, not immutable.
        # The graph continues appending trace/events after LEARN.  Freeze a deep
        # snapshot so those later in-memory mutations cannot alter a record
        # after its hash has been calculated.
        incident = incident.model_copy(deep=True)
        previous = self._incidents[-1].record_hash if self._incidents else None
        payload = incident.model_dump(exclude={"previous_hash", "record_hash"})
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        incident = incident.model_copy(update={
            "previous_hash": previous,
            "record_hash": hashlib.sha256((canonical + (previous or "")).encode("utf-8")).hexdigest(),
        })
        self._incidents.append(incident)
        self._save_local()
        if self.supabase:
            try:
                self.supabase.table("haris_incidents").upsert(incident.model_dump()).execute()
            except Exception as exc:
                logger.warning("Supabase incident write failed: %s", exc)
        if self.mem0:
            try:
                self.mem0.add(
                    incident.summary,
                    user_id="haris-system",
                    metadata=incident.model_dump(),
                )
            except Exception as exc:
                logger.warning("Mem0 incident write failed: %s", exc)

    async def set_policy(self, policy: DevicePolicy) -> None:
        self._policies[policy.device_id] = policy
        self._save_local()
        if self.supabase:
            try:
                self.supabase.table("haris_device_policies").upsert(policy.model_dump()).execute()
            except Exception as exc:
                logger.warning("Supabase policy write failed: %s", exc)

    async def get_policy(self, device_id: str) -> DevicePolicy:
        if device_id in self._policies:
            return self._policies[device_id]
        default_tier = 1 if device_id in {"ambulance-01", "scada-01", "pipeline-01", "dispatch-01"} else 3
        policy = DevicePolicy(device_id=device_id, mission_tier=default_tier)
        await self.set_policy(policy)
        return policy

    async def search_incidents(self, query: str, limit: int = 5) -> List[IncidentMemory]:
        if self.mem0:
            try:
                result = self.mem0.search(query, user_id="haris-system", limit=limit)
                if isinstance(result, dict):
                    hits = result.get("results", [])
                else:
                    hits = result or []
                ids = {str(h.get("metadata", {}).get("incident_id")) for h in hits if isinstance(h, dict)}
                matched = [i for i in self._incidents if i.incident_id in ids]
                if matched:
                    return matched[:limit]
            except Exception as exc:
                logger.warning("Mem0 search failed: %s", exc)
        terms = {x.lower() for x in query.split() if len(x) > 2}
        scored = []
        for incident in self._incidents:
            text = f"{incident.summary} {incident.storm_type} {incident.outcome}".lower()
            score = sum(1 for term in terms if term in text)
            scored.append((score, incident))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [item for score, item in scored[:limit] if score > 0]

    def count(self) -> int:
        return len(self._incidents)

    def recent_incidents(self, limit: int = 50) -> List[IncidentMemory]:
        """Newest-first append-only audit records; callers receive copies."""
        return list(reversed(self._incidents[-limit:]))

    @staticmethod
    def normalized_view(record: IncidentMemory | Dict[str, Any]) -> Dict[str, Any]:
        """Non-destructive replay view for old/partial append-only records."""
        raw = record.model_dump() if isinstance(record, IncidentMemory) else dict(record)
        audit = raw.get("audit") if isinstance(raw.get("audit"), dict) else {}
        return {
            "cycle_id": raw.get("cycle_id") or "N/A", "created_at": raw.get("created_at") or "N/A",
            "mode": raw.get("mode") or "N/A", "outcome": raw.get("outcome") or "N/A",
            "affected_cells": raw.get("affected_cells") or [], "affected_devices": raw.get("affected_devices") or [],
            "prediction": audit.get("prediction") or None, "environment": audit.get("environment") or None,
            "plan": audit.get("plan") or None, "warden": audit.get("warden") or None,
            "execution": audit.get("execution") or None,
            "trusted_dispatch": audit.get("trusted_dispatch") or None,
            "dispatch_history": audit.get("dispatch_history") or [],
            "verification": audit.get("verification") or raw.get("verification") or None,
            "rollback": audit.get("rollback") or raw.get("rollback") or None,
            "trace": audit.get("trace") or None, "events": audit.get("events") or [],
        }

    def get_incident(self, cycle_or_incident_id: str) -> Optional[IncidentMemory]:
        return next((item for item in reversed(self._incidents)
                     if item.cycle_id == cycle_or_incident_id or item.incident_id == cycle_or_incident_id), None)

    def verify_audit_chain(self) -> Dict[str, Any]:
        previous = None
        for index, record in enumerate(self._incidents):
            if not record.record_hash:  # legacy records are view-compatible, not chain-verifiable
                return {"valid": False, "reason": "legacy_record_without_hash", "index": index}
            payload = record.model_dump(exclude={"previous_hash", "record_hash"})
            canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
            expected = hashlib.sha256((canonical + (previous or "")).encode("utf-8")).hexdigest()
            if record.previous_hash != previous or record.record_hash != expected:
                return {"valid": False, "reason": "hash_mismatch", "index": index}
            previous = record.record_hash
        return {"valid": True, "records": len(self._incidents)}
