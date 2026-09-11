"""Bounded, evidence-correlated incident coordination for HARIS."""
from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any, Dict, Optional

from config import AppSettings
from network_state import NetworkStateRegistry
from runtime import RuntimeEnvironment, runtime_environment


class LegacyIncidentPathQuarantined(RuntimeError):
    """The compatibility incident manager cannot own live execution."""


class IncidentManager:
    """Correlates High evidence by logical cell and bounds incident work.

    It schedules evaluations concurrently only for independent logical cells.
    Network execution remains subject to WARDEN and the system-level execution
    semaphore; this manager never calls a Nokia mutation itself.
    """

    TERMINAL = {"mitigated", "rolled_back_safely", "verification_failed", "execution_failed", "blocked", "resolved"}

    def __init__(self, system: Any, registry: NetworkStateRegistry, settings: AppSettings, durable_core: Any = None):
        self.system, self.registry, self.settings = system, registry, settings
        self.durable_core = durable_core
        self._incidents: Dict[str, Dict[str, Any]] = {}
        self._by_entity: Dict[str, str] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._execution_lock = asyncio.Semaphore(settings.max_parallel_executions)

    def restore_from_durable(self) -> None:
        """Rebuild correlation indexes from the authoritative domain core.

        No graph work is restarted here: a recovery/reconciliation worker owns
        that policy.  This prevents a post-restart duplicate High from opening
        a second incident while preserving the legacy supervisory response.
        """
        if self.durable_core is None:
            return
        self.restore_records(self.durable_core.incidents.active())

    def restore_records(self, records: list[Dict[str, Any]]) -> None:
        """Rebuild runtime indexes from an already validated repository view."""
        for item in records:
            incident_id = item["incident_id"]
            entity_id = item["primary_entity"]
            record = {
                "incident_id": incident_id, "entity_id": entity_id,
                "trigger_level": "High", "triggered_at": item.get("trigger_source_timestamp"),
                "last_evidence_at": item.get("trigger_source_timestamp"), "evidence_updates": 1,
                "status": item.get("state", "DETECTED"), "stage": "RECONSTRUCTED",
                "outcome": item.get("outcome"), "resource_conflict": False,
            }
            self._incidents[incident_id] = record
            self._by_entity[entity_id] = incident_id
            self.registry.set_incident_state(entity_id, incident_id, item.get("state", "INCIDENT_OPEN"))

    def active_incidents(self) -> list[Dict[str, Any]]:
        return [dict(item) for item in self._incidents.values() if item["status"] not in self.TERMINAL]

    def status(self) -> Dict[str, Any]:
        return {"max_active_incidents": self.settings.max_active_incidents, "active_incidents": self.active_incidents(), "incidents": [dict(item) for item in self._incidents.values()]}

    async def ingest(self, observation: Dict[str, Any]) -> None:
        """Process only fresh categorical congestion from one observation view."""
        if (
            runtime_environment() is RuntimeEnvironment.PRODUCTION
            or self.settings.nac_mode in {"live_read_only", "live_write"}
        ):
            raise LegacyIncidentPathQuarantined(
                "legacy incident ingestion is quarantined from production/live execution"
            )
        entities = self.registry.ingest(observation)
        for entity_id, entity in entities.items():
            level = entity.get("nokia_congestion")
            if entity.get("freshness") != "FRESH":
                continue
            existing_id = self._by_entity.get(entity_id)
            if level == "High":
                if existing_id and self._incidents[existing_id]["status"] not in self.TERMINAL:
                    self._incidents[existing_id]["last_evidence_at"] = entity.get("congestion_observed_at")
                    self._incidents[existing_id]["evidence_updates"] += 1
                    continue
                if len(self.active_incidents()) >= self.settings.max_active_incidents:
                    self.registry.set_incident_state(entity_id, None, "BLOCKED")
                    continue
                incident_id = f"inc-{uuid.uuid4().hex[:12]}"
                if self.durable_core is not None:
                    durable = next(
                        (item for item in self.durable_core.incidents.active() if item.get("primary_entity") == entity_id),
                        None,
                    )
                    if durable is not None:
                        incident_id = durable["incident_id"]
                record = {"incident_id": incident_id, "entity_id": entity_id, "trigger_level": "High", "triggered_at": entity.get("congestion_observed_at"), "last_evidence_at": entity.get("congestion_observed_at"), "evidence_updates": 1, "status": "INCIDENT_OPEN", "stage": "SENTINEL", "outcome": None, "resource_conflict": False}
                self._incidents[incident_id] = record
                self._by_entity[entity_id] = incident_id
                self.registry.set_incident_state(entity_id, incident_id, "INCIDENT_OPEN")
                self._tasks[incident_id] = asyncio.create_task(self._evaluate(incident_id, observation), name=f"haris-incident-{incident_id}")
            elif level == "Medium" and not existing_id:
                self.registry.set_incident_state(entity_id, None, "WATCHING")
            elif level in {"Low", "None"} and existing_id:
                record = self._incidents[existing_id]
                record["normal_observations"] = record.get("normal_observations", 0) + 1
                if record["normal_observations"] >= self.settings.incident_recovery_low_observations:
                    record.update({"status": "RESOLVED", "stage": "RECOVERING", "outcome": "normalized_by_fresh_nokia_evidence"})
                    self.registry.set_incident_state(entity_id, None, "RESOLVED")
                    self._by_entity.pop(entity_id, None)

    async def _evaluate(self, incident_id: str, observation: Dict[str, Any]) -> None:
        if (
            runtime_environment() is RuntimeEnvironment.PRODUCTION
            or self.settings.nac_mode in {"live_read_only", "live_write"}
        ):
            raise LegacyIncidentPathQuarantined(
                "legacy incident evaluation is quarantined from production/live execution"
            )
        record = self._incidents[incident_id]
        entity_id = record["entity_id"]
        try:
            record.update({"status": "EVALUATING", "stage": "SENTINEL"})
            if self.durable_core is not None:
                from durable_core import IncidentState
                try:
                    self.durable_core.incidents.transition(
                        incident_id, IncidentState.EVALUATING, actor="SENTINEL",
                        reason_code="FRESH_HIGH_EVIDENCE", trace_id=f"incident-{incident_id}", at=time.time(),
                    )
                except Exception:
                    # A reconstructed incident can already be further through
                    # its lifecycle; never regress it or fabricate a change.
                    pass
            self.registry.set_incident_state(entity_id, incident_id, "MITIGATING")
            # The graph still owns WARDEN/Actuator/Verify.  Execution is
            # serialized conservatively; evaluations can overlap safely.
            async with self._execution_lock:
                result = await self.system.run_cycle(
                    dust_advisory=False,
                    incident_scope_cells=[entity_id],
                    incident_id=incident_id,
                    observation_snapshot=observation,
                )
            record.update({"stage": "COMPLETE", "outcome": result.get("final_status"), "warden": result.get("warden", {}).get("verified")})
            if result.get("final_status") in {"mitigated", "rolled_back_safely"}:
                record["status"] = "VERIFYING"
                self.registry.set_incident_state(entity_id, incident_id, "VERIFYING")
            elif result.get("final_status") == "waiting_for_identity_verification":
                record["status"] = "BLOCKED"
                self.registry.set_incident_state(entity_id, incident_id, "BLOCKED")
            else:
                record["status"] = "BLOCKED"
                self.registry.set_incident_state(entity_id, incident_id, "BLOCKED")
                if self.durable_core is not None:
                    from durable_core import IncidentState
                    try:
                        self.durable_core.incidents.transition(
                            incident_id, IncidentState.BLOCKED, actor="WARDEN",
                            reason_code="GRAPH_DID_NOT_AUTHORIZE_OR_VERIFY", trace_id=f"incident-{incident_id}", at=time.time(),
                        )
                    except Exception:
                        pass
        except Exception as exc:
            record.update({"status": "BLOCKED", "stage": "ERROR", "outcome": "evaluation_failed", "error_class": type(exc).__name__})
            self.registry.set_incident_state(entity_id, incident_id, "BLOCKED")
            if self.durable_core is not None:
                from durable_core import IncidentState
                try:
                    self.durable_core.incidents.transition(
                        incident_id, IncidentState.BLOCKED, actor="SYSTEM",
                        reason_code="EVALUATION_EXCEPTION", trace_id=f"incident-{incident_id}", at=time.time(),
                    )
                except Exception:
                    pass
