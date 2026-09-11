"""Phase 7B durable incident -> bounded graph reasoning -> WARDEN planning.

Only repositories are authoritative.  This module never calls a provider and
never marks a provider action executed; it persists idempotent READY plans.
"""
from __future__ import annotations

import copy
import hashlib
import logging
import time
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field

from durable_core import (
    ActionCommand,
    ActionState,
    IncidentState,
    RepositoryUnavailable,
    VersionConflict,
)
from memory import IncidentMemory


logger = logging.getLogger("haris.durable_reasoning")


TERMINAL_INCIDENT_STATES = {
    IncidentState.RESOLVED, IncidentState.BLOCKED, IncidentState.FAILED,
    IncidentState.CANCELLED,
}


def _is_integration_test_record(value: Any) -> bool:
    """Keep persistence-harness identities out of operational reasoning."""
    if getattr(value, "incident_id", None) and str(value.incident_id).startswith("PERSISTENCE-TEST-"):
        return True
    if not isinstance(value, dict):
        return False
    if value.get("source_mode") == "PERSISTENCE_INTEGRATION_TEST":
        return True
    return any(
        str(value.get(key) or "").startswith("PERSISTENCE-TEST-")
        for key in (
            "incident_id", "owner_incident_id", "primary_entity", "entity_id",
            "event_id", "resource_key", "device_id",
        )
    )


class DurableDecisionRetry(RepositoryUnavailable):
    """The durable item must remain retryable and must not be acknowledged."""


class IncidentReasoningContext(BaseModel):
    """Explicit bounded evidence supplied to the existing HARIS graph."""

    model_config = ConfigDict(extra="forbid")
    incident_id: str
    incident_version: int
    plan_version: int
    incident_state: str
    trace_id: str
    decision_id: str
    trigger_event_id: str
    source_timestamp: float
    provenance: str
    affected_entities: list[str] = Field(default_factory=list, max_length=64)
    affected_devices: list[str] = Field(default_factory=list, max_length=64)
    congestion: list[Dict[str, Any]] = Field(default_factory=list, max_length=64)
    devices: list[Dict[str, Any]] = Field(default_factory=list, max_length=64)
    locations: list[Dict[str, Any]] = Field(default_factory=list, max_length=64)
    agent_incident: Dict[str, Any]
    prediction: Dict[str, Any]
    environmental_source: str
    dust_advisory: bool
    network_state: list[Dict[str, Any]] = Field(default_factory=list, max_length=128)
    latest_verification: Optional[Dict[str, Any]] = None
    recovery: Optional[Dict[str, Any]] = None
    ownership: list[Dict[str, Any]] = Field(default_factory=list, max_length=128)
    nonterminal_actions: list[Dict[str, Any]] = Field(default_factory=list, max_length=128)
    prior_memory: list[Dict[str, Any]] = Field(default_factory=list, max_length=3)
    capability_state: Dict[str, Any] = Field(default_factory=dict)
    durable_policy: Dict[str, Any] = Field(default_factory=dict)
    trusted_dispatch: Optional[Dict[str, Any]] = None
    field_intervention_required: bool = False
    field_intervention_site: Optional[str] = None
    field_intervention_skills: list[str] = Field(default_factory=list, max_length=16)
    field_intervention_reason: Optional[str] = None
    field_intervention_evidence: Dict[str, Any] = Field(default_factory=dict)
    unavailable_evidence: list[str] = Field(default_factory=list, max_length=64)
    reasoning_ready: bool


class DurableDecisionResult(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: str
    incident_id: str
    plan_version: int
    warden_decision: str
    action_ids: list[str] = Field(default_factory=list)
    action_count: int = 0
    duplicate: bool = False
    provider_execution_performed: bool = False
    reasoning_trace: list[Dict[str, str]] = Field(default_factory=list)

    def public(self) -> Dict[str, Any]:
        return self.model_dump()


def _latest(rows: list[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not rows:
        return None

    def timestamp(row: Dict[str, Any]) -> float:
        value = row.get("verified_at") or row.get("created_at") or row.get("updated_at")
        if value is None:
            return 0.0
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
            except ValueError:
                return 0.0
        return 0.0

    return copy.deepcopy(max(rows, key=timestamp))


def _decision_id(incident_id: str, plan_version: int) -> str:
    material = f"{incident_id}|{plan_version}|DURABLE_PLAN"
    return f"decision-{hashlib.sha256(material.encode()).hexdigest()[:24]}"


def _safe_trace(state: Dict[str, Any]) -> list[Dict[str, str]]:
    allowed_agents = {
        "SENTINEL", "CARTOGRAPHER", "TRIAGE", "ACTUATOR_PLAN", "CREWAI_USED",
        "AI_PLANNER_USED", "DETERMINISTIC_NORMALIZATION", "WARDEN",
        "TRUST_CHECK", "FIELD_INTERVENTION_REQUIRED",
    }
    result = []
    for item in list(state.get("trace") or [])[:64]:
        text = str(item)
        message = text.split(" | ", 1)[-1][:512]
        agent = message.split(":", 1)[0].split("=", 1)[0].strip().upper()
        if agent in allowed_agents:
            result.append({"stage": agent, "message": message})
    return result


async def build_incident_reasoning_context(
    *, bundle: Any, agent_system: Any, incident_id: str,
) -> IncidentReasoningContext:
    """Re-read every applicable input from durable authority."""
    incident = bundle.incidents.get(incident_id)
    if incident is None:
        raise DurableDecisionRetry("durable incident is unavailable")
    trigger = bundle.events.get(str(incident.get("trigger_event_id") or ""))
    if trigger is None:
        raise DurableDecisionRetry("durable trigger event is unavailable")

    all_state = {
        key: value for key, value in bundle.network_state.load_all().items()
        if not _is_integration_test_record(value)
        and not str(key).startswith("PERSISTENCE-TEST-")
    }
    primary = str(incident.get("primary_entity") or trigger.entity_id)
    affected_entities = list(dict.fromkeys(
        [primary] + list(incident.get("affected_entities") or incident.get("affected_cells") or [])
    ))[:64]
    relevant = [
        copy.deepcopy(row) for key, row in all_state.items()
        if key in affected_entities or row.get("entity_id") in affected_entities
    ][:128]

    primary_projection = bundle.network_state.get(primary) or {}
    congestion_evidence = copy.deepcopy(
        primary_projection.get("raw_congestion_evidence") or trigger.payload
    )
    level = primary_projection.get("raw_congestion") or congestion_evidence.get("congestion_level")
    confidence = congestion_evidence.get("confidence_level")
    unavailable: list[str] = []
    if level is None:
        unavailable.append("congestion_level")
    if confidence is None:
        unavailable.append("congestion_confidence")
    congestion: list[Dict[str, Any]] = []
    if level is not None and confidence is not None:
        congestion.append({
            "cell_id": primary,
            "congestion_level": level,
            "confidence_level": confidence,
            "interval_start": congestion_evidence.get("interval_start"),
            "interval_stop": congestion_evidence.get("interval_stop"),
        })

    device_rows: Dict[str, Dict[str, Any]] = {}
    # The controlled validation stores one fresh, complete registered-fleet
    # observation in its primary projection.  Use that atomic snapshot rather
    # than mixing it with older projections from unrelated incidents.  Normal
    # durable incidents retain their established multi-projection behavior.
    device_state_rows = (
        [primary_projection]
        if trigger.source_mode == "real_qod_validation"
        else list(all_state.values())
    )
    for row in device_state_rows:
        summary = row.get("reachability_summary") or {}
        for device in summary.get("devices") or []:
            if not device.get("device_id"):
                continue
            if device.get("tier") is None or not isinstance(device.get("reachable"), bool):
                unavailable.append(f"device_context:{device.get('device_id', 'unknown')}")
                continue
            device_rows[str(device["device_id"])] = {
                "device_id": str(device["device_id"]),
                "reachable": device["reachable"],
                "roaming": bool(device.get("roaming", False)),
                "battery_pct": device.get("battery_pct"),
                "tier": int(device["tier"]),
                "cell_id": str(device["cell_id"]),
            }
    devices = list(device_rows.values())[:64]
    affected_device_ids = [
        device_id for device_id, device in device_rows.items()
        if device.get("cell_id") in affected_entities
    ]
    if not affected_device_ids:
        unavailable.append("affected_device_context")

    locations = []
    for device_id in device_rows:
        row = all_state.get(device_id) or {}
        location = row.get("location_summary") or {}
        if location.get("latitude") is not None and location.get("longitude") is not None:
            locations.append({
                "device_id": device_id,
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "accuracy_m": location.get("accuracy_m"),
            })
    if not locations:
        unavailable.append("device_locations")

    verifications = bundle.verification.for_incident(incident_id)
    latest_verification = _latest(verifications)
    recovery = bundle.recovery.for_incident(incident_id)
    ownership = [
        row for row in bundle.resource_ownership.owned_by(incident_id)
        if not _is_integration_test_record(row)
    ]
    active_ownership = [
        row for row in bundle.resource_ownership.active()
        if not _is_integration_test_record(row)
    ]
    actions = [
        vars(action) for action in bundle.actions.pending_or_unknown()
        if action.incident_id == incident_id and not _is_integration_test_record(action)
    ][:128]
    prior_memory = await agent_system.memory.search_incidents(
        "network " + " ".join(affected_entities), limit=3,
    )
    relevant_priors = [
        item.model_dump() for item in prior_memory
        if set(item.affected_cells) & set(affected_entities)
        and not _is_integration_test_record(item.model_dump())
    ][:3]

    field_required = bool(incident.get("field_intervention_required", False))
    trusted_dispatch = None
    if latest_verification and latest_verification.get("verification_type") == "TRUSTED_DISPATCH":
        recent_swap = latest_verification.get("recent_sim_swap")
        number_verified = latest_verification.get("number_verified") is True
        trusted_dispatch = {
            "decision": "ALLOW" if number_verified and recent_swap is False else "BLOCK",
            "status": "VERIFIED" if number_verified else "IDENTITY_VERIFICATION_REQUIRED",
            "number_verified": number_verified,
            "recent_sim_swap": recent_swap,
            "reason": (
                "Recent SIM swap detected." if recent_swap is True else
                "Server-authoritative trust evidence passed." if number_verified and recent_swap is False else
                "Fresh server-authoritative trust evidence is unavailable."
            ),
        }

    slice_status = "UNAVAILABLE"
    for row in all_state.values():
        candidate = row.get("slice_status") or (row.get("slice_summary") or {}).get("status")
        if candidate:
            slice_status = str(candidate).upper()
            break
    incident_total = float(bundle.cost_ledger.incident_total(incident_id))
    capabilities = agent_system.client.capability_report()
    source_timestamp = float(incident.get("trigger_source_timestamp") or trigger.source_timestamp)
    validation_scope_enforced = trigger.source_mode == "real_qod_validation"
    validation_target = str(
        trigger.payload.get("validation_target_device_id") or ""
    )
    allowed_mutation_device_ids = (
        [validation_target]
        if validation_scope_enforced and validation_target in affected_device_ids
        else []
    )
    agent_incident = {
        "incident_id": incident_id,
        "storm_advisory": bool(
            trigger.provenance.value == "FIXTURE_SIMULATED"
            and congestion_evidence.get("dust_advisory") is True
        ),
        "peak_congestion_level": level,
        "peak_confidence_level": confidence,
        "max_congestion_pct": None,
        "affected_cells": affected_entities,
        "affected_devices": affected_device_ids,
        "severity": str(incident.get("severity") or "critical").lower(),
        "created_at": float(incident.get("opened_at") or trigger.created_at),
    }
    ready = bool(level is not None and confidence is not None)
    plan_version = int(incident.get("plan_version", 0)) + 1
    return IncidentReasoningContext(
        incident_id=incident_id,
        incident_version=int(incident.get("version", 0)),
        plan_version=plan_version,
        incident_state=str(incident["state"]),
        trace_id=str(incident.get("trace_id") or trigger.trace_id),
        decision_id=_decision_id(incident_id, plan_version),
        trigger_event_id=trigger.event_id,
        source_timestamp=source_timestamp,
        provenance=trigger.provenance.value,
        affected_entities=affected_entities,
        affected_devices=affected_device_ids,
        congestion=congestion,
        devices=devices,
        locations=locations,
        agent_incident=agent_incident,
        prediction={
            "predicted_risk_level": "UNAVAILABLE",
            "confidence": None,
            "degradation_probability": None,
            "input_provenance": "UNAVAILABLE",
        },
        environmental_source=(
            "FIXTURE" if agent_incident["storm_advisory"] else "UNAVAILABLE"
        ),
        dust_advisory=agent_incident["storm_advisory"],
        network_state=relevant,
        latest_verification=latest_verification,
        recovery=recovery,
        ownership=ownership,
        nonterminal_actions=actions,
        prior_memory=relevant_priors,
        capability_state={"capabilities": capabilities, "slice_status": slice_status},
        durable_policy={
            "incident_current": True,
            "incident_cost_total_usd": incident_total,
            "cost_ceiling_usd": float(agent_system.settings.guardrails.qos_spend_ceiling_usd),
            "resource_conflicts": [
                item.get("resource_key") for item in active_ownership
                if item.get("owner_incident_id") != incident_id
                and item.get("resource_key") in {f"device:{device_id}" for device_id in affected_device_ids}
            ],
            "max_protected_devices": int(agent_system.settings.guardrails.max_devices_reconfigured_per_cycle),
            # The controlled real-QoD harness may observe a wider, truthful
            # cell cohort, but provider mutation remains bound to its single
            # configured target.  Missing/invalid target evidence therefore
            # produces an empty fail-closed allowlist.
            "mutation_device_allowlist_enforced": validation_scope_enforced,
            "allowed_mutation_device_ids": allowed_mutation_device_ids,
            "provider_execution_permitted": False,
        },
        trusted_dispatch=trusted_dispatch,
        field_intervention_required=field_required,
        field_intervention_site=incident.get("field_intervention_site"),
        field_intervention_skills=list(incident.get("field_intervention_skills") or []),
        field_intervention_reason=incident.get("field_intervention_reason"),
        field_intervention_evidence=copy.deepcopy(incident.get("field_intervention_evidence") or {}),
        unavailable_evidence=sorted(set(unavailable)),
        reasoning_ready=ready,
    )


class DurableIncidentDecisionService:
    """One retry-safe consumer for ``DURABLE_INCIDENT_READY``."""

    def __init__(
        self, *, bundle: Any, agent_system: Any,
        is_ready: Callable[[], bool], metrics: Any,
        failure_hook: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.bundle = bundle
        self.agent_system = agent_system
        self.is_ready = is_ready
        self.metrics = metrics
        self.failure_hook = failure_hook

    def _failpoint(self, stage: str) -> None:
        if self.failure_hook is not None:
            self.failure_hook(stage)

    def _existing_completion(self, incident: Dict[str, Any]) -> Optional[DurableDecisionResult]:
        decision = str(incident.get("warden_decision") or "")
        state = IncidentState(incident["state"])
        if decision not in {"ALLOW", "BLOCK", "ESCALATE"}:
            return None
        status = {
            "ALLOW": "AUTHORIZED_PLAN", "BLOCK": "BLOCKED", "ESCALATE": "ESCALATED",
        }[decision]
        if decision == "ALLOW" and state is not IncidentState.APPROVED:
            return None
        if decision == "BLOCK" and state is not IncidentState.BLOCKED:
            return None
        if decision == "ESCALATE" and state is not IncidentState.ESCALATED:
            return None
        plan_version = int(incident.get("plan_version", 0))
        actions = [
            action for action in self.bundle.actions.pending_or_unknown()
            if action.incident_id == incident["incident_id"] and action.plan_version == plan_version
        ]
        return DurableDecisionResult(
            status=status, incident_id=incident["incident_id"], plan_version=plan_version,
            warden_decision=decision, action_ids=[item.command_id for item in actions],
            action_count=len(actions), duplicate=True,
        )

    def _advance(self, incident_id: str, target: IncidentState, trace_id: str) -> Dict[str, Any]:
        paths = {
            IncidentState.APPROVED: [
                IncidentState.DETECTED, IncidentState.EVALUATING, IncidentState.PLANNED,
                IncidentState.WARDEN_REVIEW, IncidentState.APPROVED,
            ],
            IncidentState.BLOCKED: [
                IncidentState.DETECTED, IncidentState.EVALUATING, IncidentState.PLANNED,
                IncidentState.WARDEN_REVIEW, IncidentState.BLOCKED,
            ],
            IncidentState.ESCALATED: [
                IncidentState.DETECTED, IncidentState.EVALUATING, IncidentState.PLANNED,
                IncidentState.WARDEN_REVIEW, IncidentState.ESCALATED,
            ],
        }
        current = self.bundle.incidents.get(incident_id)
        if current is None:
            raise DurableDecisionRetry("incident disappeared during decision")
        desired = paths[target]
        while IncidentState(current["state"]) is not target:
            state = IncidentState(current["state"])
            if state in TERMINAL_INCIDENT_STATES:
                raise DurableDecisionRetry("incident became terminal during decision")
            try:
                index = desired.index(state)
                next_state = desired[index + 1]
            except (ValueError, IndexError) as exc:
                raise DurableDecisionRetry("incident state cannot join decision path") from exc
            current = self.bundle.incidents.transition(
                incident_id, next_state, actor="WARDEN",
                reason_code="PHASE_7B_DURABLE_DECISION", trace_id=trace_id, at=time.time(),
            )
        return current

    def _decision_outbox(
        self, *, context: IncidentReasoningContext, result: DurableDecisionResult,
        trace: list[Dict[str, str]],
    ) -> None:
        self.bundle.outbox.append({
            "outbox_id": f"out-{context.decision_id}",
            "event_id": context.trigger_event_id,
            "event_type": "DURABLE_DECISION_READY",
            "payload": {
                "incident_id": context.incident_id,
                "decision_status": result.status,
                "warden_decision": result.warden_decision,
                "plan_version": result.plan_version,
                "action_count": result.action_count,
                "action_ids": list(result.action_ids),
                "provider_execution_performed": False,
                "reasoning_trace": trace,
            },
            "trace_id": context.trace_id,
            "created_at": time.time(),
        })

    async def handle_durable_incident_ready(self, row: Dict[str, Any]) -> DurableDecisionResult:
        if not self.is_ready():
            raise DurableDecisionRetry("durable reasoning runtime is not ready")
        payload = row.get("payload") or {}
        incident_id = str(payload.get("incident_id") or "")
        if row.get("event_type") != "DURABLE_INCIDENT_READY" or not incident_id:
            raise DurableDecisionRetry("invalid durable incident reference")

        incident = self.bundle.incidents.get(incident_id)
        if incident is None:
            raise DurableDecisionRetry("durable incident is unavailable")
        existing = self._existing_completion(incident)
        if existing is not None:
            self.metrics.increment("decision_duplicate")
            return existing
        if IncidentState(incident["state"]) in TERMINAL_INCIDENT_STATES:
            return DurableDecisionResult(
                status="TERMINAL_SKIPPED", incident_id=incident_id,
                plan_version=int(incident.get("plan_version", 0)),
                warden_decision=str(incident.get("warden_decision") or "NONE"),
                duplicate=True,
            )

        context = await build_incident_reasoning_context(
            bundle=self.bundle, agent_system=self.agent_system, incident_id=incident_id,
        )
        self.metrics.increment("decision_evaluated")
        if not context.reasoning_ready:
            graph_state: Dict[str, Any] = {
                "plan": {"actions": [], "approval_required": True},
                "warden": {"verified": False, "reason": "durable_evidence_insufficient"},
                "decision_status": "ESCALATED",
                "trace": ["WARDEN: insufficient durable evidence; human escalation required"],
            }
        else:
            graph_state = await self.agent_system.run_durable_reasoning(context.model_dump())

        self._failpoint("AFTER_REASONING_BEFORE_PLAN")
        current = self.bundle.incidents.get(incident_id)
        if current is None:
            raise DurableDecisionRetry("incident disappeared during decision")
        existing = self._existing_completion(current)
        if existing is not None:
            self.metrics.increment("decision_duplicate")
            return existing
        if (
            int(current.get("version", 0)) != context.incident_version
            or current.get("state") != context.incident_state
        ):
            raise DurableDecisionRetry("incident changed during reasoning")

        plan = graph_state.get("plan") or {}
        warden = graph_state.get("warden") or {}
        trace = _safe_trace(graph_state)
        verified = warden.get("verified") is True and bool(plan.get("actions"))
        if verified:
            status, decision, target = "AUTHORIZED_PLAN", "ALLOW", IncidentState.APPROVED
        elif plan.get("approval_required") or warden.get("reason") in {
            "durable_evidence_insufficient", "identity_verification_pending",
        }:
            status, decision, target = "ESCALATED", "ESCALATE", IncidentState.ESCALATED
        else:
            status, decision, target = "BLOCKED", "BLOCK", IncidentState.BLOCKED

        action_ids: list[str] = []
        if decision == "ALLOW":
            candidate_ids = list(plan.get("candidate_ids") or [])
            for index, action in enumerate(plan.get("actions") or []):
                command_type = {
                    "qos": "QOD_PLAN", "slice_attach": "SLICE_ATTACH_PLAN",
                    "geofence": "GEOFENCE_PLAN",
                }.get(action.get("kind"))
                if command_type is None:
                    raise DurableDecisionRetry("normalized candidate is unsupported")
                device_cell = next((
                    device.get("cell_id") for device in context.devices
                    if device.get("device_id") == action["device_id"]
                ), None)
                baseline = next((
                    reading for reading in context.congestion
                    if reading.get("cell_id") == device_cell
                ), {})
                command = ActionCommand(
                    incident_id=incident_id,
                    command_type=command_type,
                    resource_key=f"device:{action['device_id']}",
                    device_id=action["device_id"],
                    plan_version=context.plan_version,
                    requested_at=time.time(),
                    parameters_safe={
                        **copy.deepcopy(action.get("parameters") or {}),
                        "candidate_id": candidate_ids[index] if index < len(candidate_ids) else f"candidate-{index}",
                        "reason": str(action.get("reason") or "")[:512],
                    },
                    preconditions={
                        "phase": "7B_PLAN_ONLY",
                        "warden_decision": "ALLOW",
                        "confidence": plan.get("confidence"),
                        "blast_radius": plan.get("blast_radius"),
                        "expected_cost_usd": plan.get("expected_cost_usd"),
                        "selected_device_count": len(plan.get("selected_device_ids") or []),
                        "trace_id": context.trace_id,
                        "provenance": context.provenance,
                        "verification_baseline": {
                            "cell_id": device_cell,
                            "congestion_level": baseline.get("congestion_level"),
                            "confidence_level": baseline.get("confidence_level"),
                            "interval_start": baseline.get("interval_start"),
                            "interval_stop": baseline.get("interval_stop"),
                        },
                        "trusted_dispatch_required": context.field_intervention_required,
                        "provider_execution_permitted": False,
                    },
                    state=ActionState.READY,
                )
                stored, _created = self.bundle.actions.create_or_get(command)
                action_ids.append(stored.command_id)
        self._failpoint("AFTER_PLAN_PERSISTENCE")

        durable_incident = self._advance(incident_id, target, context.trace_id)
        update = copy.deepcopy(durable_incident)
        update.update({
            "plan_version": context.plan_version,
            "warden_decision": decision,
            "outcome": status,
            "updated_at": time.time(),
        })
        durable_incident = self.bundle.incidents.update(
            update, expected_version=int(durable_incident.get("version", 0)),
        )
        result = DurableDecisionResult(
            status=status, incident_id=incident_id, plan_version=context.plan_version,
            warden_decision=decision, action_ids=action_ids,
            action_count=len(action_ids), reasoning_trace=trace,
        )
        self._decision_outbox(context=context, result=result, trace=trace)
        self._failpoint("AFTER_DURABLE_DECISION")
        self.agent_system.accept_durable_decision({
            **graph_state,
            "final_status": status.lower(),
            "decision_status": status,
            "execution": {"executed": False, "actions": [], "reason": "phase_7b_plan_only"},
        })
        self.metrics.increment({
            "ALLOW": "decision_authorized", "BLOCK": "decision_blocked",
            "ESCALATE": "decision_escalated",
        }[decision])
        logger.info("durable_decision status=%s action_count=%s", status, len(action_ids))
        return result
