from __future__ import annotations
import asyncio
import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TypedDict

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from langgraph.graph import END, START, StateGraph

from config import AppSettings, DevicePolicy, EnvironmentalSource, QualityLevel, get_settings
from dispatch import AuthorizedEngineerRegistry, DispatchAttempt, PendingDispatch, frontend_consent_tokens, mask_phone_number, pending_dispatches, trusted_dispatch_history
from memory import IncidentMemory, MemoryStore
from nokia_clients import BaseNokiaClient, CongestionReading, DeviceStatus, evaluate_trusted_dispatch_phone, register_dispatch_resume_handler, register_dispatch_verification_failure_handler, start_number_verification_for_dispatch, verified_identities
from playbooks import Action, PlaybookEngine
from prediction import PredictionResult, RiskForecaster
from runtime import external_access_policy

logger = logging.getLogger("haris.agents")

try:
    from crewai import Agent, Crew, LLM, Process, Task
    from crewai.tools import BaseTool
    CREWAI_AVAILABLE = True
except ImportError:
    CREWAI_AVAILABLE = False

try:
    from langchain_groq import ChatGroq
    from langchain_google_genai import ChatGoogleGenerativeAI
    LANGCHAIN_LLM_AVAILABLE = True
except ImportError:
    LANGCHAIN_LLM_AVAILABLE = False


class Incident(BaseModel):
    incident_id: str = Field(
        default_factory=lambda: f"inc-{uuid.uuid4().hex[:12]}"
    )
    storm_advisory: bool
    # Direct Nokia/CAMARA evidence
    peak_congestion_level: str
    peak_confidence_level: int = Field(ge=0, le=100)
    max_congestion_pct: Optional[float] = Field(default=None, ge=0, le=100)
    affected_cells: List[str]
    affected_devices: List[str]
    severity: QualityLevel
    created_at: float = Field(default_factory=time.time)


class RemediationPlan(BaseModel):
    incident_id: str
    actions: List[Action]
    confidence: float = Field(ge=0, le=1)
    expected_cost_usd: float = Field(ge=0)
    expected_benefit: float = Field(ge=0, le=1)
    blast_radius: float = Field(ge=0, le=1)
    approval_required: bool
    rationale: str
    # Triage records the deterministic device cohort. WARDEN re-validates it;
    # an advisory model never supplies this list.
    selected_device_ids: Optional[List[str]] = None


class CrewAdvisory(BaseModel):
    """Strictly bounded, non-executable collaboration output."""

    model_config = ConfigDict(extra="forbid")
    ranked_candidate_ids: List[str] = Field(default_factory=list)
    confidence_modifier: float = Field(default=0.0, ge=-0.05, le=0.05)
    expected_benefit: str = ""
    rationale: str = ""
    specialist_notes: Dict[str, str] = Field(default_factory=dict)


class PlannerAdvisory(BaseModel):
    """Schema accepted from Gemini/Groq; it is advisory data, never a plan."""

    model_config = ConfigDict(extra="forbid")
    ranked_candidate_ids: List[str] = Field(default_factory=list)
    confidence_adjustment: float = Field(default=0.0, ge=-0.05, le=0.05)
    expected_benefit: str = ""
    rationale: str = ""


CREWAI_ROLE_NAMES = ("SENTINEL", "CARTOGRAPHER", "TRIAGE", "ACTUATOR", "WARDEN")


def _candidate_ids(actions: List[Action]) -> List[str]:
    """Stable identifiers for the deterministic candidates supplied to a model.

    The identifier deliberately carries no mutable API parameters.  A model can
    only rank one of these pre-existing candidates; it cannot construct a new
    action or alter a candidate's target/profile.
    """
    return [f"candidate-{index}" for index, _ in enumerate(actions)]


class HarisState(TypedDict, total=False):
    cycle_id: str
    dust_advisory: bool
    environmental_source: str
    congestion: List[Dict[str, Any]]
    devices: List[Dict[str, Any]]
    locations: List[Dict[str, Any]]
    incident: Dict[str, Any]
    plan: Dict[str, Any]
    warden: Dict[str, Any]
    execution: Dict[str, Any]
    verification: Dict[str, Any]
    learning: Dict[str, Any]
    trace: List[str]
    events: List[Dict[str, Any]]
    active_playbook: Dict[str, Any]
    field_intervention_required: bool
    field_intervention_site: Optional[str]
    field_intervention_skills: List[str]
    field_intervention_reason: Optional[str]
    field_intervention_evidence: Dict[str, Any]
    trusted_dispatch: Dict[str, Any]
    explanation: str
    error: Optional[str]
    pre_execution_congestion: Dict[str, Dict[str, Any]]
    pre_execution_devices: Dict[str, str]
    rollback_attempted: bool
    rollback: Dict[str, Any]
    final_status: str
    prediction: Dict[str, Any]
    memory_context: List[Dict[str, Any]]
    crew_advisory: Dict[str, Any]
    durable_planning_only: bool
    durable_reasoning_context: Dict[str, Any]
    durable_policy: Dict[str, Any]
    decision_status: str
    isolated_fixture_demo: bool
    execution_context: str
    provenance: Optional[str]
    authority: Optional[str]
    external_access_permitted: Optional[bool]
    durable_domain_write: Optional[bool]
    durable_history_write: Optional[bool]

def _safe_json(text: str) -> Dict[str, Any]:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.S)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
    return {}


class ReasoningRouter:
    """Uses the allowed Gemini/Groq models for advisory reasoning only.

    The deterministic policy engine remains authoritative for every network action.
    """
    def __init__(self, settings: AppSettings):
        self.settings = settings
        self.gemini = None
        self.groq = None
        self.availability_reason = "missing_model_credentials"
        if not external_access_policy().allow_llm:
            self.availability_reason = "runtime_policy_blocks_llm"
            return
        if not LANGCHAIN_LLM_AVAILABLE:
            self.availability_reason = "langchain_provider_dependency_unavailable"
            return
        try:
            if settings.gemini_api_key:
                self.gemini = ChatGoogleGenerativeAI(
                    model=settings.gemini_model,
                    google_api_key=settings.gemini_api_key.get_secret_value(),
                    temperature=0,
                    max_tokens=800,
                )
            if settings.groq_api_key:
                self.groq = ChatGroq(
                    model=settings.groq_model,
                    api_key=settings.groq_api_key.get_secret_value(),
                    temperature=0,
                    max_tokens=800,
                )
        except Exception:
            # Keep provider construction optional and avoid logging details
            # which can include endpoint or credential context.
            self.gemini = None
            self.groq = None
            self.availability_reason = "provider_initialization_failed"
        else:
            if self.gemini or self.groq:
                self.availability_reason = "ready"

    def _deterministic_result(self, *, rationale: str, fallback_used: bool = True) -> Dict[str, Any]:
        return {
            "confidence": 0.86,
            "benefit": 0.80,
            "rationale": rationale,
            "ranked_candidate_ids": [],
            "confidence_adjustment": 0.0,
            "expected_benefit": "Deterministic policy estimate retained.",
            "ai_planner_used": False,
            "model": None,
            "fallback_used": fallback_used,
        }

    @staticmethod
    def _validated_planner_result(
        text: str,
        candidate_ids: List[str],
    ) -> Dict[str, Any]:
        parsed = _safe_json(text)
        if not parsed:
            raise ValueError("planner did not return a JSON object")
        advisory = PlannerAdvisory(**parsed)
        allowed = set(candidate_ids)
        ranked = advisory.ranked_candidate_ids
        if len(ranked) != len(set(ranked)):
            raise ValueError("planner returned duplicate candidate identifiers")
        if any(candidate_id not in allowed for candidate_id in ranked):
            raise ValueError("planner returned an unknown candidate identifier")
        return {
            "confidence": max(0.0, min(1.0, 0.86 + advisory.confidence_adjustment)),
            "benefit": 0.80,
            "rationale": advisory.rationale or "Bounded model advisory accepted.",
            "ranked_candidate_ids": ranked,
            "confidence_adjustment": advisory.confidence_adjustment,
            "expected_benefit": advisory.expected_benefit,
        }

    async def assess(
        self,
        incident: Incident,
        devices: List[DeviceStatus],
        actions: List[Action],
        candidate_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        candidate_ids = candidate_ids or _candidate_ids(actions)
        payload = {
            "incident": incident.model_dump(),
            "devices": [d.model_dump() for d in devices],
            "candidates": [
                {"candidate_id": candidate_id, "kind": action.kind, "device_id": action.device_id}
                for candidate_id, action in zip(candidate_ids, actions)
            ],
            "instruction": (
                "Return JSON only matching PlannerAdvisory: ranked_candidate_ids, "
                "confidence_adjustment (-0.05..0.05), expected_benefit, rationale. "
                "Only rank supplied candidate IDs. Do not propose actions, parameters, "
                "identities, cells, slices, or KPI values."
            ),
        }
        prompt = json.dumps(payload, default=str)
        providers = [
            ("gemini", self.settings.gemini_model, self.gemini),
            ("groq", self.settings.groq_model, self.groq),
        ]
        attempted_primary = False
        for provider_name, model_name, model in providers:
            if model is None:
                continue
            attempted_primary = attempted_primary or provider_name == "gemini"
            try:
                response = await asyncio.wait_for(
                    model.ainvoke(prompt), timeout=self.settings.ai_provider_timeout_seconds
                )
                text = response.content if hasattr(response, "content") else str(response)
                result = self._validated_planner_result(text, candidate_ids)
                result.update({
                    "ai_planner_used": True,
                    "model": model_name,
                    # Groq succeeding after an attempted Gemini is a truthful fallback.
                    "fallback_used": provider_name != "gemini" and attempted_primary,
                })
                return result
            except (asyncio.TimeoutError, ValidationError, ValueError, TypeError):
                logger.warning("%s planner advisory unavailable or invalid; trying safe fallback", provider_name)
            except Exception:
                # Provider errors can contain request details; do not log them verbatim.
                logger.warning("%s planner advisory failed; trying safe fallback", provider_name)
        return self._deterministic_result(
            rationale="Hosted model advisory unavailable or invalid; deterministic quality policy used."
        )


class ToolFactory:
    def __init__(self, client: BaseNokiaClient):
        self.client = client

    def build(self) -> Dict[str, Any]:
        async def congestion_insights(cell_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
            return [x.model_dump() for x in await self.client.congestion_insights(cell_ids)]

        async def device_status(device_ids: List[str]) -> List[Dict[str, Any]]:
            return [x.model_dump() for x in await self.client.device_status(device_ids)]

        async def location_retrieval(device_ids: List[str]) -> List[Dict[str, Any]]:
            return [x.model_dump() for x in await self.client.location_retrieval(device_ids)]

        async def geofence_subscribe(device_id: str, polygon_id: str) -> Dict[str, Any]:
            return (await self.client.create_geofence(device_id, polygon_id)).model_dump()

        async def qos_request(device_id: str, profile: str, duration_seconds: int = 300) -> Dict[str, Any]:
            return (await self.client.request_qos(device_id, profile, duration_seconds)).model_dump()

        async def qos_release(session_id: str) -> bool:
            return await self.client.release_qos(session_id)

        async def slice_attach(device_id: str, slice_id: str) -> Dict[str, Any]:
            return (await self.client.attach_slice(device_id, slice_id)).model_dump()

        async def slice_detach(device_id: str, slice_id: str) -> Dict[str, Any]:
            return (await self.client.detach_slice(device_id, slice_id)).model_dump()

        async def geofence_delete(subscription_id: str) -> bool:
            return await self.client.delete_geofence(subscription_id)

        

        return {
            "congestion_insights": congestion_insights,
            "device_status": device_status,
            "location_retrieval": location_retrieval,
            "geofence_subscribe": geofence_subscribe,
            "qos_request": qos_request,
            "qos_release": qos_release,
            "slice_attach": slice_attach,
            "slice_detach": slice_detach,
            "geofence_delete": geofence_delete,

        }


if CREWAI_AVAILABLE:
    class HarisNokiaTool(BaseTool):
        name: str
        description: str
        handler: Any

        def _run(self, **kwargs: Any) -> str:
            result = self.handler(**kwargs)
            if asyncio.iscoroutine(result):
                result = asyncio.run(result)
            return json.dumps(result, default=str)

    def build_crewai_tools(tool_factory: ToolFactory) -> List[Any]:
        tools = tool_factory.build()
        specs = [
            ("congestion_insights", "CAMARA Congestion Insights: read categorical provider congestion evidence; numeric fixture KPIs remain separate."),
            ("device_status", "CAMARA Device Status: read provider reachability; roaming, battery, tier and cell are HARIS metadata."),
            ("location_retrieval", "CAMARA Location Retrieval: retrieve locations for configured critical assets where provider evidence is available."),
            ("geofence_subscribe", "CAMARA Geofencing: create an event-driven storm impact subscription."),
            ("qos_request", "CAMARA Quality on Demand: request a bounded QoS profile for a device."),
            ("qos_release", "CAMARA Quality on Demand: release a QoS session after conditions normalize."),
            ("slice_attach", "CAMARA Network Slice Management: attach a critical device to a protected slice."),
            ("slice_detach", "CAMARA Network Slice Management: detach a device from a protected slice."),
            ("geofence_delete", "CAMARA Geofencing: remove an event-driven storm impact subscription."),
           
        ]
        return [HarisNokiaTool(name=name, description=description, handler=tools[name]) for name, description in specs]


class HarisAgentSystem:
    def __init__(self, client: BaseNokiaClient, memory: Optional[MemoryStore] = None, settings: Optional[AppSettings] = None):
        self.settings = settings or get_settings()
        self.client = client
        self.memory = memory or MemoryStore(self.settings)
        self.playbooks = PlaybookEngine(self.settings, client, self.memory)
        self.tools = ToolFactory(client).build()
        self.reasoning = ReasoningRouter(self.settings)
        self.forecaster = RiskForecaster()
        self.engineers = AuthorizedEngineerRegistry(self.settings.authorized_engineer_registry_path)
        self._latest_dispatch: Dict[str, Any] = {}
        self._latest_cycle: Dict[str, Any] = {}
        # Safe, symbolic progress marker consumed only by the field-demo API
        # failure boundary.  It never contains provider or identity data.
        self.field_intervention_diagnostic_stage = "FIELD_CYCLE_EXECUTION"
        register_dispatch_resume_handler(self._resume_pending_dispatch)
        register_dispatch_verification_failure_handler(self._handle_number_verification_failure)
        self._cached_environment: Optional[bool] = None
        self._congestion_history: Dict[str, List[Dict[str, Any]]] = {}
        self._observation_store: Optional[Any] = None
        self.crewai_agents: Dict[str, Any] = {}
        self._crewai_init_reason = "missing_model_credentials"
        self._init_crewai_agents()
        self.graph = self._build_graph()

    @property
    def geofencing_monitoring_enabled(self) -> bool:
        """Backend-owned policy state consumed by the Streamlit supervisor."""
        return self.settings.geofencing_monitoring_enabled

    def set_geofencing_monitoring(self, enabled: bool) -> None:
        """Set the policy only; it never creates/deletes a Nokia subscription."""
        self.settings.geofencing_monitoring_enabled = bool(enabled)

    def set_observation_store(self, store: Any) -> None:
        """Attach backend read-only evidence; it never grants action authority."""
        self._observation_store = store

    @property
    def current_dispatch_status(self) -> Dict[str, Any]:
        """Read-only, sanitized dispatch state for UI/API presentation."""
        return {
            key: value for key, value in self._latest_dispatch.items()
            if key != "authorization_url"
        }

    @property
    def dispatch_authorization_url(self) -> Optional[str]:
        """Transient consent handoff; deliberately excluded from audit/status."""
        return self._latest_dispatch.get("authorization_url")

    @property
    def current_cycle_status(self) -> Dict[str, Any]:
        """Sanitized summary for a remote supervisory UI, never an audit dump."""
        state = self._latest_cycle
        incident_id = (state.get("incident") or {}).get("incident_id") or self.current_dispatch_status.get("incident_id")
        return {
            "cycle_id": state.get("cycle_id"), "final_status": state.get("final_status"),
            "decision_status": state.get("decision_status"),
            "execution_context": state.get("execution_context"),
            "provenance": state.get("provenance"),
            "authority": state.get("authority"),
            "external_access_permitted": state.get("external_access_permitted"),
            "durable_domain_write": state.get("durable_domain_write"),
            "durable_history_write": state.get("durable_history_write"),
            "incident": state.get("incident", {}), "prediction": state.get("prediction", {}),
            # These are the structured authoritative facts needed by the
            # supervisory console; action/session internals remain excluded.
            "plan": state.get("plan", {}),
            "warden": state.get("warden", {}),
            "execution": {
                "executed": state.get("execution", {}).get("executed"),
                "reason": state.get("execution", {}).get("reason"),
                "actions": [
                    {key: value for key, value in action.items() if key not in {"session_id", "subscription_id"}}
                    for action in state.get("execution", {}).get("actions", [])
                ],
            },
            "verification": state.get("verification", {}),
            "rollback": state.get("rollback", {}),
            "learning": state.get("learning", {}),
            "congestion": state.get("congestion", []),
            "pre_execution_congestion": state.get("pre_execution_congestion", {}),
            "devices": state.get("devices", []),
            "trusted_dispatch": self.current_dispatch_status,
            "dispatch_history": [item.model_dump() for item in trusted_dispatch_history.for_incident(incident_id)] if incident_id else [],
            "field_intervention_evidence": state.get("field_intervention_evidence", {}),
            "active_playbook": state.get("active_playbook", {}),
            "trace": state.get("trace", []), "events": state.get("events", []),
        }

    @staticmethod
    def _supervisory_safe(value: Any) -> Any:
        """Defence in depth for data sent to the separate supervisory UI."""
        blocked = {
            "authorization_url", "oauth_state", "code", "access_token",
            "api_token", "client_secret", "phone_number",
            "consent_action_token", "workflow_session_token",
        }
        if isinstance(value, dict):
            return {
                key: HarisAgentSystem._supervisory_safe(item)
                for key, item in value.items()
                if key.lower() not in blocked
            }
        if isinstance(value, list):
            return [HarisAgentSystem._supervisory_safe(item) for item in value]
        return value

    @property
    def current_supervisory_status(self) -> Dict[str, Any]:
        """Backend-owned, sanitized state for a remote Streamlit supervisor.

        The single-instance prototype deliberately keeps this in the Render
        process.  It is not an OAuth handoff and never includes a consent URL.
        """
        cycle = self.current_cycle_status
        dispatch = cycle.get("trusted_dispatch", {})
        haris_state = (
            dispatch.get("status")
            if dispatch.get("status") in {"WAITING_FOR_IDENTITY_VERIFICATION", "MANUAL_INTERVENTION_REQUIRED"}
            else cycle.get("final_status") or "READY"
        )
        active = (
            cycle.get("incident", {})
            if dispatch.get("status") in {"WAITING_FOR_IDENTITY_VERIFICATION", "MANUAL_INTERVENTION_REQUIRED"}
            else {}
        )
        records = [self.memory.normalized_view(item) for item in self.memory.recent_incidents()]
        return self._supervisory_safe({
            "haris_state": haris_state,
            "cycle": cycle,
            "active_incident": active,
            "dispatch_history": cycle.get("dispatch_history", []),
            "audit": {
                "chain": self.memory.verify_audit_chain(),
                "persistence": self.memory.persistence_status,
                "records": records,
            },
        })

    async def _record_dispatch_transition(self, message: str) -> None:
        """Append a safe backend audit checkpoint after callback continuation.

        The original waiting cycle has already reached LEARN.  A callback is a
        later server-side transition, so it gets a new chained record rather
        than mutating the original record and invalidating its hash.
        """
        state = self._latest_cycle
        if not state.get("incident"):
            return
        self._trace(state, f"TRUST_CHECK: {message}")
        incident = Incident(**state["incident"])
        dispatch = self.current_dispatch_status
        record = IncidentMemory(
            incident_id=incident.incident_id,
            summary=f"Trusted Dispatch transition for {incident.incident_id}",
            storm_type="sandstorm",
            peak_congestion_level=incident.peak_congestion_level,
            peak_confidence_level=incident.peak_confidence_level,
            affected_cells=incident.affected_cells,
            affected_devices=incident.affected_devices,
            actions=[item.get("kind") for item in state.get("plan", {}).get("actions", [])],
            executed_actions=[item.get("kind") for item in state.get("execution", {}).get("actions", [])],
            outcome=dispatch.get("status", state.get("final_status", "unknown")).lower(),
            cycle_id=state.get("cycle_id"),
            mode=self.settings.nac_mode,
            checkpoint_type="trusted_dispatch_transition",
            checkpoint_ordinal=len(trusted_dispatch_history.for_incident(incident.incident_id)),
            audit={
                "incident": state.get("incident", {}),
                "trusted_dispatch": dispatch,
                "dispatch_history": [item.model_dump() for item in trusted_dispatch_history.for_incident(incident.incident_id)],
                "events": state.get("events", []),
                "trace": state.get("trace", []),
                "final_status": state.get("final_status"),
            },
            verification=state.get("verification", {}),
            rollback=state.get("rollback", {}),
        )
        await self.memory.remember_incident(record)

    def _init_crewai_agents(self) -> None:
        if not external_access_policy().allow_llm:
            self._crewai_init_reason = "runtime_policy_blocks_llm"
            return
        if not CREWAI_AVAILABLE:
            self._crewai_init_reason = "crewai_dependency_unavailable"
            logger.warning("CrewAI is not installed; deterministic role logic remains active")
            return
        gemini_key = self.settings.gemini_api_key.get_secret_value() if self.settings.gemini_api_key else None
        groq_key = self.settings.groq_api_key.get_secret_value() if self.settings.groq_api_key else None
        llm = None
        try:
            if gemini_key:
                llm = LLM(
                    model=f"gemini/{self.settings.gemini_model}", api_key=gemini_key,
                    temperature=0.0, max_tokens=300,
                    timeout=self.settings.ai_provider_timeout_seconds,
                )
            elif groq_key:
                llm = LLM(
                    model=f"groq/{self.settings.groq_model}", api_key=groq_key,
                    temperature=0.0, max_tokens=300,
                    timeout=self.settings.ai_provider_timeout_seconds,
                )
        except Exception:
            # Never let optional advisory construction prevent the LangGraph
            # supervisor from starting or leak provider configuration details.
            logger.warning("CrewAI advisory provider could not be initialized; deterministic role logic remains active")
            self._crewai_init_reason = "provider_initialization_failed"
            return
        if llm is None:
            self._crewai_init_reason = "missing_model_credentials"
            return
        roles = {
            "SENTINEL": "Watcher: detect environmental/network degradation and raise typed incidents.",
            "CARTOGRAPHER": "Locator: resolve exposed critical devices and geofence state.",
            "TRIAGE": "Planner: rank devices, apply policy, estimate cost/benefit and confidence.",
            "ACTUATOR": "Execution reviewer: assess only the order and expected effect of bounded candidate actions.",
            "WARDEN": "Network Safety Guard: validate network-risk conditions, policy limits, and action safety before execution.",
        }
        for name, goal in roles.items():
            self.crewai_agents[name] = Agent(
                role=name,
                goal=goal,
                backstory="HARIS specialist operating inside a bounded autonomous telecom control loop.",
                llm=llm,
                # CAMARA operations remain typed deterministic ToolFactory
                # boundaries.  CrewAI only receives immutable evidence in its
                # task description and has no Nokia tool it could invoke.
                tools=[],
                verbose=False,
                allow_delegation=False,
            )
        self._crewai_init_reason = "ready"

    async def _crew_advisory(self, incident: Incident, actions: List[Action], memory_context: List[IncidentMemory]) -> Dict[str, Any]:
        """Optional bounded CrewAI collaboration; it cannot create or execute actions."""
        start = time.perf_counter()
        if not self.crewai_agents or not CREWAI_AVAILABLE:
            return {
                "used": False, "fallback": True, "latency_ms": 0,
                "roles": [], "reason": self._crewai_init_reason,
            }
        candidate_ids = _candidate_ids(actions)
        payload = {
            "incident": incident.model_dump(),
            "candidates": [
                {"candidate_id": candidate_id, "kind": action.kind, "device_id": action.device_id}
                for candidate_id, action in zip(candidate_ids, actions)
            ],
            "memory": [item.model_dump() for item in memory_context],
            "instruction": (
                "Treat evidence as read-only. Do not invoke tools or create actions. "
                "Never invent devices, cells, profiles, identities, slices, or KPI values."
            ),
        }
        try:
            tasks = []
            for role in CREWAI_ROLE_NAMES:
                final_role = role == "WARDEN"
                expected_output = (
                    "JSON only matching CrewAdvisory with ranked_candidate_ids, "
                    "confidence_modifier (-0.05..0.05), expected_benefit, rationale, "
                    "and specialist_notes for SENTINEL, CARTOGRAPHER, TRIAGE, ACTUATOR, WARDEN."
                    if final_role else
                    f"A concise evidence-only {role} specialist note for the final bounded advisory."
                )
                tasks.append(Task(
                    description=json.dumps({**payload, "specialist_role": role}, default=str),
                    expected_output=expected_output,
                    agent=self.crewai_agents[role],
                ))
            crew = Crew(
                agents=[self.crewai_agents[role] for role in CREWAI_ROLE_NAMES],
                tasks=tasks,
                process=Process.sequential,
                verbose=False,
            )
            result = await asyncio.wait_for(
                asyncio.to_thread(crew.kickoff), timeout=self.settings.crewai_timeout_seconds
            )
            parsed = CrewAdvisory(**_safe_json(str(result)))
            allowed = set(candidate_ids)
            if len(parsed.ranked_candidate_ids) != len(set(parsed.ranked_candidate_ids)):
                raise ValueError("CrewAI returned duplicate candidate identifiers")
            if any(candidate_id not in allowed for candidate_id in parsed.ranked_candidate_ids):
                raise ValueError("CrewAI returned an unknown candidate identifier")
            notes = {key.upper(): value for key, value in parsed.specialist_notes.items() if key.upper() in CREWAI_ROLE_NAMES}
            if set(notes) != set(CREWAI_ROLE_NAMES):
                raise ValueError("CrewAI response omitted a required specialist note")
            parsed.specialist_notes = notes
            return {
                "used": True, "fallback": False,
                "latency_ms": round((time.perf_counter() - start) * 1000, 1),
                "roles": list(CREWAI_ROLE_NAMES), "advisory": parsed.model_dump(),
            }
        except (asyncio.TimeoutError, ValidationError, ValueError, TypeError):
            logger.warning("CrewAI advisory unavailable or invalid; deterministic triage retained")
        except Exception:
            # Providers may include request context in errors; avoid emitting it.
            logger.warning("CrewAI advisory failed; deterministic triage retained")
        return {
            "used": False, "fallback": True,
            "latency_ms": round((time.perf_counter() - start) * 1000, 1),
            "roles": [], "reason": "crew_execution_unavailable",
        }

    def _verification_route(
        self,
        state: HarisState,
    ) -> str:
        """
        Route the workflow after verification.

        Normal path:
            successful mitigation -> LEARN

        Failure path:
            failed mitigation -> ROLLBACK -> LEARN

        A successful rollback means the network was safely restored,
        even though the original mitigation did not improve the KPI.
        """

        verification = state.get(
            "verification",
            {},
        )

        verified = bool(
            verification.get(
                "verified",
                False,
            )
        )

        rollback_attempted = bool(
            state.get(
                "rollback_attempted",
                False,
            )
        )

        rollback = state.get(
            "rollback",
            {},
        )

        rollback_verified = bool(
            rollback.get(
                "rollback_verified",
                False,
            )
        )

        if verification.get("status") == "no_action_proposed":
            self._trace(
                state,
                "ROUTER: no network remediation was proposed; proceeding to LEARN",
            )
            return "learn"

        if verification.get("status") == "execution_failed":
            self._trace(
                state,
                "ROUTER: no network action executed; proceeding to LEARN without rollback",
            )
            return "learn"

        if verification.get("status") in {"live_read_only_proposal", "warden_rejected", "identity_verification_pending"}:
            self._trace(
                state,
                "ROUTER: consent pending; proceeding to LEARN checkpoint without rollback"
                if verification.get("status") == "identity_verification_pending"
                else "ROUTER: no write was attempted; proceeding to LEARN without rollback",
            )
            return "learn"

        # ---------------------------------------------------------
        # 1. Normal successful mitigation.
        # ---------------------------------------------------------
        if verified:
            
            self._trace(
                state,
                (
                    "ROUTER: mitigation verification passed; "
                    "proceeding to LEARN"
                ),
            )

            return "learn"

        # ---------------------------------------------------------
        # 2. Rollback has already been attempted.
        # ---------------------------------------------------------
        if rollback_attempted:

            if rollback_verified:
                self._trace(
                    state,
                    (
                        "ROUTER: mitigation failed but rollback "
                        "verified successfully; proceeding to LEARN"
                    ),
                )

            else:
                state["final_status"] = "rollback_failed"

                self._trace(
                    state,
                    (
                        "ROUTER: mitigation failed and rollback "
                        "verification failed; proceeding to LEARN"
                    ),
                )

            return "learn"

        # ---------------------------------------------------------
        # 3. First verification failure.
        # ---------------------------------------------------------
        

        self._trace(
            state,
            (
                "ROUTER: mitigation verification failed; "
                "routing to ROLLBACK"
            ),
        )

        return "rollback"

    def _build_graph(self):

        graph = StateGraph(HarisState)

        graph.add_node("sentinel", self._sentinel)
        graph.add_node("cartographer", self._cartographer)
        graph.add_node("triage", self._triage)
        graph.add_node("actuator_plan", self._actuator_plan)
        graph.add_node("deterministic_normalization", self._deterministic_normalization)
        graph.add_node("warden", self._warden)
        graph.add_node("actuator", self._actuator)
        graph.add_node("verify", self._verify)
        graph.add_node("rollback", self._rollback)
        graph.add_node("learn", self._learn)
        graph.add_edge(START, "sentinel")
        graph.add_edge(
            "sentinel",
            "cartographer",
        )
        graph.add_edge(
            "cartographer",
            "triage",
        )
        graph.add_edge("triage", "actuator_plan")
        graph.add_edge("actuator_plan", "deterministic_normalization")
        graph.add_edge(
            "deterministic_normalization",
            "warden",
        )
        graph.add_conditional_edges(
            "warden",
            self._post_warden_route,
            {
                "actuator": "actuator",
                "planning_complete": END,
            },
        )
        graph.add_edge(
            "actuator",
            "verify",
        )
        # ---------------------------------------------------------
        # Verification decides whether rollback is required.
        # ---------------------------------------------------------
        graph.add_conditional_edges(
            "verify",
            self._verification_route,
            {
                "learn": "learn",
                "rollback": "rollback",
            },
        )
        # Rollback must be verified again.
        graph.add_edge(
            "rollback",
            "verify",
        )
        graph.add_edge(
            "learn",
            END,
        )
        return graph.compile()

    @staticmethod
    def _post_warden_route(state: HarisState) -> str:
        return "planning_complete" if state.get("durable_planning_only") else "actuator"

    def _trace(self, state: HarisState, message: str) -> None:
        state.setdefault("trace", []).append(f"{time.strftime('%H:%M:%S')} | {message}")
        stage = message.split(":", 1)[0].strip().upper()
        pending = "pending" in message.lower() or "waiting for consent" in message.lower()
        actuator_success = any(token in message.lower() for token in (" created", " attached", " detached", " released", " deleted", " executed"))
        event_type = {
            "SENTINEL": "SENSE", "CARTOGRAPHER": "REASON", "TRIAGE": "ACTION_PROPOSED",
            "WARDEN": "WARDEN_APPROVED" if "approved" in message else "WARDEN_BLOCKED",
            "ACTUATOR": "ACTION_EXECUTED" if actuator_success else "ACTION_FAILED",
            "VERIFY": "VERIFY", "ROLLBACK": "ROLLBACK", "LEARN": "LEARN",
        }.get(stage, stage)
        if pending and stage in {"WARDEN", "ACTUATOR", "TRUST_CHECK"}:
            event_type = "TRUST_CHECK"
        state.setdefault("events", []).append({"timestamp": time.time(), "incident_id": state.get("incident", {}).get("incident_id"), "type": event_type, "agent": stage, "message": message, "status": "PENDING" if pending else "BLOCKED" if "blocked" in message or "rejected" in message else "OK", "metadata": {}})


    async def _actuator_plan(self, state: HarisState) -> HarisState:
        """Bounded feasibility review; it never invokes a provider.

        The established execution graph continues into its normal ACTUATOR
        after WARDEN.  Durable Phase 7B stops at WARDEN, so this explicit role
        records candidate feasibility before deterministic normalization and
        cannot create, execute, or mark a Nokia resource successful.
        """
        if not state.get("durable_planning_only"):
            return state
        actions = list((state.get("plan") or {}).get("actions") or [])
        feasible = 0
        for action in actions:
            if not self.client.action_safety_error(
                str(action.get("kind") or ""), dict(action.get("parameters") or {})
            ):
                feasible += 1
        state["actuator_plan"] = {
            "candidate_count": len(actions),
            "adapter_feasible_count": feasible,
            "provider_execution_permitted": False,
        }
        self._trace(
            state,
            "ACTUATOR_PLAN: reviewed deterministic candidate feasibility; provider execution prohibited",
        )
        return state


    async def _deterministic_normalization(self, state: HarisState) -> HarisState:
        """Normalize only HARIS-produced candidates before WARDEN.

        The normal execution graph retains its established behavior.  Durable
        Phase 7B planning additionally removes candidates whose capability
        truth cannot be proven from current durable/configured evidence.  An
        advisory model never supplies an action, target, or parameter here.
        """
        if not state.get("durable_planning_only"):
            return state
        plan_data = state.get("plan") or {}
        plan = RemediationPlan(**plan_data)
        context = state.get("durable_reasoning_context") or {}
        slice_status = str((context.get("capability_state") or {}).get("slice_status") or "UNAVAILABLE").upper()
        durable_policy = state.get("durable_policy") or {}
        mutation_scope_enforced = bool(
            durable_policy.get("mutation_device_allowlist_enforced")
        )
        allowed_mutation_devices = {
            str(device_id)
            for device_id in durable_policy.get("allowed_mutation_device_ids") or []
        }
        normalized: List[Action] = []
        rejected: List[Dict[str, str]] = []
        for index, action in enumerate(plan.actions):
            candidate_id = f"candidate-{index}"
            reason = self.client.action_safety_error(action.kind, action.parameters)
            if mutation_scope_enforced and action.device_id not in allowed_mutation_devices:
                reason = "candidate is outside the durable mutation scope"
            if action.kind == "slice_attach" and slice_status != "OPERATING":
                reason = "protected slice is not durably proven OPERATING"
            if reason:
                rejected.append({"candidate_id": candidate_id, "kind": action.kind, "reason": reason})
                continue
            normalized.append(action)

        selected = []
        for action in normalized:
            if action.device_id not in selected:
                selected.append(action.device_id)
        cost = sum(
            0.75 if action.parameters.get("profile") == "guaranteed" else 0.20
            for action in normalized if action.kind == "qos"
        )
        device_count = len(state.get("devices") or [])
        blast_radius = min(1.0, len(selected) / max(1, device_count))
        approval = (
            blast_radius > self.settings.guardrails.human_approval_blast_radius
            or plan.confidence < self.settings.guardrails.minimum_confidence
            or cost > self.settings.guardrails.qos_spend_ceiling_usd
        )
        state["plan"] = {
            **plan.model_dump(),
            "actions": [action.__dict__ for action in normalized],
            "selected_device_ids": selected,
            "expected_cost_usd": cost,
            "blast_radius": blast_radius,
            "approval_required": approval,
            "candidate_ids": _candidate_ids(normalized),
            "rejected_candidates": rejected,
        }
        self._trace(
            state,
            "DETERMINISTIC_NORMALIZATION: "
            f"allowed={len(normalized)} rejected={len(rejected)}; provider execution prohibited",
        )
        return state

    async def _warden(self, state: HarisState) -> HarisState:
        """
        Network Safety Guard.

        WARDEN validates the proposed network remediation plan
        before HARIS allows the actuator to execute it.

        It validates only network-risk conditions and bounded action safety.
        """

        plan_data = state.get("plan")

        if not plan_data:
            state["warden"] = {
                "verified": False,
                "required": True,
                "reason": "network_plan_missing",
            }

            self._trace(
                state,
                "WARDEN: network plan missing; execution rejected",
            )

            return state

        try:
            plan = RemediationPlan(**plan_data)
            guardrails = self.settings.guardrails
            action_errors = {
                f"{action.kind}:{action.device_id}": error
                for action in plan.actions
                if (
                    error := self.client.action_safety_error(
                        action.kind,
                        action.parameters,
                    )
                )
            }

            checks = {
                "confidence_ok": (
                    plan.confidence
                    >= guardrails.minimum_confidence
                ),
                "blast_radius_ok": (
                    plan.blast_radius
                    <= guardrails.human_approval_blast_radius
                ),
                "cost_ok": (
                    plan.expected_cost_usd
                    <= guardrails.qos_spend_ceiling_usd
                ),
                "actions_present": bool(plan.actions),
                "unique_device_count_within_limit": (
                    len({action.device_id for action in plan.actions})
                    <= guardrails.max_devices_reconfigured_per_cycle
                ),
                "actions_belong_to_selected_devices": (
                    plan.selected_device_ids is None
                    or all(action.device_id in set(plan.selected_device_ids) for action in plan.actions)
                ),
                "action_kinds_allowed": all(
                    action.kind in {"qos", "slice_attach", "geofence"}
                    for action in plan.actions
                ),
                "no_duplicate_equivalent_actions": len({
                    (action.kind, action.device_id, json.dumps(action.parameters, sort_keys=True, default=str))
                    for action in plan.actions
                }) == len(plan.actions),
                "action_safety_ok": (
                    not action_errors
                    and all(
                    (
                        action.kind == "qos"
                        and action.parameters.get("profile")
                        in {"guaranteed", "low-bandwidth", "emergency-only"}
                        and bool(action.parameters.get("duration_seconds"))
                    )
                    or (
                        action.kind == "slice_attach"
                        and bool(action.parameters.get("slice_id"))
                    )
                    or (
                        action.kind == "geofence"
                        and bool(action.parameters.get("polygon_id"))
                    )
                    for action in plan.actions
                    )
                ),
            }

            if state.get("durable_planning_only"):
                durable_policy = state.get("durable_policy") or {}
                checks.update({
                    "durable_context_current": bool(durable_policy.get("incident_current")),
                    "durable_cost_ok": (
                        float(durable_policy.get("incident_cost_total_usd", 0.0))
                        + plan.expected_cost_usd
                        <= float(durable_policy.get("cost_ceiling_usd", guardrails.qos_spend_ceiling_usd))
                    ),
                    "resource_ownership_ok": not bool(durable_policy.get("resource_conflicts")),
                    "mutation_scope_ok": (
                        not bool(durable_policy.get("mutation_device_allowlist_enforced"))
                        or all(
                            action.device_id
                            in {
                                str(device_id)
                                for device_id in durable_policy.get("allowed_mutation_device_ids") or []
                            }
                            for action in plan.actions
                        )
                    ),
                    "provider_execution_prohibited": True,
                })

            safe = all(checks.values())

            # Only a typed physical-intervention requirement enters this branch.
            # Routine autonomous QoD/geofence/slice remediation never reaches
            # Number Verification or SIM Swap.
            if state.get("field_intervention_required"):
                self._trace(
                    state,
                    "FIELD_INTERVENTION_REQUIRED: simulated fixture site condition requires an authorized engineer; network-only remediation is insufficient",
                )
                # Durable planning consumes only server-authoritative trust
                # evidence already supplied in the bounded context.  It never
                # starts OAuth or calls SIM Swap while evaluating a plan.
                if state.get("durable_planning_only"):
                    trust = dict(
                        (state.get("durable_reasoning_context") or {}).get("trusted_dispatch")
                        or {
                            "decision": "BLOCK",
                            "status": "IDENTITY_VERIFICATION_REQUIRED",
                            "number_verified": False,
                            "recent_sim_swap": None,
                            "reason": "Fresh server-authoritative trust evidence is unavailable.",
                        }
                    )
                else:
                    trust = await self._evaluate_field_intervention(state)
                state["trusted_dispatch"] = trust
                self._trace(state, f"TRUST_CHECK: decision={trust['decision']}; status={trust['status']}")
                if trust["decision"] != "ALLOW":
                    safe = False
                    checks["trusted_dispatch_ok"] = False

            pending_identity = (
                state.get("trusted_dispatch", {}).get("status")
                in {"WAITING_FOR_IDENTITY_VERIFICATION", "IDENTITY_VERIFICATION_REQUIRED"}
            )
            dispatch_blocked = (
                bool(state.get("field_intervention_required"))
                and not pending_identity
                and state.get("trusted_dispatch", {}).get("decision") == "BLOCK"
            )

            state["warden"] = {
                "verified": safe,
                "required": True,
                "safety_checks": checks,
                "confidence": plan.confidence,
                "blast_radius": plan.blast_radius,
                "expected_cost_usd": plan.expected_cost_usd,
                "selected_device_ids": plan.selected_device_ids or sorted({action.device_id for action in plan.actions}),
                "approval_required": plan.approval_required,
                "action_errors": action_errors,
                "capability_report": self.client.capability_report(),
                "execution_authority": "PLAN_ONLY" if state.get("durable_planning_only") else "EXECUTION_GATE",
                "provider_execution_permitted": False if state.get("durable_planning_only") else safe,
                "reason": "network_action_safe" if safe else (
                    "identity_verification_pending" if pending_identity else
                    "trusted_dispatch_blocked" if dispatch_blocked else
                    "network_safety_constraints_failed"
                ),
            }

            if pending_identity:
                self._trace(state, "WARDEN: identity/trust authorization pending; network mutations paused pending engineer consent")
            elif dispatch_blocked:
                self._trace(state, f"WARDEN: trusted dispatch blocked; {state['trusted_dispatch'].get('reason', 'trust policy denied dispatch')}")
            else:
                self._trace(
                    state,
                    (
                        "WARDEN: network safety "
                        f"{'approved' if safe else 'rejected'}; "
                        f"confidence={plan.confidence:.2f}, "
                        f"blast_radius={plan.blast_radius:.2f}, "
                        f"cost=${plan.expected_cost_usd:.2f}"
                    ),
                )

            return state

        except Exception:
            logger.warning("WARDEN network safety validation failed; details suppressed")

            state["warden"] = {
                "verified": False,
                "required": True,
                "reason": "network_safety_validation_error",
                "error": "network_safety_validation_error",
            }

            self._trace(
                state,
                "WARDEN: network safety validation failed; execution rejected",
            )

            return state
    
    async def _dust_advisory(self, fallback: Optional[bool]) -> tuple[bool, str]:
        if not external_access_policy().allow_external_http:
            if self._cached_environment is not None:
                return self._cached_environment, EnvironmentalSource.CACHED.value
            return bool(fallback), EnvironmentalSource.FIXTURE.value if fallback is not None else EnvironmentalSource.UNAVAILABLE.value
        url = self.settings.public_dust_feed_url
        if not url:
            return bool(fallback), EnvironmentalSource.FIXTURE.value if fallback is not None else EnvironmentalSource.UNAVAILABLE.value
        try:
            import httpx
            async with httpx.AsyncClient(timeout=3.0) as http:
                response = await http.get(url)
                response.raise_for_status()
                data = response.json()
            if isinstance(data, dict):
                value = bool(data.get("dust_advisory", data.get("dust", False)))
                self._cached_environment = value
                return value, EnvironmentalSource.LIVE.value
            raise ValueError("Environmental feed must return an object")
        except Exception:
            logger.warning("Dust advisory feed unavailable; provider details suppressed")
            if self._cached_environment is not None:
                return self._cached_environment, EnvironmentalSource.CACHED.value
            return bool(fallback), EnvironmentalSource.FIXTURE.value if fallback is not None else EnvironmentalSource.UNAVAILABLE.value

    async def _sentinel(self, state: HarisState) -> HarisState:
        """
        Sentinel collects factual network evidence from Nokia/CAMARA.

        Nokia is the source of truth for congestion observations.
        HARIS derives only deterministic incident information from
        those observations; it does not invent numeric congestion,
        latency, or prediction values.
        """

        if state.get("durable_planning_only"):
            context = state.get("durable_reasoning_context") or {}
            state.update({
                "dust_advisory": bool(context.get("dust_advisory", False)),
                "environmental_source": str(context.get("environmental_source") or "UNAVAILABLE"),
                "congestion": list(context.get("congestion") or []),
                "devices": list(context.get("devices") or []),
                "locations": list(context.get("locations") or []),
                "incident": dict(context.get("agent_incident") or {}),
                "prediction": dict(context.get("prediction") or {
                    "predicted_risk_level": "UNAVAILABLE",
                    "confidence": None,
                    "degradation_probability": None,
                    "input_provenance": "UNAVAILABLE",
                }),
                "congestion_observed_at": context.get("source_timestamp"),
                "field_intervention_required": bool(context.get("field_intervention_required", False)),
                "field_intervention_site": context.get("field_intervention_site"),
                "field_intervention_skills": list(context.get("field_intervention_skills") or []),
                "field_intervention_reason": context.get("field_intervention_reason"),
                "field_intervention_evidence": dict(context.get("field_intervention_evidence") or {}),
            })
            if not state["incident"] or not state["congestion"]:
                raise RuntimeError("durable reasoning context lacks actionable incident evidence")
            self._trace(
                state,
                "SENTINEL: interpreted current durable evidence; "
                f"provenance={context.get('provenance', 'UNAVAILABLE')}; "
                f"unavailable_evidence={len(context.get('unavailable_evidence') or [])}; "
                "no provider read performed",
            )
            return state

        self._trace(
            state,
            "SENTINEL: sensing Nokia congestion, device status, and dust advisory",
        )

        isolated_fixture_demo = bool(state.get("isolated_fixture_demo"))
        if isolated_fixture_demo:
            state["dust_advisory"] = bool(state.get("dust_advisory", True))
            state["environmental_source"] = EnvironmentalSource.FIXTURE.value
        else:
            state["dust_advisory"], state["environmental_source"] = await self._dust_advisory(
                state.get("dust_advisory", True)
            )

        # Prefer a fresh, backend-authoritative observation snapshot when the
        # separate poller is enabled.  It is factual source data, never a
        # synthetic telemetry stream.  Missing capability data remains absent
        # and the regular safe client read supplies only what is unavailable.
        snapshot = None if isolated_fixture_demo else (
            state.get("observation_snapshot")
            or (self._observation_store.latest_fresh() if self._observation_store else None)
        )
        if snapshot and isinstance(snapshot.get("congestion"), list):
            congestion = [CongestionReading(**item) for item in snapshot["congestion"]]
            scope = set(state.get("incident_scope_cells") or [])
            if scope:
                congestion = [item for item in congestion if item.cell_id in scope]
            observed_at = float(snapshot["observed_at"])
            self._trace(state, "SENTINEL: using fresh backend Nokia observation snapshot")
        else:
            congestion = await self.client.congestion_insights()
            observed_at = time.time()

        if snapshot and isinstance(snapshot.get("devices"), list):
            devices = [DeviceStatus(**item) for item in snapshot["devices"]]
            scope = set(state.get("incident_scope_cells") or [])
            if scope:
                devices = [item for item in devices if item.cell_id in scope]
        else:
            devices = await self.client.device_status(self.settings.registered_devices)

        state["congestion"] = [
            x.model_dump()
            for x in congestion
        ]
        # Factual HARIS observation history used for sustained-congestion
        # policy. It does not synthesize any Nokia KPI values.
        for reading in congestion:
            samples = self._congestion_history.setdefault(reading.cell_id, [])
            samples.append({"observed_at": observed_at, "congestion_level": reading.congestion_level})
            del samples[:-64]
        state["congestion_observed_at"] = observed_at

        state["devices"] = [
            x.model_dump()
            for x in devices
        ]

        prediction = self.forecaster.predict(congestion, state["dust_advisory"], state["environmental_source"])
        prediction = prediction.model_copy(update={
            "input_provenance": "FIXTURE_SIMULATED" if self.settings.nac_mode == "fixture" else "NOKIA_LIVE"
        })
        state["prediction"] = prediction.model_dump()

        if not congestion:
            raise RuntimeError(
                "Nokia returned no congestion observations; "
                "HARIS cannot create a network incident without "
                "current network evidence."
            )

        # ---------------------------------------------------------
        # 3. Determine the highest Nokia congestion level
        #
        # This ordering is a HARIS policy interpretation.
        # It does NOT convert Nokia levels into percentages.
        # ---------------------------------------------------------
        severity_order = {
            "None": 0,
            "Low": 1,
            "Medium": 2,
            "High": 3,
        }

        unknown_levels = [
            x.congestion_level
            for x in congestion
            if x.congestion_level not in severity_order
        ]

        if unknown_levels:
            raise RuntimeError(
                "Nokia returned unsupported congestion level(s): "
                f"{sorted(set(unknown_levels))}"
            )

        peak = max(
            congestion,
            key=lambda x: (
                severity_order[x.congestion_level],
                x.confidence_level,
            ),
        )

        peak_level = peak.congestion_level
        peak_confidence = peak.confidence_level

        # ---------------------------------------------------------
        # 4. Affected cells
        #
        # Medium and High are actionable congestion levels. Low is retained
        # as network evidence but does not expand the incident blast radius.
        # ---------------------------------------------------------
        registered_asset_cells = {device.cell_id for device in devices}
        affected_cells = sorted(
            {
                x.cell_id
                for x in congestion
                if (
                    severity_order[x.congestion_level] >= 2
                    and x.cell_id in registered_asset_cells
                )
            }
        )

        # ---------------------------------------------------------
        # 5. Map affected cells to registered devices
        # ---------------------------------------------------------
        affected_devices = sorted(
            {
                x.device_id
                for x in devices
                if x.cell_id in affected_cells
            }
        )

        # ---------------------------------------------------------
        # 6. Deterministic HARIS quality classification
        # ---------------------------------------------------------
        quality = self.settings.quality_matrix.classify(
            peak_level
        )

        observed_percentages = [
            reading.congestion_pct
            for reading in congestion
            if reading.congestion_pct is not None
        ]
        incident = Incident(
            incident_id=state.get("incident_id") or f"inc-{uuid.uuid4().hex[:12]}",
            storm_advisory=state.get("dust_advisory", True),
            peak_congestion_level=peak_level,
            peak_confidence_level=peak_confidence,
            max_congestion_pct=(max(observed_percentages) if observed_percentages else None),
            affected_cells=affected_cells,
            affected_devices=affected_devices,
            severity=quality,
        )

        state["incident"] = incident.model_dump()

        self._trace(
            state,
            (
                "SENTINEL: incident "
                f"{incident.incident_id} "
                f"severity={incident.severity.value}, "
                f"peak_congestion={incident.peak_congestion_level}, "
                f"confidence={incident.peak_confidence_level}, "
                f"cells={len(incident.affected_cells)}, "
                f"devices={len(incident.affected_devices)}"
            ),
        )
        self._trace(
            state,
            f"SENTINEL FORECAST: {prediction.predicted_risk_level} risk in "
            f"{prediction.horizon_minutes}m, confidence={prediction.confidence:.2f}",
        )

        return state

    async def _evaluate_field_intervention(self, state: HarisState) -> Dict[str, Any]:
        """Select an engineer deterministically and apply the existing trust gate.

        Number Verification remains consent-bound: missing fresh server evidence
        pauses the incident instead of inventing an approval or contacting Nokia.
        A later callback records the receipt; a resumed cycle re-enters here.
        """
        self.field_intervention_diagnostic_stage = "FIELD_ENGINEER_SELECTION"
        incident = state.get("incident", {})
        incident_id = incident.get("incident_id", state.get("cycle_id", "unknown"))
        site = state.get("field_intervention_site") or (incident.get("affected_cells") or ["unknown"])[0]
        skills = state.get("field_intervention_skills") or ["tower-inspection"]
        attempted = {item.engineer_id for item in trusted_dispatch_history.for_incident(incident_id)}
        candidates = [item for item in self.engineers.eligible(site=site, required_skills=skills) if item.engineer_id not in attempted]
        candidates = candidates[:self.settings.trusted_dispatch_max_attempts]
        if not candidates:
            return {"decision": "BLOCK", "status": "NO_ELIGIBLE_ENGINEER", "reason": "No eligible authorised engineer is available.", "attempts": len(attempted)}

        engineer = candidates[0]
        base = {"incident_id": incident_id, "engineer_id": engineer.engineer_id, "engineer_name": engineer.name, "masked_phone_number": mask_phone_number(engineer.phone_number), "site": site, "intervention_reason": state.get("field_intervention_reason") or "Physical inspection required.", "evidence_source": "FIXTURE / SIMULATED DEMO" if self.settings.nac_mode == "fixture" else "OPERATIONAL POLICY"}
        if not verified_identities.is_fresh(engineer.phone_number, self.settings.trusted_dispatch_verification_ttl_seconds):
            self.field_intervention_diagnostic_stage = "FIELD_PENDING_STATE"
            pending = pending_dispatches.create(
                incident_id=incident_id, engineer_id=engineer.engineer_id, phone_number=engineer.phone_number,
                site=site, intervention_type="physical_inspection", ttl_seconds=self.settings.trusted_dispatch_verification_ttl_seconds,
            )
            trusted_dispatch_history.record(DispatchAttempt(
                incident_id=incident_id, engineer_id=engineer.engineer_id,
                masked_phone_number=mask_phone_number(engineer.phone_number), site=site,
                intervention_type="physical_inspection", verification_status="WAITING_FOR_IDENTITY_VERIFICATION",
                reason="Fresh Number Verification consent is required before dispatch.", final_dispatch_status="PENDING",
            ))
            try:
                self.field_intervention_diagnostic_stage = "FIELD_NUMBER_VERIFICATION_START"
                started = await start_number_verification_for_dispatch(pending, self.settings)
                # The authorization URL is transient UI handoff only; it is not
                # written to trace, memory, events, or audit.
                self._latest_dispatch = {**base, "pending_id": pending.pending_id, "decision": "BLOCK", "status": "WAITING_FOR_IDENTITY_VERIFICATION", "number_verified": False, "recent_sim_swap": None, "reason": "Fresh Number Verification consent is required before dispatch.", "authorization_url": started["authorization_url"]}
            except Exception:
                pending_dispatches.complete(pending.pending_id, "BLOCKED")
                self._latest_dispatch = {**base, "pending_id": pending.pending_id, "decision": "BLOCK", "status": "WAITING_FOR_IDENTITY_VERIFICATION", "number_verified": False, "recent_sim_swap": None, "reason": "Number Verification authorization is unavailable; dispatch remains fail-closed."}
            return self.current_dispatch_status

        trust = await evaluate_trusted_dispatch_phone(engineer.phone_number, self.settings)
        trusted_dispatch_history.record(DispatchAttempt(
            incident_id=incident_id, engineer_id=engineer.engineer_id,
            masked_phone_number=mask_phone_number(engineer.phone_number), site=site,
            intervention_type="physical_inspection", verification_status="VERIFIED",
            sim_swap_status=("RECENT_SWAP" if trust.get("recent_sim_swap") else "NO_RECENT_SWAP" if trust.get("recent_sim_swap") is False else "UNAVAILABLE"),
            warden_decision=trust["decision"], reason=trust["reason"],
            final_dispatch_status="APPROVED" if trust["decision"] == "ALLOW" else "BLOCKED",
        ))
        return {**base, **trust, "status": "APPROVED" if trust["decision"] == "ALLOW" else "BLOCKED"}

    async def _resume_pending_dispatch(self, pending: PendingDispatch) -> None:
        """Callback-only continuation after atomic OAuth-state consumption."""
        trust = await evaluate_trusted_dispatch_phone(pending.phone_number, self.settings)
        status = "APPROVED" if trust["decision"] == "ALLOW" else "BLOCKED"
        pending_dispatches.complete(pending.pending_id, "COMPLETED" if status == "APPROVED" else "BLOCKED")
        trusted_dispatch_history.record(DispatchAttempt(
            incident_id=pending.incident_id, engineer_id=pending.engineer_id,
            masked_phone_number=mask_phone_number(pending.phone_number), site=pending.site,
            intervention_type=pending.intervention_type, verification_status="VERIFIED",
            sim_swap_status=("RECENT_SWAP" if trust.get("recent_sim_swap") else "NO_RECENT_SWAP" if trust.get("recent_sim_swap") is False else "UNAVAILABLE"),
            warden_decision=trust["decision"], reason=trust["reason"], final_dispatch_status=status,
        ))
        self._latest_dispatch = {"pending_id": pending.pending_id, "incident_id": pending.incident_id, "engineer_id": pending.engineer_id, "masked_phone_number": mask_phone_number(pending.phone_number), **trust, "status": status}
        if status == "BLOCKED":
            self._trace(self._latest_cycle, f"WARDEN: trusted dispatch blocked; {trust['reason']}")
            await self._start_fallback(pending, "Previous engineer blocked; awaiting fallback engineer consent.")
        await self._record_dispatch_transition(
            f"engineer={pending.engineer_id}; number_verification=VERIFIED; "
            f"sim_swap={trusted_dispatch_history.for_incident(pending.incident_id)[-1].sim_swap_status}; "
            f"warden={trust['decision']}"
        )

    async def _handle_number_verification_failure(self, pending: PendingDispatch) -> None:
        """Retire only the verified-false engineer before a fresh fallback."""
        pending_dispatches.complete(pending.pending_id, "BLOCKED")
        trusted_dispatch_history.record(DispatchAttempt(
            incident_id=pending.incident_id, engineer_id=pending.engineer_id,
            masked_phone_number=mask_phone_number(pending.phone_number), site=pending.site,
            intervention_type=pending.intervention_type, verification_status="NOT_VERIFIED",
            warden_decision="BLOCK", reason="Nokia Number Verification returned not verified.",
            final_dispatch_status="BLOCKED",
        ))
        self._latest_dispatch = {"pending_id": pending.pending_id, "incident_id": pending.incident_id, "engineer_id": pending.engineer_id, "masked_phone_number": mask_phone_number(pending.phone_number), "decision": "BLOCK", "status": "BLOCKED", "number_verified": False, "recent_sim_swap": None, "reason": "Nokia Number Verification returned not verified."}
        await self._start_fallback(pending, "Previous engineer was not verified; awaiting fallback engineer consent.")
        await self._record_dispatch_transition(
            f"engineer={pending.engineer_id}; number_verification=NOT_VERIFIED; warden=BLOCK"
        )

    async def _start_fallback(self, pending: PendingDispatch, reason: str) -> None:
        frontend_consent_tokens.invalidate_pending(pending.pending_id)
        attempted = {item.engineer_id for item in trusted_dispatch_history.for_incident(pending.incident_id)}
        candidates = [item for item in self.engineers.eligible(site=pending.site, required_skills=["tower-inspection"]) if item.engineer_id not in attempted]
        if len(attempted) >= self.settings.trusted_dispatch_max_attempts or not candidates:
            self._latest_dispatch["status"] = "MANUAL_INTERVENTION_REQUIRED"
            return
        fallback = candidates[0]
        next_pending = pending_dispatches.create(incident_id=pending.incident_id, engineer_id=fallback.engineer_id, phone_number=fallback.phone_number, site=pending.site, intervention_type=pending.intervention_type, ttl_seconds=self.settings.trusted_dispatch_verification_ttl_seconds)
        try:
            started = await start_number_verification_for_dispatch(next_pending, self.settings)
            self._latest_dispatch = {"pending_id": next_pending.pending_id, "incident_id": pending.incident_id, "engineer_id": fallback.engineer_id, "engineer_name": fallback.name, "masked_phone_number": mask_phone_number(fallback.phone_number), "decision": "BLOCK", "status": "WAITING_FOR_IDENTITY_VERIFICATION", "fallback_from": pending.engineer_id, "reason": reason, "authorization_url": started["authorization_url"]}
            self._trace(self._latest_cycle, f"TRUST_CHECK: fallback engineer selected; engineer={fallback.engineer_id}; waiting for consent")
        except Exception:
            pending_dispatches.complete(next_pending.pending_id, "BLOCKED")

    async def _cartographer(self, state: HarisState) -> HarisState:
        if state.get("durable_planning_only"):
            locations = list((state.get("durable_reasoning_context") or {}).get("locations") or [])
            state["locations"] = locations
            self._trace(
                state,
                "CARTOGRAPHER: used bounded durable topology/location evidence; no provider read performed",
            )
            return state
        device_ids = state.get(
            "incident",
            {},
        ).get(
            "affected_devices",
            [],
        )

        self._trace(
            state,
            (
                f"CARTOGRAPHER: locating "
                f"{len(device_ids)} exposed assets"
            ),
        )

        locations = await self.client.location_retrieval(
            device_ids
        )

        state["locations"] = [
            location.model_dump()
            for location in locations
        ]

        self._trace(
            state,
            (
                "CARTOGRAPHER: location evidence refreshed; "
                "no network mutation performed"
            ),
        )

        return state

    async def _triage(self, state: HarisState) -> HarisState:
        incident = Incident(**state["incident"])
        congestion = [CongestionReading(**x) for x in state["congestion"]]
        devices = [DeviceStatus(**x) for x in state["devices"]]

        evaluation = self.playbooks.evaluate(
            state.get("dust_advisory", True),
            congestion,
            devices,
            self._congestion_history,
            observed_at=state.get("congestion_observed_at"),
        )
        all_actions: List[Action] = evaluation["actions"]
        deterministic_candidate_ids = _candidate_ids(all_actions)
        candidate_id_by_action = {
            id(action): candidate_id
            for candidate_id, action in zip(deterministic_candidate_ids, all_actions)
        }
        state["active_playbook"] = {
            "name": ", ".join(evaluation["playbooks"]) or "None",
            "state": "ACTIVE" if all_actions else "IDLE",
            "trigger_reason": "Dust/congestion/battery policy evidence",
            "affected_devices": incident.affected_devices,
            "current_stage": "TRIAGE",
            "latest_outcome": "proposed" if all_actions else "no_action_proposed",
        }
        self._trace(state, f"PLAYBOOK_TRIGGERED: {state['active_playbook']['name']}")
        if state.get("isolated_fixture_demo"):
            prior_incidents = []
        elif state.get("durable_planning_only"):
            prior_incidents = [
                IncidentMemory(**item)
                for item in (state.get("durable_reasoning_context") or {}).get("prior_memory", [])[:3]
            ]
        else:
            prior_incidents = await self.memory.search_incidents(
                "sandstorm " + " ".join(incident.affected_cells), limit=3
            )
        relevant_priors = [
            prior for prior in prior_incidents
            if set(prior.affected_cells) & set(incident.affected_cells)
        ]
        state["memory_context"] = [prior.model_dump() for prior in relevant_priors]
        if state.get("isolated_fixture_demo"):
            crew = {
                "used": False, "fallback": True, "latency_ms": 0,
                "roles": [], "reason": "isolated_fixture_demo", "advisory": {},
            }
        else:
            crew = await self._crew_advisory(incident, all_actions, relevant_priors)
        state["crew_advisory"] = crew
        self._trace(
            state,
            f"CREWAI_USED={str(crew['used']).lower()} CREWAI_AGENTS={len(crew.get('roles', []))} "
            f"CREWAI_ROLES={','.join(crew.get('roles', [])) or 'none'} "
            f"CREWAI_FALLBACK={str(crew['fallback']).lower()} CREWAI_LATENCY_MS={crew['latency_ms']} "
            f"CREWAI_REASON={crew.get('reason', 'active')}",
        )

        # ---------------------------------------------------------
        # Build lookup tables from current Nokia network evidence.
        # ---------------------------------------------------------

        congestion_by_cell = {
            c.cell_id: c
            for c in congestion
        }

        # Nokia congestion is categorical.
        # HARIS uses this explicit ordering only for prioritization.
        congestion_priority = {
            "None": 0,
            "Low": 1,
            "Medium": 2,
            "High": 3,
        }

        max_devices = (
            self.settings.guardrails.max_devices_reconfigured_per_cycle
        )

        # ---------------------------------------------------------
        # Rank proposed remediation actions.
        #
        # The playbook is responsible for deciding WHAT action is
        # appropriate. Triage is responsible for deciding WHICH
        # proposed actions get bounded execution priority.
        #
        # Do not select devices independently and then filter actions:
        # that can accidentally discard a valid remediation targeting
        # a lower-tier device.
        # ---------------------------------------------------------


   
        device_by_id = {
            device.device_id: device
            for device in devices
        }
        tier1_devices_by_cell: Dict[str, int] = {}
        for device in devices:
            if device.tier == 1:
                tier1_devices_by_cell[device.cell_id] = tier1_devices_by_cell.get(device.cell_id, 0) + 1
        

        def action_priority(action: Action) -> tuple:
            device = device_by_id.get(action.device_id)

            if device is None:
                return (0, 0, 0)

            cell = congestion_by_cell.get(device.cell_id)

            if cell is None:
                congestion_rank = 0
                confidence = 0
            else:
                try:
                    congestion_rank = congestion_priority[
                        cell.congestion_level
                    ]
                except KeyError as exc:
                    raise RuntimeError(
                        "Unsupported Nokia congestion level: "
                        f"{cell.congestion_level!r}"
                    ) from exc

                confidence = cell.confidence_level

            # Mission tier remains the primary priority.
            # Network congestion and Nokia confidence refine the ranking.
            return (
                (4 - device.tier) * 1000,
                congestion_rank * 100,
                # With equal Tier/congestion evidence, protect the corridor
                # containing more exposed Tier-1 assets first.
                tier1_devices_by_cell.get(device.cell_id, 0),
                confidence,
            )

        # Rank the actions themselves rather than filtering them through
        # an independently selected device list.
        ranked_actions = sorted(
            all_actions,
            key=action_priority,
            reverse=True,
        )
        crew_order = crew.get("advisory", {}).get("ranked_candidate_ids", [])
        if crew_order:
            crew_rank = {candidate_id: index for index, candidate_id in enumerate(crew_order)}
            ranked_actions.sort(
                key=lambda action: crew_rank.get(candidate_id_by_action[id(action)], len(crew_order))
            )

        # The guardrail is a UNIQUE DEVICE limit, not an action limit. Retain
        # the bounded, de-duplicated action set for each selected device so a
        # Tier-1 asset can receive QoD + slice + geofence in one cycle.
        selected_device_ids: List[str] = []
        actions: List[Action] = []
        action_signatures = set()
        for action in ranked_actions:
            if action.device_id not in selected_device_ids:
                if len(selected_device_ids) >= max_devices:
                    continue
                selected_device_ids.append(action.device_id)
            signature = (action.kind, action.device_id, json.dumps(action.parameters, sort_keys=True, default=str))
            if signature not in action_signatures:
                action_signatures.add(signature)
                actions.append(action)

        # ---------------------------------------------------------
        # 3. Calculate QoD cost.
        # ---------------------------------------------------------
        cost = 0.0

        for action in actions:
            if action.kind == "qos":
                profile = action.parameters.get("profile")

                if profile == "guaranteed":
                    cost += 0.75
                else:
                    cost += 0.20

        # ---------------------------------------------------------
        # 4. Ask the reasoning layer to assess the proposed plan.
        # ---------------------------------------------------------
        if state.get("isolated_fixture_demo"):
            advisory = self.reasoning._deterministic_result(
                rationale="External AI is disabled for the isolated fixture demonstration."
            )
        else:
            advisory = await self.reasoning.assess(
                incident,
                devices,
                actions,
                [candidate_id_by_action[id(action)] for action in actions],
            )
        self._trace(
            state,
            "AI_PLANNER_USED=" + str(advisory["ai_planner_used"]).lower() +
            f" MODEL={advisory['model'] or 'deterministic'} FALLBACK_USED={str(advisory['fallback_used']).lower()} "
            f"AI_PLANNER_REASON={self.reasoning.availability_reason if not advisory['ai_planner_used'] else 'active'}",
        )

        planner_order = advisory.get("ranked_candidate_ids", [])
        if planner_order:
            planner_rank = {candidate_id: index for index, candidate_id in enumerate(planner_order)}
            actions.sort(
                key=lambda action: planner_rank.get(candidate_id_by_action[id(action)], len(planner_order))
            )

        confidence = float(advisory["confidence"])
        crew_modifier = float(crew.get("advisory", {}).get("confidence_modifier", 0.0))
        confidence = max(0.0, min(1.0, confidence + crew_modifier))
        successful_prior = next(
            (prior for prior in relevant_priors if prior.outcome == "verified"), None
        )
        if successful_prior:
            confidence = min(1.0, confidence + 0.03)
            advisory["rationale"] = (
                f"{advisory['rationale']} Prior verified incident "
                f"{successful_prior.incident_id} affected the same corridor; "
                "confidence increased by 0.03."
            )
            self._trace(state, f"TRIAGE MEMORY: prior verified incident {successful_prior.incident_id} influenced confidence")

        # ---------------------------------------------------------
        # 5. Blast radius is based ONLY on devices whose connectivity
        #    will actually be changed.
        #
        #    Example:
        #       2 modified / 8 registered = 0.25
        #
        #    Storm exposure does not automatically equal blast radius.
        # ---------------------------------------------------------
        # The denominator is the currently observed registered fleet, not an
        # action count. Geofencing is included because it changes protection
        # state for the selected device.
        modified_devices = set(selected_device_ids)

        blast_radius = min(
            1.0,
            len(modified_devices) / max(1, len(devices)),
        )

        # ---------------------------------------------------------
        # 6. Guardrail decision.
        #
        #    Autonomous execution is allowed only when:
        #      - blast radius is within policy
        #      - confidence is high enough
        #      - QoD cost is within the configured ceiling
        # ---------------------------------------------------------
        approval = (
            blast_radius
            > self.settings.guardrails.human_approval_blast_radius
            or confidence
            < self.settings.guardrails.minimum_confidence
            or cost
            > self.settings.guardrails.qos_spend_ceiling_usd
        )

        # ---------------------------------------------------------
        # 7. Build typed remediation plan.
        # ---------------------------------------------------------
        state["pre_execution_congestion"] = {
            reading.cell_id: {
                "congestion_level": reading.congestion_level,
                "confidence_level": reading.confidence_level,
                "interval_start": reading.interval_start,
                "interval_stop": reading.interval_stop,
            }
            for reading in congestion
        }
        state["pre_execution_devices"] = {
            device.device_id: device.cell_id
            for device in devices
        }
        plan = RemediationPlan(
            incident_id=incident.incident_id,
            actions=actions,
            confidence=confidence,
            expected_cost_usd=cost,
            expected_benefit=advisory["benefit"],
            blast_radius=blast_radius,
            approval_required=approval,
            rationale=advisory["rationale"],
            selected_device_ids=selected_device_ids,
        )

        state["plan"] = plan.model_dump()
        if state.get("durable_planning_only"):
            state["plan"].update({
                "candidate_ids": _candidate_ids(actions),
                "advisory_only": True,
                "provider_execution_permitted": False,
            })

        self._trace(
            state,
            (
                f"TRIAGE: selected_unique_devices={len(selected_device_ids)}, "
                f"bounded_actions={len(actions)}, affected_tier1={len([device_id for device_id in selected_device_ids if device_by_id.get(device_id) and device_by_id[device_id].tier == 1])}, "
                f"confidence={confidence:.2f}, "
                f"cost=${cost:.2f}, "
                f"blast_radius={blast_radius:.2f}, "
                f"approval={approval}"
            ),
        )

        return state

    

    async def _actuator(self, state: HarisState) -> HarisState:
        plan = RemediationPlan(**state["plan"])

        # Live telemetry mode is intentionally non-mutating.  This check is
        # before WARDEN's execution gate so WARDEN can still report every
        # unavailable operator capability without turning a proposal into an
        # execution failure.
        if self.settings.nac_mode == "live_read_only":
            self._trace(
                state,
                "ACTUATOR: live read-only mode; network mutation intentionally not executed",
            )
            state["execution"] = {
                "executed": False,
                "reason": "live_read_only",
                "actions": [],
            }
            return state

        # Phase 7C is the sole live-write side-effect boundary.  The legacy
        # in-process graph remains available for deterministic fixture/demo
        # cycles, but it may never bypass durable ownership, READY -> SENT
        # arbitration, reconciliation, or the explicit server-side gate.
        if self.settings.nac_mode == "live_write":
            self._trace(
                state,
                "ACTUATOR: live mutation deferred to the durable Phase 7C executor",
            )
            state["execution"] = {
                "executed": False,
                "reason": "durable_executor_required",
                "actions": [],
            }
            return state

        if plan.approval_required:
            self._trace(
                state,
                "ACTUATOR: guardrail blocked autonomous execution; "
                "human approval required",
            )

            state["execution"] = {
                "executed": False,
                "reason": "guardrail",
                "actions": [],
            }

            return state

        if (
            state.get("warden", {}).get("required")
            and not state.get("warden", {}).get("verified")
        ):
            pending_identity = (
                state.get("warden", {}).get("reason")
                == "identity_verification_pending"
            )
            self._trace(
                state,
                "ACTUATOR: identity authorization pending; network remediation paused; no action executed"
                if pending_identity else
                "ACTUATOR: WARDEN rejected network remediation; no action executed",
            )

            state["execution"] = {
                "executed": False,
                "reason": "identity_verification_pending" if pending_identity else "warden_rejected",
                "actions": [],
            }

            return state

        executed_actions = []

        try:
            for action in plan.actions:

                # -------------------------------------------------
                # QoD
                # -------------------------------------------------
                if action.kind == "qos":
                    result = await self.client.request_qos(
                        action.device_id,
                        action.parameters["profile"],
                        action.parameters["duration_seconds"],
                    )

                    executed_actions.append(
                        {
                            "kind": "qos",
                            "device_id": action.device_id,
                            "profile": action.parameters["profile"],
                            "session_id": result.session_id,
                            "success": True,
                        }
                    )

                    self._trace(
                        state,
                        (
                            f"ACTUATOR: QoD created "
                            f"device={action.device_id} "
                            f"profile={action.parameters['profile']} "
                            f"session={result.session_id}"
                        ),
                    )

                # -------------------------------------------------
                # Network Slice
                # -------------------------------------------------
                elif action.kind == "slice_attach":
                    result = await self.client.attach_slice(
                        action.device_id,
                        action.parameters["slice_id"],
                    )

                    executed_actions.append(
                        {
                            "kind": "slice_attach",
                            "device_id": action.device_id,
                            "slice_id": action.parameters["slice_id"],
                            "attached": result.attached,
                            "success": bool(result.attached),
                        }
                    )

                    self._trace(
                        state,
                        (
                            f"ACTUATOR: slice attached "
                            f"device={action.device_id} "
                            f"slice={action.parameters['slice_id']}"
                        ),
                    )

                # -------------------------------------------------
                # Geofence
                # -------------------------------------------------
                elif action.kind == "geofence":
                    result = await self.client.create_geofence(
                        action.device_id,
                        action.parameters["polygon_id"],
                    )

                    executed_actions.append(
                        {
                            "kind": "geofence",
                            "device_id": action.device_id,
                            "polygon_id": action.parameters["polygon_id"],
                            "subscription_id": result.subscription_id,
                            "success": bool(result.active),
                        }
                    )

                    self._trace(
                        state,
                        (
                            f"ACTUATOR: geofence created "
                            f"device={action.device_id} "
                            f"polygon={action.parameters['polygon_id']} "
                            f"subscription={result.subscription_id}"
                        ),
                    )

                # -------------------------------------------------
                # Unsupported action
                # -------------------------------------------------
                else:
                    raise RuntimeError(
                        f"Unsupported actuator action: {action.kind!r}"
                    )

            state["execution"] = {
                "executed": True,
                "actions": executed_actions,
            }

            self._trace(
                state,
                (
                    f"ACTUATOR: executed "
                    f"{len(executed_actions)} network actions"
                ),
            )

            return state

        except Exception:
            logger.warning("Actuator execution failed; provider details suppressed")

            state["execution"] = {
                "executed": False,
                "actions": executed_actions,
                "reason": "execution_error",
                "error": "provider_operation_failed",
            }

            self._trace(
                state,
                (
                    f"ACTUATOR: execution failed after "
                    f"{len(executed_actions)} actions; provider details suppressed"
                ),
            )

            return state
    
    async def _verify(self, state: HarisState) -> HarisState:
        """
        Verify the effect of the remediation using fresh Nokia/CAMARA
        observations.

        HARIS does not invent numeric congestion, latency, or prediction
        values. Verification compares Nokia's categorical congestion
        levels before and after remediation.
        """

        incident = Incident(**state["incident"])

        device_ids = incident.affected_devices

        # A read-only proposal has no mutation to validate.  Preserve the
        # original live evidence from TRIAGE and make the absence explicit.
        if state.get("execution", {}).get("reason") == "live_read_only":
            state["verification"] = {
                "verified": False,
                "status": "live_read_only_proposal",
                "reason": "Network mutation was intentionally not executed in live read-only mode.",
                "improvements": [],
            }
            state["final_status"] = "live_read_only_proposal"
            self._trace(
                state,
                "VERIFY: not applicable; no network mutation was executed in live read-only mode",
            )
            return state

        # ---------------------------------------------------------
        # 1. Read the network AFTER remediation.
        # ---------------------------------------------------------
        readings = await self.client.device_status(device_ids)
        congestion = await self.client.congestion_insights()

        state["congestion"] = [
            reading.model_dump()
            for reading in congestion
        ]

        reachable = sum(
            1
            for device in readings
            if device.reachable
        )

        total = len(readings)

        # ---------------------------------------------------------
        # 2. Read the BEFORE snapshot captured by TRIAGE.
        # ---------------------------------------------------------
        before = state.get(
            "pre_execution_congestion",
            {},
        )

        # ---------------------------------------------------------
        # 3. Read the device -> cell mapping captured BEFORE execution.
        # ---------------------------------------------------------
        device_to_cell = state.get(
            "pre_execution_devices",
            {},
        )

        # ---------------------------------------------------------
        # 4. Identify cells that were actually modified.
        # ---------------------------------------------------------
        plan_actions = (
            state.get("plan", {})
            .get("actions", [])
        )

        target_cells = set()

        for action in plan_actions:
            if action.get("kind") not in {
                "qos",
                "slice_attach",
                "slice_detach",
            }:
                continue

            device_id = action.get("device_id")
            cell_id = device_to_cell.get(device_id)

            if cell_id:
                target_cells.add(cell_id)

        # ---------------------------------------------------------
        # Nokia congestion levels are categorical.
        #
        # This ordering is HARIS verification policy.
        # It is NOT a conversion into percentages.
        # ---------------------------------------------------------
        congestion_rank = {
            "None": 0,
            "Low": 1,
            "Medium": 2,
            "High": 3,
        }

        # ---------------------------------------------------------
        # 5. Diagnostic baseline trace.
        # ---------------------------------------------------------
        self._trace(
            state,
            (
                "VERIFY BASELINE: "
                + ", ".join(
                    f"{cell_id}="
                    f"{values.get('congestion_level', 'Unknown')}"
                    f"/confidence="
                    f"{values.get('confidence_level', 'Unknown')}"
                    for cell_id, values in before.items()
                )
            ),
        )

        # ---------------------------------------------------------
        # 6. Diagnostic Nokia readback trace.
        # ---------------------------------------------------------
        self._trace(
            state,
            (
                "VERIFY READBACK: "
                + ", ".join(
                    f"{reading.cell_id}="
                    f"{reading.congestion_level}"
                    f"/confidence="
                    f"{reading.confidence_level}"
                    for reading in congestion
                )
            ),
        )

        # ---------------------------------------------------------
        # 7. Compare BEFORE vs AFTER for targeted cells.
        # ---------------------------------------------------------
        improvements = []

        for reading in congestion:

            if reading.cell_id not in target_cells:
                continue

            previous = before.get(reading.cell_id)

            if previous is None:
                self._trace(
                    state,
                    (
                        f"VERIFY: missing Nokia baseline for "
                        f"{reading.cell_id}"
                    ),
                )
                continue

            before_level = previous.get(
                "congestion_level"
            )

            after_level = reading.congestion_level

            if before_level not in congestion_rank:
                raise RuntimeError(
                    "Unsupported baseline Nokia congestion level: "
                    f"{before_level!r}"
                )

            if after_level not in congestion_rank:
                raise RuntimeError(
                    "Unsupported post-remediation Nokia congestion level: "
                    f"{after_level!r}"
                )

            before_rank = congestion_rank[before_level]
            after_rank = congestion_rank[after_level]

            level_improved = after_rank < before_rank
            level_degraded = after_rank > before_rank
            level_unchanged = after_rank == before_rank

            improvements.append(
                {
                    "cell_id": reading.cell_id,

                    "before_congestion_level":
                        before_level,

                    "after_congestion_level":
                        after_level,

                    "before_confidence_level":
                        previous.get("confidence_level"),

                    "after_confidence_level":
                        reading.confidence_level,

                    "before_interval_start":
                        previous.get("interval_start"),

                    "before_interval_stop":
                        previous.get("interval_stop"),

                    "after_interval_start":
                        reading.interval_start,

                    "after_interval_stop":
                        reading.interval_stop,

                    "level_improved":
                        level_improved,

                    "level_degraded":
                        level_degraded,

                    "level_unchanged":
                        level_unchanged,

                    "improved":
                        level_improved,
                }
            )

        # ---------------------------------------------------------
        # 8. Verification policy.
        # ---------------------------------------------------------
        executed = bool(
            state.get(
                "execution",
                {},
            ).get(
                "executed",
                False,
            )
        )

        has_actions = bool(plan_actions)

        reachable_ok = (
            reachable == total
            if total > 0
            else True
        )

        level_improved = any(
            item["level_improved"]
            for item in improvements
        )

        level_degraded = any(
            item["level_degraded"]
            for item in improvements
        )

        level_unchanged = bool(improvements) and all(
            item["level_unchanged"]
            for item in improvements
        )

        # ---------------------------------------------------------
        # No actions means there is nothing to verify.
        # ---------------------------------------------------------
        if not has_actions:
            verified = False
            verification_status = "no_action_proposed"

        elif not executed:
            verified = False
            verification_status = (
                "identity_verification_pending"
                if state.get("execution", {}).get("reason") == "identity_verification_pending"
                else
                "warden_rejected"
                if state.get("execution", {}).get("reason") == "warden_rejected"
                else "execution_failed_partial"
                if state.get("execution", {}).get("actions")
                else "execution_failed"
            )
        elif not improvements:
            verified = False
            verification_status = "verification_unavailable"
        else:
            verified = (
                executed
                and reachable_ok
                and bool(improvements)
                and level_improved
                and not level_degraded
            )

            if verified:
                verification_status = "mitigated"
            elif level_degraded:
                verification_status = "degraded"
            elif level_unchanged:
                verification_status = "unchanged"
            else:
                verification_status = "not_mitigated"

        # ---------------------------------------------------------
        # 9. Store verification result.
        # ---------------------------------------------------------
        state["verification"] = {
            "reachable": reachable,
            "total": total,

            "target_cells":
                sorted(target_cells),

            "level_improved":
                level_improved,

            "level_degraded":
                level_degraded,

            "level_unchanged":
                level_unchanged,

            "reachable_ok":
                reachable_ok,

            "improvements":
                improvements,

            "verified":
                verified,

            "status":
                verification_status,
        }

        if state.get("rollback_attempted") and state.get("rollback", {}).get(
            "rollback_verified", False
        ):
            state["final_status"] = "rolled_back_safely"
        elif verified:
            state["final_status"] = "mitigated"
        elif not has_actions:
            state["final_status"] = "no_action_proposed"
        elif verification_status == "execution_failed":
            state["final_status"] = "execution_failed"
        elif verification_status == "identity_verification_pending":
            state["final_status"] = "waiting_for_identity_verification"
        elif verification_status == "warden_rejected":
            state["final_status"] = (
                "waiting_for_identity_verification"
                if state.get("trusted_dispatch", {}).get("status") == "WAITING_FOR_IDENTITY_VERIFICATION"
                else "warden_rejected"
            )
        elif verification_status == "verification_unavailable":
            state["final_status"] = "verification_unavailable"
        elif level_degraded:
            state["final_status"] = "degraded"
        elif has_actions:
            state["final_status"] = "verification_failed"

        # ---------------------------------------------------------
        # 10. Human-readable verification traces.
        # ---------------------------------------------------------
        for item in improvements:
            self._trace(
                state,
                (
                    f"VERIFY: {item['cell_id']} "
                    f"congestion "
                    f"{item['before_congestion_level']}"
                    f" -> "
                    f"{item['after_congestion_level']}, "
                    f"confidence "
                    f"{item['before_confidence_level']}"
                    f" -> "
                    f"{item['after_confidence_level']}, "
                    f"improved={item['improved']}"
                ),
            )

        self._trace(
            state,
            (
                f"VERIFY: "
                f"reachable={reachable}/{total}, "
                f"target_cells={len(target_cells)}, "
                f"level_improved={level_improved}, "
                f"level_degraded={level_degraded}, "
                f"verified={verified}, "
                f"status={verification_status}"
            ),
        )

        return state

    async def _rollback(self, state: HarisState) -> HarisState:
        """
        Roll back only the network operations that HARIS actually executed.

        Rollback reverses concrete Nokia operations:
        - QoD session        -> release
        - Slice attachment  -> detach
        - Geofence          -> delete

        HARIS does not attempt to restore synthetic network KPIs.
        """

        # ---------------------------------------------------------
        # 1. Prevent infinite rollback loops.
        # ---------------------------------------------------------
        if state.get("rollback_attempted", False):
            self._trace(
                state,
                "ROLLBACK: already attempted; refusing another rollback",
            )

            return state

        state["rollback_attempted"] = True

        # ---------------------------------------------------------
        # 2. Read the ACTUAL operations executed by the Actuator.
        # ---------------------------------------------------------
        execution = state.get(
            "execution",
            {},
        )

        executed = bool(
            execution.get(
                "executed",
                False,
            )
        )

        executed_actions = execution.get(
            "actions",
            [],
        )

        if not executed_actions:
            self._trace(
                state,
                "ROLLBACK: no executed network actions to reverse",
            )

            state["rollback"] = {
                "attempted": True,
                "executed": False,
                "rollback_verified": True,
                "actions": [],
                "reason": "no_executed_network_actions",
            }

            return state

        if not executed:
            self._trace(
                state,
                (
                    "ROLLBACK: execution was not fully successful; "
                    "reversing only actions confirmed as executed"
                ),
            )

        # ---------------------------------------------------------
        # 3. Reverse operations in reverse execution order.
        #
        #    This is important when multiple dependent actions
        #    were executed during the same remediation cycle.
        # ---------------------------------------------------------
        rollback_results = []

        rollback_success = True

        for action in reversed(executed_actions):

            kind = action.get("kind")
            device_id = action.get("device_id")

            try:

                # -------------------------------------------------
                # QoD
                # -------------------------------------------------
                if kind == "qos":
                    session_id = action.get("session_id")

                    if not session_id:
                        raise RuntimeError(
                            "Executed QoD action has no session_id"
                        )

                    released = await self.client.release_qos(
                        session_id
                    )

                    success = bool(released)

                    rollback_results.append(
                        {
                            "kind": "qos",
                            "device_id": device_id,
                            "session_id": session_id,
                            "operation": "release_qos",
                            "success": success,
                        }
                    )

                    self._trace(
                        state,
                        (
                            f"ROLLBACK: QoD release "
                            f"device={device_id} "
                            f"session={session_id} "
                            f"success={success}"
                        ),
                    )

                # -------------------------------------------------
                # Network Slice
                # -------------------------------------------------
                elif kind == "slice_attach":
                    slice_id = action.get("slice_id")

                    if not device_id or not slice_id:
                        raise RuntimeError(
                            "Executed slice action is missing "
                            "device_id or slice_id"
                        )

                    result = await self.client.detach_slice(
                        device_id,
                        slice_id,
                    )

                    success = bool(
                        result.attached is False
                    )

                    rollback_results.append(
                        {
                            "kind": "slice_attach",
                            "device_id": device_id,
                            "slice_id": slice_id,
                            "operation": "detach_slice",
                            "success": success,
                            "attached": result.attached,
                        }
                    )

                    self._trace(
                        state,
                        (
                            f"ROLLBACK: slice detach "
                            f"device={device_id} "
                            f"slice={slice_id} "
                            f"attached={result.attached}"
                        ),
                    )

                # -------------------------------------------------
                # Geofence
                # -------------------------------------------------
                elif kind == "geofence":
                    subscription_id = action.get(
                        "subscription_id"
                    )

                    if not subscription_id:
                        raise RuntimeError(
                            "Executed geofence action has no "
                            "subscription_id"
                        )

                    deleted = await self.client.delete_geofence(
                        subscription_id
                    )

                    success = bool(deleted)

                    rollback_results.append(
                        {
                            "kind": "geofence",
                            "device_id": device_id,
                            "subscription_id": subscription_id,
                            "operation": "delete_geofence",
                            "success": success,
                        }
                    )

                    self._trace(
                        state,
                        (
                            f"ROLLBACK: geofence delete "
                            f"device={device_id} "
                            f"subscription={subscription_id} "
                            f"success={success}"
                        ),
                    )

                else:
                    raise RuntimeError(
                        f"Unsupported executed action during rollback: "
                        f"{kind!r}"
                    )

                if not success:
                    rollback_success = False

            except Exception:
                rollback_success = False

                rollback_results.append(
                    {
                        "kind": kind,
                        "device_id": device_id,
                        "operation": "rollback",
                        "success": False,
                        "error": "rollback_operation_failed",
                    }
                )

                logger.warning(
                    "Rollback operation failed; kind=%s device=%s; provider details suppressed",
                    kind,
                    device_id,
                )

                self._trace(
                    state,
                    (
                        f"ROLLBACK: FAILED "
                        f"kind={kind} "
                        f"device={device_id} "
                        "error=rollback_operation_failed"
                    ),
                )

        # ---------------------------------------------------------
        # 4. Independent rollback verification.
        #
        #    We verify the resulting operation states where the
        #    adapter exposes them. We do NOT compare synthetic KPIs.
        # ---------------------------------------------------------
        rollback_verified = rollback_success

        for result in rollback_results:
            if not result.get("success", False):
                rollback_verified = False

        # ---------------------------------------------------------
        # 5. Persist complete rollback evidence.
        # ---------------------------------------------------------
        state["rollback"] = {
            "attempted": True,
            "executed": rollback_success,
            "rollback_verified": rollback_verified,
            "actions": rollback_results,
            "reason": (
                "executed_actions_reversed"
                if rollback_verified
                else "one_or_more_reverse_operations_failed"
            ),
        }

        if rollback_verified:
            state["final_status"] = "rolled_back_safely"

            self._trace(
                state,
                (
                    "ROLLBACK VERIFY: SUCCESS; "
                    f"reversed {len(rollback_results)} "
                    "executed network actions"
                ),
            )

        else:
            state["final_status"] = "rollback_failed"

            self._trace(
                state,
                (
                    "ROLLBACK VERIFY: FAILED; "
                    "one or more network operations could not be reversed"
                ),
            )

        return state

    async def _learn(self, state: HarisState) -> HarisState:
        self._trace(
            state,
            f"LEARN: final_status={state.get('final_status', 'MISSING')}",
        )

        # The dashboard's active-playbook card follows the actual graph result,
        # rather than retaining the provisional TRIAGE state.
        if state.get("active_playbook"):
            final_status = state.get("final_status", "unknown")
            state["active_playbook"] = {
                **state["active_playbook"],
                "state": final_status.upper(),
                "current_stage": "LEARN",
                "latest_outcome": final_status,
            }

        incident = Incident(**state["incident"])
        verification = state.get("verification", {})
        rollback = state.get("rollback", {})
        plan = state.get("plan", {})
        execution = state.get("execution", {})

        # ---------------------------------------------------------
        # Build evidence from the actual Nokia/CAMARA observations.
        # ---------------------------------------------------------
        summary = (
            f"Dust={incident.storm_advisory}; "
            f"severity={incident.severity.value}; "
            f"peak_congestion={incident.peak_congestion_level}; "
            f"peak_confidence={incident.peak_confidence_level}; "
            f"affected_cells={','.join(incident.affected_cells)}"
        )

        # ---------------------------------------------------------
        # Store the actions HARIS actually planned.
        # ---------------------------------------------------------
        planned_actions = [
            action.get("kind")
            for action in plan.get("actions", [])
        ]

        # ---------------------------------------------------------
        # Store the actions HARIS actually executed.
        # ---------------------------------------------------------
        executed_actions = [
            action.get("kind")
            for action in execution.get("actions", [])
        ]

        # ---------------------------------------------------------
        # Determine the final learning outcome.
        # ---------------------------------------------------------
        if state.get("final_status") == "mitigated":
            outcome = "verified"

        elif state.get("final_status") == "rolled_back_safely":
            outcome = "rolled_back_safely"

        elif state.get("final_status") == "recovered":
            outcome = "recovered"

        elif state.get("final_status") == "recovery_cleanup_failed":
            outcome = "recovery_cleanup_failed"

        elif state.get("final_status") == "rollback_failed":
            outcome = "rollback_failed"

        elif state.get("final_status") == "execution_failed":
            outcome = "execution_failed"

        elif state.get("final_status") == "live_read_only_proposal":
            outcome = "live_read_only_proposal"

        elif state.get("final_status") == "warden_rejected":
            outcome = "warden_rejected"

        elif state.get("final_status") == "verification_unavailable":
            outcome = "verification_unavailable"

        elif state.get("final_status") == "no_action_proposed":
            outcome = "no_action_proposed"

        elif state.get("final_status") == "waiting_for_identity_verification":
            outcome = "identity_verification_pending"

        elif plan.get("approval_required"):
            outcome = "guardrail_or_approval_required"

        else:
            outcome = "failed_or_unverified"

        memory = IncidentMemory(
            incident_id=incident.incident_id,

            summary=(
                f"Dust={incident.storm_advisory}; "
                f"severity={incident.severity.value}; "
                f"peak_congestion={incident.peak_congestion_level}; "
                f"peak_confidence={incident.peak_confidence_level}; "
                f"affected_cells={','.join(incident.affected_cells)}"
            ),

            storm_type="sandstorm",

            peak_congestion_level=incident.peak_congestion_level,
            peak_confidence_level=incident.peak_confidence_level,

            affected_cells=incident.affected_cells,
            affected_devices=incident.affected_devices,

            actions=[
                x["kind"]
                for x in state.get("plan", {}).get("actions", [])
            ],

            executed_actions=[
                x["kind"]
                for x in state.get("execution", {}).get("actions", [])
            ],

            outcome=outcome,
            cycle_id=state.get("cycle_id"),
            mode=self.settings.nac_mode,
            checkpoint_type="normalization_recovery" if state.get("recovery", {}).get("attempted") else "learn",
            checkpoint_ordinal=0,
            completed_at=(
                datetime.now(timezone.utc).isoformat()
                if outcome != "identity_verification_pending" else None
            ),
            audit={
                "environment": {"dust_advisory": state.get("dust_advisory"), "source": state.get("environmental_source")},
                "field_intervention_evidence": state.get("field_intervention_evidence", {}),
                "congestion": state.get("congestion", []),
                "prediction": state.get("prediction", {}),
                "incident": state.get("incident", {}),
                "memory_context": state.get("memory_context", []),
                "crew_advisory": state.get("crew_advisory", {}),
                "plan": plan,
                "warden": state.get("warden", {}),
                "trusted_dispatch": state.get("trusted_dispatch", {}),
                "dispatch_history": [item.model_dump() for item in trusted_dispatch_history.for_incident(incident.incident_id)],
                "execution": execution,
                "verification": verification,
                "rollback": rollback,
                "recovery": state.get("recovery", {}),
                "trace": state.get("trace", []),
                "events": state.get("events", []),
                "final_status": state.get("final_status"),
            },

            verification=verification,
            rollback=rollback,
        )

        if state.get("isolated_fixture_demo"):
            state["learning"] = {
                "incident_saved": False,
                "memory_count": None,
                "outcome": outcome,
                "reason": "durable_history_disabled_for_isolated_fixture_demo",
            }
        else:
            await self.memory.remember_incident(memory)
            state["learning"] = {
                "incident_saved": True,
                "memory_count": self.memory.count(),
                "outcome": outcome,
            }

        state["explanation"] = self._explain_cycle(state)

        self._trace(
            state,
            (
                (
                    "LEARN: isolated fixture result retained in process only; "
                    if state.get("isolated_fixture_demo") else
                    "LEARN: workflow checkpoint stored; "
                )
                + f"outcome={outcome}; "
                "episodic memory count="
                + ("not_persisted" if state.get("isolated_fixture_demo") else str(self.memory.count()))
            ),
        )

        return state

    def _explain_cycle(self, state: HarisState) -> str:
        """Concise explanation grounded solely in recorded cycle evidence."""
        incident = state.get("incident", {})
        source = state.get("environmental_source", "UNAVAILABLE")
        simulated = " Simulated fixture evidence was used." if self.settings.nac_mode == "fixture" or source == "FIXTURE" else ""
        playbook = state.get("active_playbook", {}).get("name", "No playbook")
        actions = state.get("execution", {}).get("actions", [])
        action_text = ", ".join(item.get("kind", "action") for item in actions) or "no network action"
        return (
            f"HARIS observed {incident.get('peak_congestion_level', 'unavailable')} congestion "
            f"at {', '.join(incident.get('affected_cells', [])) or 'no affected registered cell'}, "
            f"selected {playbook}, and WARDEN completed the cycle with {state.get('final_status', 'unknown')}. "
            f"Executed: {action_text}." + simulated
        )

    async def run_durable_reasoning(self, context: Dict[str, Any]) -> HarisState:
        """Run the existing LangGraph through WARDEN in PLAN-only mode.

        The caller owns durable reload and persistence.  This method performs
        no Nokia mutation and deliberately does not update the public latest
        cycle until the caller confirms the durable decision boundary.
        """
        initial: HarisState = {
            "cycle_id": str(context.get("decision_id") or uuid.uuid4().hex[:10]),
            "incident_id": str(context["incident_id"]),
            "incident_scope_cells": list(context.get("affected_entities") or []),
            "durable_planning_only": True,
            "durable_reasoning_context": context,
            "durable_policy": dict(context.get("durable_policy") or {}),
            "trace": [],
            "events": [],
        }
        result = await self.graph.ainvoke(initial)
        result["execution"] = {
            "executed": False,
            "reason": "phase_7b_authorized_plan_only",
            "actions": [],
        }
        result["decision_status"] = (
            "AUTHORIZED_PLAN" if result.get("warden", {}).get("verified")
            else "ESCALATED" if result.get("plan", {}).get("approval_required")
            else "BLOCKED"
        )
        return result

    def accept_durable_decision(self, state: HarisState) -> None:
        """Refresh the disposable NOC view only after durable persistence."""
        self._latest_cycle = state

    async def run_cycle(
        self,
        dust_advisory: bool = True,
        *,
        field_intervention_required: bool = False,
        field_intervention_site: Optional[str] = None,
        field_intervention_skills: Optional[List[str]] = None,
        field_intervention_reason: Optional[str] = None,
        incident_scope_cells: Optional[List[str]] = None,
        incident_id: Optional[str] = None,
        observation_snapshot: Optional[Dict[str, Any]] = None,
        isolated_fixture_demo: bool = False,
    ) -> HarisState:
        if isolated_fixture_demo and self.settings.nac_mode != "fixture":
            raise RuntimeError("Isolated fixture demonstrations require FIXTURE mode.")

        initial: HarisState = {
            "cycle_id": uuid.uuid4().hex[:10],
            "incident_id": incident_id,
            "incident_scope_cells": incident_scope_cells or [],
            "observation_snapshot": None if isolated_fixture_demo else observation_snapshot,
            "isolated_fixture_demo": isolated_fixture_demo,
            "execution_context": "ISOLATED_FIXTURE_DEMO" if isolated_fixture_demo else "STANDARD",
            "provenance": "SIMULATED" if isolated_fixture_demo else None,
            "authority": "PROCESS_LOCAL_FIXTURE_DEMO" if isolated_fixture_demo else None,
            "external_access_permitted": False if isolated_fixture_demo else None,
            "durable_domain_write": False if isolated_fixture_demo else None,
            "durable_history_write": False if isolated_fixture_demo else None,
            "dust_advisory": dust_advisory,
            "trace": [],
            "events": [],
            "field_intervention_required": field_intervention_required,
            "field_intervention_site": field_intervention_site,
            "field_intervention_skills": field_intervention_skills or [],
            "field_intervention_reason": field_intervention_reason,
            "field_intervention_evidence": (
                {
                    "source": "FIXTURE / SIMULATED DEMO",
                    "kind": "site_power_reserve_critical",
                    "reason": field_intervention_reason or "Physical inspection required.",
                    "note": "This is HARIS fixture evidence, not a Nokia Network as Code measurement.",
                } if field_intervention_required else {}
            ),
        }

        try:
            result = await self.graph.ainvoke(initial)
        finally:
            if isolated_fixture_demo:
                reset_fixture = getattr(self.client, "reset_isolated_demo_state", None)
                if callable(reset_fixture):
                    reset_fixture()
        self._latest_cycle = result
        return result

    async def run_field_intervention_demo(
        self, *, isolated_fixture_demo: bool = False
    ) -> HarisState:
        """Fixture-only demo of a physical condition beyond Nokia network APIs."""
        if self.settings.nac_mode != "fixture":
            raise RuntimeError("Field Intervention Demo is available only in FIXTURE mode.")
        self.field_intervention_diagnostic_stage = "FIELD_CYCLE_EXECUTION"
        return await self.run_cycle(
            dust_advisory=True,
            field_intervention_required=True,
            field_intervention_site="T03",
            field_intervention_skills=["tower-inspection", "power"],
            field_intervention_reason="Simulated critical tower power reserve requires physical inspection.",
            isolated_fixture_demo=isolated_fixture_demo,
        )

    async def recover_normalized_incident(self, *, dust_advisory: bool = False) -> Dict[str, Any]:
        """Release only HARIS-owned temporary resources after normalization.

        This is intentionally separate from rollback: cleanup is permitted only
        for a previously verified mitigation, a declared clear dust condition,
        and fresh categorical Nokia evidence showing no Medium/High congestion
        in the affected incident cells.  It never discovers, adopts, or deletes
        external operator resources.
        """
        state = self._latest_cycle
        execution = state.get("execution", {})
        incident = state.get("incident", {})
        if state.get("recovery", {}).get("attempted"):
            return state
        if state.get("final_status") != "mitigated" or not execution.get("executed"):
            return state
        if dust_advisory:
            self._trace(state, "RECOVERY: dust condition remains active; temporary resources retained")
            return state

        current = await self.client.congestion_insights()
        affected = set(incident.get("affected_cells", []))
        normalized = bool(affected) and all(
            item.congestion_level in {"None", "Low"}
            for item in current if item.cell_id in affected
        ) and affected.issubset({item.cell_id for item in current})
        if not normalized:
            self._trace(state, "RECOVERY: fresh categorical evidence is not normalized; temporary resources retained")
            return state

        state["recovery"] = {"attempted": True, "actions": []}
        successes = True
        for action in reversed(execution.get("actions", [])):
            kind, device_id = action.get("kind"), action.get("device_id")
            try:
                if kind == "qos" and action.get("session_id"):
                    success, operation = bool(await self.client.release_qos(action["session_id"])), "release_qos"
                elif kind == "slice_attach" and device_id and action.get("slice_id"):
                    result = await self.client.detach_slice(device_id, action["slice_id"])
                    success, operation = result.attached is False, "detach_slice"
                elif kind == "geofence" and action.get("subscription_id"):
                    success, operation = bool(await self.client.delete_geofence(action["subscription_id"])), "delete_geofence"
                else:
                    # An incomplete action record cannot prove ownership.
                    continue
                state["recovery"]["actions"].append({"kind": kind, "device_id": device_id, "operation": operation, "success": success})
                successes = successes and success
            except Exception:
                successes = False
                state["recovery"]["actions"].append({"kind": kind, "device_id": device_id, "operation": "cleanup", "success": False})

        state["recovery"]["verified"] = successes
        state["final_status"] = "recovered" if successes else "recovery_cleanup_failed"
        self._trace(state, f"RECOVERY: owned temporary resources cleanup verified={str(successes).lower()}")
        await self._learn(state)
        self._latest_cycle = state
        return state
