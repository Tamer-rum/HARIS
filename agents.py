from __future__ import annotations
import asyncio
import json
import logging
import re
import time
import uuid
from typing import Any, Dict, List, Optional, TypedDict

from pydantic import BaseModel, Field
from langgraph.graph import END, START, StateGraph

from config import AppSettings, DevicePolicy, EnvironmentalSource, QualityLevel, get_settings
from dispatch import AuthorizedEngineerRegistry, DispatchAttempt, PendingDispatch, mask_phone_number, pending_dispatches, trusted_dispatch_history
from memory import IncidentMemory, MemoryStore
from nokia_clients import BaseNokiaClient, CongestionReading, DeviceStatus, evaluate_trusted_dispatch_phone, register_dispatch_resume_handler, start_number_verification_for_dispatch, verified_identities
from playbooks import Action, PlaybookEngine
from prediction import PredictionResult, RiskForecaster

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


class CrewAdvisory(BaseModel):
    recommended_action_order: List[str] = Field(default_factory=list)
    reasoning_summary: str = ""
    key_risks: List[str] = Field(default_factory=list)
    memory_observations: List[str] = Field(default_factory=list)
    confidence_modifier: float = Field(default=0.0, ge=-0.05, le=0.05)
    concerns: List[str] = Field(default_factory=list)


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
        if LANGCHAIN_LLM_AVAILABLE:
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

    async def assess(self, incident: Incident, devices: List[DeviceStatus], actions: List[Action]) -> Dict[str, Any]:
        payload = {
            "incident": incident.model_dump(),
            "devices": [d.model_dump() for d in devices],
            "actions": [a.__dict__ for a in actions],
            "instruction": "Return compact JSON with confidence 0..1, benefit 0..1, rationale. Do not propose actions not present in the input.",
        }
        prompt = json.dumps(payload, default=str)
        model = self.gemini or self.groq
        if model is None:
            return {"confidence": 0.86, "benefit": 0.80, "rationale": "Deterministic policy evidence is sufficient; no hosted model key configured.", "ai_planner_used": False, "model": None, "fallback_used": True}
        try:
            response = await model.ainvoke(prompt)
            text = response.content if hasattr(response, "content") else str(response)
            parsed = _safe_json(text)
            return {
                "confidence": max(0.0, min(1.0, float(parsed.get("confidence", 0.86)))),
                "benefit": max(0.0, min(1.0, float(parsed.get("benefit", 0.80)))),
                "rationale": str(parsed.get("rationale", "Model advisory accepted within deterministic policy bounds.")),
                "ai_planner_used": True,
                "model": self.settings.gemini_model if self.gemini else self.settings.groq_model,
                "fallback_used": False,
            }
        except Exception as exc:
            logger.warning("LLM advisory failed; deterministic policy remains authoritative: %s", exc)
            return {"confidence": 0.80, "benefit": 0.75, "rationale": "Hosted model unavailable; deterministic quality policy used.", "ai_planner_used": False, "model": self.settings.gemini_model if self.gemini else self.settings.groq_model, "fallback_used": True}


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
            ("congestion_insights", "CAMARA Congestion Insights: read live/predicted congestion per cell."),
            ("device_status", "CAMARA Device Status: read reachability, roaming, battery and cell state."),
            ("location_retrieval", "CAMARA Location Retrieval: locate registered critical assets."),
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
        self.latest_dispatch: Dict[str, Any] = {}
        register_dispatch_resume_handler(self._resume_pending_dispatch)
        self._cached_environment: Optional[bool] = None
        self.crewai_agents: Dict[str, Any] = {}
        self._init_crewai_agents()
        self.graph = self._build_graph()

    def _init_crewai_agents(self) -> None:
        if not CREWAI_AVAILABLE:
            logger.warning("CrewAI is not installed; deterministic role logic remains active")
            return
        gemini_key = self.settings.gemini_api_key.get_secret_value() if self.settings.gemini_api_key else None
        groq_key = self.settings.groq_api_key.get_secret_value() if self.settings.groq_api_key else None
        llm = None
        if gemini_key:
            llm = LLM(model=f"gemini/{self.settings.gemini_model}", api_key=gemini_key, temperature=0.0)
        elif groq_key:
            llm = LLM(model=f"groq/{self.settings.groq_model}", api_key=groq_key, temperature=0.0)
        if llm is None:
            return
        roles = {
            "SENTINEL": "Watcher: detect environmental/network degradation and raise typed incidents.",
            "CARTOGRAPHER": "Locator: resolve exposed critical devices and geofence state.",
            "TRIAGE": "Planner: rank devices, apply policy, estimate cost/benefit and confidence.",
            "ACTUATOR": "Executor: perform only bounded approved network actions.",
            "WARDEN": "Network Safety Guard: validate network-risk conditions, policy limits, and action safety before execution.",
        }
        role_tools = build_crewai_tools(ToolFactory(self.client))
        for name, goal in roles.items():
            owned = {
                "SENTINEL": {"congestion_insights", "device_status"},
                "CARTOGRAPHER": {"location_retrieval", "geofence_subscribe"},
                "TRIAGE": set(),
                "ACTUATOR": {"qos_request", "qos_release", "slice_attach", "slice_detach", "geofence_delete"},
                "WARDEN": set(),
            }[name]
            selected = [t for t in role_tools if t.name in owned]
            self.crewai_agents[name] = Agent(
                role=name,
                goal=goal,
                backstory="HARIS specialist operating inside a bounded autonomous telecom control loop.",
                llm=llm,
                tools=selected,
                verbose=False,
                allow_delegation=False,
            )

    async def _crew_advisory(self, incident: Incident, actions: List[Action], memory_context: List[IncidentMemory]) -> Dict[str, Any]:
        """Optional bounded CrewAI collaboration; it cannot create or execute actions."""
        start = time.perf_counter()
        if not self.crewai_agents or not CREWAI_AVAILABLE:
            return {"used": False, "fallback": True, "latency_ms": 0, "reason": "CrewAI or model credentials unavailable"}
        payload = {
            "incident": incident.model_dump(),
            "allowed_action_kinds": [action.kind for action in actions],
            "memory": [item.model_dump() for item in memory_context],
            "instruction": "Return JSON only. Do not create actions. confidence_modifier must be between -0.05 and 0.05.",
        }
        try:
            task = Task(
                description=json.dumps(payload, default=str),
                expected_output="JSON CrewAdvisory object only",
                agent=self.crewai_agents["TRIAGE"],
            )
            crew = Crew(agents=[self.crewai_agents["TRIAGE"], self.crewai_agents["WARDEN"]], tasks=[task], process=Process.sequential, verbose=False)
            result = await asyncio.to_thread(crew.kickoff)
            parsed = CrewAdvisory(**_safe_json(str(result)))
            allowed = {action.kind for action in actions}
            parsed.recommended_action_order = [kind for kind in parsed.recommended_action_order if kind in allowed]
            return {"used": True, "fallback": False, "latency_ms": round((time.perf_counter() - start) * 1000, 1), "advisory": parsed.model_dump()}
        except Exception as exc:
            logger.warning("CrewAI advisory failed; deterministic triage retained: %s", exc)
            return {"used": False, "fallback": True, "latency_ms": round((time.perf_counter() - start) * 1000, 1), "reason": "CrewAI advisory unavailable"}

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

        if verification.get("status") in {"live_read_only_proposal", "warden_rejected"}:
            self._trace(
                state,
                "ROUTER: no write was attempted; proceeding to LEARN without rollback",
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
        graph.add_edge(
            "triage",
            "warden",
        )
        graph.add_edge(
            "warden",
            "actuator",
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

    def _trace(self, state: HarisState, message: str) -> None:
        state.setdefault("trace", []).append(f"{time.strftime('%H:%M:%S')} | {message}")
        stage = message.split(":", 1)[0].strip().upper()
        event_type = {
            "SENTINEL": "SENSE", "CARTOGRAPHER": "REASON", "TRIAGE": "ACTION_PROPOSED",
            "WARDEN": "WARDEN_APPROVED" if "approved" in message else "WARDEN_BLOCKED",
            "ACTUATOR": "ACTION_EXECUTED" if "executed" in message else "ACTION_FAILED",
            "VERIFY": "VERIFY", "ROLLBACK": "ROLLBACK", "LEARN": "LEARN",
        }.get(stage, stage)
        state.setdefault("events", []).append({"timestamp": time.time(), "incident_id": state.get("incident", {}).get("incident_id"), "type": event_type, "agent": stage, "message": message, "status": "BLOCKED" if "blocked" in message or "rejected" in message else "OK", "metadata": {}})


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
                "action_count_within_limit": (
                    len(plan.actions)
                    <= guardrails.max_devices_reconfigured_per_cycle
                ),
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

            safe = all(checks.values())

            # Only a typed physical-intervention requirement enters this branch.
            # Routine autonomous QoD/geofence/slice remediation never reaches
            # Number Verification or SIM Swap.
            if state.get("field_intervention_required"):
                trust = await self._evaluate_field_intervention(state)
                state["trusted_dispatch"] = trust
                self._trace(state, f"TRUST_CHECK: decision={trust['decision']}; status={trust['status']}")
                if trust["decision"] != "ALLOW":
                    safe = False
                    checks["trusted_dispatch_ok"] = False

            state["warden"] = {
                "verified": safe,
                "required": True,
                "safety_checks": checks,
                "confidence": plan.confidence,
                "blast_radius": plan.blast_radius,
                "expected_cost_usd": plan.expected_cost_usd,
                "approval_required": plan.approval_required,
                "action_errors": action_errors,
                "capability_report": self.client.capability_report(),
                "reason": (
                    "network_action_safe"
                    if safe
                    else "network_safety_constraints_failed"
                ),
            }

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

        except Exception as exc:
            logger.exception(
                "WARDEN network safety validation failed"
            )

            state["warden"] = {
                "verified": False,
                "required": True,
                "reason": "network_safety_validation_error",
                "error": str(exc),
            }

            self._trace(
                state,
                "WARDEN: network safety validation failed; execution rejected",
            )

            return state
    
    async def _dust_advisory(self, fallback: Optional[bool]) -> tuple[bool, str]:
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
        except Exception as exc:
            logger.warning("Dust advisory feed unavailable; using fallback state: %s", exc)
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

        self._trace(
            state,
            "SENTINEL: sensing Nokia congestion, device status, and dust advisory",
        )

        state["dust_advisory"], state["environmental_source"] = await self._dust_advisory(
            state.get("dust_advisory", True)
        )

        # ---------------------------------------------------------
        # 1. Read real Nokia/CAMARA congestion observations
        # ---------------------------------------------------------
        congestion = await self.client.congestion_insights()

        # ---------------------------------------------------------
        # 2. Read device status from Nokia + HARIS device metadata
        # ---------------------------------------------------------
        devices = await self.client.device_status(
            self.settings.registered_devices
        )

        state["congestion"] = [
            x.model_dump()
            for x in congestion
        ]

        state["devices"] = [
            x.model_dump()
            for x in devices
        ]

        prediction = self.forecaster.predict(congestion, state["dust_advisory"], state["environmental_source"])
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
        base = {"engineer_id": engineer.engineer_id, "engineer_name": engineer.name, "masked_phone_number": mask_phone_number(engineer.phone_number), "site": site}
        if not verified_identities.is_fresh(engineer.phone_number, self.settings.trusted_dispatch_verification_ttl_seconds):
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
                started = await start_number_verification_for_dispatch(pending, self.settings)
                # The authorization URL is transient UI handoff only; it is not
                # written to trace, memory, events, or audit.
                self.latest_dispatch = {**base, "pending_id": pending.pending_id, "decision": "BLOCK", "status": "WAITING_FOR_IDENTITY_VERIFICATION", "number_verified": False, "recent_sim_swap": None, "reason": "Fresh Number Verification consent is required before dispatch.", "authorization_url": started["authorization_url"]}
            except Exception:
                pending_dispatches.complete(pending.pending_id, "BLOCKED")
                self.latest_dispatch = {**base, "pending_id": pending.pending_id, "decision": "BLOCK", "status": "WAITING_FOR_IDENTITY_VERIFICATION", "number_verified": False, "recent_sim_swap": None, "reason": "Number Verification authorization is unavailable; dispatch remains fail-closed."}
            return self.latest_dispatch

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
        self.latest_dispatch = {"pending_id": pending.pending_id, "incident_id": pending.incident_id, "engineer_id": pending.engineer_id, "masked_phone_number": mask_phone_number(pending.phone_number), **trust, "status": status}
        if status == "BLOCKED":
            attempted = {item.engineer_id for item in trusted_dispatch_history.for_incident(pending.incident_id)}
            candidates = [item for item in self.engineers.eligible(site=pending.site, required_skills=["tower-inspection"]) if item.engineer_id not in attempted]
            if len(attempted) < self.settings.trusted_dispatch_max_attempts and candidates:
                fallback = candidates[0]
                next_pending = pending_dispatches.create(incident_id=pending.incident_id, engineer_id=fallback.engineer_id, phone_number=fallback.phone_number, site=pending.site, intervention_type=pending.intervention_type, ttl_seconds=self.settings.trusted_dispatch_verification_ttl_seconds)
                try:
                    started = await start_number_verification_for_dispatch(next_pending, self.settings)
                    self.latest_dispatch = {"pending_id": next_pending.pending_id, "incident_id": pending.incident_id, "engineer_id": fallback.engineer_id, "engineer_name": fallback.name, "masked_phone_number": mask_phone_number(fallback.phone_number), "decision": "BLOCK", "status": "WAITING_FOR_IDENTITY_VERIFICATION", "fallback_from": pending.engineer_id, "reason": "Previous engineer blocked; awaiting fallback engineer consent.", "authorization_url": started["authorization_url"]}
                except Exception:
                    pending_dispatches.complete(next_pending.pending_id, "BLOCKED")
            else:
                self.latest_dispatch["status"] = "MANUAL_INTERVENTION_REQUIRED"

    async def _cartographer(self, state: HarisState) -> HarisState:
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
        )
        all_actions: List[Action] = evaluation["actions"]
        state["active_playbook"] = {
            "name": ", ".join(evaluation["playbooks"]) or "None",
            "state": "ACTIVE" if all_actions else "IDLE",
            "trigger_reason": "Dust/congestion/battery policy evidence",
            "affected_devices": incident.affected_devices,
            "current_stage": "TRIAGE",
            "latest_outcome": "proposed" if all_actions else "no_action_proposed",
        }
        self._trace(state, f"PLAYBOOK_TRIGGERED: {state['active_playbook']['name']}")
        prior_incidents = await self.memory.search_incidents(
            "sandstorm " + " ".join(incident.affected_cells), limit=3
        )
        relevant_priors = [
            prior for prior in prior_incidents
            if set(prior.affected_cells) & set(incident.affected_cells)
        ]
        state["memory_context"] = [prior.model_dump() for prior in relevant_priors]
        crew = await self._crew_advisory(incident, all_actions, relevant_priors)
        state["crew_advisory"] = crew
        self._trace(state, f"CREWAI_USED={str(crew['used']).lower()} CREWAI_AGENTS={len(self.crewai_agents)} CREWAI_FALLBACK={str(crew['fallback']).lower()} CREWAI_LATENCY_MS={crew['latency_ms']}")

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
                confidence,
            )

        # Rank the actions themselves rather than filtering them through
        # an independently selected device list.
        ranked_actions = sorted(
            all_actions,
            key=action_priority,
            reverse=True,
        )
        crew_order = crew.get("advisory", {}).get("recommended_action_order", [])
        if crew_order:
            ranked_actions.sort(key=lambda action: crew_order.index(action.kind) if action.kind in crew_order else len(crew_order))

        # Enforce the configured autonomous action limit.
        actions = ranked_actions[:max_devices]

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
        advisory = await self.reasoning.assess(
            incident,
            devices,
            actions,
        )
        self._trace(
            state,
            "AI_PLANNER_USED=" + str(advisory["ai_planner_used"]).lower() +
            f" MODEL={advisory['model'] or 'deterministic'} FALLBACK_USED={str(advisory['fallback_used']).lower()}",
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
        modified_devices = {
            action.device_id
            for action in actions
            if action.kind in {
                "qos",
                "slice_attach",
                "slice_detach",
            }
        }

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
        )

        state["plan"] = plan.model_dump()

        self._trace(
            state,
            (
                f"TRIAGE: {len(actions)} bounded actions, "
                f"devices={len(modified_devices)}, "
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
            self._trace(
                state,
                "ACTUATOR: WARDEN rejected network remediation; "
                "no action executed",
            )

            state["execution"] = {
                "executed": False,
                "reason": "warden_rejected",
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

        except Exception as exc:
            logger.exception("Actuator execution failed")

            state["execution"] = {
                "executed": False,
                "actions": executed_actions,
                "reason": "execution_error",
                "error": str(exc),
            }

            self._trace(
                state,
                (
                    f"ACTUATOR: execution failed after "
                    f"{len(executed_actions)} actions: {exc}"
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

            except Exception as exc:
                rollback_success = False

                rollback_results.append(
                    {
                        "kind": kind,
                        "device_id": device_id,
                        "operation": "rollback",
                        "success": False,
                        "error": str(exc),
                    }
                )

                logger.exception(
                    "Rollback operation failed: kind=%s device=%s",
                    kind,
                    device_id,
                )

                self._trace(
                    state,
                    (
                        f"ROLLBACK: FAILED "
                        f"kind={kind} "
                        f"device={device_id} "
                        f"error={exc}"
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
            audit={
                "environment": {"dust_advisory": state.get("dust_advisory"), "source": state.get("environmental_source")},
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
                "trace": state.get("trace", []),
                "events": state.get("events", []),
                "final_status": state.get("final_status"),
            },

            verification=verification,
            rollback=rollback,
        )

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
                "LEARN: incident stored; "
                f"outcome={outcome}; "
                f"episodic memory count={self.memory.count()}"
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

    async def run_cycle(
        self,
        dust_advisory: bool = True,
        *,
        field_intervention_required: bool = False,
        field_intervention_site: Optional[str] = None,
        field_intervention_skills: Optional[List[str]] = None,
    ) -> HarisState:
       
        initial: HarisState = {
            "cycle_id": uuid.uuid4().hex[:10],
            "dust_advisory": dust_advisory,
            "trace": [],
            "events": [],
            "field_intervention_required": field_intervention_required,
            "field_intervention_site": field_intervention_site,
            "field_intervention_skills": field_intervention_skills or [],
        }

        result = await self.graph.ainvoke(initial)
        return result
