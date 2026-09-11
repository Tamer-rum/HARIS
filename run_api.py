import asyncio
import hmac
import time
import uvicorn
from types import SimpleNamespace
from fastapi import Header, HTTPException, Response, WebSocket, WebSocketDisconnect
from agents import HarisAgentSystem
from config import get_settings
from nokia_clients import (
    app, build_nokia_client, register_dispatch_system_factory,
    register_supervisory_status_factory,
)
from nokia_clients import register_observation_store_factory
from observations import ObservationStore
from network_state import NetworkStateRegistry
from incidents import IncidentManager
from event_bus import InMemoryEventBus
from platform_events import NokiaCongestionWebhook
from scheduler import HarisScheduler
from durable_core import DurablePlatformCore
from durable_core import ProjectionIntegrityError, RepositoryUnavailable, VersionConflict
from postgres_persistence import (
    PersistenceAuthenticationFailed, PersistenceNotConfigured,
    PersistenceSchemaNotReady, PersistenceTransportUnavailable,
    build_repository_bundle,
)
from platform_lifecycle import PlatformLifecycle, PlatformLifecycleState, reconstruct_platform_state
from runtime import RuntimeEnvironment, external_access_policy, runtime_environment
from runtime_events import (
    DurableOutboxWakeupConsumer, RuntimeEventIngestor,
    RuntimeEventNotReady, RuntimeIngestionMetrics,
    ingest_observation_view, projection_availability,
)
from durable_reasoning import DurableIncidentDecisionService
from durable_execution import DurableActionExecutionService, ExistingNokiaActuatorAdapter
from durable_reconciliation import (
    DurableActionReconciliationService, DurableReconciliationScheduler,
    reconciliation_public_view,
)
from api_security import (
    OperationalAuthError, RouteClass, authenticate_bearer,
    operational_rate_limiter, redact_provider_identifiers,
)

_scheduler = None
_system = None
_scheduler_task = None
_observations = None
_observation_task = None
_network_state = None
_incident_manager = None
_durable_core = None
_repository_bundle = None
_persistence_error = None
_platform_lifecycle = PlatformLifecycle()
_event_bus = InMemoryEventBus()
_websocket_clients: set[WebSocket] = set()
_runtime_ingestor = None
_runtime_consumer = None
_durable_decision_service = None
_durable_execution_service = None
_durable_reconciliation_service = None
_durable_reconciliation_scheduler = None
_reconciliation_task = None
_runtime_task = None
_runtime_metrics = RuntimeIngestionMetrics()


def initialize_platform(*, settings=None, runtime=None, transport_factory=None) -> bool:
    """Build persistence and reconstruct runtime views before workers start."""
    global _durable_core, _repository_bundle, _persistence_error
    global _network_state, _incident_manager
    global _runtime_ingestor, _runtime_consumer, _runtime_metrics
    global _durable_decision_service, _durable_execution_service
    global _durable_reconciliation_service, _durable_reconciliation_scheduler
    settings = settings or get_settings()
    mode = str(settings.haris_persistence_mode).lower().strip()
    _platform_lifecycle.reset(mode)
    _platform_lifecycle.transition(PlatformLifecycleState.PERSISTENCE_CONFIGURING)
    _durable_core = None; _repository_bundle = None
    _network_state = None; _incident_manager = None
    _runtime_ingestor = None; _runtime_consumer = None
    _durable_decision_service = None
    _durable_execution_service = None
    _durable_reconciliation_service = None
    _durable_reconciliation_scheduler = None
    _runtime_metrics = RuntimeIngestionMetrics()
    try:
        effective_runtime = runtime or runtime_environment()
        if effective_runtime is RuntimeEnvironment.PRODUCTION and mode != "postgres":
            raise PersistenceNotConfigured("PERSISTENCE_NOT_CONFIGURED")
        _platform_lifecycle.configured = mode == "memory" or bool(settings.supabase_url and settings.supabase_key)
        _platform_lifecycle.transition(PlatformLifecycleState.PERSISTENCE_CONNECTING)
        bundle = build_repository_bundle(settings, runtime=runtime, transport_factory=transport_factory)
        _repository_bundle = bundle
        _platform_lifecycle.connected = True
        _platform_lifecycle.transition(PlatformLifecycleState.RECONSTRUCTING)
        _platform_lifecycle.reconstruction = "IN_PROGRESS"
        result = reconstruct_platform_state(bundle)
        core = DurablePlatformCore(
            events=bundle.events, network=bundle.network_state,
            incidents=bundle.incidents, actions=bundle.actions,
            ownership=bundle.resource_ownership,
            verifications=bundle.verification, recoveries=bundle.recovery,
            outbox=bundle.outbox, inbox=bundle.inbox,
            checkpoints=bundle.checkpoints, cost_ledger=bundle.cost_ledger,
        )
        core.restore_reconstruction(result)
        registry = NetworkStateRegistry(
            mode=settings.nac_mode,
            stale_after_seconds=settings.nokia_congestion_interval_seconds * 2,
        )
        registry.restore_projection(result.network_state)
        manager = IncidentManager(None, registry, settings, durable_core=core)
        manager.restore_records(result.active_incidents)
        _durable_core, _network_state, _incident_manager = core, registry, manager
        _platform_lifecycle.ready(result)
        _runtime_metrics.increment("reconstruction_completed")
        _persistence_error = None
        return True
    except PersistenceNotConfigured:
        _persistence_error = "PERSISTENCE_NOT_CONFIGURED"
        _platform_lifecycle.configured = False
        _platform_lifecycle.fail(_persistence_error)
    except PersistenceAuthenticationFailed:
        _persistence_error = "PERSISTENCE_AUTH_FAILED"
        _platform_lifecycle.fail(_persistence_error)
    except PersistenceSchemaNotReady:
        _persistence_error = "PERSISTENCE_SCHEMA_NOT_READY"
        _platform_lifecycle.fail(_persistence_error)
    except PersistenceTransportUnavailable:
        _persistence_error = "PERSISTENCE_UNAVAILABLE"
        _platform_lifecycle.fail(_persistence_error)
    except ProjectionIntegrityError:
        _persistence_error = "PROJECTION_INTEGRITY_FAILED"
        _platform_lifecycle.fail(_persistence_error, failed=True)
    except RepositoryUnavailable:
        _persistence_error = "RECONSTRUCTION_FAILED" if _repository_bundle is not None else "PERSISTENCE_UNAVAILABLE"
        _platform_lifecycle.fail(_persistence_error, failed=_repository_bundle is not None)
    except Exception:
        _persistence_error = "RECONSTRUCTION_FAILED"
        _platform_lifecycle.fail(_persistence_error, failed=True)
    _durable_core = None
    return False


def get_durable_core() -> DurablePlatformCore:
    """Return the backend-owned domain core without starting external work.

    Repository selection is intentionally behind the core interfaces.  In the
    offline suite this is strictly in-memory; a production adapter is wired by
    server configuration, never by Streamlit/browser code.
    """
    global _durable_core, _repository_bundle, _persistence_error
    if _durable_core is None:
        settings = get_settings()
        if _platform_lifecycle.state is PlatformLifecycleState.STARTING and str(settings.haris_persistence_mode).lower().strip() == "memory":
            initialize_platform(settings=settings)
        if _durable_core is None:
            raise RepositoryUnavailable(_persistence_error or "PERSISTENCE_UNAVAILABLE")
    return _durable_core


def get_network_state_registry() -> NetworkStateRegistry:
    global _network_state
    if _network_state is None:
        settings = get_settings()
        _network_state = NetworkStateRegistry(mode=settings.nac_mode, stale_after_seconds=settings.nokia_congestion_interval_seconds * 2)
        _network_state.restore_projection(get_durable_core().snapshot()["network_state"])
    return _network_state


def get_observation_store() -> ObservationStore:
    """Lazily construct read-only evidence state without polling on import."""
    global _observations
    if _observations is None:
        settings = get_settings()
        # ObservationStore is a read cache. Durable ingestion owns projection;
        # it must not mutate the operational registry before commit.
        _observations = ObservationStore(build_nokia_client(settings), settings, registry=None)
    return _observations


def get_incident_manager() -> IncidentManager:
    global _incident_manager
    if _incident_manager is None:
        settings = get_settings()
        _incident_manager = IncidentManager(_system, get_network_state_registry(), settings, durable_core=get_durable_core())
        _incident_manager.restore_from_durable()
    if _system is not None:
        _incident_manager.system = _system
    return _incident_manager


def get_haris_system() -> HarisAgentSystem:
    """Construct the expensive agent graph only when a backend feature needs it."""
    global _system
    if _system is None:
        settings = get_settings()
        _system = HarisAgentSystem(build_nokia_client(settings), settings=settings)
        _system.set_observation_store(get_observation_store())
        get_incident_manager().system = _system
    return _system


def _runtime_ready() -> bool:
    return (
        _platform_lifecycle.state is PlatformLifecycleState.READY
        and _durable_core is not None
        and _durable_core.readiness == "READY"
    )


async def _publish_committed_runtime_event(_event) -> None:
    """Refresh disposable views and broadcast only committed durable state."""
    snapshot = _authoritative_snapshot()
    registry = get_network_state_registry()
    registry.restore_projection(snapshot["network_state"])
    await _broadcast({
        "type": "network.state.changed",
        "version": snapshot["state_version"],
        "event_id": _event.event_id,
        "timestamp": _event.received_at,
        "data": snapshot,
    })


def get_runtime_outbox_consumer() -> DurableOutboxWakeupConsumer:
    global _runtime_consumer
    if _runtime_consumer is None:
        if _repository_bundle is None:
            get_durable_core()
        _runtime_consumer = DurableOutboxWakeupConsumer(
            bundle=_repository_bundle, event_bus=_event_bus,
            is_ready=_runtime_ready, post_commit=_publish_committed_runtime_event,
            incident_ready=get_durable_incident_decision_service().handle_durable_incident_ready,
            decision_ready=get_durable_action_execution_service().handle_durable_decision_ready,
            reconciliation_ready=get_durable_action_reconciliation_service().handle_durable_reconciliation_ready,
            action_ready=get_durable_action_execution_service().handle_durable_action_ready,
            metrics=_runtime_metrics,
        )
    return _runtime_consumer


def get_durable_incident_decision_service() -> DurableIncidentDecisionService:
    """Lazily bind the existing HARIS graph to durable incident authority."""
    global _durable_decision_service
    if _durable_decision_service is None:
        if _repository_bundle is None:
            get_durable_core()
        _durable_decision_service = DurableIncidentDecisionService(
            bundle=_repository_bundle,
            agent_system=get_haris_system(),
            is_ready=_runtime_ready,
            metrics=_runtime_metrics,
        )
    return _durable_decision_service


def get_durable_action_execution_service() -> DurableActionExecutionService:
    """Lazily bind the Phase 7C executor to durable backend authority."""
    global _durable_execution_service
    if _durable_execution_service is None:
        if _repository_bundle is None:
            get_durable_core()
        settings = get_settings()
        _durable_execution_service = DurableActionExecutionService(
            bundle=_repository_bundle,
            adapter=ExistingNokiaActuatorAdapter(get_haris_system().client, settings),
            settings=settings,
            is_ready=_runtime_ready,
            metrics=_runtime_metrics,
        )
    return _durable_execution_service


def get_durable_action_reconciliation_service() -> DurableActionReconciliationService:
    """Lazily bind Phase 7D safe-read continuation to durable authority."""
    global _durable_reconciliation_service
    if _durable_reconciliation_service is None:
        executor = get_durable_action_execution_service()
        _durable_reconciliation_service = DurableActionReconciliationService(
            bundle=_repository_bundle, adapter=executor.adapter,
            settings=get_settings(), execution_service=executor,
            is_ready=_runtime_ready, metrics=_runtime_metrics,
        )
    return _durable_reconciliation_service


def get_durable_reconciliation_scheduler() -> DurableReconciliationScheduler:
    """Create the scanner that schedules only due work from durable records."""
    global _durable_reconciliation_scheduler
    if _durable_reconciliation_scheduler is None:
        _durable_reconciliation_scheduler = DurableReconciliationScheduler(
            bundle=_repository_bundle,
            service=get_durable_action_reconciliation_service(),
            is_ready=_runtime_ready,
        )
    return _durable_reconciliation_scheduler


def get_runtime_event_ingestor() -> RuntimeEventIngestor:
    global _runtime_ingestor
    if _runtime_ingestor is None:
        consumer = get_runtime_outbox_consumer()
        _runtime_ingestor = RuntimeEventIngestor(
            bundle=_repository_bundle, core=get_durable_core(),
            is_ready=_runtime_ready, wakeup=consumer.wake,
            metrics=_runtime_metrics,
        )
    return _runtime_ingestor


def _authoritative_snapshot() -> dict:
    core = get_durable_core()
    snapshot = core.snapshot()
    from recovery_consistency import overlay_authoritative_recovery
    snapshot["active_incidents"] = [
        overlay_authoritative_recovery(_repository_bundle or SimpleNamespace(recovery=core.recoveries), row)
        for row in snapshot.get("active_incidents", [])
    ]
    settings = get_settings()
    snapshot["network_state_status"] = projection_availability(
        snapshot["network_state"],
        stale_after_seconds=settings.nokia_congestion_interval_seconds * 2,
    )
    snapshot["event_runtime"] = {
        "authority": "DURABLE_REPOSITORY",
        "cache_role": "VIEW_ONLY",
        "metrics": _runtime_metrics.snapshot(),
        "outbox_consumer": (
            _runtime_consumer.status() if _runtime_consumer is not None
            else {"state": "STOPPED", "owner_type": "HARIS_RUNTIME"}
        ),
    }
    # Some embedded/offline callers construct the canonical DurablePlatformCore
    # directly. Its repositories are still authoritative even when the
    # composition-root bundle reference is absent.
    reconciliation_bundle = _repository_bundle or SimpleNamespace(
        incidents=core.incidents, actions=core.actions,
        verification=core.verifications,
    )
    snapshot["reconciliation_runtime"] = {
        "authority": "DURABLE_REPOSITORY",
        "worker": (
            _durable_reconciliation_scheduler.status()
            if _durable_reconciliation_scheduler is not None
            else {"state": "STOPPED", "authority": "DURABLE_REPOSITORY"}
        ),
        "actions": reconciliation_public_view(
            reconciliation_bundle, settings, time.time(),
        ),
    }
    snapshot.update(_durable_noc_read_model(core))
    # This process-local field is a bounded post-commit view only.  The
    # durable incident/action repositories above remain authoritative, and
    # the field is absent until the decision service has persisted and
    # explicitly accepted a completed Phase 7B boundary.
    if _system is not None:
        cycle = _system.current_cycle_status
        decision_status = cycle.get("decision_status")
        if decision_status in {"EVALUATING", "BLOCKED", "ESCALATED", "AUTHORIZED_PLAN"}:
            snapshot["latest_decision"] = {
                "incident_id": (cycle.get("incident") or {}).get("incident_id"),
                "status": decision_status,
                "provider_execution_performed": False,
                "authority": "DURABLE_DECISION_POSTCOMMIT_VIEW",
            }
    return redact_provider_identifiers(snapshot)


def _record_timestamp(record: dict, *names: str) -> float:
    for name in names:
        value = record.get(name)
        if isinstance(value, (int, float)):
            return float(value)
    return 0.0


def _durable_noc_read_model(core: DurablePlatformCore, *, limit: int = 50) -> dict:
    """Build a deterministic operator view from durable repositories only.

    This is a read model, not a second authority. It intentionally excludes
    provider resource identities and mutation parameters.
    """
    recent = getattr(core.incidents, "recent", None)
    incidents = recent(limit) if callable(recent) else core.incidents.active()
    event_sequence = core.events.sequence()
    recent_events = core.events.events_after(max(0, event_sequence - 500))
    history = []
    timeline = []
    terminal = {"RESOLVED", "BLOCKED", "FAILED", "CANCELLED"}
    for incident in incidents:
        from recovery_consistency import overlay_authoritative_recovery
        incident = overlay_authoritative_recovery(
            _repository_bundle or SimpleNamespace(recovery=core.recoveries), incident,
        )
        incident_id = str(incident.get("incident_id") or "")
        actions = core.actions.for_incident(incident_id)
        verifications = core.verifications.for_incident(incident_id)
        recovery = core.recoveries.for_incident(incident_id)
        transitions = core.incidents.transitions(incident_id)
        history.append({
            "incident": incident,
            "actions": [vars(item) for item in actions],
            "verifications": verifications,
            "recovery": recovery,
            "transitions": transitions,
            "terminal": str(incident.get("state")) in terminal,
            "final_truth": incident.get("outcome") or "UNAVAILABLE",
            "authority": "DURABLE_REPOSITORY",
        })
        for _, event in recent_events:
            if event.correlation_key != incident.get("correlation_key") and event.event_id != incident.get("trigger_event_id"):
                continue
            timeline.append({
                "timestamp": float(event.source_timestamp),
                "incident_id": incident_id,
                "type": "OBSERVATION",
                "agent": "SENTINEL",
                "message": event.event_type.value,
                "status": "OBSERVED",
                "provenance": event.provenance.value,
            })
        for row in transitions:
            timeline.append({
                "timestamp": _record_timestamp(row, "timestamp", "occurred_at"),
                "incident_id": incident_id,
                "type": "INCIDENT_TRANSITION",
                "agent": row.get("actor") or "HARIS",
                "message": row.get("reason_code") or "incident_state_transition",
                "status": row.get("to_state") or "UNAVAILABLE",
                "provenance": incident.get("trigger_provenance") or "UNAVAILABLE",
            })
        for action in actions:
            timeline.append({
                "timestamp": float(action.completed_at or action.last_attempt_at or action.requested_at or 0),
                "incident_id": incident_id,
                "type": "ACTION",
                "agent": "ACTUATOR",
                "message": action.command_type,
                "status": action.state.value,
                "provenance": "DERIVED",
                "action_id": action.command_id,
            })
        for row in verifications:
            timeline.append({
                "timestamp": _record_timestamp(row, "verified_at", "updated_at", "created_at"),
                "incident_id": incident_id,
                "type": "VERIFICATION",
                "agent": "VERIFY",
                "message": row.get("reason") or row.get("outcome") or "network_verification",
                "status": row.get("state") or "UNAVAILABLE",
                "provenance": row.get("source_provenance") or row.get("provenance") or "UNAVAILABLE",
                "action_id": row.get("action_id"),
            })
        if recovery:
            timeline.append({
                "timestamp": _record_timestamp(recovery, "completed_at", "updated_at", "created_at", "started_at"),
                "incident_id": incident_id,
                "type": "RECOVERY",
                "agent": "RECOVERY",
                "message": recovery.get("reason") or recovery.get("outcome") or "recovery_state",
                "status": recovery.get("state") or "UNAVAILABLE",
                "provenance": recovery.get("provenance") or "DERIVED",
                "action_id": recovery.get("action_id"),
            })
    timeline.sort(key=lambda row: (float(row.get("timestamp") or 0), str(row.get("incident_id")), str(row.get("type")), str(row.get("action_id") or "")))
    pending_actions = core.actions.pending_or_unknown()
    pending_reconciliation = core.actions.reconciliation_required()
    metrics = {
        "authority": "DURABLE_REPOSITORY",
        "active_incidents": len(core.incidents.active()),
        "terminal_incidents_in_view": sum(1 for item in history if item["terminal"]),
        "pending_actions": len(pending_actions),
        "reconciliation_pending": len(pending_reconciliation),
        "verification_pending": len(core.verifications.pending()),
        "recovery_pending": len(core.recoveries.pending()),
        "outbox_pending": len(core.outbox.unsent()),
        "websocket_clients": len(_websocket_clients),
        "websocket_clients_scope": "PROCESS_LOCAL_DELIVERY_ONLY",
        "reconstruction": _platform_lifecycle.reconstruction,
        "reconstruction_duration_ms": round(_platform_lifecycle.metrics.reconstruction_duration_ms, 3),
    }
    return {
        "incident_history": history,
        "timeline": timeline,
        "operational_metrics": metrics,
        "timeline_authority": "DERIVED_FROM_DURABLE",
    }


def _durable_supervisory_status(system=None) -> dict:
    """Build the operational console view from durable domain authority.

    Fixture/development demos retain their existing in-process presentation
    model. Production and live modes never allow a stale graph cycle or the
    legacy MemoryStore to override durable incident/action/verification state.
    Trusted Dispatch remains explicitly process-local until its later durable
    persistence phase.
    """
    settings = get_settings()
    production_or_live = (
        runtime_environment() is RuntimeEnvironment.PRODUCTION
        or settings.nac_mode in {"live_read_only", "live_write"}
    )
    if not production_or_live:
        return (system or get_haris_system()).current_supervisory_status
    if not _runtime_ready():
        return {
            "haris_state": "NOT_READY", "cycle": {}, "active_incident": {},
            "dispatch_history": [],
            "trusted_dispatch_authority": "PROCESS_LOCAL_SINGLE_INSTANCE",
            "authority": "DURABLE_REPOSITORY", "persistence": _platform_lifecycle.public_status(),
        }

    snapshot = _authoritative_snapshot()
    incidents = list(snapshot.get("active_incidents") or [])
    incidents.sort(
        key=lambda row: float(row.get("updated_at") or row.get("opened_at") or 0),
        reverse=True,
    )
    incident = incidents[0] if incidents else {}
    incident_id = incident.get("incident_id")
    actions = []
    verifications = []
    recovery = None
    if incident_id:
        core = get_durable_core()
        actions = [vars(item) for item in core.actions.for_incident(incident_id)]
        verifications = core.verifications.for_incident(incident_id)
        recovery = core.recoveries.for_incident(incident_id)

    incident_timeline = [
        item for item in snapshot.get("timeline", [])
        if item.get("incident_id") == incident_id
    ]

    dispatch = {}
    dispatch_history = []
    if _system is not None:
        candidate = _system.current_dispatch_status
        if candidate.get("incident_id") and (
            candidate.get("incident_id") == incident_id or not incident_id
        ):
            dispatch = {**candidate, "authority": "PROCESS_LOCAL_SINGLE_INSTANCE", "durable": False}
            dispatch_history = [
                {**item, "authority": "PROCESS_LOCAL_SINGLE_INSTANCE", "durable": False}
                for item in _system.current_cycle_status.get("dispatch_history", [])
            ]

    if not incident and dispatch:
        local_incident = _system.current_cycle_status.get("incident") or {}
        if local_incident.get("incident_id") == dispatch.get("incident_id"):
            incident = {
                **local_incident,
                "authority": "PROCESS_LOCAL_SINGLE_INSTANCE",
                "durable": False,
            }
    durable_state = str(incident.get("state") or "READY")
    haris_state = dispatch.get("status") or durable_state
    cycle = {
        "cycle_id": None,
        "final_status": incident.get("outcome"),
        "decision_status": incident.get("warden_decision"),
        "incident": incident,
        "plan": {
            "plan_version": incident.get("plan_version"),
            "confidence": incident.get("effective_confidence"),
            "blast_radius": incident.get("blast_radius"),
            "actions": actions,
        },
        "warden": {"decision": incident.get("warden_decision")},
        "execution": {"actions": actions},
        "verification": verifications[-1] if verifications else {},
        "rollback": recovery or {},
        "trusted_dispatch": dispatch,
        "dispatch_history": dispatch_history,
        "trace": [], "events": incident_timeline,
        "authority": "DURABLE_REPOSITORY",
    }
    return redact_provider_identifiers({
        "haris_state": haris_state,
        "cycle": cycle,
        "active_incident": incident,
        "dispatch_history": dispatch_history,
        "audit": {
            "authority": "DURABLE_REPOSITORY",
            "state_version": snapshot.get("state_version"),
            "records": [],
        },
        "incident_history": snapshot.get("incident_history", []),
        "timeline": snapshot.get("timeline", []),
        "operational_metrics": snapshot.get("operational_metrics", {}),
        "timeline_authority": snapshot.get("timeline_authority"),
        "trusted_dispatch_authority": "PROCESS_LOCAL_SINGLE_INSTANCE",
        "authority": "DURABLE_REPOSITORY",
        "persistence": _platform_lifecycle.public_status(),
    })


register_dispatch_system_factory(get_haris_system)
register_observation_store_factory(get_observation_store)
register_supervisory_status_factory(_durable_supervisory_status)


@app.get("/api/nac/network-state")
async def network_state() -> dict:
    """Sanitized logical-cell state for the supervisory map."""
    if not _runtime_ready():
        return {"status": "UNAVAILABLE", "source": "PERSISTENCE_LIFECYCLE", "entities": {}}
    try:
        durable = _authoritative_snapshot()
        registry = get_network_state_registry()
        registry.restore_projection(durable["network_state"])
        view = registry.snapshot()
        view.update({"status": durable["network_state_status"], "authority": "DURABLE_REPOSITORY", "cache_role": "VIEW_ONLY"})
        return view
    except RepositoryUnavailable:
        _platform_lifecycle.fail("PERSISTENCE_UNAVAILABLE")
        return {"status": "UNAVAILABLE", "source": "PERSISTENCE_LIFECYCLE", "entities": {}}


@app.get("/api/nac/incidents/active")
async def active_incidents() -> dict:
    """Backend-owned correlated incidents; no action/session secrets."""
    if not _runtime_ready():
        return {"status": "UNAVAILABLE", "source": "PERSISTENCE_LIFECYCLE", "active_incidents": [], "incidents": []}
    try:
        records = _authoritative_snapshot()["active_incidents"]
        return {"status": "AVAILABLE", "source": "DURABLE_REPOSITORY", "active_incidents": records, "incidents": records}
    except RepositoryUnavailable:
        _platform_lifecycle.fail("PERSISTENCE_UNAVAILABLE")
        return {"status": "UNAVAILABLE", "source": "PERSISTENCE_LIFECYCLE", "active_incidents": [], "incidents": []}


@app.get("/api/platform/health")
async def platform_health(response: Response) -> dict:
    """Public readiness: HTTP 200 only after durable reconstruction succeeds."""
    settings = get_settings()
    if _durable_core is None and _platform_lifecycle.state is PlatformLifecycleState.STARTING and str(settings.haris_persistence_mode).lower().strip() == "memory":
        initialize_platform(settings=settings)
    persistence = _platform_lifecycle.public_status()
    if _durable_core is None or _durable_core.readiness != "READY":
        response.status_code = 503
        return {"status": "NOT_READY", "reconstruction": persistence["reconstruction"], "persistence": persistence}
    if _platform_lifecycle.state is PlatformLifecycleState.STARTING:
        persistence.update({"status":"READY","lifecycle_state":"READY","configured":True,"connected":True,"reconstruction":"COMPLETE","repository_ready":True,"reason":None})
    try:
        state_version = _durable_core.events.sequence()
    except RepositoryUnavailable:
        _platform_lifecycle.fail("PERSISTENCE_UNAVAILABLE")
        persistence = _platform_lifecycle.public_status()
        response.status_code = 503
        return {"status":"NOT_READY","reconstruction":persistence["reconstruction"],"persistence":persistence}
    try:
        operational = _authoritative_snapshot().get("operational_metrics", {})
    except RepositoryUnavailable:
        _platform_lifecycle.fail("PERSISTENCE_UNAVAILABLE")
        response.status_code = 503
        persistence = _platform_lifecycle.public_status()
        return {"status":"NOT_READY","reconstruction":persistence["reconstruction"],"persistence":persistence}
    degraded_reasons = []
    if operational.get("reconciliation_pending"):
        degraded_reasons.append("RECONCILIATION_PENDING")
    if operational.get("recovery_pending"):
        degraded_reasons.append("RECOVERY_PENDING")
    return {
        "status":"READY", "readiness":"READY",
        "operational_status":"DEGRADED" if degraded_reasons else "HEALTHY",
        "degraded_reasons": degraded_reasons,
        "reconstruction":"READY", "state_version":state_version,
        "source":"DURABLE_REPOSITORY", "persistence":persistence,
    }


@app.get("/api/v1/noc/snapshot")
async def noc_snapshot() -> dict:
    """Sanitized recovery snapshot for reconnecting supervisory clients."""
    if _durable_core is not None and _durable_core.readiness == "READY":
        try:return {**_authoritative_snapshot(), "persistence": _platform_lifecycle.public_status()}
        except RepositoryUnavailable:_platform_lifecycle.fail("PERSISTENCE_UNAVAILABLE")
    status = _platform_lifecycle.public_status()
    return {"platform_status":"NOT_READY","lifecycle_state":status["lifecycle_state"],"reason":status["reason"],"source":"PERSISTENCE_LIFECYCLE","persistence":status}


@app.post("/api/events/nokia/congestion", status_code=202)
async def ingest_nokia_congestion(event: NokiaCongestionWebhook, x_haris_event_secret: str | None = Header(default=None)) -> dict:
    """Authenticate, normalize, enqueue, and acknowledge quickly.

    The handler intentionally does not invoke LangGraph or a Nokia mutation.
    """
    settings = get_settings()
    expected = settings.nokia_event_webhook_secret.get_secret_value() if settings.nokia_event_webhook_secret else None
    if not expected or x_haris_event_secret is None or not hmac.compare_digest(
        x_haris_event_secret.encode("utf-8"), expected.encode("utf-8")
    ):
        raise HTTPException(status_code=401, detail="Unauthorized event notification.")
    canonical = event.canonical(mode=settings.nac_mode)
    try:
        result = await get_runtime_event_ingestor().ingest_runtime_event(canonical)
        return result.public()
    except RuntimeEventNotReady as exc:
        raise HTTPException(status_code=503, detail="Durable event runtime is not ready.") from exc
    except VersionConflict as exc:
        raise HTTPException(status_code=409, detail="Concurrent state update; retry delivery.") from exc
    except RepositoryUnavailable as exc:
        _platform_lifecycle.fail("PERSISTENCE_UNAVAILABLE")
        raise HTTPException(status_code=503, detail="Durable event processing is unavailable.") from exc


@app.websocket("/ws/noc")
async def noc_websocket(websocket: WebSocket) -> None:
    try:
        principal_digest = authenticate_bearer(
            get_settings().haris_operational_api_token,
            websocket.headers.get("authorization"),
        )
    except OperationalAuthError as exc:
        await websocket.close(code={401: 4401, 403: 4403}.get(exc.status_code, 1013))
        return
    client_address = websocket.client.host if websocket.client else "unknown"
    if not operational_rate_limiter.allow(principal_digest, RouteClass.OPERATIONAL_READ, client_address):
        await websocket.close(code=4429)
        return
    await websocket.accept(); _websocket_clients.add(websocket)
    try:
        if _durable_core is None or _durable_core.readiness != "READY":
            await websocket.send_json({"type":"platform_state","data":_platform_lifecycle.public_status()})
        else:
            core = _durable_core
            try:
                snapshot = redact_provider_identifiers(_authoritative_snapshot())
                await websocket.send_json({"type": "snapshot", "version": snapshot["state_version"], "data": {**snapshot, "event_bus": _event_bus.health(), "persistence": _platform_lifecycle.public_status()}})
            except RepositoryUnavailable:
                _platform_lifecycle.fail("PERSISTENCE_UNAVAILABLE")
                await websocket.send_json({"type":"platform_state","data":_platform_lifecycle.public_status()})
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _websocket_clients.discard(websocket)


async def _broadcast(message: dict) -> None:
    safe_message = redact_provider_identifiers(message)
    for client in list(_websocket_clients):
        try: await client.send_json(safe_message)
        except Exception: _websocket_clients.discard(client)


@app.on_event("startup")
async def start_haris_scheduler():
    global _scheduler_task, _observation_task, _runtime_task, _reconciliation_task
    # Reconstruction completes before any worker/scheduler can be started.
    # It performs no Nokia, LLM, Redis, or Supabase call in the offline mode.
    if not initialize_platform():
        # Persistence-enabled production never starts observation/action work
        # against an empty process-local fallback.
        return
    settings = get_settings()
    # The durable outbox consumer is the first post-reconstruction worker.
    # It publishes only rows claimed after a committed transaction.
    if not external_access_policy().is_test:
        _runtime_task = asyncio.create_task(_start_runtime_events(), name="haris-runtime-events")
        _reconciliation_task = asyncio.create_task(
            get_durable_reconciliation_scheduler().run(),
            name="haris-durable-reconciliation",
        )
    if settings.nokia_observation_enabled:
        # A store/client can be constructed synchronously; polling starts in a
        # background task after the ASGI server is ready to bind its port.
        _observation_task = asyncio.create_task(_start_observations(), name="haris-observation-bootstrap")
    if settings.enable_continuous_loop:
        if settings.is_live:
            # Live incidents are driven by fresh High evidence from the
            # ObservationStore; do not run arbitrary global graph cycles.
            return
        # Do not delay web readiness with graph/CrewAI construction. The
        # scheduled task builds it immediately after startup returns.
        _scheduler_task = asyncio.create_task(_start_scheduler(), name="haris-scheduler-bootstrap")


async def _start_scheduler() -> None:
    global _scheduler
    settings = get_settings()
    _scheduler = HarisScheduler(get_haris_system(), settings)
    await _scheduler.start()


async def _start_observations() -> None:
    store = get_observation_store()
    store.add_listener(lambda view: ingest_observation_view(get_runtime_event_ingestor(), view))
    await store.start()


async def _start_runtime_events() -> None:
    await get_runtime_outbox_consumer().run()


@app.on_event("shutdown")
async def stop_haris_scheduler():
    tasks = []
    if _scheduler_task:
        _scheduler_task.cancel()
        tasks.append(_scheduler_task)
    if _scheduler:
        await _scheduler.stop()
    if _observation_task:
        _observation_task.cancel()
        tasks.append(_observation_task)
    if _observations:
        await _observations.stop()
    if _runtime_consumer:
        _runtime_consumer.stop()
    if _runtime_task:
        _runtime_task.cancel()
        tasks.append(_runtime_task)
    if _durable_reconciliation_scheduler:
        _durable_reconciliation_scheduler.stop()
    if _reconciliation_task:
        _reconciliation_task.cancel()
        tasks.append(_reconciliation_task)
    for client in list(_websocket_clients):
        try:
            await client.close(code=1001)
        except Exception:
            pass
        finally:
            _websocket_clients.discard(client)
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    settings = get_settings()
    # Trusted Dispatch and rate limiting are deliberately single-process for
    # this release.  An ambient WEB_CONCURRENCY value must not widen topology.
    uvicorn.run(app, host=settings.api_host, port=settings.api_port, workers=1)
