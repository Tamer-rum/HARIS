"""Durable-first runtime event ingestion and post-commit wake-up.

The database/repository bundle is authoritative.  Process-local queues and
views are disposable notification caches that can be rebuilt after restart.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import logging
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Optional

from durable_core import (
    IncidentState,
    RecoveryState,
    RepositoryUnavailable,
    ResourceAlreadyOwned,
    VerificationState,
    VersionConflict,
)
from platform_events import EventType, HarisEvent, Provenance


logger = logging.getLogger("haris.runtime_events")


class ProcessingState(str, Enum):
    RECEIVED = "RECEIVED"
    VALIDATED = "VALIDATED"
    DURABLE = "DURABLE"
    CORRELATED = "CORRELATED"
    READY_FOR_REASONING = "READY_FOR_REASONING"
    FAILED = "FAILED"


class RuntimeEventNotReady(RepositoryUnavailable):
    pass


@dataclass(frozen=True)
class RuntimeIngestionResult:
    status: str
    event_id: str
    processing_state: ProcessingState
    projection_updated: bool
    incident_id: Optional[str]
    incident_created: bool
    wakeup_enqueued: bool
    trace_id: str

    def public(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "event_id": self.event_id,
            "processing_state": self.processing_state.value,
            "projection_updated": self.projection_updated,
            "incident_id": self.incident_id,
            "incident_created": self.incident_created,
            "wakeup_enqueued": self.wakeup_enqueued,
        }


class RuntimeIngestionMetrics:
    """Safe aggregate counters; payloads and entity identities are excluded."""

    NAMES = (
        "event_received", "event_duplicate", "event_persisted",
        "incident_created", "incident_reused", "projection_updated",
        "outbox_enqueued", "outbox_claimed", "processing_failed",
        "reconstruction_completed", "version_conflict",
        "outbox_lease_lost",
        "decision_evaluated", "decision_authorized", "decision_blocked",
        "decision_escalated", "decision_duplicate",
        "execution_evaluated", "execution_blocked", "execution_sent",
        "execution_verified", "execution_unknown", "execution_rolled_back",
        "reconciliation_started", "reconciliation_completed",
        "reconciliation_pending", "reconciliation_unknown",
        "reconciliation_escalated", "provider_safe_read",
        "reverification_started", "reverification_improved",
        "reverification_unchanged", "reverification_degraded",
        "reconciliation_lease_lost",
    )

    def __init__(self) -> None:
        self._counts: Counter[str] = Counter()

    def increment(self, name: str) -> None:
        if name not in self.NAMES:
            raise ValueError("unsupported runtime metric")
        self._counts[name] += 1

    def snapshot(self) -> Dict[str, int]:
        return {name: int(self._counts[name]) for name in self.NAMES}


@dataclass
class _InboundPlan:
    projection: Optional[Dict[str, Any]]
    incident: Optional[Dict[str, Any]]
    existing_incident: Optional[Dict[str, Any]]
    outbox: Dict[str, Any]


def _incident_for(event: HarisEvent) -> Dict[str, Any]:
    stable = hashlib.sha256(f"{event.correlation_key}|{event.event_id}".encode()).hexdigest()[:12]
    now = float(event.created_at)
    return {
        "incident_id": f"inc-{stable}",
        "schema_version": 1,
        "correlation_key": event.correlation_key,
        "primary_entity": event.entity_id,
        "affected_entities": [event.entity_id],
        "affected_devices": [],
        "trigger_event_id": event.event_id,
        "trigger_provenance": event.provenance.value,
        "trigger_source_timestamp": event.source_timestamp,
        "opened_at": now,
        "updated_at": now,
        "severity": "critical",
        "priority": "P1",
        "state": IncidentState.DETECTED.value,
        "plan_version": 0,
        "warden_decision": None,
        "verification_state": VerificationState.PENDING.value,
        "recovery_state": RecoveryState.PENDING.value,
        "outcome": None,
        "closed_at": None,
        "version": 0,
        "trace_id": event.trace_id,
    }


def _projection_for(event: HarisEvent, current: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    observed_key = {
        EventType.NETWORK_CONGESTION_CHANGED: "raw_congestion_observed_at",
        EventType.DEVICE_REACHABILITY_CHANGED: "reachability_observed_at",
        EventType.DEVICE_LOCATION_UPDATED: "location_observed_at",
    }.get(event.event_type)
    if observed_key is None:
        return None
    if current and current.get(observed_key) is not None:
        if float(event.source_timestamp) < float(current[observed_key]):
            return None

    now = float(event.created_at)
    item = copy.deepcopy(current or {
        "entity_id": event.entity_id,
        "entity_type": event.entity_type,
        "mapping_source": "HARIS_CONFIGURED_LOGICAL_CELL_MAPPING",
        "provenance": event.provenance.value,
        "raw_congestion": None,
        "raw_congestion_observed_at": None,
        "reachability_summary": None,
        "reachability_observed_at": None,
        "location_summary": None,
        "location_observed_at": None,
        "freshness": "UNAVAILABLE",
        "haris_operational_state": "MONITORING",
        "active_incident_ids": [],
        "last_source_change_at": None,
        "last_operational_change_at": None,
        "updated_at": now,
        "version": 0,
    })
    item.update({
        "entity_type": event.entity_type,
        "provenance": event.provenance.value,
        "freshness": "FRESH",
        "last_source_change_at": now,
        "updated_at": now,
        "expected_version": int(current.get("version", 0)) if current else 0,
    })
    if event.event_type is EventType.NETWORK_CONGESTION_CHANGED:
        level = event.payload.get("congestion_level")
        item.update({
            "raw_congestion": level,
            "raw_congestion_evidence": copy.deepcopy(event.payload),
            "raw_congestion_observed_at": event.source_timestamp,
            "haris_operational_state": (
                "INCIDENT_OPEN" if level == "High" else
                "WATCHING" if level == "Medium" else "STABLE"
            ),
            "last_operational_change_at": now,
        })
    elif event.event_type is EventType.DEVICE_REACHABILITY_CHANGED:
        item.update({
            "reachability_summary": copy.deepcopy(event.payload),
            "reachability_observed_at": event.source_timestamp,
        })
    elif event.event_type is EventType.DEVICE_LOCATION_UPDATED:
        item.update({
            "location_summary": copy.deepcopy(event.payload),
            "location_observed_at": event.source_timestamp,
        })
    return item


def _plan(bundle: Any, event: HarisEvent) -> _InboundPlan:
    current = bundle.network_state.get(event.entity_id)
    projection = _projection_for(event, current)
    existing_incident = None
    incident = None
    if (
        projection is not None
        and
        event.event_type is EventType.NETWORK_CONGESTION_CHANGED
        and event.payload.get("congestion_level") == "High"
    ):
        existing_incident = bundle.incidents.find_active(event.correlation_key)
        incident = None if existing_incident else _incident_for(event)
        incident_id = (existing_incident or incident)["incident_id"]
        if projection is not None:
            projection["active_incident_ids"] = sorted(set(
                list(projection.get("active_incident_ids") or []) + [incident_id]
            ))
    durable_incident = existing_incident or incident
    signal = "DURABLE_INCIDENT_READY" if durable_incident else "RUNTIME_VIEW_REFRESH"
    outbox = {
        "outbox_id": f"out-{event.event_id}",
        "event_id": event.event_id,
        "event_type": signal,
        "payload": {
            "processing_target": signal,
            "provenance": event.provenance.value,
            "schema_version": event.schema_version,
            "incident_id": durable_incident.get("incident_id") if durable_incident else None,
        },
        "trace_id": event.trace_id,
        "created_at": event.created_at,
    }
    return _InboundPlan(
        projection=projection,
        incident=incident,
        existing_incident=existing_incident,
        outbox=outbox,
    )


def _validate_runtime_event(event: HarisEvent) -> None:
    if event.source_mode == "fixture" and event.provenance is Provenance.NOKIA_LIVE:
        raise ValueError("fixture evidence cannot be classified as live")
    if event.event_type is EventType.NETWORK_CONGESTION_CHANGED:
        if event.entity_type != "HARIS_CONFIGURED_LOGICAL_CELL":
            raise ValueError("congestion event has invalid entity type")
        if event.correlation_key != f"cell:{event.entity_id}":
            raise ValueError("congestion event has invalid correlation identity")


class RuntimeEventIngestor:
    """One canonical validate -> durable transaction -> wake-up path."""

    def __init__(
        self,
        *,
        bundle: Any,
        core: Any,
        is_ready: Callable[[], bool],
        wakeup: Callable[[], None],
        metrics: Optional[RuntimeIngestionMetrics] = None,
        max_version_attempts: int = 2,
    ) -> None:
        self.bundle = bundle
        self.core = core
        self.is_ready = is_ready
        self.wakeup = wakeup
        self.metrics = metrics or RuntimeIngestionMetrics()
        self.max_version_attempts = max(1, max_version_attempts)

    async def ingest_runtime_event(self, event: HarisEvent) -> RuntimeIngestionResult:
        self.metrics.increment("event_received")
        if not isinstance(event, HarisEvent):
            self.metrics.increment("processing_failed")
            raise ValueError("invalid canonical runtime event")
        try:
            _validate_runtime_event(event)
        except ValueError:
            self.metrics.increment("processing_failed")
            raise
        if not self.is_ready() or getattr(self.core, "readiness", None) != "READY":
            self.metrics.increment("processing_failed")
            raise RuntimeEventNotReady("runtime persistence is not ready")

        result = None
        plan = None
        for attempt in range(self.max_version_attempts):
            plan = _plan(self.bundle, event)
            try:
                result = self.bundle.inbound.process(
                    event,
                    projection=plan.projection,
                    incident=plan.incident,
                    # Migration 002 creates/reuses the incident atomically. A
                    # transition referring to a speculative incident ID would
                    # be unsafe when a concurrent event wins correlation.
                    transition=None,
                    outbox=plan.outbox,
                )
                break
            except VersionConflict:
                self.metrics.increment("version_conflict")
                if attempt + 1 >= self.max_version_attempts:
                    self.metrics.increment("processing_failed")
                    raise
                # Re-read and rebuild from durable authority. Never overwrite
                # blindly with the stale expected version.
                continue
            except RepositoryUnavailable:
                self.metrics.increment("processing_failed")
                raise
            except Exception:
                self.metrics.increment("processing_failed")
                raise

        if result is None or plan is None:
            self.metrics.increment("processing_failed")
            raise RepositoryUnavailable("inbound processing produced no result")

        if result.get("status") == "duplicate":
            existing = self.bundle.events.lookup(event.idempotency_key)
            canonical_id = existing.event_id if existing is not None else event.event_id
            self.metrics.increment("event_duplicate")
            logger.info(
                "runtime_event status=duplicate event_type=%s provenance=%s",
                event.event_type.value, event.provenance.value,
            )
            return RuntimeIngestionResult(
                status="duplicate", event_id=canonical_id,
                processing_state=ProcessingState.DURABLE,
                projection_updated=False, incident_id=None,
                incident_created=False, wakeup_enqueued=False,
                trace_id=event.trace_id,
            )

        if result.get("status") != "accepted":
            self.metrics.increment("processing_failed")
            raise RepositoryUnavailable("invalid inbound processing status")

        projection = result.get("projection")
        self.core.apply_committed_projection(projection)
        self.metrics.increment("event_persisted")
        if projection is not None:
            self.metrics.increment("projection_updated")

        returned_incident = result.get("incident")
        incident = returned_incident or plan.existing_incident
        incident_created = bool(returned_incident and returned_incident.get("_created", True))
        if incident:
            self.metrics.increment("incident_created" if incident_created else "incident_reused")
        self.metrics.increment("outbox_enqueued")

        # This is intentionally after the atomic repository call. The wake-up
        # carries no domain payload; the consumer claims the durable outbox.
        try:
            self.wakeup()
        except Exception:
            # The durable outbox remains pending and is sufficient for a later
            # polling consumer. A local wake-up failure cannot undo or conceal
            # the already committed authoritative event.
            self.metrics.increment("processing_failed")
        state = ProcessingState.READY_FOR_REASONING if incident else ProcessingState.DURABLE
        logger.info(
            "runtime_event status=durable event_type=%s provenance=%s incident=%s",
            event.event_type.value, event.provenance.value, bool(incident),
        )
        return RuntimeIngestionResult(
            status="accepted", event_id=event.event_id,
            processing_state=state,
            projection_updated=projection is not None,
            incident_id=incident.get("incident_id") if incident else None,
            incident_created=incident_created,
            wakeup_enqueued=True,
            trace_id=event.trace_id,
        )


class DurableOutboxWakeupConsumer:
    """Claims committed outbox work and publishes disposable runtime deltas."""

    def __init__(
        self,
        *,
        bundle: Any,
        event_bus: Any,
        is_ready: Callable[[], bool],
        post_commit: Optional[Callable[[HarisEvent], Awaitable[None]]] = None,
        incident_ready: Optional[Callable[[Dict[str, Any]], Awaitable[Any]]] = None,
        decision_ready: Optional[Callable[[Dict[str, Any]], Awaitable[Any]]] = None,
        reconciliation_ready: Optional[Callable[[Dict[str, Any]], Awaitable[Any]]] = None,
        action_ready: Optional[Callable[[Dict[str, Any]], Awaitable[Any]]] = None,
        metrics: Optional[RuntimeIngestionMetrics] = None,
        owner: Optional[str] = None,
        lease_seconds: int = 30,
    ) -> None:
        self.bundle = bundle
        self.event_bus = event_bus
        self.is_ready = is_ready
        self.post_commit = post_commit
        self.incident_ready = incident_ready
        self.decision_ready = decision_ready
        self.reconciliation_ready = reconciliation_ready
        self.action_ready = action_ready
        self.metrics = metrics or RuntimeIngestionMetrics()
        self.owner = owner or f"HARIS-RUNTIME-{uuid.uuid4().hex}"
        self.lease_seconds = lease_seconds
        self._wake = asyncio.Event()
        self._running = False

    async def _renew_claim(self, outbox_id: str, generation: int, stopped: asyncio.Event) -> None:
        """One bounded heartbeat for the single record processed by this consumer."""
        interval = max(0.05, self.lease_seconds / 3)
        while not stopped.is_set():
            try:
                await asyncio.wait_for(stopped.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                self.bundle.outbox.renew(outbox_id, self.owner, generation, self.lease_seconds)

    def wake(self) -> None:
        self._wake.set()

    async def process_once(self, *, limit: int = 25) -> int:
        if not self.is_ready():
            return 0
        claimed = self.bundle.outbox.claim(self.owner, limit=limit, lease_seconds=self.lease_seconds)
        processed = 0
        for row in claimed:
            if row.get("claim_owner") != self.owner:
                self.metrics.increment("outbox_lease_lost")
                continue
            if row.get("claim_expires_at") is not None and float(row["claim_expires_at"]) <= time.time():
                self.metrics.increment("outbox_lease_lost")
                continue
            outbox_id = str(row.get("outbox_id") or "")
            generation = int(row.get("claim_generation", 0))
            heartbeat_stop = asyncio.Event()
            heartbeat = asyncio.create_task(self._renew_claim(outbox_id, generation, heartbeat_stop))
            try:
                # A reasoning-ready signal is completed durably before any
                # runtime publication or ACK.  A crash or failure therefore
                # leaves the claimed item retryable and cannot expose a
                # pre-commit AUTHORIZED/BLOCKED decision.
                if row.get("event_type") == "DURABLE_INCIDENT_READY":
                    if self.incident_ready is None:
                        raise RepositoryUnavailable("durable incident consumer is unavailable")
                    await self.incident_ready(copy.deepcopy(row))
                if row.get("event_type") == "DURABLE_DECISION_READY":
                    if self.decision_ready is None:
                        raise RepositoryUnavailable("durable action consumer is unavailable")
                    await self.decision_ready(copy.deepcopy(row))
                if row.get("event_type") == "DURABLE_RECONCILIATION_READY":
                    if self.reconciliation_ready is None:
                        raise RepositoryUnavailable("durable reconciliation consumer is unavailable")
                    await self.reconciliation_ready(copy.deepcopy(row))
                if row.get("event_type") == "DURABLE_ACTION_EXECUTION_READY":
                    if self.action_ready is None:
                        raise RepositoryUnavailable("durable action consumer is unavailable")
                    await self.action_ready(copy.deepcopy(row))
                event = self.bundle.events.get(str(row.get("event_id") or ""))
                if event is None:
                    raise RepositoryUnavailable("outbox event is unavailable")
                await self.event_bus.publish(event)
                if self.post_commit is not None:
                    await self.post_commit(event)
                if heartbeat.done() and heartbeat.exception() is not None:
                    raise heartbeat.exception()
                self.bundle.outbox.ack(outbox_id, self.owner, generation)
                self.metrics.increment("outbox_claimed")
                processed += 1
            except ResourceAlreadyOwned:
                # The lease/claim is no longer ours. Do not ack, fail, or make
                # another authoritative decision for this item.
                self.metrics.increment("outbox_lease_lost")
                if row.get("event_type") == "DURABLE_RECONCILIATION_READY":
                    self.metrics.increment("reconciliation_lease_lost")
            except Exception:
                self.metrics.increment("processing_failed")
                try:
                    self.bundle.outbox.fail(outbox_id, self.owner, "runtime_notification_failed", True, generation)
                except ResourceAlreadyOwned:
                    self.metrics.increment("outbox_lease_lost")
            finally:
                heartbeat_stop.set()
                if not heartbeat.done():
                    heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
        return processed

    async def run(self, *, poll_seconds: float = 0.5) -> None:
        self._running = True
        while self._running and self.is_ready():
            try:
                await self.process_once()
            except RepositoryUnavailable:
                self.metrics.increment("processing_failed")
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=poll_seconds)
            except asyncio.TimeoutError:
                pass

    def stop(self) -> None:
        self._running = False
        self._wake.set()

    def status(self) -> Dict[str, Any]:
        return {
            "state": "RUNNING" if self._running else "STOPPED",
            "owner_type": "HARIS_RUNTIME",
            "lease_seconds": self.lease_seconds,
        }


def _source_timestamp(value: Any, fallback: float) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
    return float(fallback)


def canonical_events_from_observation(observation: Dict[str, Any]) -> list[HarisEvent]:
    """Normalize an existing read-only observation without inventing values."""
    mode = str(observation.get("mode") or "live_read_only")
    provenance = Provenance.FIXTURE_SIMULATED if mode == "fixture" else Provenance.NOKIA_LIVE
    observed_at = float(observation.get("observed_at") or time.time())
    capabilities = observation.get("capabilities") or {}
    congestion_at = float((capabilities.get("congestion") or {}).get("last_success_at") or observed_at)
    reachability_at = float((capabilities.get("reachability") or {}).get("last_success_at") or observed_at)
    location_at = float((capabilities.get("location") or {}).get("last_success_at") or observed_at)
    events: list[HarisEvent] = []

    for row in observation.get("congestion") or []:
        cell_id = row.get("cell_id")
        level = row.get("congestion_level")
        if not cell_id or level not in {"None", "Low", "Medium", "High"}:
            continue
        payload: Dict[str, Any] = {
            "congestion_level": level,
            "timestamp_basis": "NOKIA_INTERVAL_STOP" if row.get("interval_stop") else "HARIS_OBSERVED_AT",
        }
        if row.get("confidence_level") is not None:
            payload["confidence_level"] = row["confidence_level"]
        events.append(HarisEvent(
            event_type=EventType.NETWORK_CONGESTION_CHANGED,
            source="nokia_reconciliation",
            source_mode=mode,
            source_timestamp=_source_timestamp(row.get("interval_stop"), congestion_at),
            entity_type="HARIS_CONFIGURED_LOGICAL_CELL",
            entity_id=str(cell_id), correlation_key=f"cell:{cell_id}",
            provenance=provenance, payload=payload,
        ))

    reachability: Dict[str, list[Dict[str, Any]]] = {}
    for row in observation.get("devices") or []:
        if row.get("cell_id") is not None and isinstance(row.get("reachable"), bool):
            reachability.setdefault(str(row["cell_id"]), []).append(row)
    for cell_id, rows in reachability.items():
        device_evidence = []
        for row in rows:
            item = {
                "device_id": row["device_id"], "cell_id": row["cell_id"],
                "reachable": row["reachable"],
                "field_provenance": {
                    "reachable": provenance.value,
                    "device_id": "HARIS_DERIVED",
                    "cell_id": "HARIS_DERIVED",
                },
            }
            if row.get("roaming") is not None:
                item["roaming"] = bool(row["roaming"])
                item["field_provenance"]["roaming"] = "HARIS_DERIVED"
            if row.get("tier") is not None:
                item["tier"] = int(row["tier"])
                item["field_provenance"]["tier"] = "HARIS_DERIVED"
            # Battery in the current live adapter is configured HARIS metadata,
            # not Nokia evidence.  Keep it unavailable in live durable events.
            if mode == "fixture" and row.get("battery_pct") is not None:
                item["battery_pct"] = row["battery_pct"]
                item["field_provenance"]["battery_pct"] = provenance.value
            device_evidence.append(item)
        events.append(HarisEvent(
            event_type=EventType.DEVICE_REACHABILITY_CHANGED,
            source="nokia_reconciliation", source_mode=mode,
            source_timestamp=reachability_at,
            entity_type="HARIS_CONFIGURED_LOGICAL_CELL",
            entity_id=cell_id, correlation_key=f"cell:{cell_id}",
            provenance=provenance,
            payload={
                "reachable": sum(bool(row["reachable"]) for row in rows),
                "unreachable": sum(not bool(row["reachable"]) for row in rows),
                "devices": device_evidence,
                "timestamp_basis": "HARIS_OBSERVED_AT",
            },
        ))

    for row in observation.get("locations") or []:
        device_id = row.get("device_id")
        if not device_id or row.get("latitude") is None or row.get("longitude") is None:
            continue
        payload = {
            "latitude": row["latitude"], "longitude": row["longitude"],
            "timestamp_basis": "HARIS_OBSERVED_AT",
        }
        if row.get("accuracy_m") is not None:
            payload["accuracy_m"] = row["accuracy_m"]
        events.append(HarisEvent(
            event_type=EventType.DEVICE_LOCATION_UPDATED,
            source="nokia_reconciliation", source_mode=mode,
            source_timestamp=location_at,
            entity_type="HARIS_REGISTERED_DEVICE",
            entity_id=str(device_id), correlation_key=f"device:{device_id}",
            provenance=provenance, payload=payload,
        ))
    return events


async def ingest_observation_view(ingestor: RuntimeEventIngestor, observation: Dict[str, Any]) -> None:
    for event in canonical_events_from_observation(observation):
        await ingestor.ingest_runtime_event(event)


def projection_availability(
    projection: Dict[str, Dict[str, Any]], *, now: Optional[float] = None,
    stale_after_seconds: float = 30.0,
) -> str:
    if not projection:
        return "UNAVAILABLE"
    timestamps = []
    for row in projection.values():
        for key in (
            "raw_congestion_observed_at", "reachability_observed_at",
            "location_observed_at",
        ):
            if row.get(key) is not None:
                timestamps.append(float(row[key]))
    if not timestamps:
        return "UNAVAILABLE"
    current = time.time() if now is None else float(now)
    return "CURRENT" if max(timestamps) >= current - stale_after_seconds else "STALE"
