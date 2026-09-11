"""Offline contract tests for Phase 6B's durable event/incident core."""
from __future__ import annotations

import asyncio
import unittest
from fastapi import Response

from durable_core import (
    ActionCommand, ActionState, DurablePlatformCore, InMemoryActionRepository,
    InMemoryEventRepository, InMemoryInboxRepository, InMemoryIncidentRepository,
    InMemoryNetworkStateRepository, InMemoryOutboxRepository,
    InMemoryRecoveryRepository, InMemoryResourceOwnershipRepository,
    InMemoryVerificationRepository, InvalidTransition, ProjectionIntegrityError,
    PostgresRepositoryAdapter, ResourceAlreadyOwned, VersionConflict,
)
from platform_events import EventType, HarisEvent, Provenance


class FixedClock:
    def __init__(self, value: float = 1_700_000_000.0):
        self.value = value

    def now(self) -> float:
        return self.value


def event(*, event_id: str = "evt-1", source_event_id: str | None = "source-1", cell: str = "T03", level: str = "High", timestamp: float = 100.0) -> HarisEvent:
    return HarisEvent(
        event_id=event_id, event_type=EventType.NETWORK_CONGESTION_CHANGED,
        source="nokia", source_mode="fixture", source_event_id=source_event_id,
        source_timestamp=timestamp, received_at=timestamp + 1, created_at=timestamp + 2,
        entity_type="HARIS_CONFIGURED_LOGICAL_CELL", entity_id=cell,
        correlation_key=f"cell:{cell}", provenance=Provenance.FIXTURE_SIMULATED,
        payload={"congestion_level": level}, trace_id=f"trace-{event_id}",
    )


class DurableCoreTests(unittest.TestCase):
    def repositories(self):
        return {
            "events": InMemoryEventRepository(), "network": InMemoryNetworkStateRepository(),
            "incidents": InMemoryIncidentRepository(), "actions": InMemoryActionRepository(),
            "ownership": InMemoryResourceOwnershipRepository(),
            "verifications": InMemoryVerificationRepository(), "recoveries": InMemoryRecoveryRepository(),
            "outbox": InMemoryOutboxRepository(), "inbox": InMemoryInboxRepository(),
            "clock": FixedClock(),
        }

    def core(self, repos=None):
        return DurablePlatformCore(**(repos or self.repositories()))

    def test_event_append_dedupe_and_stale_event_history(self):
        core = self.core(); newest = event(timestamp=200.0)
        older = event(event_id="evt-old", source_event_id="source-old", level="Low", timestamp=100.0)
        self.assertTrue(core.append_event(newest))
        self.assertFalse(core.append_event(newest))
        self.assertTrue(core.append_event(older))
        self.assertEqual(core.events.sequence(), 2)
        self.assertEqual(core.snapshot()["network_state"]["T03"]["raw_congestion"], "High")

    def test_restart_reconstructs_incident_resource_pending_action_and_dedupes(self):
        repos = self.repositories(); first = self.core(repos); first.reconstruct()
        trigger = event(); self.assertTrue(first.append_event(trigger))
        incident = first.incidents.active()[0]
        command = ActionCommand(incident_id=incident["incident_id"], command_type="QOD_CREATE", resource_key="device:ambulance-01", device_id="ambulance-01", plan_version=1, requested_at=10.0)
        self.assertEqual(first.queue_action(command).command_id, command.command_id)
        first.acquire_resource({"resource_key": "device:ambulance-01", "resource_type": "DEVICE", "owner_incident_id": incident["incident_id"], "acquired_at": 10.0, "lease_started_at": 10.0, "lease_expires_at": 20.0, "renewable": False, "adopted_by_incident": False})
        restarted = self.core(repos); restarted.reconstruct()
        self.assertEqual(restarted.readiness, "READY")
        self.assertEqual(restarted.incidents.active()[0]["incident_id"], incident["incident_id"])
        self.assertEqual(restarted.ownership.owned_by(incident["incident_id"])[0]["resource_key"], "device:ambulance-01")
        self.assertEqual(restarted.queue_action(command).command_id, command.command_id)
        self.assertFalse(restarted.append_event(event(event_id="evt-repeat", source_event_id="source-1", timestamp=300.0)))
        self.assertEqual(len(restarted.incidents.active()), 1)

    def test_concurrent_incidents_remain_isolated_across_restart(self):
        repos = self.repositories(); first = self.core(repos); first.reconstruct()
        first.append_event(event(cell="T03", event_id="evt-a", source_event_id="a"))
        first.append_event(event(cell="T05", event_id="evt-b", source_event_id="b"))
        restarted = self.core(repos); restarted.reconstruct()
        active = {item["primary_entity"]: item for item in restarted.incidents.active()}
        self.assertEqual(set(active), {"T03", "T05"})
        self.assertNotEqual(active["T03"]["incident_id"], active["T05"]["incident_id"])

    def test_resource_conflict_cannot_overwrite_owner(self):
        core = self.core()
        core.acquire_resource({"resource_key": "device:ambulance-01", "resource_type": "DEVICE", "owner_incident_id": "inc-a", "acquired_at": 1.0})
        with self.assertRaises(ResourceAlreadyOwned):
            core.acquire_resource({"resource_key": "device:ambulance-01", "resource_type": "DEVICE", "owner_incident_id": "inc-b", "acquired_at": 2.0})

    def test_action_idempotency_and_sent_restart_unknown(self):
        repos = self.repositories(); first = self.core(repos); first.reconstruct()
        command = ActionCommand(incident_id="inc-a", command_type="QOD_CREATE", resource_key="device:a", device_id="a", plan_version=3, requested_at=1.0)
        first.queue_action(command); repos["actions"]._by_id[command.command_id].state = ActionState.SENT
        restarted = self.core(repos); restarted.reconstruct()
        pending = restarted.actions.pending_or_unknown()
        self.assertEqual(pending[0].state, ActionState.OUTCOME_UNKNOWN)
        self.assertNotEqual(pending[0].state, ActionState.SUCCESS)
        self.assertEqual(restarted.queue_action(command).command_id, command.command_id)

    def test_verification_recovery_outbox_and_inbox_persist(self):
        repos = self.repositories(); first = self.core(repos); first.reconstruct()
        first.record_verification({"verification_id": "verify-a", "incident_id": "inc-a", "evidence_event_ids": ["evt-a"], "verification_type": "NETWORK_KPI", "state": "INSUFFICIENT_EVIDENCE", "started_at": 1.0, "updated_at": 2.0, "result": None, "reason": "unavailable", "source_provenance": "UNAVAILABLE"})
        first.record_recovery({"recovery_id": "recovery-a", "incident_id": "inc-a", "resource_keys": ["device:a"], "state": "RELEASING", "started_at": 1.0, "completed_at": None, "failure_reason": None})
        first.append_event(event())
        restarted = self.core(repos); restarted.reconstruct()
        self.assertEqual(restarted.verifications.for_incident("inc-a")[0]["state"], "INSUFFICIENT_EVIDENCE")
        self.assertEqual(restarted.recoveries.for_incident("inc-a")["state"], "RELEASING")
        self.assertEqual(len(restarted.outbox.unsent()), 1)
        item = restarted.outbox.unsent()[0]; restarted.outbox.mark_sent(item["outbox_id"], 3.0)
        self.assertEqual(restarted.outbox.unsent(), [])

    def test_transition_validation_version_conflict_and_replay_integrity(self):
        repos = self.repositories(); core = self.core(repos); core.reconstruct(); core.append_event(event())
        incident = core.incidents.active()[0]
        with self.assertRaises(InvalidTransition):
            core.incidents.transition(incident["incident_id"], __import__("durable_core").IncidentState.RESOLVED, actor="test", reason_code="invalid", trace_id="trace", at=1.0)
        entity = repos["network"].load_all()["T03"]
        with self.assertRaises(VersionConflict):
            repos["network"].save(entity, expected_version=0)
        core.checkpoint(); self.assertTrue(core.verify_projection_integrity())
        repos["network"]._items["T03"]["raw_congestion"] = "Low"
        with self.assertRaises(ProjectionIntegrityError):
            core.verify_projection_integrity()

    def test_no_secret_fields_reach_durable_domain_or_snapshot(self):
        with self.assertRaises(ValueError):
            event().__class__(**{**event().model_dump(), "payload": {"nested": {"oauth_state": "not-safe"}}})
        with self.assertRaises(ValueError):
            event().__class__(**{**event().model_dump(), "payload": {"diagnostic": "https://example.invalid/verify?state=not-safe"}})
        with self.assertRaises(ValueError):
            ActionCommand(incident_id="inc", command_type="X", resource_key="r", device_id=None, plan_version=1, requested_at=1.0, parameters_safe={"phone_number": "+99999991000"})
        core = self.core(); core.reconstruct(); core.append_event(event())
        rendered = str(core.snapshot()).lower()
        self.assertNotIn("oauth_state", rendered); self.assertNotIn("phone_number", rendered)

    def test_api_snapshot_is_sanitized_and_available_after_reconstruction(self):
        import run_api
        repos = self.repositories(); core = self.core(repos); core.reconstruct(); core.append_event(event())
        previous = run_api._durable_core
        try:
            run_api._durable_core = core
            snapshot = asyncio.run(run_api.noc_snapshot())
            health = asyncio.run(run_api.platform_health(Response()))
        finally:
            run_api._durable_core = previous
        self.assertEqual(snapshot["platform_status"], "READY")
        self.assertEqual(snapshot["state_version"], 1)
        self.assertEqual(health["reconstruction"], "READY")
        self.assertNotIn("token", str(snapshot).lower())

    def test_websocket_sends_reconstructed_snapshot_before_deltas(self):
        import run_api

        class Socket:
            def __init__(self):
                self.sent = []
                self.headers = {"authorization": "Bearer test-operational-token"}
                self.client = type("Client", (), {"host": "127.0.0.1"})()
            async def accept(self): pass
            async def send_json(self, value): self.sent.append(value)
            async def receive_text(self):
                from fastapi import WebSocketDisconnect
                raise WebSocketDisconnect()

        class Registry:
            def snapshot(self): return {"entities": {}}

        class Manager:
            def status(self): return {"active_incidents": []}

        repos = self.repositories(); core = self.core(repos); core.reconstruct(); core.append_event(event())
        old_core, old_registry, old_manager = run_api._durable_core, run_api._network_state, run_api._incident_manager
        socket = Socket()
        try:
            run_api._durable_core, run_api._network_state, run_api._incident_manager = core, Registry(), Manager()
            from config import AppSettings
            from unittest.mock import patch
            authenticated = AppSettings(
                nac_mode="fixture", haris_operational_api_token="test-operational-token",
                gemini_api_key=None, groq_api_key=None,
            )
            with patch.object(run_api, "get_settings", return_value=authenticated):
                asyncio.run(run_api.noc_websocket(socket))
        finally:
            run_api._durable_core, run_api._network_state, run_api._incident_manager = old_core, old_registry, old_manager
        self.assertEqual(socket.sent[0]["type"], "snapshot")
        self.assertEqual(socket.sent[0]["data"]["platform_status"], "READY")
        self.assertEqual(socket.sent[0]["data"]["network_state"]["T03"]["raw_congestion"], "High")

    def test_postgres_adapter_contract_is_injected_and_never_connects(self):
        calls = []
        adapter = PostgresRepositoryAdapter(lambda sql, args: calls.append((sql, args)) or {"inserted": True})
        adapter.append_event(event())
        self.assertEqual(len(calls), 1)
        self.assertIn("haris_append_domain_event", calls[0][0])
        self.assertIn("idempotency_key", calls[0][1][0])


if __name__ == "__main__":
    unittest.main()
