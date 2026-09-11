"""Offline regressions for the Phase 10 durable NOC read model."""
from __future__ import annotations

import time
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import run_api
from config import AppSettings
from durable_core import ActionCommand, ActionState, IncidentState
from platform_lifecycle import PlatformLifecycle
from platform_events import EventType, HarisEvent, Provenance
from runtime import RuntimeEnvironment


def settings() -> AppSettings:
    return AppSettings(
        nac_mode="live_read_only", haris_persistence_mode="memory",
        haris_operational_api_token="phase10-test-token",
        enable_continuous_loop=False, nokia_observation_enabled=False,
    )


def incident_record(now: float) -> dict:
    return {
        "incident_id": "inc-phase10", "schema_version": 1,
        "correlation_key": "cell:T03", "primary_entity": "T03",
        "affected_entities": ["T03"], "affected_devices": ["critical-asset"],
        "trigger_event_id": "evt-phase10", "trigger_provenance": "NOKIA_LIVE",
        "trigger_source_timestamp": now, "opened_at": now, "updated_at": now,
        "severity": "critical", "priority": "P1", "state": "DETECTED",
        "plan_version": 1, "warden_decision": "ALLOW",
        "verification_state": "PENDING", "recovery_state": "PENDING",
        "outcome": None, "closed_at": None, "version": 0,
        "trace_id": "trace-phase10",
    }


class Phase10DurableNocTests(unittest.TestCase):
    def setUp(self):
        self.saved = {
            name: getattr(run_api, name) for name in (
                "_durable_core", "_repository_bundle", "_platform_lifecycle",
                "_system", "_runtime_consumer", "_durable_reconciliation_scheduler",
            )
        }
        configured = settings()
        self.configured = configured
        self.assertTrue(run_api.initialize_platform(settings=configured, runtime=RuntimeEnvironment.DEVELOPMENT))

    def tearDown(self):
        for name, value in self.saved.items():
            setattr(run_api, name, value)

    def _complete_real_partial_incident(self):
        core = run_api.get_durable_core()
        now = time.time()
        core.append_event(HarisEvent(
            event_id="evt-phase10-observation", event_type=EventType.NETWORK_CONGESTION_CHANGED,
            source="nokia", source_mode="live_read_only", source_timestamp=now - 1,
            received_at=now - 1, created_at=now - 1,
            entity_type="HARIS_CONFIGURED_LOGICAL_CELL", entity_id="T03",
            correlation_key="cell:T03", provenance=Provenance.NOKIA_LIVE,
            payload={"congestion_level": "Medium", "confidence_level": 90},
            trace_id="trace-phase10",
        ))
        core.incidents.create_or_get_active(incident_record(now))
        for state, actor in (
            (IncidentState.EVALUATING, "TRIAGE"),
            (IncidentState.PLANNED, "TRIAGE"),
            (IncidentState.WARDEN_REVIEW, "WARDEN"),
            (IncidentState.APPROVED, "WARDEN"),
            (IncidentState.MITIGATING, "ACTUATOR"),
            (IncidentState.VERIFYING, "VERIFY"),
        ):
            core.incidents.transition(
                "inc-phase10", state, actor=actor, reason_code=f"ENTER_{state.value}",
                trace_id="trace-phase10", at=now + len(core.incidents.transitions("inc-phase10")),
            )
        current = core.incidents.get("inc-phase10")
        current.update(outcome="REAL_PARTIAL", verification_state="UNCHANGED")
        core.incidents.update(current, int(current["version"]))
        action = ActionCommand(
            incident_id="inc-phase10", command_type="QOD_PLAN",
            resource_key="qod:critical-asset", device_id="critical-asset",
            plan_version=1, requested_at=now + 7,
            state=ActionState.ROLLED_BACK, provider_resource_id="provider-secret-id",
            completed_at=now + 8,
        )
        core.actions.create_or_get(action)
        core.verifications.save({
            "verification_id": "verify-phase10", "incident_id": "inc-phase10",
            "action_id": action.command_id, "state": "UNCHANGED",
            "outcome": "NETWORK_UNCHANGED", "source_provenance": "NOKIA_LIVE",
            "verified_at": now + 9,
        })
        core.recoveries.save({
            "recovery_id": "recovery-phase10", "incident_id": "inc-phase10",
            "action_id": action.command_id, "state": "COMPLETE",
            "outcome": "VERIFIED_ROLLBACK", "reason": "provider_resource_absent",
            "provenance": "NOKIA_LIVE", "completed_at": now + 10,
        }, expected_version=-1)
        current = core.incidents.get("inc-phase10")
        current["recovery_state"] = "COMPLETE"
        core.incidents.update(current, int(current["version"]))
        core.incidents.transition(
            "inc-phase10", IncidentState.RESOLVED, actor="RECOVERY",
            reason_code="CLEANUP_VERIFIED_NETWORK_UNCHANGED",
            trace_id="trace-phase10", at=now + 11,
        )
        return core

    def test_terminal_history_timeline_and_truth_survive_restart(self):
        core = self._complete_real_partial_incident()
        first = run_api._authoritative_snapshot()
        self.assertEqual(first["active_incidents"], [])
        self.assertEqual(first["incident_history"][0]["final_truth"], "REAL_PARTIAL")
        self.assertEqual(first["incident_history"][0]["incident"]["verification_state"], "UNCHANGED")
        self.assertEqual(first["incident_history"][0]["recovery"]["state"], "COMPLETE")
        self.assertEqual(first["incident_history"][0]["actions"][0]["provider_resource_id"], "[REDACTED]")
        timestamps = [item["timestamp"] for item in first["timeline"]]
        self.assertEqual(timestamps, sorted(timestamps))
        self.assertEqual(first["timeline_authority"], "DERIVED_FROM_DURABLE")
        observations = [item for item in first["timeline"] if item["type"] == "OBSERVATION"]
        self.assertEqual(observations[0]["provenance"], "NOKIA_LIVE")

        bundle = run_api._repository_bundle
        run_api._durable_core = None
        run_api._repository_bundle = None
        with patch.object(run_api, "build_repository_bundle", return_value=bundle):
            self.assertTrue(run_api.initialize_platform(settings=self.configured, runtime=RuntimeEnvironment.DEVELOPMENT))
        second = run_api._authoritative_snapshot()
        self.assertEqual(first["incident_history"], second["incident_history"])
        self.assertEqual(first["timeline"], second["timeline"])

    def test_snapshot_metrics_are_durable_and_websocket_count_is_delivery_only(self):
        self._complete_real_partial_incident()
        snapshot = run_api._authoritative_snapshot()
        metrics = snapshot["operational_metrics"]
        self.assertEqual(metrics["authority"], "DURABLE_REPOSITORY")
        self.assertEqual(metrics["active_incidents"], 0)
        self.assertEqual(metrics["terminal_incidents_in_view"], 1)
        self.assertEqual(metrics["reconciliation_pending"], 0)
        self.assertEqual(metrics["websocket_clients_scope"], "PROCESS_LOCAL_DELIVERY_ONLY")

    def test_live_reconciliation_metric_excludes_only_persistence_test_actions(self):
        core = run_api.get_durable_core()
        now = time.time()
        reconciliation_states = (
            ActionState.SENT,
            ActionState.OUTCOME_UNKNOWN,
            ActionState.RECONCILIATION_REQUIRED,
        )
        for index, state in enumerate(reconciliation_states):
            core.actions.create_or_get(ActionCommand(
                incident_id=f"PERSISTENCE-TEST-incident-{index}",
                command_type="PERSISTENCE_TEST_NO_PROVIDER",
                resource_key=f"PERSISTENCE-TEST-resource-{index}",
                device_id=None,
                plan_version=0,
                requested_at=now + index,
                state=state,
            ))

        filtered = run_api._authoritative_snapshot()["operational_metrics"]
        self.assertEqual(filtered["reconciliation_pending"], 0)
        self.assertEqual(
            run_api._platform_lifecycle.metrics.reconciliation_required_actions, 0,
        )

        for index, state in enumerate(reconciliation_states):
            core.actions.create_or_get(ActionCommand(
                incident_id=f"inc-operational-{index}",
                command_type="QOD_PLAN",
                resource_key=f"device:operational-{index}",
                device_id=f"operational-{index}",
                plan_version=1,
                requested_at=now + 10 + index,
                state=state,
            ))

        operational = run_api._authoritative_snapshot()["operational_metrics"]
        self.assertEqual(operational["reconciliation_pending"], 3)

    def test_health_separates_public_liveness_from_durable_readiness(self):
        with patch("run_api.get_settings", return_value=self.configured), patch(
            "nokia_clients.get_settings", return_value=self.configured
        ), TestClient(run_api.app) as client:
            liveness = client.get("/api/nac/health")
            readiness = client.get("/api/platform/health")
        self.assertEqual(liveness.status_code, 200)
        self.assertEqual(liveness.json()["health_type"], "LIVENESS")
        self.assertEqual(
            set(liveness.json()), {"status", "health_type", "service", "mode"},
        )
        self.assertEqual(readiness.status_code, 200)
        self.assertEqual(readiness.json()["readiness"], "READY")
        self.assertIn(readiness.json()["operational_status"], {"HEALTHY", "DEGRADED"})

    def test_supervisory_cycle_uses_durable_timeline_and_terminal_history(self):
        self._complete_real_partial_incident()
        with patch.object(run_api, "get_settings", return_value=self.configured), patch.object(
            run_api, "runtime_environment", return_value=RuntimeEnvironment.PRODUCTION,
        ):
            status = run_api._durable_supervisory_status()
        self.assertEqual(status["authority"], "DURABLE_REPOSITORY")
        self.assertEqual(status["incident_history"][0]["final_truth"], "REAL_PARTIAL")
        self.assertTrue(status["timeline"])
        self.assertNotIn("provider-secret-id", repr(status))

    def test_live_capability_truth_contract_is_explicit(self):
        from nokia_clients import LiveNokiaClient
        client = object.__new__(LiveNokiaClient)
        client.settings = self.configured
        client.client = type("Sdk", (), {"geofencing": object(), "slices": object()})()
        with patch.object(LiveNokiaClient, "action_safety_error", return_value=None):
            report = client.capability_report()
        self.assertEqual(report["qod"]["truth_status"], "REAL_PARTIAL")
        self.assertEqual(report["slicing"]["truth_status"], "SANDBOX_LIMITED")
        self.assertEqual(report["geofencing"]["truth_status"], "FAIL_CLOSED_AUTH_UNPROVEN")
        self.assertIn("PRIVILEGED_ONLY", report["trusted_dispatch"]["truth_status"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
