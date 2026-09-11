"""Focused offline regressions for Phase 8C durable authority quarantine."""
from __future__ import annotations

import asyncio
import time
import unittest
from unittest.mock import patch

import run_api
from config import AppSettings
from durable_core import IncidentState
from incidents import IncidentManager, LegacyIncidentPathQuarantined
from network_state import NetworkStateRegistry
from platform_lifecycle import PlatformLifecycleState
from runtime import RuntimeEnvironment


def settings(mode: str = "fixture") -> AppSettings:
    return AppSettings(
        nac_mode=mode, fixture_dir="fixtures", haris_persistence_mode="memory",
        enable_continuous_loop=False, nokia_observation_enabled=False,
    )


def incident_record(incident_id: str = "inc-durable-authority") -> dict:
    now = time.time()
    return {
        "incident_id": incident_id, "schema_version": 1,
        "correlation_key": "cell:T03", "primary_entity": "T03",
        "affected_entities": ["T03"], "affected_devices": ["ambulance-01"],
        "trigger_event_id": "evt-authority", "trigger_provenance": "NOKIA_LIVE",
        "trigger_source_timestamp": now, "opened_at": now, "updated_at": now,
        "severity": "critical", "priority": "P1", "state": IncidentState.EVALUATING.value,
        "plan_version": 0, "warden_decision": None,
        "verification_state": "PENDING", "recovery_state": "PENDING",
        "outcome": None, "closed_at": None, "version": 0,
        "trace_id": "trace-authority",
    }


class StaleSystem:
    def __init__(self) -> None:
        self.calls = []
        self.current_dispatch_status = {
            "incident_id": "inc-stale", "status": "APPROVED",
        }
        self.current_cycle_status = {
            "incident": {"incident_id": "inc-stale"},
            "final_status": "mitigated", "decision_status": "AUTHORIZED_PLAN",
            "dispatch_history": [],
        }

    async def run_cycle(self, **kwargs):
        self.calls.append(kwargs)
        return dict(self.current_cycle_status)


class Phase8CDurableAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.globals = {
            name: getattr(run_api, name) for name in (
                "_durable_core", "_repository_bundle", "_system", "_network_state",
                "_incident_manager", "_platform_lifecycle",
            )
        }

    def tearDown(self):
        for name, value in self.globals.items():
            setattr(run_api, name, value)

    def _initialize(self):
        configured = settings("live_read_only")
        self.assertTrue(run_api.initialize_platform(settings=configured))
        run_api._repository_bundle.incidents.create_or_get_active(incident_record())
        run_api._system = StaleSystem()
        return configured

    def test_production_status_uses_durable_incident_not_stale_cycle(self):
        configured = self._initialize()
        with patch.object(run_api, "get_settings", return_value=configured), patch.object(
            run_api, "runtime_environment", return_value=RuntimeEnvironment.PRODUCTION
        ):
            status = run_api._durable_supervisory_status()
        self.assertEqual(status["authority"], "DURABLE_REPOSITORY")
        self.assertEqual(status["active_incident"]["incident_id"], "inc-durable-authority")
        self.assertEqual(status["cycle"]["incident"]["state"], "EVALUATING")
        self.assertNotEqual(status["cycle"]["final_status"], "mitigated")
        self.assertEqual(status["trusted_dispatch_authority"], "PROCESS_LOCAL_SINGLE_INSTANCE")
        self.assertEqual(status["cycle"]["trusted_dispatch"], {})

    def test_reconstruction_reproduces_authoritative_supervisory_result(self):
        configured = self._initialize()
        bundle = run_api._repository_bundle
        with patch.object(run_api, "get_settings", return_value=configured), patch.object(
            run_api, "runtime_environment", return_value=RuntimeEnvironment.PRODUCTION
        ):
            before = run_api._durable_supervisory_status()
            run_api._durable_core = None
            run_api._repository_bundle = None
            with patch.object(run_api, "build_repository_bundle", return_value=bundle):
                # Reconstruction uses the deterministic test repository even
                # though the supervisory read below is evaluated as production.
                self.assertTrue(run_api.initialize_platform(
                    settings=configured, runtime=RuntimeEnvironment.TEST,
                ))
            after = run_api._durable_supervisory_status()
        self.assertEqual(before["active_incident"], after["active_incident"])
        self.assertEqual(before["haris_state"], after["haris_state"])
        self.assertEqual(after["authority"], "DURABLE_REPOSITORY")

    def test_non_durable_dispatch_is_visible_but_explicitly_process_local(self):
        configured = settings("fixture")
        self.assertTrue(run_api.initialize_platform(settings=configured))
        system = StaleSystem()
        system.current_dispatch_status = {
            "incident_id": "inc-local-dispatch", "pending_id": "pending-local",
            "status": "WAITING_FOR_IDENTITY_VERIFICATION",
        }
        system.current_cycle_status = {
            "incident": {"incident_id": "inc-local-dispatch"},
            "dispatch_history": [],
        }
        run_api._system = system
        with patch.object(run_api, "get_settings", return_value=configured), patch.object(
            run_api, "runtime_environment", return_value=RuntimeEnvironment.PRODUCTION
        ):
            status = run_api._durable_supervisory_status()
        self.assertEqual(status["cycle"]["trusted_dispatch"]["durable"], False)
        self.assertEqual(
            status["active_incident"]["authority"],
            "PROCESS_LOCAL_SINGLE_INSTANCE",
        )
        self.assertEqual(status["authority"], "DURABLE_REPOSITORY")


class Phase8CLegacyQuarantineTests(unittest.TestCase):
    def test_legacy_ingest_cannot_enter_live_cycle(self):
        system = StaleSystem()
        manager = IncidentManager(
            system, NetworkStateRegistry(mode="live_read_only"),
            settings("live_read_only"),
        )
        observation = {
            "observed_at": time.time(), "mode": "live_read_only", "source": "live",
            "congestion": [{"cell_id": "T03", "congestion_level": "High"}],
            "devices": [], "locations": [],
        }
        with patch("incidents.runtime_environment", return_value=RuntimeEnvironment.PRODUCTION):
            with self.assertRaises(LegacyIncidentPathQuarantined):
                asyncio.run(manager.ingest(observation))
        self.assertEqual(system.calls, [])

    def test_legacy_evaluate_is_independently_quarantined(self):
        system = StaleSystem()
        manager = IncidentManager(system, NetworkStateRegistry(mode="fixture"), settings("fixture"))
        with patch("incidents.runtime_environment", return_value=RuntimeEnvironment.PRODUCTION):
            with self.assertRaises(LegacyIncidentPathQuarantined):
                asyncio.run(manager._evaluate("missing", {}))
        self.assertEqual(system.calls, [])

    def test_fixture_compatibility_remains_bounded(self):
        system = StaleSystem()
        manager = IncidentManager(system, NetworkStateRegistry(mode="fixture"), settings("fixture"))
        manager._incidents["inc-fixture"] = {
            "incident_id": "inc-fixture", "entity_id": "T03", "status": "INCIDENT_OPEN",
        }
        with patch("incidents.runtime_environment", return_value=RuntimeEnvironment.TEST):
            asyncio.run(manager._evaluate("inc-fixture", {}))
        self.assertEqual(len(system.calls), 1)


if __name__ == "__main__":
    unittest.main()
