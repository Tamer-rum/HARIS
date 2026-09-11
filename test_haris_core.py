"""Deterministic behavioral tests for the HARIS network-resilience loop."""

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from agents import HarisAgentSystem, RemediationPlan
from config import AppSettings, GeofenceArea
from memory import MemoryStore
from fastapi import HTTPException
import nokia_clients
from nokia_clients import FixtureNokiaClient, LiveNokiaClient
from playbooks import Action, PlaybookEngine


class UnreachableFixtureNokiaClient(FixtureNokiaClient):
    """Network evidence remains actionable when critical assets are unreachable."""

    async def device_status(self, device_ids):
        statuses = await super().device_status(device_ids)
        return [
            status.model_copy(update={"reachable": False})
            if status.tier == 1 else status
            for status in statuses
        ]


class FailingGeofenceFixtureNokiaClient(FixtureNokiaClient):
    async def create_geofence(self, device_id, polygon_id):
        raise RuntimeError("configured geofencing capability unavailable")


class HarisCoreTests(unittest.TestCase):
    def settings(self, **updates):
        return AppSettings(nac_mode="fixture", fixture_dir="fixtures", **updates)

    def live_settings(self, **updates):
        defaults = {
            "nac_mode": "live",
            "nac_api_token": "test-token",
            "nac_geofence_sink": "https://operator.example/haris/geofence-events",
            "nac_geofence_areas": {
                "storm-impact": GeofenceArea(latitude=24.7136, longitude=46.6753, radius_m=5000),
            },
            "nac_qod_profile_map": {
                "guaranteed": "OPERATOR_GOLD",
                "low-bandwidth": "OPERATOR_STANDARD",
                "emergency-only": "OPERATOR_EMERGENCY",
            },
            "nac_qod_service_ipv4": "203.0.113.10",
        }
        defaults.update(updates)
        return AppSettings(**defaults)

    def test_storm_shield_proposes_actions_when_tier_one_assets_are_unreachable(self):
        settings = self.settings()
        client = UnreachableFixtureNokiaClient(settings)
        engine = PlaybookEngine(settings, client, memory=None)
        actions = engine.storm_shield(
            True,
            asyncio.run(client.congestion_insights()),
            asyncio.run(client.device_status(settings.registered_devices)),
        )
        self.assertTrue(actions)
        self.assertTrue(any(action.kind == "qos" for action in actions))
        self.assertTrue(any(action.kind == "slice_attach" for action in actions))

    def test_safe_fixture_cycle_executes_verifies_and_learns(self):
        settings = self.settings()
        result = asyncio.run(HarisAgentSystem(FixtureNokiaClient(settings), settings=settings).run_cycle(True))
        self.assertEqual(result["incident"]["affected_cells"], ["T02", "T03", "T05"])
        self.assertLessEqual(len(set(result["plan"]["selected_device_ids"])), settings.guardrails.max_devices_reconfigured_per_cycle)
        self.assertGreater(len(result["plan"]["actions"]), settings.guardrails.max_devices_reconfigured_per_cycle)
        self.assertTrue(result["warden"]["verified"])
        self.assertTrue(result["execution"]["executed"])
        self.assertTrue(result["verification"]["verified"])
        self.assertEqual(result["final_status"], "mitigated")
        self.assertTrue(result["learning"]["incident_saved"])
        self.assertEqual(result["active_playbook"]["state"], "MITIGATED")
        self.assertEqual(result["active_playbook"]["current_stage"], "LEARN")
        self.assertEqual(result["active_playbook"]["latest_outcome"], "mitigated")

    def test_device_limit_selects_two_tier_one_devices_and_retains_protection_actions(self):
        settings = self.settings()
        result = asyncio.run(HarisAgentSystem(FixtureNokiaClient(settings), settings=settings).run_cycle(True))
        selected = result["plan"]["selected_device_ids"]
        self.assertEqual(selected, ["ambulance-01", "scada-01"])
        self.assertEqual(len(selected), 2)
        self.assertGreaterEqual(len(result["plan"]["actions"]), 5)
        for device_id in selected:
            kinds = {item["kind"] for item in result["execution"]["actions"] if item["device_id"] == device_id and item["success"]}
            self.assertTrue({"qos", "slice_attach", "geofence"}.issubset(kinds))
        self.assertEqual(result["plan"]["blast_radius"], .25)

    def test_warden_counts_unique_devices_not_actions_and_rejects_duplicates_or_third_device(self):
        settings = self.settings()
        system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
        actions = [
            Action("qos", "ambulance-01", {"profile": "guaranteed", "duration_seconds": 300}, "test"),
            Action("slice_attach", "ambulance-01", {"slice_id": "haris-emergency"}, "test"),
            Action("geofence", "ambulance-01", {"polygon_id": "storm-impact"}, "test"),
            Action("qos", "scada-01", {"profile": "guaranteed", "duration_seconds": 300}, "test"),
            Action("slice_attach", "scada-01", {"slice_id": "haris-emergency"}, "test"),
        ]
        plan = RemediationPlan(
            incident_id="two-devices", actions=actions, confidence=.9,
            expected_cost_usd=1.5, expected_benefit=.8, blast_radius=.25,
            approval_required=False, rationale="test",
            selected_device_ids=["ambulance-01", "scada-01"],
        )
        approved = asyncio.run(system._warden({"plan": plan.model_dump(), "trace": []}))
        self.assertTrue(approved["warden"]["verified"])
        third = plan.model_copy(update={"actions": actions + [Action("qos", "pipeline-01", {"profile": "guaranteed", "duration_seconds": 300}, "test")], "selected_device_ids": ["ambulance-01", "scada-01", "pipeline-01"]})
        rejected = asyncio.run(system._warden({"plan": third.model_dump(), "trace": []}))
        self.assertFalse(rejected["warden"]["safety_checks"]["unique_device_count_within_limit"])
        duplicate = plan.model_copy(update={"actions": actions + [actions[0]]})
        rejected_duplicate = asyncio.run(system._warden({"plan": duplicate.model_dump(), "trace": []}))
        self.assertFalse(rejected_duplicate["warden"]["safety_checks"]["no_duplicate_equivalent_actions"])

    def test_failed_fixture_verification_reverses_executed_actions(self):
        settings = self.settings(rollback_test_mode=True)
        result = asyncio.run(HarisAgentSystem(FixtureNokiaClient(settings), settings=settings).run_cycle(True))
        self.assertTrue(result["execution"]["executed"])
        self.assertFalse(result["verification"]["verified"])
        self.assertEqual(result["verification"]["status"], "unchanged")
        self.assertTrue(result["rollback"]["rollback_verified"])
        self.assertTrue(any(item["success"] for item in result["rollback"]["actions"]))
        self.assertEqual(result["final_status"], "rolled_back_safely")

    def test_warden_rejects_an_unsafe_network_plan(self):
        settings = self.settings()
        system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
        unsafe_plan = RemediationPlan(
            incident_id="unsafe-plan",
            actions=[Action("qos", "ambulance-01", {"profile": "guaranteed", "duration_seconds": 300}, "test")],
            confidence=0.90,
            expected_cost_usd=settings.guardrails.qos_spend_ceiling_usd + 1,
            expected_benefit=0.8,
            blast_radius=0.1,
            approval_required=True,
            rationale="Cost exceeds autonomous ceiling.",
        )
        state = asyncio.run(system._warden({"plan": unsafe_plan.model_dump(), "trace": []}))
        self.assertFalse(state["warden"]["verified"])
        self.assertFalse(state["warden"]["safety_checks"]["cost_ok"])

    def test_execution_failure_does_not_become_a_successful_rollback(self):
        settings = self.settings()
        result = asyncio.run(
            HarisAgentSystem(FailingGeofenceFixtureNokiaClient(settings), settings=settings).run_cycle(True)
        )
        self.assertTrue(result["plan"]["actions"])
        self.assertTrue(result["warden"]["verified"])
        self.assertFalse(result["execution"]["executed"])
        self.assertEqual(result["execution"]["reason"], "execution_error")
        self.assertEqual(result["verification"]["status"], "execution_failed")
        self.assertEqual(result["final_status"], "execution_failed")
        self.assertEqual(result["learning"]["outcome"], "execution_failed")
        self.assertNotIn("rollback", result)

    def test_live_read_only_proposal_never_attempts_a_network_mutation(self):
        settings = AppSettings(nac_mode="live_read_only", fixture_dir="fixtures")
        client = FixtureNokiaClient(settings)
        result = asyncio.run(HarisAgentSystem(client, settings=settings).run_cycle(True))
        self.assertTrue(result["plan"]["actions"])
        self.assertTrue(result["warden"]["verified"])
        self.assertFalse(result["execution"]["executed"])
        self.assertEqual(result["execution"]["reason"], "live_read_only")
        self.assertEqual(result["verification"]["status"], "live_read_only_proposal")
        self.assertEqual(result["final_status"], "live_read_only_proposal")
        self.assertEqual(result["learning"]["outcome"], "live_read_only_proposal")
        self.assertNotIn("rollback", result)

    def test_legacy_live_mode_is_read_only_by_default(self):
        settings = AppSettings(nac_mode="live", nac_api_token="test-token")
        self.assertEqual(settings.nac_mode, "live_read_only")
        self.assertTrue(settings.is_live)
        self.assertFalse(settings.allows_network_writes)

    def test_direct_mutation_api_is_fixture_only_and_cannot_bypass_warden(self):
        fixture = SimpleNamespace(settings=AppSettings(nac_mode="fixture"))
        live = SimpleNamespace(settings=AppSettings(nac_mode="live_write", nac_api_token="test-token"))
        with patch.object(nokia_clients, "get_api_client", return_value=fixture):
            nokia_clients._require_write_mode()
        with patch.object(nokia_clients, "get_api_client", return_value=live):
            with self.assertRaises(HTTPException) as caught:
                nokia_clients._require_write_mode()
        self.assertEqual(caught.exception.status_code, 403)
        self.assertIn("durable WARDEN-authorized workflow", caught.exception.detail)

    def test_manual_autonomous_graph_route_is_fixture_only_in_live_modes(self):
        live_system = SimpleNamespace(
            settings=AppSettings(nac_mode="live_write", nac_api_token="test-token"),
            run_cycle=unittest.mock.AsyncMock(),
        )
        with patch.object(nokia_clients, "_authoritative_haris_system", return_value=live_system):
            with self.assertRaises(HTTPException) as caught:
                asyncio.run(nokia_clients.authoritative_autonomous_run())
        self.assertEqual(caught.exception.status_code, 403)
        live_system.run_cycle.assert_not_awaited()

    def test_camara_api_error_does_not_expose_provider_exception_details(self):
        sensitive = "https://provider.invalid/resource?token=secret-provider-value"

        async def fail():
            raise RuntimeError(sensitive)

        with self.assertLogs("haris.nokia", level="WARNING") as captured:
            with self.assertRaises(HTTPException) as caught:
                asyncio.run(nokia_clients._wrap(fail))
        self.assertEqual(caught.exception.status_code, 502)
        combined = " ".join(captured.output) + " " + str(caught.exception.detail)
        self.assertNotIn(sensitive, combined)
        self.assertNotIn("secret-provider-value", combined)

    def test_live_geofence_request_uses_installed_sdk_contract(self):
        client = LiveNokiaClient(self.live_settings())
        fake_subscription = SimpleNamespace(event_subscription_id="geo-live-1")
        with patch.object(client.client.geofencing, "subscribe", return_value=fake_subscription) as subscribe:
            result = asyncio.run(client.create_geofence("ambulance-01", "storm-impact"))
        self.assertEqual(result.subscription_id, "geo-live-1")
        device, sink, event_types, area = subscribe.call_args.args[:4]
        self.assertEqual(device.phone_number, "+999900000001")
        self.assertEqual(sink, "https://operator.example/haris/geofence-events")
        self.assertEqual(len(event_types), 2)
        self.assertEqual(area.center.latitude, 24.7136)
        self.assertEqual(area.radius, 5000)

    def test_live_qod_request_uses_operator_profile_mapping(self):
        client = LiveNokiaClient(self.live_settings())
        fake_session = SimpleNamespace(id="qod-live-1", status="REQUESTED")
        with patch("nokia_clients.Device.create_qod_session", return_value=fake_session) as create_session:
            result = asyncio.run(client.request_qos("ambulance-01", "guaranteed", 300))
        self.assertEqual(result.session_id, "qod-live-1")
        self.assertEqual(result.profile, "guaranteed")
        self.assertEqual(create_session.call_args.args[:3], ("OPERATOR_GOLD", 300, "203.0.113.10"))

    def test_live_missing_contract_config_fails_closed(self):
        settings = AppSettings(nac_mode="live", nac_api_token="test-token")
        client = LiveNokiaClient(settings)
        self.assertIn("NAC_GEOFENCE_SINK", client.action_safety_error("geofence", {"polygon_id": "storm-impact"}))
        self.assertIn("NAC_QOD_SERVICE_IPV4", client.action_safety_error("qos", {"profile": "guaranteed"}))
        self.assertEqual(client.capability_report()["qod"]["status"], "OPERATOR_VALUE_REQUIRED")
        self.assertEqual(client.capability_report()["trusted_dispatch"]["status"], "PRIVILEGED_ONLY")

    def test_live_geofence_event_types_are_validated_against_sdk_enum(self):
        client = LiveNokiaClient(self.live_settings(
            nac_geofence_event_types=["org.camaraproject.geofencing-subscriptions.v0.area-entered", "invalid-event"],
        ))
        error = client.action_safety_error("geofence", {"polygon_id": "storm-impact"})
        self.assertIn("unsupported SDK values", error)

    def test_live_slice_alias_requires_and_resolves_operator_slice_id(self):
        missing = LiveNokiaClient(self.live_settings())
        self.assertIn("NAC_EMERGENCY_SLICE_ID", missing.action_safety_error("slice_attach", {"slice_id": "haris-emergency"}))
        configured = LiveNokiaClient(self.live_settings(nac_emergency_slice_id="operator-slice-42"))
        self.assertIsNone(configured.action_safety_error("slice_attach", {"slice_id": "haris-emergency"}))
        self.assertEqual(configured._resolve_slice_id("haris-emergency"), "operator-slice-42")

    def test_warden_accepts_constructible_live_actions(self):
        settings = self.live_settings()
        client = LiveNokiaClient(settings)
        system = HarisAgentSystem(client, settings=settings)
        plan = RemediationPlan(
            incident_id="live-contract-test",
            actions=[
                Action("geofence", "ambulance-01", {"polygon_id": "storm-impact"}, "test"),
                Action("qos", "ambulance-01", {"profile": "guaranteed", "duration_seconds": 300}, "test"),
            ],
            confidence=0.9,
            expected_cost_usd=0.75,
            expected_benefit=0.8,
            blast_radius=0.125,
            approval_required=False,
            rationale="All installed SDK inputs are configured.",
        )
        state = asyncio.run(system._warden({"plan": plan.model_dump(), "trace": []}))
        self.assertTrue(state["warden"]["verified"])
        self.assertEqual(state["warden"]["action_errors"], {})
        self.assertEqual(state["warden"]["capability_report"]["geofencing"]["status"], "SUPPORTED_AND_CONFIGURED")
        self.assertEqual(state["warden"]["capability_report"]["qod"]["status"], "SUPPORTED_AND_CONFIGURED")

    def test_no_action_cycle_is_not_reported_as_mitigated(self):
        settings = self.settings()
        client = FixtureNokiaClient(settings)
        for reading in client.state["network"].values():
            reading["congestion_level"] = "None"
            reading["congestion_pct"] = 0.0
            reading["predicted_congestion_pct"] = 0.0
        result = asyncio.run(HarisAgentSystem(client, settings=settings).run_cycle(False))
        self.assertEqual(result["plan"]["actions"], [])
        self.assertFalse(result["warden"]["verified"])
        self.assertEqual(result["verification"]["status"], "no_action_proposed")
        self.assertEqual(result["final_status"], "no_action_proposed")

    def test_energy_guard_requires_sustained_timestamped_high_congestion(self):
        settings = self.settings(energy_guard_sustained_congestion_seconds=600)
        client = FixtureNokiaClient(settings)
        engine = PlaybookEngine(settings, client, memory=None)
        congestion = asyncio.run(client.congestion_insights())
        devices = asyncio.run(client.device_status(settings.registered_devices))
        now = 10_000.0
        isolated = {"T05": [{"observed_at": now, "congestion_level": "High"}]}
        self.assertEqual(engine.energy_guard(congestion, devices, isolated, observed_at=now), [])
        sustained = {"T05": [{"observed_at": now - offset, "congestion_level": "High"} for offset in range(600, -1, -60)]}
        self.assertTrue(engine.energy_guard(congestion, devices, sustained, observed_at=now))
        recovered = {"T05": [{"observed_at": now - 60, "congestion_level": "High"}, {"observed_at": now, "congestion_level": "Low"}]}
        self.assertEqual(engine.energy_guard(congestion, devices, recovered, observed_at=now), [])
        self.assertEqual(engine.energy_guard(congestion, devices, {"T05": [{"observed_at": now - 3600, "congestion_level": "High"}, {"observed_at": now, "congestion_level": "High"}]}, observed_at=now), [])
        self.assertEqual(engine.energy_guard(congestion, devices, {"T05": [{"observed_at": "bad", "congestion_level": "High"}]}, observed_at=now), [])

    def test_capacity_harvest_requires_bulk_cohort_and_records_policy_only(self):
        settings = self.settings(capacity_harvest_min_bulk_devices=2)
        client = FixtureNokiaClient(settings)
        engine = PlaybookEngine(settings, client, memory=None)
        congestion = asyncio.run(client.congestion_insights())
        devices = asyncio.run(client.device_status(settings.registered_devices))
        actions = engine.capacity_harvest(congestion, devices)
        self.assertTrue(actions)
        self.assertTrue(all(action.parameters["defer_noncritical_uploads"] is True for action in actions))
        only_one = [device for device in devices if device.device_id not in {"fleet-02", "telemetry-01"}]
        self.assertFalse(any(action.device_id == "fleet-01" for action in engine.capacity_harvest(congestion, only_one)))

    def test_normalization_releases_only_owned_resources_and_audits_recovery(self):
        settings = self.settings()
        client = FixtureNokiaClient(settings)
        memory = MemoryStore(settings)
        memory._incidents = []
        memory._save_local = lambda: None
        system = HarisAgentSystem(client, memory=memory, settings=settings)
        asyncio.run(system.run_cycle(True))
        client.state["qos"]["foreign-session"] = {"active": True}
        for reading in client.state["network"].values():
            reading["congestion_level"] = "Low"
        recovered = asyncio.run(system.recover_normalized_incident(dust_advisory=False))
        self.assertEqual(recovered["final_status"], "recovered")
        self.assertTrue(recovered["recovery"]["verified"])
        self.assertTrue(any(item["operation"] == "release_qos" for item in recovered["recovery"]["actions"]))
        self.assertTrue(client.state["qos"]["foreign-session"]["active"])
        self.assertTrue(memory.verify_audit_chain()["valid"])
        self.assertEqual(memory.recent_incidents()[0].outcome, "recovered")

    def test_failed_normalization_cleanup_is_terminal_and_does_not_loop(self):
        class FailingReleaseClient(FixtureNokiaClient):
            async def release_qos(self, session_id):
                return False

        settings = self.settings()
        client = FailingReleaseClient(settings)
        system = HarisAgentSystem(client, settings=settings)
        asyncio.run(system.run_cycle(True))
        for reading in client.state["network"].values():
            reading["congestion_level"] = "Low"
        failed = asyncio.run(system.recover_normalized_incident(dust_advisory=False))
        actions = list(failed["recovery"]["actions"])
        self.assertEqual(failed["final_status"], "recovery_cleanup_failed")
        self.assertFalse(failed["recovery"]["verified"])
        self.assertEqual(asyncio.run(system.recover_normalized_incident(dust_advisory=False))["recovery"]["actions"], actions)

    def test_normalization_never_cleans_resources_from_a_previous_incident(self):
        settings = self.settings()
        client = FixtureNokiaClient(settings)
        system = HarisAgentSystem(client, settings=settings)
        first = asyncio.run(system.run_cycle(True))
        owned_session = next(item["session_id"] for item in first["execution"]["actions"] if item["kind"] == "qos")
        for reading in client.state["network"].values():
            reading["congestion_level"] = "None"
        asyncio.run(system.run_cycle(False))
        current = asyncio.run(system.recover_normalized_incident(dust_advisory=False))
        self.assertEqual(current["final_status"], "no_action_proposed")
        self.assertTrue(client.state["qos"][owned_session]["active"])

    def test_supervisory_cycle_exposes_structured_authoritative_decision_evidence(self):
        settings = self.settings()
        system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
        asyncio.run(system.run_cycle(True))
        cycle = system.current_cycle_status
        self.assertGreater(len(cycle["plan"]["actions"]), 0)
        self.assertEqual(
            len(cycle["plan"]["actions"]),
            len(cycle["execution"]["actions"]),
        )
        self.assertTrue(cycle["warden"]["verified"])
        self.assertTrue(cycle["execution"]["executed"])
        self.assertTrue(cycle["verification"]["verified"])
        self.assertIn("T03", cycle["pre_execution_congestion"])
        self.assertTrue(cycle["devices"])
        self.assertNotIn("session_id", str(cycle["execution"]))

    def test_actuator_event_types_match_success_and_failure_semantics(self):
        system = HarisAgentSystem(FixtureNokiaClient(self.settings()), settings=self.settings())
        state = {"events": [], "trace": [], "incident": {"incident_id": "event-test"}}
        system._trace(state, "ACTUATOR: QoD created device=ambulance-01")
        system._trace(state, "ACTUATOR: geofence created device=ambulance-01")
        system._trace(state, "ACTUATOR: execution failed after 0 actions")
        self.assertEqual(state["events"][0]["type"], "ACTION_EXECUTED")
        self.assertEqual(state["events"][1]["type"], "ACTION_EXECUTED")
        self.assertEqual(state["events"][2]["type"], "ACTION_FAILED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
