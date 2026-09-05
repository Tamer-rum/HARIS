"""Deterministic behavioral tests for the HARIS network-resilience loop."""

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from agents import HarisAgentSystem, RemediationPlan
from config import AppSettings, GeofenceArea
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
        self.assertLessEqual(len(result["plan"]["actions"]), settings.guardrails.max_devices_reconfigured_per_cycle)
        self.assertTrue(result["warden"]["verified"])
        self.assertTrue(result["execution"]["executed"])
        self.assertTrue(result["verification"]["verified"])
        self.assertEqual(result["final_status"], "mitigated")
        self.assertTrue(result["learning"]["incident_saved"])
        self.assertEqual(result["active_playbook"]["state"], "MITIGATED")
        self.assertEqual(result["active_playbook"]["current_stage"], "LEARN")
        self.assertEqual(result["active_playbook"]["latest_outcome"], "mitigated")

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
