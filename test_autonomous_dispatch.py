"""Safe, mocked coverage for autonomous field-intervention orchestration."""
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from agents import HarisAgentSystem
from config import AppSettings
from dispatch import AuthorizedEngineerRegistry, trusted_dispatch_history
from nokia_clients import FixtureNokiaClient, verified_identities


class AutonomousDispatchTests(unittest.TestCase):
    def setUp(self):
        self.settings = AppSettings(
            nac_mode="fixture", fixture_dir="fixtures",
            authorized_engineer_registry_path="fixtures/authorized_engineers.json",
            gemini_api_key=None, groq_api_key=None,
        )
        trusted_dispatch_history._attempts = []
        verified_identities._verified_at = {}

    def system(self):
        return HarisAgentSystem(FixtureNokiaClient(self.settings), settings=self.settings)

    def test_registry_ranks_enabled_available_site_coverage_by_priority(self):
        choices = AuthorizedEngineerRegistry(self.settings.authorized_engineer_registry_path).eligible(
            site="T03", required_skills=["tower-inspection"]
        )
        self.assertEqual([item.engineer_id for item in choices], ["eng-demo-01", "eng-demo-02"])

    def test_routine_autonomous_remediation_does_not_call_trusted_dispatch(self):
        system = self.system()
        with patch("agents.evaluate_trusted_dispatch_phone", new=AsyncMock()) as trust:
            result = asyncio.run(system.run_cycle(dust_advisory=True))
        trust.assert_not_awaited()
        self.assertIn(result["final_status"], {"mitigated", "rolled_back_safely", "verification_failed"})

    def test_field_intervention_selects_engineer_and_pauses_for_consent(self):
        system = self.system()
        state = {"cycle_id": "dispatch-wait", "incident": {"incident_id": "dispatch-wait", "affected_cells": ["T03"]}, "field_intervention_site": "T03", "field_intervention_skills": ["tower-inspection"]}
        result = asyncio.run(system._evaluate_field_intervention(state))
        self.assertEqual(result["status"], "WAITING_FOR_IDENTITY_VERIFICATION")
        self.assertEqual(result["engineer_id"], "eng-demo-01")
        self.assertEqual(result["masked_phone_number"], "***1000")
        self.assertEqual(trusted_dispatch_history.for_incident("dispatch-wait")[0].verification_status, "WAITING_FOR_IDENTITY_VERIFICATION")

    def test_langgraph_field_intervention_path_reaches_warden_and_waits_closed(self):
        result = asyncio.run(self.system().run_cycle(
            dust_advisory=True,
            field_intervention_required=True,
            field_intervention_site="T03",
            field_intervention_skills=["tower-inspection"],
        ))
        self.assertEqual(result["trusted_dispatch"]["status"], "WAITING_FOR_IDENTITY_VERIFICATION")
        self.assertEqual(result["final_status"], "waiting_for_identity_verification")
        self.assertFalse(result["warden"]["verified"])

    def test_recent_sim_swap_blocks_selected_engineer_and_records_attempt(self):
        system = self.system()
        verified_identities.record("+99999991000")
        state = {"cycle_id": "dispatch-block", "incident": {"incident_id": "dispatch-block", "affected_cells": ["T03"]}, "field_intervention_site": "T03", "field_intervention_skills": ["tower-inspection"]}
        with patch("agents.evaluate_trusted_dispatch_phone", new=AsyncMock(return_value={"decision": "BLOCK", "number_verified": True, "recent_sim_swap": True, "reason": "Recent SIM swap detected."})):
            result = asyncio.run(system._evaluate_field_intervention(state))
        self.assertEqual(result["decision"], "BLOCK")
        attempt = trusted_dispatch_history.for_incident("dispatch-block")[0]
        self.assertEqual(attempt.sim_swap_status, "RECENT_SWAP")
        self.assertEqual(attempt.final_dispatch_status, "BLOCKED")

    def test_evidence_grounded_explanation_labels_fixture_data(self):
        text = self.system()._explain_cycle({
            "incident": {"peak_congestion_level": "High", "affected_cells": ["T03"]},
            "active_playbook": {"name": "Storm Shield"}, "execution": {"actions": [{"kind": "qos"}]},
            "final_status": "mitigated", "environmental_source": "FIXTURE",
        })
        self.assertIn("High congestion", text)
        self.assertIn("Simulated fixture evidence", text)
        self.assertNotIn("latency", text.lower())

    def test_fixture_field_intervention_demo_requires_dispatch_and_labels_evidence(self):
        system = self.system()
        result = asyncio.run(system.run_field_intervention_demo())
        self.assertEqual(result["final_status"], "waiting_for_identity_verification")
        self.assertEqual(result["trusted_dispatch"]["engineer_id"], "eng-demo-01")
        evidence = result["field_intervention_evidence"]
        self.assertEqual(evidence["source"], "FIXTURE / SIMULATED DEMO")
        self.assertIn("not a Nokia", evidence["note"])
        self.assertTrue(any("FIELD_INTERVENTION_REQUIRED" in line for line in result["trace"]))
        record = system.memory.recent_incidents()[0]
        self.assertEqual(record.audit["field_intervention_evidence"]["source"], "FIXTURE / SIMULATED DEMO")

    def test_field_intervention_demo_is_unavailable_outside_fixture_mode(self):
        settings = self.settings.model_copy(update={"nac_mode": "live_read_only"})
        system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
        with self.assertRaises(RuntimeError):
            asyncio.run(system.run_field_intervention_demo())


if __name__ == "__main__":
    unittest.main(verbosity=2)
