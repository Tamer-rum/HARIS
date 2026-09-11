"""Safe regression coverage for HARIS bounded multi-agent reasoning."""

import asyncio
import json
import unittest
from unittest.mock import patch

from agents import CREWAI_ROLE_NAMES, HarisAgentSystem, Incident, ReasoningRouter
from config import AppSettings
from nokia_clients import FixtureNokiaClient
from playbooks import Action


class _Response:
    def __init__(self, content):
        self.content = content


class _Model:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error

    async def ainvoke(self, _prompt):
        if self.error:
            raise self.error
        return _Response(self.response)


class AiReasoningTests(unittest.TestCase):
    def setUp(self):
        self.settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", gemini_api_key=None, groq_api_key=None)
        self.incident = Incident(
            storm_advisory=True, peak_congestion_level="High", peak_confidence_level=90,
            affected_cells=["T03"], affected_devices=["ambulance-01"], severity="critical",
        )
        self.actions = [
            Action("qos", "ambulance-01", {"profile": "guaranteed"}, "bounded candidate"),
            Action("slice_attach", "ambulance-01", {"slice_id": "haris-emergency"}, "bounded candidate"),
        ]

    @staticmethod
    def valid_planner(candidate_ids=("candidate-1",)):
        return json.dumps({
            "ranked_candidate_ids": list(candidate_ids),
            "confidence_adjustment": 0.03,
            "expected_benefit": "Prioritize protected traffic.",
            "rationale": "Evidence supports the supplied candidates.",
        })

    def test_gemini_success_uses_only_deterministic_candidates(self):
        router = ReasoningRouter(self.settings)
        router.gemini = _Model(self.valid_planner())
        result = asyncio.run(router.assess(self.incident, [], self.actions))
        self.assertTrue(result["ai_planner_used"])
        self.assertEqual(result["model"], self.settings.gemini_model)
        self.assertFalse(result["fallback_used"])
        self.assertEqual(result["ranked_candidate_ids"], ["candidate-1"])
        self.assertEqual(result["confidence"], 0.89)

    def test_gemini_failure_uses_groq_with_truthful_fallback(self):
        router = ReasoningRouter(self.settings)
        router.gemini = _Model(error=RuntimeError("provider unavailable"))
        router.groq = _Model(self.valid_planner(("candidate-0",)))
        result = asyncio.run(router.assess(self.incident, [], self.actions))
        self.assertTrue(result["ai_planner_used"])
        self.assertEqual(result["model"], self.settings.groq_model)
        self.assertTrue(result["fallback_used"])

    def test_all_provider_failures_and_malformed_output_fall_back_deterministically(self):
        router = ReasoningRouter(self.settings)
        router.gemini = _Model("not json")
        router.groq = _Model(error=RuntimeError("unavailable"))
        result = asyncio.run(router.assess(self.incident, [], self.actions))
        self.assertFalse(result["ai_planner_used"])
        self.assertTrue(result["fallback_used"])
        self.assertEqual(result["ranked_candidate_ids"], [])

    def test_hallucinated_candidates_and_excessive_adjustments_are_rejected(self):
        router = ReasoningRouter(self.settings)
        router.gemini = _Model(self.valid_planner(("candidate-999",)))
        self.assertFalse(asyncio.run(router.assess(self.incident, [], self.actions))["ai_planner_used"])
        router.gemini = _Model(json.dumps({
            "ranked_candidate_ids": ["candidate-0"], "confidence_adjustment": 0.9,
            "expected_benefit": "unsafe", "rationale": "unsafe",
        }))
        self.assertFalse(asyncio.run(router.assess(self.incident, [], self.actions))["ai_planner_used"])

    def test_five_role_crew_output_is_required_and_has_no_nokia_tools(self):
        system = HarisAgentSystem(FixtureNokiaClient(self.settings), settings=self.settings)
        system.crewai_agents = {role: object() for role in CREWAI_ROLE_NAMES}
        notes = {role: f"{role} evidence note" for role in CREWAI_ROLE_NAMES}

        class FakeCrew:
            def __init__(self, **kwargs):
                self.kwargs = kwargs
            def kickoff(self):
                return json.dumps({
                    "ranked_candidate_ids": ["candidate-0"], "confidence_modifier": 0.01,
                    "expected_benefit": "bounded", "rationale": "bounded", "specialist_notes": notes,
                })

        with patch("agents.Task", lambda **_: object()), patch("agents.Crew", FakeCrew):
            advisory = asyncio.run(system._crew_advisory(self.incident, self.actions, []))
        self.assertTrue(advisory["used"])
        self.assertEqual(advisory["roles"], list(CREWAI_ROLE_NAMES))
        self.assertEqual(set(advisory["advisory"]["specialist_notes"]), set(CREWAI_ROLE_NAMES))

    def test_test_runtime_blocks_role_provider_construction(self):
        created = []

        class FakeAgent:
            def __init__(self, **kwargs):
                created.append(kwargs)

        with patch("agents.LLM", lambda **_: object()), patch("agents.Agent", FakeAgent), patch(
            "agents.ChatGoogleGenerativeAI", lambda **_: object()
        ):
            configured = AppSettings(
                nac_mode="fixture", fixture_dir="fixtures", gemini_api_key="test-only-key", groq_api_key=None,
            )
            system = HarisAgentSystem(FixtureNokiaClient(configured), settings=configured)
        self.assertEqual(created, [])
        self.assertEqual(system._crewai_init_reason, "runtime_policy_blocks_llm")

    def test_crew_failure_cannot_bypass_warden_or_execute_raw_model_output(self):
        system = HarisAgentSystem(FixtureNokiaClient(self.settings), settings=self.settings)
        system.crewai_agents = {role: object() for role in CREWAI_ROLE_NAMES}

        class BrokenCrew:
            def __init__(self, **_):
                pass
            def kickoff(self):
                raise RuntimeError("mock failure")

        with patch("agents.Task", lambda **_: object()), patch("agents.Crew", BrokenCrew):
            advisory = asyncio.run(system._crew_advisory(self.incident, self.actions, []))
        self.assertFalse(advisory["used"])
        self.assertTrue(advisory["fallback"])
        # The advisory API returns no executable action object or Nokia result.
        self.assertNotIn("actions", advisory)
        self.assertNotIn("execution", advisory)

    def test_trace_contains_only_safe_ai_metadata(self):
        system = HarisAgentSystem(FixtureNokiaClient(self.settings), settings=self.settings)
        result = asyncio.run(system.run_cycle(True))
        trace = "\n".join(result["trace"])
        self.assertIn("CREWAI_USED=false", trace)
        self.assertIn("AI_PLANNER_USED=false", trace)
        for forbidden in ("api_key", "access_token", "oauth_state", "authorization_url", "phone_number"):
            self.assertNotIn(forbidden, trace.lower())


if __name__ == "__main__":
    unittest.main(verbosity=2)
