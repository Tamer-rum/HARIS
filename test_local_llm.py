"""The optional on-machine advisory model is bounded exactly like the hosted ones."""

import asyncio
import json
import unittest
from unittest.mock import AsyncMock, MagicMock

from agents import HarisAgentSystem, Incident, LocalChatModel, ReasoningRouter
from config import AppSettings, get_settings
from memory import MemoryStore
from nokia_clients import FixtureNokiaClient
from playbooks import Action


class _Response:
    def __init__(self, content):
        self.content = content


class _Model:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.calls = 0

    async def ainvoke(self, _prompt):
        self.calls += 1
        if self.error:
            raise self.error
        return _Response(self.response)


def _planner(candidate_ids=("candidate-0",)):
    return json.dumps({
        "ranked_candidate_ids": list(candidate_ids),
        "confidence_adjustment": 0.02,
        "expected_benefit": "Protect the ambulance first.",
        "rationale": "Tier-1 asset in the congested cell.",
    })


class LocalModelTests(unittest.TestCase):
    def setUp(self):
        self.settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures")
        self.incident = Incident(
            storm_advisory=True, peak_congestion_level="High", peak_confidence_level=90,
            affected_cells=["T03"], affected_devices=["ambulance-01"], severity="critical",
        )
        self.actions = [
            Action("qos", "ambulance-01", {"profile": "guaranteed"}, "bounded candidate"),
            Action("slice_attach", "ambulance-01", {"slice_id": "haris-emergency"}, "bounded candidate"),
        ]
        self.local_name = f"{self.settings.local_llm_model} (local)"

    def test_only_loopback_urls_are_accepted(self):
        for url in ("http://127.0.0.1:11434", "http://localhost:11434/", "http://[::1]:11434"):
            self.assertTrue(LocalChatModel(url, "m", 5).base_url.startswith("http"))
        self.assertEqual(LocalChatModel("http://localhost:11434/", "m", 5).base_url, "http://localhost:11434")
        for url in ("http://example.com:11434", "http://10.0.0.5:11434", "file:///etc/passwd", "localhost:11434"):
            with self.assertRaises(ValueError):
                LocalChatModel(url, "m", 5)

    def test_test_runtime_never_builds_a_local_model(self):
        router = ReasoningRouter(AppSettings(local_llm_base_url="http://127.0.0.1:11434"))
        self.assertIsNone(router.local)
        self.assertEqual(router.availability_reason, "runtime_policy_blocks_llm")
        self.assertIsNone(get_settings().local_llm_base_url)

    def test_local_model_is_the_last_link_of_the_chain(self):
        router = ReasoningRouter(self.settings)
        router.gemini = _Model(error=RuntimeError("backhaul down"))
        router.groq = _Model(error=RuntimeError("backhaul down"))
        router.local = _Model(_planner())
        result = asyncio.run(router.assess(self.incident, [], self.actions))
        self.assertTrue(result["ai_planner_used"])
        self.assertEqual(result["model"], self.local_name)
        self.assertTrue(result["fallback_used"])
        self.assertEqual(result["ranked_candidate_ids"], ["candidate-0"])

    def test_hosted_success_never_reaches_the_local_model(self):
        router = ReasoningRouter(self.settings)
        router.gemini = _Model(_planner())
        router.local = _Model(_planner())
        asyncio.run(router.assess(self.incident, [], self.actions))
        self.assertEqual(router.local.calls, 0)

    def test_local_only_skips_every_hosted_provider(self):
        router = ReasoningRouter(self.settings)
        router.gemini = _Model(_planner())
        router.groq = _Model(_planner())
        router.local = _Model(_planner(("candidate-1",)))
        result = asyncio.run(router.assess(self.incident, [], self.actions, local_only=True))
        self.assertEqual((router.gemini.calls, router.groq.calls), (0, 0))
        self.assertEqual(result["model"], self.local_name)
        self.assertFalse(result["fallback_used"])

    def test_local_hallucinations_fall_back_to_deterministic_policy(self):
        router = ReasoningRouter(self.settings)
        for bad in (_planner(("candidate-999",)), "not json", json.dumps({
            "ranked_candidate_ids": ["candidate-0"], "confidence_adjustment": 0.9,
            "expected_benefit": "x", "rationale": "x",
        })):
            router.local = _Model(bad)
            result = asyncio.run(router.assess(self.incident, [], self.actions, local_only=True))
            self.assertFalse(result["ai_planner_used"])
            self.assertEqual(result["ranked_candidate_ids"], [])
            self.assertIn("Local model advisory unavailable", result["rationale"])

    def _isolated_system(self):
        memory = MagicMock(spec=MemoryStore)
        memory.search_incidents = AsyncMock(side_effect=AssertionError("durable history read"))
        memory.remember_incident = AsyncMock(side_effect=AssertionError("durable history write"))
        system = HarisAgentSystem(FixtureNokiaClient(self.settings), memory=memory, settings=self.settings)
        system._dust_advisory = AsyncMock(side_effect=AssertionError("environmental HTTP path used"))
        system._crew_advisory = AsyncMock(side_effect=AssertionError("CrewAI path used"))
        system.reasoning.gemini = _Model(error=AssertionError("hosted provider used"))
        system.reasoning.groq = _Model(error=AssertionError("hosted provider used"))
        return system

    def test_isolated_demo_consults_only_the_local_model(self):
        system = self._isolated_system()
        system.reasoning.local = _Model(_planner())
        result = asyncio.run(system.run_cycle(True, isolated_fixture_demo=True))
        self.assertEqual(system.reasoning.local.calls, 1)
        self.assertEqual((system.reasoning.gemini.calls, system.reasoning.groq.calls), (0, 0))
        self.assertTrue(result["warden"]["verified"])
        self.assertFalse(result["durable_history_write"])
        trace = "\n".join(str(line) for line in result.get("trace", []))
        self.assertIn(f"MODEL={self.local_name}", trace)

    def test_isolated_demo_without_local_model_stays_deterministic(self):
        system = self._isolated_system()
        result = asyncio.run(system.run_cycle(True, isolated_fixture_demo=True))
        trace = "\n".join(str(line) for line in result.get("trace", []))
        self.assertIn("MODEL=deterministic", trace)
        self.assertTrue(result["warden"]["verified"])


if __name__ == "__main__":
    unittest.main()
