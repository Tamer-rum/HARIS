import asyncio
import os
import time
import unittest
from unittest.mock import patch

from config import AppSettings
from incidents import IncidentManager
from network_state import NetworkStateRegistry
from nokia_clients import _assert_external_test_call_allowed


def observation(levels, *, now=None):
    return {
        "observed_at": time.time() if now is None else now, "mode": "live_read_only", "source": "live",
        "congestion": [{"cell_id": cell, "congestion_level": level} for cell, level in levels.items()],
        "devices": [{"cell_id": cell, "reachable": True} for cell in levels],
        "locations": [],
    }


class FakeSystem:
    def __init__(self): self.calls = []
    async def run_cycle(self, **kwargs):
        self.calls.append(kwargs)
        return {"final_status": "live_read_only_proposal", "warden": {"verified": True}}


class PhaseFiveStateTests(unittest.TestCase):
    def settings(self, **updates):
        defaults = {"nac_mode": "fixture", "fixture_dir": "fixtures", "max_active_incidents": 2}
        defaults.update(updates)
        return AppSettings(**defaults)

    def test_low_medium_high_use_separate_source_and_haris_states(self):
        registry = NetworkStateRegistry(mode="live_read_only")
        registry.ingest(observation({"T02": "Low", "T03": "Medium", "T05": "High"}))
        entities = registry.snapshot()["entities"]
        self.assertEqual(entities["T02"]["nokia_congestion"], "Low")
        self.assertEqual(entities["T02"]["haris_state"], "STABLE")
        self.assertEqual(entities["T03"]["haris_state"], "WATCHING")
        self.assertEqual(entities["T05"]["haris_state"], "INCIDENT_OPEN")
        self.assertEqual(entities["T05"]["source"], "NOKIA_LIVE")

    def test_repeated_high_deduplicates_and_two_cells_are_independent(self):
        async def exercise():
            registry = NetworkStateRegistry(mode="live_read_only")
            system = FakeSystem(); manager = IncidentManager(system, registry, self.settings())
            first = observation({"T03": "High", "T05": "High"})
            await manager.ingest(first); await asyncio.sleep(0)
            await manager.ingest(observation({"T03": "High", "T05": "High"})); await asyncio.sleep(0)
            return manager, system
        manager, system = asyncio.run(exercise())
        self.assertEqual(len(manager.status()["incidents"]), 2)
        self.assertEqual(len(system.calls), 2)
        self.assertEqual({call["incident_scope_cells"][0] for call in system.calls}, {"T03", "T05"})
        self.assertTrue(all(item["evidence_updates"] == 2 for item in manager.status()["incidents"]))

    def test_medium_never_starts_unsafe_evaluation_and_capacity_is_bounded(self):
        async def exercise():
            registry = NetworkStateRegistry(mode="live_read_only")
            system = FakeSystem(); manager = IncidentManager(system, registry, self.settings(max_active_incidents=1))
            await manager.ingest(observation({"T03": "Medium"})); await asyncio.sleep(0)
            await manager.ingest(observation({"T03": "High", "T05": "High"})); await asyncio.sleep(0)
            return manager, system
        manager, system = asyncio.run(exercise())
        self.assertEqual(len(system.calls), 1)
        self.assertEqual(len(manager.status()["incidents"]), 1)

    def test_live_real_credentials_are_blocked_during_unittest(self):
        settings = AppSettings(nac_mode="live_read_only", nac_api_token="not-a-test-token")
        with patch.dict(os.environ, {"HARIS_ALLOW_EXTERNAL_TESTS": "false"}, clear=False):
            with self.assertRaisesRegex(RuntimeError, "External Nokia calls are blocked"):
                _assert_external_test_call_allowed(settings)


if __name__ == "__main__":
    unittest.main()
