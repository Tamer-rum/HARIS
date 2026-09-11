import asyncio
import json
import tempfile
import unittest

from agents import HarisAgentSystem
from config import AppSettings
from nokia_clients import FixtureNokiaClient
from observations import ObservationStore, SanitizedObservationDiagnostic


class ObservationClient(FixtureNokiaClient):
    def __init__(self, settings):
        super().__init__(settings)
        self.calls = {"congestion": 0, "devices": 0, "locations": 0}
        self.fail = {}
        self.delay = 0

    async def congestion_insights(self, *args):
        self.calls["congestion"] += 1
        if self.delay: await asyncio.sleep(self.delay)
        if self.fail.get("congestion"): raise self.fail["congestion"]
        return await super().congestion_insights(*args)

    async def device_status(self, *args):
        self.calls["devices"] += 1
        if self.fail.get("devices"): raise self.fail["devices"]
        return await super().device_status(*args)

    async def location_retrieval(self, *args):
        self.calls["locations"] += 1
        if self.fail.get("locations"): raise self.fail["locations"]
        return await super().location_retrieval(*args)

    async def request_qos(self, *args, **kwargs):
        raise AssertionError("observation loop must never mutate QoD")


class ObservationTests(unittest.TestCase):
    def settings(self, **updates):
        return AppSettings(nac_mode="fixture", fixture_dir="fixtures", nokia_observation_enabled=True, **updates)

    def test_repeated_read_only_snapshots_preserve_identical_source_data(self):
        settings = self.settings(nokia_observation_history_limit=2)
        client = ObservationClient(settings)
        store = ObservationStore(client, settings)
        first, second = asyncio.run(store.poll_once()), asyncio.run(store.poll_once())
        # The fixture source stamps each real read interval independently; no
        # HARIS smoothing is applied to its categorical/numeric evidence.
        evidence = lambda snapshot: [
            (item["cell_id"], item.get("congestion_level"), item.get("congestion_pct"), item.get("latency_ms"))
            for item in snapshot["congestion"]
        ]
        self.assertEqual(evidence(first), evidence(second))
        self.assertEqual(client.calls, {"congestion": 2, "devices": 2, "locations": 2})
        self.assertEqual(store.status()["connection_status"], "CONNECTED")

    def test_one_completed_poll_emits_one_reconciliation_view(self):
        store = ObservationStore(ObservationClient(self.settings()), self.settings())
        received = []

        async def listener(view):
            received.append(view["observed_at"])

        async def exercise():
            store.add_listener(listener)
            await store.poll_once()
            if store._listener_tasks:
                await asyncio.gather(*list(store._listener_tasks))

        asyncio.run(exercise())
        self.assertEqual(len(received), 1)

    def test_no_overlapping_poll_and_bounded_history(self):
        settings = self.settings(nokia_observation_history_limit=2)
        client = ObservationClient(settings); client.delay = .03
        store = ObservationStore(client, settings)
        async def exercise():
            first = asyncio.create_task(store.poll_once())
            await asyncio.sleep(.005)
            second = await store.poll_once()
            await first
            await store.poll_once(); await store.poll_once()
            return second
        self.assertIsNone(asyncio.run(exercise()))
        self.assertEqual(len(store.history()), 2)

    def test_timeout_rate_limit_and_partial_failure_are_truthful(self):
        settings = self.settings(nokia_observation_timeout_seconds=1, nokia_observation_interval_seconds=3)
        client = ObservationClient(settings); store = ObservationStore(client, settings)
        asyncio.run(store.poll_once())
        client.fail["congestion"] = RuntimeError("429 simulated")
        partial = asyncio.run(store.poll_once())
        self.assertEqual(partial["connection_status"], "DEGRADED")
        self.assertEqual(partial["capabilities"]["congestion"]["status"], "RATE_LIMITED")
        self.assertGreaterEqual(store.status()["next_poll_at"] - partial["observed_at"], 6)
        client.fail = {"congestion": RuntimeError("down"), "devices": RuntimeError("down"), "locations": RuntimeError("down")}
        stale = asyncio.run(store.poll_once())
        self.assertEqual(stale["connection_status"], "STALE")
        self.assertTrue(any(item["connection_status"] == "CONNECTED" for item in store.history()))

    def test_fresh_snapshot_feeds_sentinel_without_extra_network_reads(self):
        settings = self.settings()
        client = ObservationClient(settings); store = ObservationStore(client, settings)
        asyncio.run(store.poll_once())
        counts = dict(client.calls)
        system = HarisAgentSystem(client, settings=settings)
        system.set_observation_store(store)
        state = asyncio.run(system._sentinel({"dust_advisory": True, "trace": []}))
        self.assertTrue(state["congestion"])
        self.assertEqual(client.calls, counts)
        self.assertIn("fresh backend Nokia observation snapshot", " ".join(state["trace"]))

    def test_fixture_mode_is_explicitly_simulated(self):
        settings = self.settings()
        snapshot = asyncio.run(ObservationStore(ObservationClient(settings), settings).poll_once())
        self.assertEqual(snapshot["mode"], "fixture")
        self.assertEqual(snapshot["source"], "fixture")

    def test_diagnostic_persists_only_aggregate_safe_evidence(self):
        settings = self.settings()
        with tempfile.TemporaryDirectory() as directory:
            path = f"{directory}/observations.jsonl"
            store = ObservationStore(
                ObservationClient(settings), settings, SanitizedObservationDiagnostic(path)
            )
            asyncio.run(store.poll_once())
            with open(path, encoding="utf-8") as handle:
                records = [json.loads(line) for line in handle]
        congestion = next(record for record in records if record["capability"] == "congestion")
        reachability = next(record for record in records if record["capability"] == "reachability")
        location = next(record for record in records if record["capability"] == "location")
        self.assertIn("congestion_levels", congestion["evidence_summary"])
        self.assertEqual(reachability["evidence_summary"], {"reachable": 8, "unreachable": 0})
        self.assertEqual(location["evidence_summary"], {"location_results": 8})
        self.assertNotIn("device_id", json.dumps(records))
        self.assertNotIn("latitude", json.dumps(records))


if __name__ == "__main__":
    unittest.main(verbosity=2)
