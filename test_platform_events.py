import asyncio
import unittest
from unittest.mock import patch

from config import AppSettings
from event_bus import InMemoryEventBus
from network_state import NetworkStateRegistry
from platform_events import NokiaCongestionWebhook


class PlatformEventTests(unittest.TestCase):
    def test_canonical_event_deduplicates_and_preserves_source_time(self):
        async def exercise():
            bus = InMemoryEventBus()
            event = NokiaCongestionWebhook(event_id="nokia-1", event_timestamp=123.0, cell_id="T03", congestion_level="High", confidence_level=88).canonical(mode="live_read_only")
            return event, await bus.publish(event), await bus.publish(event), bus
        event, first, duplicate, bus = asyncio.run(exercise())
        self.assertEqual(first, "accepted"); self.assertEqual(duplicate, "duplicate")
        self.assertEqual(event.source_timestamp, 123.0)
        self.assertEqual(len(bus.recent()), 1)

    def test_out_of_order_event_cannot_overwrite_newer_projection(self):
        registry = NetworkStateRegistry(mode="live_read_only", stale_after_seconds=999999999)
        newer = NokiaCongestionWebhook(event_timestamp=200.0, cell_id="T03", congestion_level="High").canonical(mode="live_read_only")
        older = NokiaCongestionWebhook(event_timestamp=100.0, cell_id="T03", congestion_level="Low").canonical(mode="live_read_only")
        self.assertTrue(registry.apply_event(newer)); self.assertFalse(registry.apply_event(older))
        self.assertEqual(registry.snapshot()["entities"]["T03"]["nokia_congestion"], "High")

    def test_webhook_rejects_missing_or_wrong_secret(self):
        from fastapi import HTTPException
        import run_api
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", nokia_event_webhook_secret="server-secret")
        event = NokiaCongestionWebhook(event_timestamp=123.0, cell_id="T03", congestion_level="High")
        with patch.object(run_api, "get_settings", return_value=settings):
            with self.assertRaises(HTTPException) as context:
                asyncio.run(run_api.ingest_nokia_congestion(event, x_haris_event_secret="wrong"))
        self.assertEqual(context.exception.status_code, 401)


if __name__ == "__main__":
    unittest.main()
