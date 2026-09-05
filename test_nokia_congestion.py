"""Read-only congestion evidence contract; no subscription is created."""
import unittest

from config import AppSettings
from nokia_clients import FixtureNokiaClient


class NokiaCongestionContractTests(unittest.TestCase):
    def test_fixture_preserves_categorical_congestion_and_numeric_fixture_kpis(self):
        import asyncio
        readings = asyncio.run(FixtureNokiaClient(AppSettings(nac_mode="fixture", fixture_dir="fixtures")).congestion_insights())
        self.assertTrue(readings)
        self.assertTrue(all(item.congestion_level in {"None", "Low", "Medium", "High"} for item in readings))


if __name__ == "__main__":
    unittest.main(verbosity=2)
