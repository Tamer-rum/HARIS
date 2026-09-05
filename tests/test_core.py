import asyncio

from config import AppSettings
from nokia_clients import FixtureNokiaClient
from agents import HarisAgentSystem


def test_fixture_cycle():
    settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures")
    system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
    result = asyncio.run(system.run_cycle(True))
    assert result["incident"]["max_congestion_pct"] >= 70
    assert result["plan"]["actions"]
    assert result["learning"]["incident_saved"] is True
