from config import get_settings
from memory import MemoryStore
from nokia_clients import CongestionReading, DeviceStatus
from playbooks import PlaybookEngine


settings = get_settings()

engine = PlaybookEngine(
    settings=settings,
    client=None,
    memory=MemoryStore(),
)

congestion = [
    CongestionReading(
        cell_id="T03",
        congestion_level="High",
        confidence_level=81,
        interval_start="2026-08-31T04:20:00Z",
        interval_stop="2026-08-31T04:25:00Z",
    )
]

devices = [
    DeviceStatus(
        device_id="ambulance-01",
        reachable=True,
        roaming=False,
        battery_pct=71,
        tier=1,
        cell_id="T03",
    ),
    DeviceStatus(
        device_id="telemetry-01",
        reachable=True,
        roaming=False,
        battery_pct=20,
        tier=3,
        cell_id="T03",
    ),
]

print("=" * 60)
print("HARIS - PLAYBOOK ENGINE TEST")
print("=" * 60)

print("\n[1] Classification")
print(engine.classify(congestion[0]))

print("\n[2] Storm Shield")
print(
    engine.storm_shield(
        dust_advisory=True,
        congestion=congestion,
        devices=devices,
    )
)

print("\n[3] Capacity Harvest")
print(
    engine.capacity_harvest(
        congestion=congestion,
        devices=devices,
    )
)

print("\n[4] Energy Guard")
print(
    engine.energy_guard(
        congestion=congestion,
        devices=devices,
    )
)

print("\n[5] Evaluation")
result = engine.evaluate(
    dust_advisory=True,
    congestion=congestion,
    devices=devices,
)

print(result)

print("\n" + "=" * 60)
print("PLAYBOOK TEST COMPLETED")
print("=" * 60)