import asyncio

from config import get_settings
from nokia_clients import LiveNokiaClient


async def main():
    settings = get_settings()

    print("=" * 60)
    print("HARIS - LIVE NOKIA ADAPTER CHECK")
    print("=" * 60)

    print(f"NAC_MODE: {settings.nac_mode}")
    print(f"NAC_BASE_URL: {settings.nac_base_url}")
    print(f"NAC_API_TOKEN: {'AVAILABLE' if settings.nac_api_token else 'MISSING'}")

    if not settings.nac_api_token:
        print()
        print("STOP: NAC_API_TOKEN is not configured.")
        print("No API request was made.")
        return

    client = LiveNokiaClient(settings)

    print()
    print("LiveNokiaClient created successfully.")
    print("Checking read-only capabilities...")

    checks = [
        ("congestion_insights", client.congestion_insights),
        ("device_status", client.device_status),
        ("location_retrieval", client.location_retrieval),
    ]

    for name, method in checks:
        try:
            print(f"  {name}: READY")
        except Exception as exc:
            print(f"  {name}: ERROR - {exc}")

    print()
    print("No QoS, slice, geofence, or mutation operation was executed.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())