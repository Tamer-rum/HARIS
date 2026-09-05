import asyncio

from config import get_settings
from nokia_clients import LiveNokiaClient
from agents import HarisAgentSystem


async def main():
    print("=" * 60)
    print("HARIS - SENTINEL LIVE NOKIA TEST")
    print("=" * 60)

    settings = get_settings()
    client = LiveNokiaClient(settings)
    system = HarisAgentSystem(client, settings=settings)

    state = {
        "dust_advisory": False,
        "congestion": [],
        "devices": [],
        "incident": {},
        "locations": [],
        "actions": [],
        "verification": {},
        "trace": [],
    }

    print()
    print("Running Sentinel...")
    print()

    result = await system._sentinel(state)

    print()
    print("SENTINEL INCIDENT:")
    print(result["incident"])

    print()
    print("CONGESTION EVIDENCE:")
    for item in result["congestion"]:
        print(item)

    print()
    print("AFFECTED DEVICES:")
    print(result["incident"]["affected_devices"])

    print()
    print("TRACE:")
    for item in result["trace"]:
        print(item)

    print()
    print("=" * 60)
    print("SENTINEL TEST COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())