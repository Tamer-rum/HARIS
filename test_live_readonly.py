import asyncio

from config import get_settings
from nokia_clients import LiveNokiaClient


async def main():
    settings = get_settings()
    client = LiveNokiaClient(settings)

    print("=" * 60)
    print("HARIS - LIVE ADAPTER READ-ONLY TEST")
    print("=" * 60)

    print("\n[1] Device Status")
    result = await client.device_status(["ambulance-01"])
    print(result)

    print("\n[2] Location")
    result = await client.location_retrieval(["ambulance-01"])
    print(result)

    print("\n" + "=" * 60)
    print("TEST COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())