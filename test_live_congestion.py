import asyncio

from config import get_settings
from nokia_clients import LiveNokiaClient


async def main():
    print("=" * 60)
    print("HARIS - LIVE NOKIA CONGESTION TEST")
    print("=" * 60)

    settings = get_settings()

    if not settings.nac_api_token:
        raise RuntimeError("NAC_API_TOKEN is not available.")

    client = LiveNokiaClient(settings)

    print("Nokia Live client: CREATED")
    print()
    print("Requesting congestion from Nokia...")
    print()

    result = await client.congestion_insights(["T03"])

    print("HARIS CONGESTION RESULT:")

    for item in result:
        print(item)
        print(item.model_dump())

    print()
    print("=" * 60)
    print("TEST COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())