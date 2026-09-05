import asyncio
import json

from agents import HarisAgentSystem
from config import get_settings
from nokia_clients import build_nokia_client


async def main():
    settings = get_settings()
    system = HarisAgentSystem(build_nokia_client(settings), settings=settings)
    result = await system.run_cycle(dust_advisory=True)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(main())
