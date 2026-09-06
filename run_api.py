import asyncio
import uvicorn
from agents import HarisAgentSystem
from config import get_settings
from nokia_clients import app, build_nokia_client, register_dispatch_system_factory
from scheduler import HarisScheduler

_scheduler = None
_system = None
_scheduler_task = None


def get_haris_system() -> HarisAgentSystem:
    """Construct the expensive agent graph only when a backend feature needs it."""
    global _system
    if _system is None:
        settings = get_settings()
        _system = HarisAgentSystem(build_nokia_client(settings), settings=settings)
    return _system


register_dispatch_system_factory(get_haris_system)


@app.on_event("startup")
async def start_haris_scheduler():
    global _scheduler_task
    settings = get_settings()
    if settings.enable_continuous_loop:
        # Do not delay web readiness with graph/CrewAI construction. The
        # scheduled task builds it immediately after startup returns.
        _scheduler_task = asyncio.create_task(_start_scheduler(), name="haris-scheduler-bootstrap")


async def _start_scheduler() -> None:
    global _scheduler
    settings = get_settings()
    _scheduler = HarisScheduler(get_haris_system(), settings)
    await _scheduler.start()


@app.on_event("shutdown")
async def stop_haris_scheduler():
    if _scheduler_task:
        _scheduler_task.cancel()
    if _scheduler:
        await _scheduler.stop()

if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
