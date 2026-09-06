import uvicorn
from agents import HarisAgentSystem
from config import get_settings
from nokia_clients import app, build_nokia_client
from scheduler import HarisScheduler

_scheduler = None
_system = None


@app.on_event("startup")
async def start_haris_scheduler():
    global _scheduler, _system
    settings = get_settings()
    # The OAuth callback needs a server-owned continuation handler even when
    # the periodic scheduler is disabled.
    _system = HarisAgentSystem(build_nokia_client(settings), settings=settings)
    if settings.enable_continuous_loop:
        _scheduler = HarisScheduler(
            _system, settings
        )
        await _scheduler.start()


@app.on_event("shutdown")
async def stop_haris_scheduler():
    if _scheduler:
        await _scheduler.stop()

if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
