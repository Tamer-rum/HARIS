"""Backend-owned, non-overlapping HARIS control-loop scheduler."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

from agents import HarisAgentSystem
from config import AppSettings

logger = logging.getLogger("haris.scheduler")


class HarisScheduler:
    def __init__(self, system: HarisAgentSystem, settings: AppSettings):
        self.system, self.settings = system, settings
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self.last_result: Optional[Dict[str, Any]] = None
        self.last_error: Optional[str] = None
        self.cycles_completed = 0

    @property
    def running(self) -> bool:
        return self._running

    async def start(self) -> bool:
        if self._running:
            return False
        if self.settings.nac_mode == "live_write" and not self.settings.enable_live_write_loop:
            raise RuntimeError("Continuous LIVE_WRITE requires ENABLE_LIVE_WRITE_LOOP=true.")
        self._running = True
        self._task = asyncio.create_task(self._run(), name="haris-control-loop")
        return True

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        while self._running:
            try:
                self.last_result = await self.system.run_cycle(dust_advisory=True)
                self.cycles_completed += 1
                self.last_error = None
            except Exception as exc:  # isolate one bad cycle from the next
                self.last_error = str(exc)
                logger.exception("HARIS scheduled cycle failed")
            await asyncio.sleep(self.settings.cycle_seconds)
