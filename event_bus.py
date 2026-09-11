"""Disposable post-commit event bus; durable repositories remain authoritative."""
from __future__ import annotations

import asyncio
from collections import deque
from typing import Dict, List, Optional

from platform_events import HarisEvent


class InMemoryEventBus:
    def __init__(self, retention: int = 1000):
        if retention < 1:
            raise ValueError("retention must be positive")
        self._retention = retention
        self._events: deque[HarisEvent] = deque(maxlen=retention)
        self._keys: set[str] = set()
        self._queues: Dict[str, asyncio.Queue[HarisEvent]] = {}
        self._acks: Dict[str, set[str]] = {}
        self._coalesced_notifications = 0

    async def publish(self, event: HarisEvent) -> str:
        key = event.idempotency_key
        if key in self._keys:
            return "duplicate"
        self._keys.add(key); self._events.append(event)
        for queue in self._queues.values():
            if queue.full():
                queue.get_nowait()
                queue.task_done()
                self._coalesced_notifications += 1
            queue.put_nowait(event)
        return "accepted"

    def subscribe(self, group: str) -> asyncio.Queue[HarisEvent]:
        return self._queues.setdefault(group, asyncio.Queue(maxsize=self._retention))

    async def consume(self, group: str, timeout: Optional[float] = None) -> Optional[HarisEvent]:
        queue = self.subscribe(group)
        try: return await asyncio.wait_for(queue.get(), timeout) if timeout else await queue.get()
        except asyncio.TimeoutError: return None

    def ack(self, group: str, event_id: str) -> None:
        self._acks.setdefault(group, set()).add(event_id)

    def recent(self) -> List[HarisEvent]: return list(self._events)
    def health(self) -> Dict[str, int | str]: return {"status": "READY", "retained_events": len(self._events), "consumer_groups": len(self._queues), "notification_queue_capacity": self._retention, "coalesced_notifications": self._coalesced_notifications}
