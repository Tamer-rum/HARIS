"""Backend-owned capability-specific Nokia read-only observation state."""
from __future__ import annotations
import asyncio, json, time
from collections import deque
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional
from config import AppSettings
from nokia_clients import BaseNokiaClient
from runtime import external_access_policy

CAPABILITIES = {"congestion": ("congestion_insights", "nokia_congestion_interval_seconds"), "reachability": ("device_status", "nokia_reachability_interval_seconds"), "location": ("location_retrieval", "nokia_location_interval_seconds")}

def explicit_request_volume_estimate(settings: AppSettings, *, mapped_cell_count: int, registered_device_count: Optional[int] = None) -> Dict[str, Any]:
    """Explicit adapter-call estimate; SDK/internal retries are excluded."""
    devices = registered_device_count if registered_device_count is not None else len(settings.registered_devices)
    calls = {"congestion": mapped_cell_count * 60 / settings.nokia_congestion_interval_seconds, "reachability": devices * 60 / settings.nokia_reachability_interval_seconds, "location": devices * 60 / settings.nokia_location_interval_seconds}
    return {"label": "explicit HARIS adapter calls; SDK/internal retries excluded", "calls_per_minute": {k: round(v, 2) for k, v in calls.items()}, "total_calls_per_minute": round(sum(calls.values()), 2)}

class SanitizedObservationDiagnostic:
    def __init__(self, path: str = ".haris_observation_diagnostic.jsonl"): self.path = Path(path)
    def append(self, record: Dict[str, Any]) -> None:
        blocked = {"token", "authorization", "phone_number", "oauth_state", "access_token", "client_secret"}
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({k: v for k, v in record.items() if k not in blocked}, sort_keys=True) + "\n")

class ObservationStore:
    def __init__(self, client: BaseNokiaClient, settings: AppSettings, diagnostic: Optional[SanitizedObservationDiagnostic] = None, registry: Optional[Any] = None):
        self.client, self.settings, self._running, self._task = client, settings, False, None
        self._diagnostic = diagnostic
        self.registry = registry
        self._listeners: list[Callable[[Dict[str, Any]], Awaitable[None]]] = []
        self._listener_queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        self._listener_worker: Optional[asyncio.Task] = None
        self._listener_tasks: set[asyncio.Task] = set()  # compatibility view; bounded to one worker
        self._listener_overload_count = 0
        self._history = deque(maxlen=settings.nokia_observation_history_limit)
        self._locks = {name: asyncio.Lock() for name in CAPABILITIES}
        self._cycle_lock = asyncio.Lock()
        self._state = {name: {"last_attempt_at": None, "last_success_at": None, "next_due_at": 0.0, "status": "DISCONNECTED", "backoff_seconds": 0.0, "evidence": None} for name in CAPABILITIES}
    def _interval(self, name): return float(getattr(self.settings, CAPABILITIES[name][1]))
    async def _poll(self, name):
        state, lock = self._state[name], self._locks[name]
        if lock.locked(): return
        async with lock:
            now = time.time(); started = time.perf_counter(); state["last_attempt_at"] = now; method = getattr(self.client, CAPABILITIES[name][0])
            try:
                values = await asyncio.wait_for(method() if name == "congestion" else method(self.settings.registered_devices), timeout=self.settings.nokia_observation_timeout_seconds)
                state.update({"last_success_at": now, "next_due_at": now + self._interval(name), "status": "LIVE", "backoff_seconds": 0.0, "evidence": [x.model_dump() for x in values]})
                if self._diagnostic:
                    # Persist only aggregate, non-identifying evidence.  This is a
                    # diagnostic audit trail, not a copy of Nokia responses.
                    if name == "congestion":
                        evidence_summary = {"congestion_levels": sorted({str(item.congestion_level) for item in values})}
                    elif name == "reachability":
                        evidence_summary = {"reachable": sum(bool(item.reachable) for item in values), "unreachable": sum(not bool(item.reachable) for item in values)}
                    else:
                        evidence_summary = {"location_results": len(values)}
                    self._diagnostic.append({"timestamp": now, "capability": name, "status": "success", "source": self.client.name, "mode": self.settings.nac_mode, "latency_ms": round((time.perf_counter() - started) * 1000, 1), "evidence_count": len(values), "evidence_summary": evidence_summary, "last_success_at": now, "next_due_at": state["next_due_at"], "rate_limited": False})
            except asyncio.TimeoutError:
                state["status"] = "TIMEOUT"; state["backoff_seconds"] = min(max(state["backoff_seconds"] * 2, self._interval(name)), self.settings.nokia_observation_max_backoff_seconds); state["next_due_at"] = now + state["backoff_seconds"]
                if self._diagnostic: self._diagnostic.append({"timestamp": now, "capability": name, "status": "timeout", "latency_ms": round((time.perf_counter() - started) * 1000, 1), "next_due_at": state["next_due_at"], "rate_limited": False, "error_class": "TimeoutError"})
            except Exception as exc:
                state["status"] = "RATE_LIMITED" if "429" in str(exc) else "UNAVAILABLE"; state["backoff_seconds"] = min(max(state["backoff_seconds"] * 2, self._interval(name)), self.settings.nokia_observation_max_backoff_seconds); state["next_due_at"] = now + state["backoff_seconds"]
                if self._diagnostic: self._diagnostic.append({"timestamp": now, "capability": name, "status": state["status"].lower(), "latency_ms": round((time.perf_counter() - started) * 1000, 1), "next_due_at": state["next_due_at"], "rate_limited": state["status"] == "RATE_LIMITED", "error_class": type(exc).__name__})
    def _cap(self, name, now):
        state = dict(self._state[name]); state["stale"] = state["last_success_at"] is None or now - state["last_success_at"] > self._interval(name) * 2
        if state["stale"] and state["status"] == "LIVE": state["status"] = "STALE"
        return state
    def current_view(self):
        now = time.time(); caps = {name: self._cap(name, now) for name in CAPABILITIES}; statuses = {v["status"] for v in caps.values()}
        aggregate = "CONNECTED" if statuses == {"LIVE"} else "DISCONNECTED" if all(v["last_success_at"] is None for v in caps.values()) else "STALE" if all(v["status"] != "LIVE" for v in caps.values()) else "DEGRADED"
        return {"observed_at": now, "source": self.client.name, "mode": self.settings.nac_mode, "connection_status": aggregate, "capabilities": caps, "congestion": caps["congestion"]["evidence"], "devices": caps["reachability"]["evidence"], "locations": caps["location"]["evidence"], "network_state": self.registry.snapshot() if self.registry else None}
    async def poll_once(self, *, force=True):
        if self._cycle_lock.locked(): return None
        async with self._cycle_lock:
            now = time.time(); await asyncio.gather(*(self._poll(name) for name, state in self._state.items() if force or now >= state["next_due_at"])); view = self.current_view()
            if self.registry:
                self.registry.ingest(view)
            if self._listeners:
                if self._listener_worker is None or self._listener_worker.done():
                    self._listener_worker = asyncio.create_task(self._run_listener_queue())
                    self._listener_tasks = {self._listener_worker}
                    self._listener_worker.add_done_callback(self._listener_tasks.discard)
                # Coalesce presentation notifications only. Durable ingestion is
                # completed by registry.ingest above and is never silently lost.
                if self._listener_queue.full():
                    self._listener_queue.get_nowait(); self._listener_queue.task_done()
                    self._listener_overload_count += 1
                self._listener_queue.put_nowait(view)
            self._history.append(view); return view
    async def _run_listener_queue(self):
        while self._running or not self._listener_queue.empty():
            try:
                view = await asyncio.wait_for(self._listener_queue.get(), timeout=.1)
            except asyncio.TimeoutError:
                if not self._running and self._listener_queue.empty(): return
                continue
            try:
                for listener in self._listeners:
                    await listener(view)
            finally:
                self._listener_queue.task_done()
    def latest(self): return self._history[-1] if self._history else None
    def add_listener(self, listener: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
        self._listeners.append(listener)
    def latest_fresh(self):
        view = self.latest(); return view if view and view["capabilities"]["congestion"]["status"] == "LIVE" else None
    def last_successful(self): return self.latest_fresh()
    def history(self): return list(self._history)
    def status(self):
        view = self.current_view(); caps = view["capabilities"]; return {"enabled": self.settings.nokia_observation_enabled, "running": self._running, "connection_status": view["connection_status"], "source": self.client.name, "mode": self.settings.nac_mode, "capabilities": caps, "history_limit": self.settings.nokia_observation_history_limit, "last_success_at": max((item["last_success_at"] or 0 for item in caps.values()), default=0) or None, "last_attempt_at": max((item["last_attempt_at"] or 0 for item in caps.values()), default=0) or None, "next_poll_at": min((item["next_due_at"] for item in caps.values()), default=None), "interval_seconds": self.settings.nokia_observation_interval_seconds, "listener_queue_depth": self._listener_queue.qsize(), "listener_queue_capacity": self._listener_queue.maxsize, "listener_overload_count": self._listener_overload_count}
    async def start(self):
        if (external_access_policy().is_test or self._running
                or not self.settings.nokia_observation_enabled):
            return False
        self._running = True; self._task = asyncio.create_task(self._run()); return True
    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._listener_worker:
            await asyncio.gather(self._listener_worker, return_exceptions=True)
            self._listener_worker = None
    async def _run(self):
        while self._running:
            await self.poll_once(force=False); await asyncio.sleep(max(.05, min(v["next_due_at"] for v in self._state.values()) - time.time()))
