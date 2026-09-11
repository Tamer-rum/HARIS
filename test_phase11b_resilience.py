import asyncio
import time
import unittest

from durable_core import ActionState, InMemoryRepositoryBundle
from durable_execution import DurableActionExecutionService
from event_bus import InMemoryEventBus
from platform_events import EventType, HarisEvent, Provenance
from test_durable_execution import MockProvider, seed, settings


class Phase11BExecutionBoundaryTests(unittest.TestCase):
    def test_expired_lease_blocks_create_after_sent(self):
        bundle = InMemoryRepositoryBundle()
        action = seed(bundle)
        adapter = MockProvider(bundle)
        service = DurableActionExecutionService(
            bundle=bundle, adapter=adapter, settings=settings(), is_ready=lambda: True,
            failure_hook=lambda stage: (
                bundle.resource_ownership._items[action.resource_key].update(
                    lease_expires_at=time.time() - 1
                )
                if stage == "AFTER_SENT_BEFORE_PROVIDER" else None
            ),
        )
        result = asyncio.run(service.execute_ready_action(action.command_id))
        self.assertEqual(result.reason, "ownership_lost_before_provider")
        self.assertEqual(bundle.actions.get(action.command_id).state, ActionState.OUTCOME_UNKNOWN)
        self.assertEqual(adapter.execute_count, 0)

    def test_stale_warden_blocks_at_final_pre_provider_reload(self):
        bundle = InMemoryRepositoryBundle()
        action = seed(bundle)
        adapter = MockProvider(bundle)

        def invalidate(stage):
            if stage != "AFTER_SENT_BEFORE_PROVIDER":
                return
            incident = bundle.incidents.get(action.incident_id)
            incident["warden_decision"] = "BLOCK"
            bundle.incidents.update(incident, expected_version=incident["version"])

        service = DurableActionExecutionService(
            bundle=bundle, adapter=adapter, settings=settings(), is_ready=lambda: True,
            failure_hook=invalidate,
        )
        result = asyncio.run(service.execute_ready_action(action.command_id))
        self.assertEqual(result.reason, "execution_authority_lost_before_provider")
        self.assertEqual(bundle.actions.get(action.command_id).state, ActionState.FAILED)
        self.assertEqual(adapter.execute_count, 0)


class Phase11BPresentationBackpressureTests(unittest.TestCase):
    def test_disposable_event_notifications_are_bounded_and_recoverable(self):
        async def scenario():
            bus = InMemoryEventBus(retention=2)
            queue = bus.subscribe("slow-view")
            for index in range(3):
                event = HarisEvent(
                    event_id=f"event-{index}",
                    event_type=EventType.NETWORK_CONGESTION_CHANGED,
                    source_timestamp=float(index + 1), received_at=float(index + 1),
                    source="TEST", source_mode="fixture",
                    source_event_id=f"source-{index}",
                    provenance=Provenance.FIXTURE_SIMULATED,
                    entity_type="CELL", entity_id="T03",
                    correlation_key="cell:T03", payload={"sequence": index},
                    trace_id="trace-test",
                )
                await bus.publish(event)
            self.assertEqual(queue.qsize(), 2)
            self.assertEqual(bus.health()["coalesced_notifications"], 1)
            self.assertEqual(len(bus.recent()), 2)

        asyncio.run(scenario())


class Phase11BShutdownTests(unittest.TestCase):
    def test_shutdown_cancels_awaits_workers_and_closes_websockets(self):
        import run_api

        async def scenario():
            names = (
                "_scheduler_task", "_observation_task", "_runtime_task",
                "_reconciliation_task", "_scheduler", "_observations",
                "_runtime_consumer", "_durable_reconciliation_scheduler",
            )
            previous = {name: getattr(run_api, name) for name in names}
            previous_clients = set(run_api._websocket_clients)
            stopped = []

            async def parked():
                await asyncio.Event().wait()

            class Stopper:
                def stop(self):
                    stopped.append(True)

            class Socket:
                closed = False
                async def close(self, code):
                    self.closed = code == 1001

            tasks = [asyncio.create_task(parked()) for _ in range(4)]
            socket = Socket()
            try:
                run_api._scheduler_task, run_api._observation_task = tasks[:2]
                run_api._runtime_task, run_api._reconciliation_task = tasks[2:]
                run_api._scheduler = None
                run_api._observations = None
                run_api._runtime_consumer = Stopper()
                run_api._durable_reconciliation_scheduler = Stopper()
                run_api._websocket_clients.clear()
                run_api._websocket_clients.add(socket)
                await run_api.stop_haris_scheduler()
                self.assertTrue(all(task.done() for task in tasks))
                self.assertTrue(socket.closed)
                self.assertEqual(len(stopped), 2)
                self.assertFalse(run_api._websocket_clients)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                for name, value in previous.items():
                    setattr(run_api, name, value)
                run_api._websocket_clients.clear()
                run_api._websocket_clients.update(previous_clients)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
