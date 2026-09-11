import asyncio
import time
import unittest
from unittest.mock import patch

from durable_core import (
    DurablePlatformCore, InMemoryRepositoryBundle, RepositoryUnavailable,
    ResourceAlreadyOwned, VersionConflict,
)
from event_bus import InMemoryEventBus
from platform_events import HarisEvent, NokiaCongestionWebhook, EventType, Provenance
from postgres_persistence import MockPostgresTransport, PostgresRepositoryBundle
from runtime_events import (
    DurableOutboxWakeupConsumer, ProcessingState, RuntimeEventIngestor,
    RuntimeEventNotReady, RuntimeIngestionMetrics,
    canonical_events_from_observation, projection_availability,
)


def core_for(bundle):
    return DurablePlatformCore(
        events=bundle.events, network=bundle.network_state,
        incidents=bundle.incidents, actions=bundle.actions,
        ownership=bundle.resource_ownership,
        verifications=bundle.verification, recoveries=bundle.recovery,
        outbox=bundle.outbox, inbox=bundle.inbox,
        checkpoints=bundle.checkpoints, cost_ledger=bundle.cost_ledger,
    )


def congestion(*, source_id="source-1", timestamp=100.0, level="High", mode="live_read_only"):
    return NokiaCongestionWebhook(
        event_id=source_id, event_timestamp=timestamp, cell_id="T03",
        congestion_level=level, confidence_level=88,
    ).canonical(mode=mode)


async def durable_ready(_row):
    return {"status": "DURABLE_TEST_BOUNDARY"}


def runtime(bundle=None, *, ready=True):
    bundle = bundle or InMemoryRepositoryBundle()
    core = core_for(bundle)
    core.reconstruct()
    bus = InMemoryEventBus()
    metrics = RuntimeIngestionMetrics()
    consumer = DurableOutboxWakeupConsumer(
        bundle=bundle, event_bus=bus, is_ready=lambda: ready,
        incident_ready=durable_ready, metrics=metrics, owner="HARIS-RUNTIME-TEST",
    )
    ingestor = RuntimeEventIngestor(
        bundle=bundle, core=core, is_ready=lambda: ready,
        wakeup=consumer.wake, metrics=metrics,
    )
    return bundle, core, bus, metrics, consumer, ingestor


class RuntimeEventIngestionTests(unittest.TestCase):
    def test_duplicate_callback_delivery_is_durable_and_idempotent(self):
        bundle, _core, _bus, metrics, _consumer, ingestor = runtime()
        first = asyncio.run(ingestor.ingest_runtime_event(congestion()))
        duplicate = asyncio.run(ingestor.ingest_runtime_event(congestion()))
        self.assertEqual(first.status, "accepted")
        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual(duplicate.event_id, first.event_id)
        self.assertEqual(bundle.events.sequence(), 1)
        self.assertEqual(len(bundle.incidents.active()), 1)
        self.assertEqual(len(bundle.outbox.unsent()), 1)
        self.assertEqual(metrics.snapshot()["event_duplicate"], 1)

    def test_callback_and_reconciliation_reuse_one_active_incident(self):
        bundle, _core, _bus, metrics, _consumer, ingestor = runtime()
        callback = asyncio.run(ingestor.ingest_runtime_event(congestion(source_id="callback-1")))
        reconciliation_event = canonical_events_from_observation({
            "observed_at": 100.0, "mode": "live_read_only",
            "congestion": [{"cell_id": "T03", "congestion_level": "High", "confidence_level": 88}],
            "devices": [], "locations": [],
        })[0]
        reconciled = asyncio.run(ingestor.ingest_runtime_event(reconciliation_event))
        self.assertTrue(callback.incident_created)
        self.assertFalse(reconciled.incident_created)
        self.assertEqual(callback.incident_id, reconciled.incident_id)
        self.assertEqual(len(bundle.incidents.active()), 1)
        self.assertEqual(metrics.snapshot()["incident_reused"], 1)

    def test_stale_high_event_is_durable_but_cannot_open_incident(self):
        bundle, _core, _bus, _metrics, _consumer, ingestor = runtime()
        asyncio.run(ingestor.ingest_runtime_event(congestion(source_id="new", timestamp=200.0, level="Low")))
        stale = asyncio.run(ingestor.ingest_runtime_event(congestion(source_id="old", timestamp=100.0, level="High")))
        self.assertEqual(stale.status, "accepted")
        self.assertFalse(stale.projection_updated)
        self.assertIsNone(stale.incident_id)
        self.assertEqual(bundle.network_state.get("T03")["raw_congestion"], "Low")
        self.assertEqual(bundle.incidents.active(), [])

    def test_concurrent_duplicate_ingestion_converges(self):
        bundle, _core, _bus, _metrics, _consumer, ingestor = runtime()

        async def exercise():
            return await asyncio.gather(
                ingestor.ingest_runtime_event(congestion()),
                ingestor.ingest_runtime_event(congestion()),
            )

        results = asyncio.run(exercise())
        self.assertEqual({item.status for item in results}, {"accepted", "duplicate"})
        self.assertEqual(bundle.events.sequence(), 1)
        self.assertEqual(len(bundle.incidents.active()), 1)

    def test_persistence_failure_never_publishes_or_wakes(self):
        bundle, core, bus, metrics, _consumer, _ingestor = runtime()
        wakeups = []

        class FailedInbound:
            def process(self, *_args, **_kwargs):
                raise RepositoryUnavailable("private upstream detail")

        bundle.inbound = FailedInbound()
        ingestor = RuntimeEventIngestor(
            bundle=bundle, core=core, is_ready=lambda: True,
            wakeup=lambda: wakeups.append(True), metrics=metrics,
        )
        with self.assertRaises(RepositoryUnavailable):
            asyncio.run(ingestor.ingest_runtime_event(congestion()))
        self.assertEqual(bus.recent(), [])
        self.assertEqual(wakeups, [])
        self.assertEqual(bundle.events.sequence(), 0)

    def test_version_conflict_rereads_and_retries_once(self):
        bundle, _core, _bus, metrics, _consumer, ingestor = runtime()
        delegate = bundle.inbound

        class ConflictOnce:
            calls = 0
            def process(self, *args, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    raise VersionConflict("stale projection")
                return delegate.process(*args, **kwargs)

        wrapper = ConflictOnce()
        bundle.inbound = wrapper
        result = asyncio.run(ingestor.ingest_runtime_event(congestion()))
        self.assertEqual(result.status, "accepted")
        self.assertEqual(wrapper.calls, 2)
        self.assertEqual(metrics.snapshot()["version_conflict"], 1)

    def test_not_ready_fails_before_repository_use(self):
        bundle, core, _bus, _metrics, consumer, _ingestor = runtime(ready=False)
        with patch.object(bundle.inbound, "process", side_effect=AssertionError("must not persist")):
            ingestor = RuntimeEventIngestor(
                bundle=bundle, core=core, is_ready=lambda: False,
                wakeup=consumer.wake,
            )
            with self.assertRaises(RuntimeEventNotReady):
                asyncio.run(ingestor.ingest_runtime_event(congestion()))

    def test_invalid_truth_or_correlation_identity_is_rejected_before_persistence(self):
        bundle, _core, _bus, _metrics, _consumer, ingestor = runtime()
        invalid = congestion(mode="fixture").model_copy(update={"provenance": Provenance.NOKIA_LIVE})
        with self.assertRaisesRegex(ValueError, "cannot be classified as live"):
            asyncio.run(ingestor.ingest_runtime_event(invalid))
        mismatched = congestion().model_copy(update={"correlation_key": "cell:T99"})
        with self.assertRaisesRegex(ValueError, "invalid correlation"):
            asyncio.run(ingestor.ingest_runtime_event(mismatched))
        self.assertEqual(bundle.events.sequence(), 0)

    def test_runtime_cache_is_reconstructable_not_authoritative(self):
        bundle, core, _bus, _metrics, _consumer, ingestor = runtime()
        result = asyncio.run(ingestor.ingest_runtime_event(congestion()))
        self.assertIn("T03", core.snapshot()["network_state"])
        restarted = core_for(bundle)
        restarted.reconstruct()
        self.assertEqual(restarted.snapshot()["network_state"]["T03"]["raw_congestion"], "High")
        self.assertEqual(restarted.snapshot()["active_incidents"][0]["incident_id"], result.incident_id)

    def test_no_precommit_publish_and_outbox_drives_postcommit_publish(self):
        bundle, core, bus, metrics, consumer, _ingestor = runtime()
        delegate = bundle.inbound
        observations = []

        class InspectingInbound:
            def process(self, *args, **kwargs):
                observations.append(len(bus.recent()))
                return delegate.process(*args, **kwargs)

        bundle.inbound = InspectingInbound()
        ingestor = RuntimeEventIngestor(
            bundle=bundle, core=core, is_ready=lambda: True,
            wakeup=consumer.wake, metrics=metrics,
        )
        asyncio.run(ingestor.ingest_runtime_event(congestion()))
        self.assertEqual(observations, [0])
        self.assertEqual(bus.recent(), [])
        self.assertEqual(asyncio.run(consumer.process_once()), 1)
        self.assertEqual(len(bus.recent()), 1)
        self.assertEqual(bundle.outbox.unsent(), [])

    def test_outbox_lease_loss_stops_item_authority(self):
        metrics = RuntimeIngestionMetrics()

        class Outbox:
            def claim(self, *_args, **_kwargs):
                return [{"outbox_id": "out-1", "event_id": "evt-1", "claim_owner": "OTHER"}]
            def ack(self, *_args):
                raise AssertionError("must not ack")
            def fail(self, *_args):
                raise AssertionError("must not fail another owner")
        class Bundle:
            outbox = Outbox()
            class Events:
                def get(self, _key):
                    raise AssertionError("must not read after lease loss")
            events = Events()

        consumer = DurableOutboxWakeupConsumer(
            bundle=Bundle(), event_bus=InMemoryEventBus(),
            is_ready=lambda: True, metrics=metrics, owner="HARIS-RUNTIME-TEST",
        )
        self.assertEqual(asyncio.run(consumer.process_once()), 0)
        self.assertEqual(metrics.snapshot()["outbox_lease_lost"], 1)

    def test_acknowledgement_conflict_does_not_fail_or_ack_again(self):
        bundle, _core, bus, metrics, _consumer, ingestor = runtime()
        asyncio.run(ingestor.ingest_runtime_event(congestion()))
        original_ack = bundle.outbox.ack
        calls = []
        def lost(*_args):
            calls.append("ack")
            raise ResourceAlreadyOwned("lease lost")
        bundle.outbox.ack = lost
        consumer = DurableOutboxWakeupConsumer(
            bundle=bundle, event_bus=bus, is_ready=lambda: True,
            incident_ready=durable_ready, metrics=metrics, owner="HARIS-RUNTIME-LEASE",
        )
        self.assertEqual(asyncio.run(consumer.process_once()), 0)
        self.assertEqual(calls, ["ack"])
        bundle.outbox.ack = original_ack

    def test_projection_availability_is_explicit(self):
        self.assertEqual(projection_availability({}, now=100), "UNAVAILABLE")
        self.assertEqual(projection_availability({"T03": {}}, now=100), "UNAVAILABLE")
        self.assertEqual(projection_availability({"T03": {"raw_congestion_observed_at": 1}}, now=100, stale_after_seconds=10), "STALE")
        self.assertEqual(projection_availability({"T03": {"raw_congestion_observed_at": 95}}, now=100, stale_after_seconds=10), "CURRENT")

    def test_reconciliation_normalization_preserves_truth_model(self):
        events = canonical_events_from_observation({
            "observed_at": 123.0, "mode": "live_read_only",
            "congestion": [{"cell_id": "T03", "congestion_level": "High", "confidence_level": 91, "congestion_pct": None, "latency_ms": None}],
            "devices": [{"device_id": "asset-1", "cell_id": "T03", "reachable": True, "battery_pct": 12}],
            "locations": [{"device_id": "asset-1", "latitude": 1.5, "longitude": 2.5}],
        })
        self.assertEqual({event.provenance for event in events}, {Provenance.NOKIA_LIVE})
        rendered = str([event.payload for event in events])
        self.assertNotIn("congestion_pct", rendered)
        self.assertNotIn("latency_ms", rendered)
        self.assertNotIn("battery", rendered)
        fixture = canonical_events_from_observation({
            "observed_at": 123.0, "mode": "fixture",
            "congestion": [{"cell_id": "T03", "congestion_level": "High"}],
            "devices": [], "locations": [],
        })[0]
        self.assertEqual(fixture.provenance, Provenance.FIXTURE_SIMULATED)

    def test_cached_reconciliation_evidence_keeps_capability_timestamp(self):
        observation = {
            "observed_at": 999.0, "mode": "live_read_only",
            "capabilities": {
                "congestion": {"last_success_at": 100.0},
                "reachability": {"last_success_at": 200.0},
                "location": {"last_success_at": 300.0},
            },
            "congestion": [{"cell_id": "T03", "congestion_level": "Medium"}],
            "devices": [{"device_id": "asset-1", "cell_id": "T03", "reachable": True}],
            "locations": [{"device_id": "asset-1", "latitude": 1.0, "longitude": 2.0}],
        }
        events = canonical_events_from_observation(observation)
        timestamps = {event.event_type: event.source_timestamp for event in events}
        self.assertEqual(timestamps[EventType.NETWORK_CONGESTION_CHANGED], 100.0)
        self.assertEqual(timestamps[EventType.DEVICE_REACHABILITY_CHANGED], 200.0)
        self.assertEqual(timestamps[EventType.DEVICE_LOCATION_UPDATED], 300.0)

    def test_integration_records_stay_out_of_operational_snapshot(self):
        bundle, core, _bus, _metrics, _consumer, _ingestor = runtime()
        bundle.network_state.save({
            "entity_id": "PERSISTENCE-TEST-deadbeef", "entity_type": "TEST",
            "mapping_source": "TEST", "provenance": "FIXTURE_SIMULATED",
            "freshness": "FRESH", "haris_operational_state": "MONITORING",
            "active_incident_ids": [], "updated_at": 1.0,
        }, 0)
        core.reconstruct()
        self.assertEqual(core.snapshot()["network_state"], {})

    def test_sensitive_identity_fields_are_rejected_before_persistence(self):
        with self.assertRaises(ValueError):
            HarisEvent(
                event_type=EventType.GEOFENCE_ENTERED,
                source="nokia", source_mode="live_read_only",
                source_event_id="callback?state=raw-secret",
                source_timestamp=1.0, entity_type="AREA", entity_id="area-1",
                correlation_key="area:1", provenance=Provenance.NOKIA_LIVE,
                payload={},
            )

    def test_postgres_bundle_uses_existing_atomic_rpc(self):
        event = congestion()
        response = {
            "status": "accepted",
            "event": {"inserted": True, "event_id": event.event_id},
            "projection": None, "incident": None,
        }
        transport = MockPostgresTransport({"haris_process_inbound_event": response})
        bundle = PostgresRepositoryBundle(transport)
        result = bundle.inbound.process(
            event, projection=None, incident=None, transition=None,
            outbox={"outbox_id": f"out-{event.event_id}", "event_id": event.event_id, "event_type": "RUNTIME_VIEW_REFRESH", "payload": {}, "trace_id": event.trace_id, "created_at": event.created_at},
        )
        self.assertEqual(result["status"], "accepted")
        name, arguments = transport.calls[0]
        self.assertEqual(name, "haris_process_inbound_event")
        self.assertEqual(arguments["p_event"]["idempotency_key"], event.idempotency_key)

    def test_authenticated_api_ingestion_and_noc_snapshot_share_durable_authority(self):
        import run_api
        from config import AppSettings
        from runtime import RuntimeEnvironment

        names = (
            "_durable_core", "_repository_bundle", "_persistence_error",
            "_platform_lifecycle", "_network_state", "_incident_manager",
            "_runtime_ingestor", "_runtime_consumer", "_runtime_metrics",
            "_durable_decision_service",
        )
        previous = {name: getattr(run_api, name) for name in names}
        settings = AppSettings(
            nac_mode="fixture", fixture_dir="fixtures",
            haris_persistence_mode="memory",
            nokia_event_webhook_secret="event-secret-placeholder",
        )
        try:
            with patch.object(run_api, "get_settings", return_value=settings):
                self.assertTrue(run_api.initialize_platform(settings=settings, runtime=RuntimeEnvironment.TEST))
                result = asyncio.run(run_api.ingest_nokia_congestion(
                    NokiaCongestionWebhook(
                        event_id="api-event-1", event_timestamp=time.time(),
                        cell_id="T03", congestion_level="High", confidence_level=90,
                    ),
                    x_haris_event_secret="event-secret-placeholder",
                ))
                snapshot = asyncio.run(run_api.noc_snapshot())
            self.assertEqual(result["status"], "accepted")
            self.assertEqual(result["processing_state"], "READY_FOR_REASONING")
            self.assertEqual(snapshot["source"], "DURABLE_REPOSITORY")
            self.assertEqual(snapshot["network_state_status"], "CURRENT")
            self.assertEqual(len(snapshot["active_incidents"]), 1)
        finally:
            for name, value in previous.items():
                setattr(run_api, name, value)

    def test_metrics_and_logs_contain_no_payload_or_secret_values(self):
        _bundle, _core, _bus, metrics, _consumer, ingestor = runtime()
        with self.assertLogs("haris.runtime_events", level="INFO") as captured:
            asyncio.run(ingestor.ingest_runtime_event(congestion()))
        rendered = " ".join(captured.output) + str(metrics.snapshot())
        for forbidden in ("authorization", "token", "phone_number", "oauth_state", "T03"):
            self.assertNotIn(forbidden, rendered)


if __name__ == "__main__":
    unittest.main()
