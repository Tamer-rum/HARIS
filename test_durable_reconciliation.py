"""OFFLINE_SAFE contracts for Phase 7D durable reconciliation."""
from __future__ import annotations

import asyncio
import copy
import unittest

from durable_core import ActionState, InMemoryRepositoryBundle, IncidentState, RepositoryUnavailable
from durable_execution import (
    ActionVerificationResult, ExistingNokiaActuatorAdapter, ProviderMutationResult,
)
from durable_reconciliation import (
    DurableActionReconciliationService, DurableReconciliationScheduler,
    reconciliation_public_view,
)
from event_bus import InMemoryEventBus
from runtime import provider_access_count, reset_provider_accesses
from runtime_events import DurableOutboxWakeupConsumer, RuntimeIngestionMetrics
from test_durable_execution import NOW, MockProvider, seed, settings
from nokia_clients import CongestionReading


class ManualClock:
    def __init__(self, value=NOW + 1): self.value = float(value)
    def __call__(self): return self.value
    def advance(self, seconds): self.value += seconds


class ReadOnlySequenceAdapter(MockProvider):
    def __init__(self, bundle, reads=None, verifications=None):
        super().__init__(bundle)
        self.reads = list(reads or ["AVAILABLE"])
        self.verifications = list(verifications or ["improved"])

    async def execute(self, action):
        self.execute_count += 1
        raise AssertionError("reconciliation must never execute a mutation")

    async def rollback(self, action, provider_resource_id):
        self.rollback_count += 1
        raise AssertionError("reconciliation must never execute rollback")

    async def reconcile(self, action):
        self.reconcile_count += 1
        value = self.reads.pop(0) if len(self.reads) > 1 else self.reads[0]
        if isinstance(value, BaseException): raise value
        if value == "MALFORMED": return {"status": "secret-provider-body"}
        if value == "NOT_FOUND":
            return ProviderMutationResult(
                outcome="FAILED", provider_resource_id=action.provider_resource_id,
                reason="provider_resource_absent", provenance=self.execution_provenance,
            )
        if value == "UNKNOWN":
            return ProviderMutationResult(
                outcome="UNKNOWN", provider_resource_id=action.provider_resource_id,
                reason="provider_reconciliation_unavailable", provenance="UNAVAILABLE",
            )
        return ProviderMutationResult(
            outcome="ACCEPTED", provider_resource_id=action.provider_resource_id,
            provider_state=value, reason="provider_resource_found",
            provenance=self.execution_provenance,
        )

    async def verify(self, action, provider_resource_id, baseline):
        self.verify_count += 1
        value = self.verifications.pop(0) if len(self.verifications) > 1 else self.verifications[0]
        if isinstance(value, BaseException): raise value
        outcome = {
            "improved": "VERIFIED_IMPROVED",
            "unchanged": "VERIFIED_NO_IMPROVEMENT",
            "degraded": "VERIFIED_DEGRADED",
            "unavailable": "VERIFICATION_UNAVAILABLE",
            "pending": "VERIFICATION_PENDING",
        }[value]
        return ActionVerificationResult(
            outcome=outcome, reason=f"mock_{value}",
            provenance=self.execution_provenance,
            mitigation_improved=value == "improved",
            evidence={"cell_id": baseline.get("cell_id"),
                      "before_level": baseline.get("congestion_level"),
                      "after_level": "Low" if value == "improved" else "High"},
        )


class ExecutionBoundaryStub:
    def __init__(self, adapter):
        self.adapter = adapter
        self.resolutions = []
    def resolve_if_verified(self, incident_id, plan_version):
        self.resolutions.append((incident_id, plan_version))


class Phase7DCase(unittest.TestCase):
    def setUp(self):
        reset_provider_accesses()
        self.bundle = InMemoryRepositoryBundle()
        self.action = seed(self.bundle, action_state=ActionState.RECONCILIATION_REQUIRED)
        current = self.bundle.actions.get(self.action.command_id)
        current.provider_resource_id = "provider-resource-1"
        self.action = self.bundle.actions.update(current, current.version)
        self.clock = ManualClock()
        self.adapter = ReadOnlySequenceAdapter(self.bundle)
        self.metrics = RuntimeIngestionMetrics()
        self.executor = ExecutionBoundaryStub(self.adapter)
        self.settings = settings(
            durable_reconciliation_max_attempts=4,
            durable_reconciliation_base_backoff_seconds=5,
            durable_reconciliation_max_backoff_seconds=20,
            durable_reconciliation_deadline_seconds=120,
            durable_reconciliation_scan_seconds=5,
        )

    def service(self, *, hook=None, ready=True, adapter=None, settings_override=None):
        chosen = adapter or self.adapter
        executor = self.executor if chosen is self.adapter else ExecutionBoundaryStub(chosen)
        return DurableActionReconciliationService(
            bundle=self.bundle, adapter=chosen, settings=settings_override or self.settings,
            execution_service=executor, is_ready=lambda: ready,
            clock=self.clock, metrics=self.metrics, failure_hook=hook,
        )

    def reconcile(self, service=None):
        return asyncio.run((service or self.service()).reconcile_action(self.action.command_id))

    def update_action(self, **changes):
        current = self.bundle.actions.get(self.action.command_id)
        for name, value in changes.items(): setattr(current, name, value)
        self.action = self.bundle.actions.update(current, current.version)
        return self.action

    def transition_incident_to_verifying(self):
        for state in (IncidentState.MITIGATING, IncidentState.VERIFYING):
            row = self.bundle.incidents.get("inc-7c")
            if row["state"] != state.value:
                self.bundle.incidents.transition(
                    "inc-7c", state, actor="TEST", reason_code="TEST",
                    trace_id="trace-7d", at=self.clock(),
                )


class ReconciliationEligibilityTests(Phase7DCase):
    def test_eligible_states(self):
        for state in (ActionState.OUTCOME_UNKNOWN, ActionState.RECONCILIATION_REQUIRED, ActionState.ACKNOWLEDGED):
            with self.subTest(state=state):
                self.update_action(state=state)
                self.assertIn(self.service().work_status(self.action.command_id).status, {"DUE", "BACKOFF"})

    def test_terminal_action_is_ignored(self):
        self.update_action(state=ActionState.SUCCESS)
        result = self.reconcile()
        self.assertEqual((result.status, result.duplicate, self.adapter.reconcile_count), ("TERMINAL", True, 0))

    def test_invalid_action_state_is_rejected(self):
        self.update_action(state=ActionState.READY)
        self.assertEqual(self.reconcile().status, "NOT_ELIGIBLE")
        self.assertEqual(self.adapter.reconcile_count, 0)

    def test_not_ready_fails_closed(self):
        with self.assertRaises(RepositoryUnavailable): self.reconcile(self.service(ready=False))

    def test_unknown_action_fails_closed(self):
        with self.assertRaises(RepositoryUnavailable):
            asyncio.run(self.service().reconcile_action("missing-action"))

    def test_authoritative_action_is_reloaded(self):
        self.update_action(provider_resource_id=None)
        result = self.reconcile()
        self.assertEqual(result.status, "STILL_UNKNOWN")
        self.assertEqual(self.adapter.reconcile_count, 0)

    def test_resume_rechecks_current_runtime_configuration(self):
        with self.assertRaises(RuntimeError):
            self.reconcile(self.service(hook=lambda stage: (_ for _ in ()).throw(RuntimeError("crash")) if stage == "AFTER_RECONCILIATION_PERSIST_BEFORE_VERIFICATION" else None))
        blocked_settings = settings(nac_mode="live_read_only")
        result = self.reconcile(self.service(settings_override=blocked_settings))
        self.assertEqual((result.status, result.reason), ("WAIT_AND_REVERIFY", "provider_read_adapter_unavailable"))
        self.assertEqual((self.adapter.reconcile_count, self.adapter.verify_count), (1, 0))

    def test_foreign_durable_resource_owner_blocks_before_provider_read(self):
        self.bundle.resource_ownership.acquire({
            "resource_key": self.action.resource_key, "resource_type": "DEVICE",
            "owner_incident_id": "other-incident", "acquired_at": self.clock(),
            "ownership_state": "OWNED", "version": 1,
        })
        result = self.reconcile()
        self.assertEqual((result.status, result.reason), ("NOT_ELIGIBLE", "resource_ownership_conflict"))
        self.assertEqual(self.adapter.reconcile_count, 0)


class QodReconciliationTests(Phase7DCase):
    def test_requested_remains_pending(self):
        self.adapter.reads = ["REQUESTED"]
        result = self.reconcile()
        self.assertEqual((result.status, result.action_state), ("WAITING_FOR_PROVIDER", "RECONCILIATION_REQUIRED"))
        self.assertEqual(self.adapter.verify_count, 0)

    def test_requested_then_available(self):
        self.adapter.reads = ["REQUESTED", "AVAILABLE"]
        first = self.reconcile(); self.clock.advance(5); second = self.reconcile()
        self.assertEqual(first.status, "WAITING_FOR_PROVIDER")
        self.assertEqual(second.status, "VERIFIED_IMPROVED")
        self.assertEqual(self.adapter.reconcile_count, 2)

    def test_provider_available_does_not_prove_recovery(self):
        self.adapter.verifications = ["unavailable"]
        result = self.reconcile()
        self.assertEqual((result.status, result.verification_state), ("WAIT_AND_REVERIFY", "INSUFFICIENT_EVIDENCE"))
        self.assertEqual(self.bundle.actions.get(self.action.command_id).state, ActionState.RECONCILIATION_REQUIRED)

    def test_delayed_reverification_gets_a_new_durable_attempt(self):
        self.adapter.verifications = ["unavailable", "improved"]
        first = self.reconcile()
        self.assertEqual(first.status, "WAIT_AND_REVERIFY")
        self.assertEqual(self.service().work_status(self.action.command_id).status, "BACKOFF")
        self.clock.advance(5)
        second = self.reconcile()
        self.assertEqual(second.status, "VERIFIED_IMPROVED")
        self.assertEqual((self.adapter.reconcile_count, self.adapter.verify_count), (1, 2))
        attempts = [r for r in self.bundle.verification.for_incident("inc-7c") if r.get("verification_type") == "PROVIDER_RECONCILIATION"]
        self.assertEqual(len(attempts), 2)

    def test_persistent_insufficient_evidence_is_bounded(self):
        self.adapter.verifications = ["unavailable"]
        for delay in (0, 5, 10, 20):
            self.clock.advance(delay)
            self.reconcile()
        result = self.reconcile()
        self.assertEqual(result.status, "ESCALATED")
        self.assertEqual((self.adapter.reconcile_count, self.adapter.verify_count), (1, 4))

    def test_improved_verification_marks_success(self):
        result = self.reconcile()
        self.assertEqual(result.status, "VERIFIED_IMPROVED")
        self.assertEqual(self.bundle.actions.get(self.action.command_id).state, ActionState.SUCCESS)
        self.assertEqual(self.executor.resolutions, [("inc-7c", 1)])

    def test_unchanged_routes_rollback_to_phase7c_outbox(self):
        self.adapter.verifications = ["unchanged"]
        result = self.reconcile()
        self.assertEqual(result.status, "RECOVERY_REQUIRED")
        self.assertEqual(self.bundle.actions.get(self.action.command_id).state, ActionState.ROLLBACK_REQUIRED)
        self.assertIn("DURABLE_ACTION_EXECUTION_READY", {row["event_type"] for row in self.bundle.outbox.unsent()})
        self.assertEqual(self.adapter.rollback_count, 0)

    def test_degraded_routes_rollback_to_phase7c_outbox(self):
        self.adapter.verifications = ["degraded"]
        result = self.reconcile()
        self.assertEqual(result.verification_state, "DEGRADED")
        self.assertEqual(self.adapter.rollback_count, 0)

    def test_insufficient_evidence_waits(self):
        self.adapter.verifications = [RuntimeError("token=SECRET")]
        result = self.reconcile()
        self.assertEqual(result.status, "WAIT_AND_REVERIFY")
        self.assertEqual(result.reason, "verification_adapter_unavailable")

    def test_geofence_existence_does_not_resolve_network_incident(self):
        self.update_action(command_type="GEOFENCE_PLAN")
        self.adapter.verifications = ["unavailable"]
        self.assertEqual(self.reconcile().status, "WAIT_AND_REVERIFY")

    def test_slice_reconciliation_does_not_retry_attachment(self):
        self.update_action(command_type="SLICE_ATTACH_PLAN")
        result = self.reconcile()
        self.assertEqual(result.status, "STILL_UNKNOWN")
        self.assertEqual((self.adapter.reconcile_count, self.adapter.execute_count), (0, 0))


class UnknownAndFailureTests(Phase7DCase):
    def test_unknown_resource_id_never_recreates(self):
        self.update_action(provider_resource_id=None)
        result = self.reconcile()
        self.assertEqual(result.reason, "provider_resource_identity_unavailable")
        self.assertEqual((self.adapter.execute_count, self.adapter.reconcile_count), (0, 0))

    def test_known_resource_not_found_is_recovery_required(self):
        self.adapter.reads = ["NOT_FOUND"]
        result = self.reconcile()
        self.assertEqual((result.status, result.action_state), ("RECOVERY_REQUIRED", "FAILED"))

    def test_malformed_provider_response_is_bounded_unknown(self):
        self.adapter.reads = ["MALFORMED"]
        result = self.reconcile()
        self.assertEqual(result.status, "STILL_UNKNOWN")
        self.assertNotIn("secret-provider-body", str(result.public()))

    def test_provider_timeout_is_sanitized(self):
        self.adapter.reads = [TimeoutError("https://provider/?token=SECRET")]
        result = self.reconcile()
        self.assertEqual(result.reason, "provider_safe_read_unavailable")
        self.assertNotIn("SECRET", str(result.public()))

    def test_provenance_mismatch_is_insufficient(self):
        self.adapter.execution_provenance = "NOKIA_LIVE"
        result = self.reconcile()
        self.assertEqual((result.status, result.provenance), ("WAIT_AND_REVERIFY", "UNAVAILABLE"))
        saved = self.bundle.verification.get(f"reverify-{self.action.command_id}-1")
        self.assertEqual(saved["reason"], "verification_provenance_mismatch")

    def test_no_fabricated_numeric_kpis(self):
        self.reconcile()
        saved = self.bundle.verification.get(f"reverify-{self.action.command_id}-1")
        text = str(saved)
        self.assertNotIn("latency_ms", text)
        self.assertNotIn("throughput", text)
        self.assertNotIn("signal_pct", text)


class BoundedPolicyTests(Phase7DCase):
    def test_deterministic_backoff(self):
        self.adapter.reads = ["REQUESTED"]
        self.reconcile()
        self.assertEqual(self.service().work_status(self.action.command_id).status, "BACKOFF")
        self.clock.advance(5)
        self.assertEqual(self.service().work_status(self.action.command_id).status, "DUE")

    def test_bounded_attempts_escalate(self):
        self.adapter.reads = ["UNKNOWN"]
        for delay in (0, 5, 10, 20):
            self.clock.advance(delay)
            self.reconcile()
        result = self.reconcile()
        self.assertEqual(result.status, "ESCALATED")
        self.assertEqual(self.adapter.reconcile_count, 4)

    def test_deadline_escalates_without_provider_read(self):
        self.clock.advance(121)
        result = self.reconcile()
        self.assertEqual(result.status, "ESCALATED")
        self.assertEqual(self.adapter.reconcile_count, 0)
        self.assertEqual(self.bundle.incidents.get("inc-7c")["outcome"], "MANUAL_RECONCILIATION_REQUIRED")

    def test_escalation_is_idempotent(self):
        self.clock.advance(121)
        first = self.reconcile(); second = self.reconcile()
        self.assertEqual((first.status, second.status), ("ESCALATED", "ESCALATED"))
        terminal = [r for r in self.bundle.verification.for_incident("inc-7c") if (r.get("result") or {}).get("terminal")]
        self.assertEqual(len(terminal), 1)

    def test_scheduler_does_not_busy_loop_or_read_provider(self):
        scheduler = DurableReconciliationScheduler(
            bundle=self.bundle, service=self.service(), is_ready=lambda: True, clock=self.clock,
        )
        self.assertEqual(scheduler.enqueue_due(), 1)
        self.assertEqual(scheduler.enqueue_due(), 0)
        self.assertEqual(self.adapter.reconcile_count, 0)


class CrashReplayTests(Phase7DCase):
    def crashing(self, stage):
        return lambda point: (_ for _ in ()).throw(RuntimeError("crash")) if point == stage else None

    def test_crash_a_before_read_safe_retry(self):
        with self.assertRaises(RuntimeError): self.reconcile(self.service(hook=self.crashing("AFTER_CLAIM_BEFORE_PROVIDER_READ")))
        self.assertEqual(self.adapter.reconcile_count, 0)
        self.assertEqual(self.reconcile().status, "VERIFIED_IMPROVED")

    def test_crash_b_after_read_may_repeat_read_but_not_mutation(self):
        with self.assertRaises(RuntimeError): self.reconcile(self.service(hook=self.crashing("AFTER_PROVIDER_READ_BEFORE_RECONCILIATION_PERSIST")))
        self.assertEqual(self.reconcile().status, "VERIFIED_IMPROVED")
        self.assertEqual((self.adapter.reconcile_count, self.adapter.execute_count), (2, 0))

    def test_crash_c_after_reconciliation_resumes_verification_without_read(self):
        with self.assertRaises(RuntimeError): self.reconcile(self.service(hook=self.crashing("AFTER_RECONCILIATION_PERSIST_BEFORE_VERIFICATION")))
        self.assertEqual(self.bundle.actions.get(self.action.command_id).state, ActionState.RECONCILIATION_REQUIRED)
        self.assertEqual(self.reconcile().status, "VERIFIED_IMPROVED")
        self.assertEqual(self.adapter.reconcile_count, 1)

    def test_crash_d_after_verification_is_idempotent(self):
        with self.assertRaises(RuntimeError): self.reconcile(self.service(hook=self.crashing("AFTER_VERIFICATION_PERSIST_BEFORE_ACTION_UPDATE")))
        self.assertEqual(self.reconcile().status, "VERIFIED_IMPROVED")
        records = [r for r in self.bundle.verification.for_incident("inc-7c") if r.get("verification_type") == "NETWORK_ACTION"]
        self.assertEqual(len(records), 1)
        self.assertEqual(self.adapter.reconcile_count, 1)

    def test_crash_e_durable_escalation_reconstructs(self):
        self.clock.advance(121); self.reconcile()
        view = reconciliation_public_view(self.bundle, self.settings, self.clock())
        self.assertEqual(self.bundle.incidents.get("inc-7c")["state"], "ESCALATED")
        self.assertEqual(view[0]["status"], "ESCALATED")

    def test_duplicate_delivery_after_success_does_not_read_again(self):
        self.reconcile(); result = self.reconcile()
        self.assertTrue(result.duplicate)
        self.assertEqual(self.adapter.reconcile_count, 1)

    def test_ambiguous_create_never_calls_create(self):
        self.adapter.reads = ["UNKNOWN"]
        self.reconcile(); self.clock.advance(5); self.reconcile()
        self.assertEqual((self.adapter.reconcile_count, self.adapter.execute_count), (2, 0))

    def test_rollback_ambiguity_never_calls_delete(self):
        self.adapter.reads = ["UNKNOWN"]
        self.reconcile(); self.clock.advance(5); self.reconcile()
        self.assertEqual(self.adapter.rollback_count, 0)


class DurableSchedulingTests(Phase7DCase):
    def test_restart_scanner_rediscovers_pending_action(self):
        first = DurableReconciliationScheduler(bundle=self.bundle, service=self.service(), is_ready=lambda: True, clock=self.clock)
        self.assertEqual(first.enqueue_due(), 1)
        rows = self.bundle.outbox.claim("dead-worker", lease_seconds=0)
        self.assertEqual(len(rows), 1)
        second = DurableReconciliationScheduler(bundle=self.bundle, service=self.service(), is_ready=lambda: True, clock=self.clock)
        self.assertEqual(second.enqueue_due(), 0)

    def test_two_workers_only_one_claims_work(self):
        scheduler = DurableReconciliationScheduler(bundle=self.bundle, service=self.service(), is_ready=lambda: True, clock=self.clock)
        scheduler.enqueue_due()
        first = self.bundle.outbox.claim("worker-a", limit=1, lease_seconds=30)
        second = self.bundle.outbox.claim("worker-b", limit=1, lease_seconds=30)
        self.assertEqual((len(first), len(second)), (1, 0))

    def test_outbox_ack_and_postcommit_publication(self):
        scheduler = DurableReconciliationScheduler(bundle=self.bundle, service=self.service(), is_ready=lambda: True, clock=self.clock)
        scheduler.enqueue_due(); published = []
        async def post(event): published.append(event.event_id)
        consumer = DurableOutboxWakeupConsumer(
            bundle=self.bundle, event_bus=InMemoryEventBus(), is_ready=lambda: True,
            reconciliation_ready=self.service().handle_durable_reconciliation_ready,
            post_commit=post, owner="worker-a",
        )
        self.assertEqual(asyncio.run(consumer.process_once()), 1)
        self.assertEqual(published, ["evt-7c"])
        self.assertEqual(self.bundle.outbox.unsent(), [])

    def test_lease_loss_is_counted(self):
        scheduler = DurableReconciliationScheduler(bundle=self.bundle, service=self.service(), is_ready=lambda: True, clock=self.clock)
        scheduler.enqueue_due(); metrics = RuntimeIngestionMetrics()
        original = self.bundle.outbox.ack
        self.bundle.outbox.ack = lambda *args, **kwargs: (_ for _ in ()).throw(__import__("durable_core").ResourceAlreadyOwned("lost"))
        consumer = DurableOutboxWakeupConsumer(
            bundle=self.bundle, event_bus=InMemoryEventBus(), is_ready=lambda: True,
            reconciliation_ready=self.service().handle_durable_reconciliation_ready,
            metrics=metrics, owner="worker-a",
        )
        asyncio.run(consumer.process_once())
        self.bundle.outbox.ack = original
        self.assertEqual(metrics.snapshot()["reconciliation_lease_lost"], 1)

    def test_handler_filters_wrong_event_type(self):
        with self.assertRaises(RepositoryUnavailable):
            asyncio.run(self.service().handle_durable_reconciliation_ready({"event_type": "OTHER", "payload": {"action_id": self.action.command_id}}))

    def test_noc_view_is_safe_and_structured(self):
        view = reconciliation_public_view(self.bundle, self.settings, self.clock())
        self.assertEqual(view[0]["mutation_retry_performed"], False)
        self.assertNotIn("parameters_safe", view[0])

    def test_noc_view_preserves_terminal_reconciliation_outcome(self):
        self.reconcile()
        view = reconciliation_public_view(self.bundle, self.settings, self.clock())
        self.assertEqual(view[0]["status"], "RESOLVED")
        self.assertEqual(view[0]["last_verification_outcome"], "IMPROVED")

    def test_offline_reconciliation_does_not_use_external_provider_or_llm(self):
        self.reconcile()
        self.assertEqual(provider_access_count(), 0)
        self.assertEqual(self.adapter.execute_count, 0)


class TemporalEvidenceTests(Phase7DCase):
    class FixtureReadClient:
        def __init__(self, interval_stop):
            self.state = {"qos": {"provider-resource-1": {"active": True}}}
            self.interval_stop = interval_stop
        async def congestion_insights(self, cells):
            return [CongestionReading(
                cell_id=cells[0], congestion_level="Low", confidence_level=90,
                interval_start="2026-01-01T00:05:00+00:00",
                interval_stop=self.interval_stop,
            )]

    def actual_adapter_result(self, interval_stop):
        client = self.FixtureReadClient(interval_stop)
        adapter = ExistingNokiaActuatorAdapter(client, self.settings)
        action = self.bundle.actions.get(self.action.command_id)
        baseline = {
            "cell_id": "T03", "congestion_level": "High",
            "interval_stop": "2026-01-01T00:05:00+00:00",
        }
        return asyncio.run(adapter.verify(action, action.provider_resource_id, baseline))

    def test_newer_same_entity_categorical_evidence_is_comparable(self):
        result = self.actual_adapter_result("2026-01-01T00:10:00+00:00")
        self.assertEqual(result.outcome, "VERIFIED_IMPROVED")
        self.assertEqual(result.evidence["cell_id"], "T03")

    def test_non_newer_evidence_is_insufficient(self):
        result = self.actual_adapter_result("2026-01-01T00:05:00+00:00")
        self.assertEqual((result.outcome, result.reason), ("VERIFICATION_UNAVAILABLE", "verification_interval_not_newer"))


if __name__ == "__main__":
    unittest.main()
