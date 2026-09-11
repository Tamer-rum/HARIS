"""OFFLINE_SAFE contracts for Phase 7C durable controlled actuation."""
from __future__ import annotations

import asyncio
import copy
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
from network_as_code.errors import APIError, AuthenticationException, NotFound, ServiceError

from config import AppSettings, Guardrails
from agents import HarisAgentSystem
from durable_core import (
    ActionCommand, ActionState, InMemoryRepositoryBundle, IncidentState,
    RepositoryUnavailable, ResourceAlreadyOwned,
)
from durable_execution import (
    ActionVerificationResult, DurableActionExecutionService, ExistingNokiaActuatorAdapter,
    ProviderAmbiguousOutcome, ProviderExplicitFailure, ProviderMutationResult,
)
from event_bus import InMemoryEventBus
from nokia_clients import FixtureNokiaClient
from platform_events import EventType, HarisEvent, Provenance
from platform_lifecycle import reconstruct_platform_state
from runtime import provider_access_count, reset_provider_accesses
from runtime_events import DurableOutboxWakeupConsumer, RuntimeIngestionMetrics


NOW = time.time()


def settings(mode="fixture", gate=False, **changes):
    values = {
        "nac_mode": mode, "enable_live_write_loop": gate,
        "nokia_observation_enabled": False, "enable_continuous_loop": False,
        "gemini_api_key": None, "groq_api_key": None, "supabase_url": None,
        "supabase_key": None, "haris_history_persistence_enabled": False,
    }
    values.update(changes)
    return AppSettings(_env_file=None, **values)


class MockProvider:
    execution_provenance = "FIXTURE_SIMULATED"

    def __init__(self, bundle):
        self.bundle = bundle
        self.execute_count = 0
        self.rollback_count = 0
        self.reconcile_count = 0
        self.verify_count = 0
        self.supported = True
        self.rollback_supported = True
        self.execute_mode = "success"
        self.reconcile_mode = "accepted"
        self.verify_mode = "improved"
        self.rollback_mode = "success"
        self.sent_seen = False
        self.delay = False

    def capability(self, action, context):
        slice_ok = action.command_type != "SLICE_ATTACH_PLAN" or context.get("slice_status") == "OPERATING"
        return {
            "action_type": action.command_type,
            "provider_adapter": "MockProvider",
            "mutation_supported": self.supported and slice_ok,
            "live_write_eligible": self.supported and slice_ok,
            "required_preconditions": ["CURRENT_WARDEN_ALLOW", "DURABLE_OWNERSHIP"],
            "provider_native_idempotency": False,
            "reconciliation_read_supported": True,
            "verification_supported": True,
            "rollback_supported": self.rollback_supported,
            "current_evidence_status": "SIMULATED" if self.supported else "UNAVAILABLE",
            "reason": "capability_preconditions_satisfied" if self.supported and slice_ok else (
                "slice_not_operating" if not slice_ok else "unsupported_action"
            ),
        }

    async def execute(self, action):
        self.execute_count += 1
        current = self.bundle.actions.get(action.command_id)
        self.sent_seen = bool(current and current.state is ActionState.SENT)
        if self.delay:
            await asyncio.sleep(0.01)
        if self.execute_mode == "explicit_failure":
            raise ProviderExplicitFailure("safe")
        if self.execute_mode == "ambiguous":
            raise ProviderAmbiguousOutcome("safe")
        if self.execute_mode == "failed_result":
            return ProviderMutationResult(
                outcome="FAILED", reason="provider_explicit_failure",
                provenance=self.execution_provenance,
            )
        return ProviderMutationResult(
            outcome="ACCEPTED", provider_resource_id="provider-resource-1",
            provider_state="AVAILABLE", reason="provider_request_accepted",
            provenance=self.execution_provenance,
        )

    async def reconcile(self, action):
        self.reconcile_count += 1
        if self.reconcile_mode == "accepted":
            return ProviderMutationResult(
                outcome="ACCEPTED",
                provider_resource_id=action.provider_resource_id or "provider-resource-1",
                provider_state="AVAILABLE", reason="provider_resource_found",
                provenance=self.execution_provenance,
            )
        if self.reconcile_mode == "failed":
            return ProviderMutationResult(
                outcome="FAILED", provider_resource_id=action.provider_resource_id,
                reason="provider_resource_absent", provenance=self.execution_provenance,
            )
        return ProviderMutationResult(
            outcome="UNKNOWN", provider_resource_id=action.provider_resource_id,
            reason="provider_reconciliation_unavailable", provenance="UNAVAILABLE",
        )

    async def verify(self, action, provider_resource_id, baseline):
        self.verify_count += 1
        if self.verify_mode == "error":
            raise RuntimeError("sanitized upstream failure")
        outcome = {
            "improved": "VERIFIED_IMPROVED",
            "unchanged": "VERIFIED_NO_IMPROVEMENT",
            "degraded": "VERIFIED_DEGRADED",
            "unavailable": "VERIFICATION_UNAVAILABLE",
            "pending": "VERIFICATION_PENDING",
        }[self.verify_mode]
        return ActionVerificationResult(
            outcome=outcome, reason=f"mock_{self.verify_mode}",
            provenance="FIXTURE_SIMULATED",
            mitigation_improved=self.verify_mode == "improved",
            evidence={"before_level": baseline.get("congestion_level"), "after_level": "Low"},
        )

    async def rollback(self, action, provider_resource_id):
        self.rollback_count += 1
        if self.delay:
            await asyncio.sleep(0.01)
        if self.rollback_mode == "ambiguous":
            raise ProviderAmbiguousOutcome("safe")
        if self.rollback_mode == "failure":
            raise ProviderExplicitFailure("safe")
        return ProviderMutationResult(
            outcome="ACCEPTED", provider_resource_id=provider_resource_id,
            provider_state="RELEASED", reason="rollback_accepted",
            provenance=self.execution_provenance,
        )

    async def verify_rollback(self, action, provider_resource_id):
        return ActionVerificationResult(
            outcome="VERIFIED_ROLLBACK", reason="provider_resource_inactive",
            provenance="FIXTURE_SIMULATED", evidence={"resource_inactive": True},
        )


def seed(
    bundle, *, action_state=ActionState.READY, command_type="QOD_PLAN",
    plan_version=1, incident_id="inc-7c",
):
    event = HarisEvent(
        event_id="evt-7c", event_type=EventType.NETWORK_CONGESTION_CHANGED,
        source="fixture", source_mode="fixture", source_event_id="phase-7c",
        source_timestamp=NOW, received_at=NOW, created_at=NOW,
        entity_type="HARIS_CONFIGURED_LOGICAL_CELL", entity_id="T03",
        correlation_key=f"cell:T03:{incident_id}", provenance=Provenance.FIXTURE_SIMULATED,
        payload={"congestion_level": "High", "confidence_level": 90},
        trace_id="trace-7c",
    )
    bundle.events.append(event)
    bundle.network_state.save({
        "entity_id": "T03", "entity_type": "HARIS_CONFIGURED_LOGICAL_CELL",
        "raw_congestion": "High", "slice_status": "OPERATING",
        "provenance": "FIXTURE_SIMULATED", "version": 0,
    }, 0)
    incident = {
        "incident_id": incident_id, "correlation_key": f"cell:T03:{incident_id}",
        "primary_entity": "T03", "affected_entities": ["T03"],
        "trigger_event_id": event.event_id, "trigger_provenance": "FIXTURE_SIMULATED",
        "trigger_source_timestamp": NOW, "opened_at": NOW, "updated_at": NOW,
        "severity": "critical", "priority": "P1", "state": IncidentState.APPROVED.value,
        "plan_version": plan_version, "warden_decision": "ALLOW",
        "verification_state": "PENDING", "recovery_state": "PENDING",
        "outcome": "AUTHORIZED_PLAN", "closed_at": None, "version": 0,
        "trace_id": event.trace_id,
    }
    bundle.incidents.create_or_get_active(incident)
    parameters = ({"profile": "guaranteed", "duration_seconds": 60}
                  if command_type == "QOD_PLAN" else
                  {"slice_id": "protected-existing"} if command_type == "SLICE_ATTACH_PLAN" else
                  {"polygon_id": "configured-area"})
    action = ActionCommand(
        incident_id=incident_id, command_type=command_type,
        resource_key="device:critical-1", device_id="critical-1",
        plan_version=plan_version, requested_at=NOW,
        parameters_safe=parameters,
        preconditions={
            "warden_decision": "ALLOW", "confidence": .90, "blast_radius": .10,
            "selected_device_count": 1, "expected_cost_usd": .75,
            "trusted_dispatch_required": False,
            "verification_baseline": {"cell_id": "T03", "congestion_level": "High"},
            "provenance": "FIXTURE_SIMULATED",
        },
        state=action_state,
    )
    return bundle.actions.create_or_get(action)[0]


class Phase7CCase(unittest.TestCase):
    def setUp(self):
        reset_provider_accesses()
        self.bundle = InMemoryRepositoryBundle()
        self.action = seed(self.bundle)
        self.adapter = MockProvider(self.bundle)
        self.metrics = RuntimeIngestionMetrics()
        self.settings = settings()

    def service(self, *, settings_override=None, hook=None):
        return DurableActionExecutionService(
            bundle=self.bundle, adapter=self.adapter,
            settings=settings_override or self.settings, is_ready=lambda: True,
            metrics=self.metrics, failure_hook=hook,
        )

    def run(self, result=None, *, service=None, action_id=None):
        if result is not None:
            return super().run(result)
        return asyncio.run((service or self.service()).execute_ready_action(
            action_id or self.action.command_id
        ))

    def incident_update(self, **changes):
        current = self.bundle.incidents.get("inc-7c")
        current.update(changes)
        return self.bundle.incidents.update(current, current["version"])

    def action_update(self, action=None, **changes):
        current = self.bundle.actions.get((action or self.action).command_id)
        for key, value in changes.items():
            setattr(current, key, value)
        return self.bundle.actions.update(current, current.version)


class ExecutionEligibilityTests(Phase7CCase):
    def test_legacy_graph_cannot_bypass_durable_live_write_boundary(self):
        live = settings("live_write", True)
        client = FixtureNokiaClient(live)
        system = HarisAgentSystem(client, settings=live)
        state = {
            "trace": [],
            "plan": {
                "incident_id": "inc-legacy", "actions": [{
                    "kind": "qos", "device_id": "ambulance-01",
                    "parameters": {"profile": "guaranteed", "duration_seconds": 60},
                    "reason": "bounded test candidate",
                }],
                "confidence": .9, "expected_cost_usd": .75,
                "expected_benefit": .5, "blast_radius": .1,
                "approval_required": False, "rationale": "test",
                "selected_device_ids": ["ambulance-01"],
            },
            "warden": {"required": True, "verified": True},
        }
        result = asyncio.run(system._actuator(state))
        self.assertEqual(result["execution"]["reason"], "durable_executor_required")
        self.assertFalse(result["execution"]["executed"])
        self.assertEqual(client.state["qos"], {})

    def test_execute_ready_action_and_preserve_identity(self):
        result = self.run()
        self.assertEqual(result.action_id, self.action.command_id)
        self.assertEqual(result.status, "VERIFIED")
        self.assertEqual(self.adapter.execute_count, 1)

    def test_non_ready_action_rejected(self):
        self.action_update(state=ActionState.PENDING)
        result = self.run()
        self.assertEqual(result.reason, "action_not_ready")
        self.assertEqual(self.adapter.execute_count, 0)

    def test_terminal_incident_rejected(self):
        self.bundle.incidents.transition("inc-7c", IncidentState.FAILED, actor="test", reason_code="test", trace_id="t", at=NOW)
        result = self.run()
        self.assertEqual(result.reason, "incident_not_actionable")

    def test_stale_action_authorization_is_rejected(self):
        self.action_update(requested_at=time.time() - self.settings.guardrails.rollback_seconds - 1)
        result = self.run()
        self.assertEqual(result.reason, "action_authorization_stale")
        self.assertEqual(self.adapter.execute_count, 0)

    def test_changed_warden_authority_blocks(self):
        self.incident_update(warden_decision="BLOCK")
        result = self.run()
        self.assertEqual(result.reason, "warden_authorization_not_current")
        self.assertEqual(self.adapter.execute_count, 0)

    def test_changed_action_warden_precondition_blocks(self):
        current = self.bundle.actions.get(self.action.command_id)
        current.preconditions["warden_decision"] = "BLOCK"
        self.bundle.actions.update(current, current.version)
        self.assertEqual(self.run().reason, "action_warden_precondition_invalid")

    def test_superseded_plan_blocks(self):
        self.incident_update(plan_version=2)
        self.assertEqual(self.run().reason, "action_plan_superseded")

    def test_newer_action_plan_blocks(self):
        newer = copy.deepcopy(self.action)
        newer.command_id = "newer-command"
        newer.plan_version = 2
        self.bundle.actions.create_or_get(newer)
        self.assertEqual(self.run().reason, "newer_action_plan_exists")

    def test_confidence_guard_blocks(self):
        current = self.bundle.actions.get(self.action.command_id)
        current.preconditions["confidence"] = .1
        self.bundle.actions.update(current, current.version)
        self.assertEqual(self.run().reason, "confidence_guard_failed")

    def test_blast_radius_guard_blocks(self):
        current = self.bundle.actions.get(self.action.command_id)
        current.preconditions["blast_radius"] = .9
        self.bundle.actions.update(current, current.version)
        self.assertEqual(self.run().reason, "blast_radius_guard_failed")

    def test_device_limit_guard_blocks(self):
        current = self.bundle.actions.get(self.action.command_id)
        current.preconditions["selected_device_count"] = 3
        self.bundle.actions.update(current, current.version)
        self.assertEqual(self.run().reason, "protected_device_guard_failed")

    def test_cost_guard_blocks_without_charging(self):
        strict = settings(guardrails=Guardrails(qos_spend_ceiling_usd=.5))
        result = self.run(service=self.service(settings_override=strict))
        self.assertEqual(result.reason, "cost_guard_failed")
        self.assertEqual(self.bundle.cost_ledger.incident_total("inc-7c"), 0)

    def test_live_read_only_never_mutates(self):
        result = self.run(service=self.service(settings_override=settings("live_read_only")))
        self.assertEqual(result.reason, "live_read_only_mutation_prohibited")
        self.assertEqual(self.adapter.execute_count, 0)

    def test_live_write_requires_explicit_gate(self):
        self.adapter.execution_provenance = "NOKIA_LIVE"
        result = self.run(service=self.service(settings_override=settings("live_write", False)))
        self.assertEqual(result.reason, "explicit_live_write_gate_disabled")

    def test_live_write_requires_live_adapter(self):
        result = self.run(service=self.service(settings_override=settings("live_write", True)))
        self.assertEqual(result.reason, "live_provider_adapter_not_configured")

    def test_unsupported_capability_blocks(self):
        self.adapter.supported = False
        self.assertEqual(self.run().reason, "unsupported_action")

    def test_slice_non_operating_blocks(self):
        bundle = InMemoryRepositoryBundle()
        action = seed(bundle, command_type="SLICE_ATTACH_PLAN")
        state = bundle.network_state.get("T03")
        state["slice_status"] = "AVAILABLE"
        bundle.network_state.save(state, state["version"])
        adapter = MockProvider(bundle)
        service = DurableActionExecutionService(
            bundle=bundle, adapter=adapter, settings=settings(), is_ready=lambda: True,
        )
        result = asyncio.run(service.execute_ready_action(action.command_id))
        self.assertEqual(result.reason, "slice_not_operating")
        self.assertEqual(adapter.execute_count, 0)

    def test_foreign_resource_owner_blocks(self):
        self.bundle.resource_ownership.acquire({
            "resource_key": self.action.resource_key, "resource_type": "DEVICE",
            "owner_incident_id": "other-incident", "acquired_at": NOW,
            "lease_started_at": NOW, "lease_expires_at": NOW + 300,
            "renewable": False, "adopted_by_incident": False,
        })
        self.assertEqual(self.run().reason, "resource_ownership_conflict")
        self.assertEqual(self.adapter.execute_count, 0)

    def test_trusted_dispatch_requires_fresh_server_evidence(self):
        current = self.bundle.actions.get(self.action.command_id)
        current.preconditions["trusted_dispatch_required"] = True
        self.bundle.actions.update(current, current.version)
        self.assertEqual(self.run().reason, "trusted_dispatch_guard_failed")

    def test_recent_sim_swap_blocks(self):
        current = self.bundle.actions.get(self.action.command_id)
        current.preconditions["trusted_dispatch_required"] = True
        self.bundle.actions.update(current, current.version)
        self.bundle.verification.save({
            "verification_id": "trust", "incident_id": "inc-7c",
            "verification_type": "TRUSTED_DISPATCH", "number_verified": True,
            "recent_sim_swap": True, "verified_at": time.time(),
        })
        self.assertEqual(self.run().reason, "trusted_dispatch_guard_failed")

    def test_expired_trust_evidence_blocks(self):
        current = self.bundle.actions.get(self.action.command_id)
        current.preconditions["trusted_dispatch_required"] = True
        self.bundle.actions.update(current, current.version)
        self.bundle.verification.save({
            "verification_id": "trust", "incident_id": "inc-7c",
            "verification_type": "TRUSTED_DISPATCH", "number_verified": True,
            "recent_sim_swap": False, "verified_at": time.time() - 1000,
        })
        self.assertEqual(self.run().reason, "trusted_dispatch_guard_failed")

    def test_fresh_clean_trust_evidence_allows(self):
        current = self.bundle.actions.get(self.action.command_id)
        current.preconditions["trusted_dispatch_required"] = True
        self.bundle.actions.update(current, current.version)
        self.bundle.verification.save({
            "verification_id": "trust", "incident_id": "inc-7c",
            "verification_type": "TRUSTED_DISPATCH", "number_verified": True,
            "recent_sim_swap": False, "verified_at": time.time(),
        })
        self.assertEqual(self.run().status, "VERIFIED")


class SideEffectAndVerificationTests(Phase7CCase):
    def test_existing_fixture_actuator_executes_and_verifies_categorical_improvement(self):
        current = self.bundle.actions.get(self.action.command_id)
        current.device_id = "ambulance-01"
        current.resource_key = "device:ambulance-01"
        self.bundle.actions.update(current, current.version)
        fixture_client = FixtureNokiaClient(self.settings)
        service = DurableActionExecutionService(
            bundle=self.bundle,
            adapter=ExistingNokiaActuatorAdapter(fixture_client, self.settings),
            settings=self.settings, is_ready=lambda: True,
        )
        result = asyncio.run(service.execute_ready_action(self.action.command_id))
        self.assertEqual(result.status, "VERIFIED")
        self.assertEqual(result.provider_execution_provenance, "FIXTURE_SIMULATED")

    def test_sent_is_durable_before_provider(self):
        self.run()
        self.assertTrue(self.adapter.sent_seen)

    def test_explicit_provider_failure_is_not_unknown(self):
        self.adapter.execute_mode = "explicit_failure"
        result = self.run()
        self.assertEqual(result.status, "PROVIDER_FAILED")
        self.assertEqual(result.action_state, "FAILED")

    def test_failed_provider_result_is_explicit_failure(self):
        self.adapter.execute_mode = "failed_result"
        self.assertEqual(self.run().status, "PROVIDER_FAILED")

    def test_ambiguous_provider_result_stays_unknown(self):
        self.adapter.execute_mode = "ambiguous"
        result = self.run()
        self.assertEqual(result.status, "OUTCOME_UNKNOWN")
        self.assertEqual(result.action_state, "OUTCOME_UNKNOWN")

    def test_ambiguous_redelivery_never_resends(self):
        self.adapter.execute_mode = "ambiguous"
        self.adapter.reconcile_mode = "unknown"
        self.run()
        second = self.run()
        self.assertEqual(second.status, "OUTCOME_UNKNOWN")
        self.assertEqual(self.adapter.execute_count, 1)

    def test_sent_in_live_process_is_not_resubmitted_or_reclassified(self):
        self.action_update(state=ActionState.SENT)
        result = self.run()
        self.assertEqual(result.status, "EXECUTION_CLAIMED")
        self.assertEqual(self.adapter.execute_count, 0)
        self.assertEqual(self.bundle.actions.get(self.action.command_id).state, ActionState.SENT)

    def test_restart_marks_sent_unknown_without_provider_call(self):
        self.action_update(state=ActionState.SENT)
        reconstruct_platform_state(self.bundle)
        self.adapter.reconcile_mode = "unknown"
        result = self.run()
        self.assertEqual(result.status, "OUTCOME_UNKNOWN")
        self.assertEqual(self.adapter.execute_count, 0)

    def test_reconciliation_can_classify_known_resource(self):
        self.action_update(state=ActionState.OUTCOME_UNKNOWN, provider_resource_id="known")
        result = self.run()
        self.assertEqual(result.status, "VERIFIED")
        self.assertEqual(self.adapter.execute_count, 0)

    def test_reconciliation_can_classify_definite_failure(self):
        self.action_update(state=ActionState.OUTCOME_UNKNOWN)
        self.adapter.reconcile_mode = "failed"
        self.assertEqual(self.run().status, "PROVIDER_FAILED")

    def test_http_acceptance_alone_does_not_resolve(self):
        self.adapter.verify_mode = "unavailable"
        result = self.run()
        self.assertEqual(result.status, "VERIFICATION_UNAVAILABLE")
        self.assertNotEqual(self.bundle.incidents.get("inc-7c")["state"], "RESOLVED")

    def test_verification_improvement_resolves(self):
        result = self.run()
        self.assertEqual(result.verification_state, "IMPROVED")
        self.assertEqual(self.bundle.incidents.get("inc-7c")["state"], "RESOLVED")

    def test_verification_unavailable_requests_reconciliation(self):
        self.adapter.verify_mode = "unavailable"
        result = self.run()
        self.assertEqual(result.action_state, "RECONCILIATION_REQUIRED")

    def test_verification_adapter_error_is_unavailable(self):
        self.adapter.verify_mode = "error"
        result = self.run()
        self.assertEqual(result.status, "VERIFICATION_UNAVAILABLE")

    def test_no_improvement_runs_supported_rollback(self):
        self.adapter.verify_mode = "unchanged"
        result = self.run()
        self.assertEqual(result.status, "ROLLED_BACK")
        self.assertEqual(self.adapter.rollback_count, 1)

    def test_degradation_runs_supported_rollback(self):
        self.adapter.verify_mode = "degraded"
        self.assertEqual(self.run().status, "ROLLED_BACK")

    def test_unsupported_rollback_escalates(self):
        self.adapter.verify_mode = "unchanged"
        self.adapter.rollback_supported = False
        result = self.run()
        self.assertEqual(result.status, "RECOVERY_REQUIRED")
        self.assertEqual(result.rollback_state, "UNSUPPORTED")

    def test_rollback_ambiguity_is_unknown(self):
        self.adapter.verify_mode = "unchanged"
        self.adapter.rollback_mode = "ambiguous"
        result = self.run()
        self.assertEqual(result.status, "OUTCOME_UNKNOWN")

    def test_cost_is_not_double_counted(self):
        self.run()
        self.run()
        rows = self.bundle.cost_ledger.for_incident("inc-7c")
        self.assertEqual(len(rows), 1)
        self.assertEqual(self.bundle.cost_ledger.incident_total("inc-7c"), .75)

    def test_fixture_provenance_is_truthful(self):
        result = self.run()
        self.assertEqual(result.provider_execution_provenance, "FIXTURE_SIMULATED")

    def test_capability_matrix_has_no_claim_of_native_idempotency(self):
        matrix = self.service().capability_matrix(self.action.command_id)
        self.assertFalse(matrix["provider_native_idempotency"])
        self.assertEqual(matrix["current_evidence_status"], "SIMULATED")


class ConcurrencyAndCrashTests(Phase7CCase):
    def test_two_executors_cross_provider_boundary_once(self):
        self.adapter.delay = True
        service_a, service_b = self.service(), self.service()

        async def race():
            return await asyncio.gather(
                service_a.execute_ready_action(self.action.command_id),
                service_b.execute_ready_action(self.action.command_id),
            )

        results = asyncio.run(race())
        self.assertEqual(self.adapter.execute_count, 1)
        self.assertIn("EXECUTION_CLAIMED", {item.status for item in results})

    def test_two_rollback_workers_cross_boundary_once(self):
        self.adapter.delay = True
        self.adapter.verify_mode = "unchanged"
        service_a, service_b = self.service(), self.service()

        async def race():
            return await asyncio.gather(
                service_a.execute_ready_action(self.action.command_id),
                service_b.execute_ready_action(self.action.command_id),
            )

        asyncio.run(race())
        self.assertLessEqual(self.adapter.rollback_count, 1)

    def test_crash_before_ownership_leaves_ready(self):
        def hook(stage):
            if stage == "BEFORE_OWNERSHIP":
                raise RuntimeError("crash")
        with self.assertRaises(RuntimeError):
            self.run(service=self.service(hook=hook))
        self.assertEqual(self.bundle.actions.get(self.action.command_id).state, ActionState.READY)

    def test_crash_after_ownership_before_sent_is_retryable(self):
        def hook(stage):
            if stage == "AFTER_OWNERSHIP_BEFORE_SENT":
                raise RuntimeError("crash")
        with self.assertRaises(RuntimeError):
            self.run(service=self.service(hook=hook))
        self.assertEqual(self.run().status, "VERIFIED")
        self.assertEqual(self.adapter.execute_count, 1)

    def test_crash_after_sent_before_provider_never_resends(self):
        def hook(stage):
            if stage == "AFTER_SENT_BEFORE_PROVIDER":
                raise RuntimeError("crash")
        with self.assertRaises(RuntimeError):
            self.run(service=self.service(hook=hook))
        self.assertEqual(self.adapter.execute_count, 0)
        reconstruct_platform_state(self.bundle)
        self.adapter.reconcile_mode = "unknown"
        self.run()
        self.assertEqual(self.adapter.execute_count, 0)

    def test_crash_after_provider_becomes_unknown(self):
        def hook(stage):
            if stage == "AFTER_PROVIDER_BEFORE_RESULT":
                raise RuntimeError("crash")
        result = self.run(service=self.service(hook=hook))
        self.assertEqual(result.status, "OUTCOME_UNKNOWN")
        self.assertEqual(self.adapter.execute_count, 1)

    def test_crash_after_provider_result_resumes_verification_only(self):
        def hook(stage):
            if stage == "AFTER_RESULT_BEFORE_VERIFICATION":
                raise RuntimeError("crash")
        with self.assertRaises(RuntimeError):
            self.run(service=self.service(hook=hook))
        self.assertEqual(self.bundle.actions.get(self.action.command_id).state, ActionState.ACKNOWLEDGED)
        self.assertEqual(self.run().status, "VERIFIED")
        self.assertEqual(self.adapter.execute_count, 1)

    def test_crash_after_verification_does_not_repeat_mutation(self):
        triggered = {"done": False}
        def hook(stage):
            if stage == "AFTER_VERIFICATION_BEFORE_PUBLICATION" and not triggered["done"]:
                triggered["done"] = True
                raise RuntimeError("crash")
        with self.assertRaises(RuntimeError):
            self.run(service=self.service(hook=hook))
        self.assertEqual(self.run().status, "VERIFIED")
        self.assertEqual(self.adapter.execute_count, 1)

    def test_rollback_crash_after_sent_is_not_repeated(self):
        self.adapter.verify_mode = "unchanged"
        def hook(stage):
            if stage == "AFTER_ROLLBACK_SENT_BEFORE_PROVIDER":
                raise RuntimeError("crash")
        with self.assertRaises(RuntimeError):
            self.run(service=self.service(hook=hook))
        second = self.run()
        self.assertEqual(second.status, "OUTCOME_UNKNOWN")
        self.assertEqual(self.adapter.rollback_count, 0)


class OutboxAndSecurityTests(Phase7CCase):
    def test_decision_handler_uses_only_action_ids(self):
        row = {"event_type": "DURABLE_DECISION_READY", "payload": {
            "action_ids": [self.action.command_id], "actions": [{"unsafe": "ignored"}],
        }}
        results = asyncio.run(self.service().handle_durable_decision_ready(row))
        self.assertEqual(results[0].action_id, self.action.command_id)

    def test_invalid_decision_reference_fails_closed(self):
        with self.assertRaises(Exception):
            asyncio.run(self.service().handle_durable_decision_ready({
                "event_type": "DURABLE_DECISION_READY", "payload": {"action_ids": [{}]},
            }))

    def test_outbox_ack_occurs_after_durable_execution(self):
        decision = {
            "outbox_id": "out-decision-7c", "event_id": "evt-7c",
            "event_type": "DURABLE_DECISION_READY",
            "payload": {"action_ids": [self.action.command_id]},
            "trace_id": "trace-7c", "created_at": NOW,
        }
        self.bundle.outbox.append(decision)
        consumer = DurableOutboxWakeupConsumer(
            bundle=self.bundle, event_bus=InMemoryEventBus(), is_ready=lambda: True,
            decision_ready=self.service().handle_durable_decision_ready,
            metrics=self.metrics, owner="worker-7c",
        )
        processed = asyncio.run(consumer.process_once(limit=1))
        self.assertEqual(processed, 1)
        self.assertEqual(self.bundle.actions.get(self.action.command_id).state, ActionState.SUCCESS)
        self.assertNotIn("out-decision-7c", {row["outbox_id"] for row in self.bundle.outbox.unsent()})

    def test_outbox_failure_does_not_ack_first(self):
        decision = {
            "outbox_id": "out-decision-7c", "event_id": "evt-7c",
            "event_type": "DURABLE_DECISION_READY",
            "payload": {"action_ids": [self.action.command_id]},
            "trace_id": "trace-7c", "created_at": NOW,
        }
        self.bundle.outbox.append(decision)
        async def fail(_row):
            raise RuntimeError("safe")
        consumer = DurableOutboxWakeupConsumer(
            bundle=self.bundle, event_bus=InMemoryEventBus(), is_ready=lambda: True,
            decision_ready=fail, metrics=self.metrics, owner="worker-7c",
        )
        self.assertEqual(asyncio.run(consumer.process_once(limit=1)), 0)
        self.assertIn("out-decision-7c", {row["outbox_id"] for row in self.bundle.outbox.unsent()})

    def test_outbox_redelivery_does_not_repeat_provider(self):
        row = {"event_type": "DURABLE_DECISION_READY", "payload": {"action_ids": [self.action.command_id]}}
        service = self.service()
        asyncio.run(service.handle_durable_decision_ready(row))
        asyncio.run(service.handle_durable_decision_ready(row))
        self.assertEqual(self.adapter.execute_count, 1)

    def test_safe_durable_contract_rejects_secret_fields(self):
        current = self.bundle.actions.get(self.action.command_id)
        current.parameters_safe["access_token"] = "must-not-persist"
        with self.assertRaises(ValueError):
            ActionCommand(**{**vars(current), "command_id": "secret-command"})

    def test_result_and_outbox_do_not_contain_provider_exception(self):
        self.adapter.execute_mode = "ambiguous"
        result = self.run().public()
        serialized = str(result) + str(self.bundle.outbox.unsent())
        self.assertNotIn("authorization", serialized.lower())
        self.assertNotIn("access_token", serialized.lower())

    def test_execution_does_not_construct_llm_or_real_provider(self):
        self.run()
        self.assertEqual(provider_access_count(), 0)

    def test_metrics_are_safe_aggregates(self):
        self.run()
        metrics = self.metrics.snapshot()
        self.assertEqual(metrics["execution_evaluated"], 1)
        self.assertEqual(metrics["execution_sent"], 1)
        self.assertEqual(metrics["execution_verified"], 1)
        self.assertNotIn("critical-1", str(metrics))


class ValidationCleanupBoundaryTests(unittest.TestCase):
    RUN_ID = "REAL-QOD-TEST-" + "a" * 32

    def build(self):
        bundle = InMemoryRepositoryBundle()
        action = seed(bundle, incident_id=f"{self.RUN_ID}-INCIDENT")
        adapter = MockProvider(bundle)
        service = DurableActionExecutionService(
            bundle=bundle, adapter=adapter, settings=settings(),
            is_ready=lambda: True, metrics=RuntimeIngestionMetrics(),
        )
        return bundle, action, adapter, service

    def test_validation_cleanup_reacquires_only_its_resolved_resource(self):
        bundle, action, _adapter, service = self.build()
        self.assertEqual(asyncio.run(service.execute_ready_action(action.command_id)).status, "VERIFIED")
        prepared = service.prepare_validation_cleanup(action.command_id, self.RUN_ID)
        self.assertEqual(prepared.state, ActionState.ROLLBACK_REQUIRED)
        owner = bundle.resource_ownership.get_active(prepared.resource_key)
        self.assertEqual(owner["owner_incident_id"], f"{self.RUN_ID}-INCIDENT")

    def test_validation_cleanup_rejects_wrong_run(self):
        _bundle, action, _adapter, service = self.build()
        with self.assertRaises(RepositoryUnavailable):
            service.prepare_validation_cleanup(action.command_id, "REAL-QOD-TEST-" + "b" * 32)

    def test_validation_cleanup_rejects_unknown_provider_identity(self):
        _bundle, action, _adapter, service = self.build()
        with self.assertRaises(RepositoryUnavailable):
            service.prepare_validation_cleanup(action.command_id, self.RUN_ID)

    def test_validation_cleanup_rejects_foreign_owner(self):
        bundle, action, _adapter, service = self.build()
        executed = asyncio.run(service.execute_ready_action(action.command_id))
        self.assertEqual(executed.status, "VERIFIED")
        bundle.resource_ownership.acquire({
            "resource_key": action.resource_key, "resource_type": "DEVICE",
            "owner_incident_id": "foreign-incident", "acquired_at": NOW,
            "lease_started_at": NOW, "lease_expires_at": NOW + 300,
            "renewable": False, "adopted_by_incident": False,
            "provider_resource_id": "foreign-resource",
        })
        with self.assertRaises(ResourceAlreadyOwned):
            service.prepare_validation_cleanup(action.command_id, self.RUN_ID)


class LiveQodCleanupReadbackTests(unittest.TestCase):
    def action(self):
        return ActionCommand(
            incident_id="REAL-QOD-TEST-" + "c" * 32 + "-INCIDENT",
            command_type="QOD_PLAN", resource_key="device:validation",
            device_id="validation", plan_version=1, requested_at=NOW,
            parameters_safe={"profile": "guaranteed", "duration_seconds": 60},
            preconditions={"warden_decision": "ALLOW"},
            state=ActionState.ROLLBACK_REQUIRED, provider_resource_id="provider-qod-1",
        )

    def adapter(self, getter):
        sessions = SimpleNamespace(get=getter)
        client = SimpleNamespace(client=SimpleNamespace(sessions=sessions))
        return ExistingNokiaActuatorAdapter(client, settings(mode="live_write", gate=True))

    def assert_unavailable(self, exception):
        def fail(_resource_id):
            raise exception

        result = asyncio.run(
            self.adapter(fail).verify_rollback(self.action(), "provider-qod-1")
        )
        self.assertEqual(
            (result.outcome, result.reason, result.provenance),
            (
                "VERIFICATION_UNAVAILABLE",
                "live_rollback_readback_unavailable",
                "UNAVAILABLE",
            ),
        )
        rendered = result.model_dump_json()
        self.assertNotIn("SECRET_PROVIDER_BODY", rendered)
        self.assertNotIn("provider.invalid", rendered)
        return result

    def test_not_found_live_qod_readback_proves_absence(self):
        def absent(_resource_id):
            raise NotFound("SECRET_PROVIDER_BODY")

        result = asyncio.run(
            self.adapter(absent).verify_rollback(self.action(), "provider-qod-1")
        )
        self.assertEqual(
            (result.outcome, result.reason, result.provenance),
            ("VERIFIED_ROLLBACK", "provider_resource_absent", "NOKIA_LIVE"),
        )
        self.assertEqual(result.evidence, {"resource_state": "ABSENT"})
        self.assertNotIn("SECRET_PROVIDER_BODY", result.model_dump_json())

    def test_terminal_live_qod_readback_verifies_cleanup(self):
        result = asyncio.run(self.adapter(
            lambda _resource_id: SimpleNamespace(status="TERMINATED")
        ).verify_rollback(self.action(), "provider-qod-1"))
        self.assertEqual(
            (result.outcome, result.reason, result.provenance, result.evidence),
            (
                "VERIFIED_ROLLBACK", "provider_resource_inactive", "NOKIA_LIVE",
                {"resource_state": "TERMINATED"},
            ),
        )

    def test_active_live_qod_readback_does_not_verify_cleanup(self):
        result = asyncio.run(self.adapter(
            lambda _resource_id: SimpleNamespace(status="AVAILABLE")
        ).verify_rollback(self.action(), "provider-qod-1"))
        self.assertEqual(
            (result.outcome, result.reason, result.provenance, result.evidence),
            (
                "VERIFICATION_UNAVAILABLE", "live_rollback_not_yet_terminal",
                "NOKIA_LIVE", {"resource_state": "AVAILABLE"},
            ),
        )

    def test_auth_failure_never_proves_absence(self):
        self.assert_unavailable(AuthenticationException("SECRET_PROVIDER_BODY"))

    def test_api_error_never_proves_absence(self):
        self.assert_unavailable(APIError("SECRET_PROVIDER_BODY"))

    def test_service_error_never_proves_absence(self):
        self.assert_unavailable(ServiceError("SECRET_PROVIDER_BODY"))

    def test_transport_failure_never_proves_absence(self):
        self.assert_unavailable(httpx.ConnectError("SECRET_PROVIDER_BODY provider.invalid"))

    def test_parsing_failure_never_proves_absence(self):
        self.assert_unavailable(ValueError("SECRET_PROVIDER_BODY provider.invalid"))

    def test_generic_exception_never_proves_absence(self):
        self.assert_unavailable(RuntimeError("SECRET_PROVIDER_BODY provider.invalid"))

    def test_verification_does_not_invoke_create_or_delete(self):
        def absent(_resource_id):
            raise NotFound("not found")

        request_qos = AsyncMock(side_effect=AssertionError("CREATE forbidden"))
        release_qos = AsyncMock(side_effect=AssertionError("DELETE forbidden"))
        client = SimpleNamespace(
            client=SimpleNamespace(sessions=SimpleNamespace(get=absent)),
            request_qos=request_qos,
            release_qos=release_qos,
        )
        result = asyncio.run(
            ExistingNokiaActuatorAdapter(
                client, settings(mode="live_write", gate=True),
            ).verify_rollback(self.action(), "provider-qod-1")
        )
        self.assertEqual(result.outcome, "VERIFIED_ROLLBACK")
        request_qos.assert_not_awaited()
        release_qos.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
