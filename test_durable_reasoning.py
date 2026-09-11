"""OFFLINE_SAFE contracts for Phase 7B durable reasoning and WARDEN."""
from __future__ import annotations

import asyncio
import copy
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from agents import HarisAgentSystem, ReasoningRouter
from config import AppSettings, Guardrails
from durable_core import (
    ActionCommand, ActionState, DurablePlatformCore, InMemoryRepositoryBundle,
    IncidentState,
)
from durable_reasoning import (
    DurableDecisionRetry, DurableIncidentDecisionService,
    build_incident_reasoning_context,
)
from event_bus import InMemoryEventBus
from memory import IncidentMemory, MemoryStore
from nokia_clients import FixtureNokiaClient
from platform_events import EventType, HarisEvent, Provenance
from runtime_events import DurableOutboxWakeupConsumer, RuntimeIngestionMetrics


NOW = 1_700_000_000.0


class NoProviderFixtureClient(FixtureNokiaClient):
    """Fixture capability metadata with provider methods as trip wires."""

    def __init__(self, settings: AppSettings):
        super().__init__(settings)
        self.provider_calls = 0

    async def _provider_forbidden(self, *_args, **_kwargs):
        self.provider_calls += 1
        raise AssertionError("Phase 7B must not invoke a provider")

    congestion_insights = _provider_forbidden
    device_status = _provider_forbidden
    location_retrieval = _provider_forbidden
    create_geofence = _provider_forbidden
    delete_geofence = _provider_forbidden
    request_qos = _provider_forbidden
    release_qos = _provider_forbidden
    attach_slice = _provider_forbidden
    detach_slice = _provider_forbidden
    rollback_network_state = _provider_forbidden


def _settings(**changes) -> AppSettings:
    values = {
        "nac_mode": "fixture",
        "nokia_observation_enabled": False,
        "enable_continuous_loop": False,
        "gemini_api_key": None,
        "groq_api_key": None,
        "supabase_url": None,
        "supabase_key": None,
        "haris_history_persistence_enabled": False,
    }
    values.update(changes)
    return AppSettings(_env_file=None, **values)


def _memory(settings: AppSettings, root: Path) -> MemoryStore:
    memory = MemoryStore(settings)
    memory.local_file = root / "memory.json"
    memory._incidents = []
    memory._policies = {}
    memory._history_loaded = True
    return memory


def _devices(tier1: int = 2, fleet: int = 4) -> list[dict]:
    rows = [
        {
            "device_id": f"critical-{index + 1}", "reachable": True,
            "roaming": False, "battery_pct": 80.0, "tier": 1,
            "cell_id": "T03",
        }
        for index in range(tier1)
    ]
    while len(rows) < fleet:
        index = len(rows) + 1
        rows.append({
            "device_id": f"bulk-{index}", "reachable": True,
            "roaming": False, "battery_pct": 70.0, "tier": 3,
            "cell_id": "T05",
        })
    return rows


def _seed(
    bundle: InMemoryRepositoryBundle, *, confidence=90, tier1: int = 2,
    fleet: int = 4, provenance: Provenance = Provenance.FIXTURE_SIMULATED,
    field_intervention: bool = False,
) -> tuple[str, dict]:
    payload = {"congestion_level": "High", "dust_advisory": True}
    if confidence is not None:
        payload["confidence_level"] = confidence
    event = HarisEvent(
        event_id="evt-phase7b", event_type=EventType.NETWORK_CONGESTION_CHANGED,
        source="fixture", source_mode="fixture", source_event_id="phase7b",
        source_timestamp=NOW, received_at=NOW + 1, created_at=NOW + 2,
        entity_type="HARIS_CONFIGURED_LOGICAL_CELL", entity_id="T03",
        correlation_key="cell:T03", provenance=provenance, payload=payload,
        trace_id="trace-phase7b",
    )
    bundle.events.append(event)
    rows = _devices(tier1=tier1, fleet=fleet)
    for cell_id in ("T03", "T05"):
        cell_devices = [row for row in rows if row["cell_id"] == cell_id]
        projection = {
            "entity_id": cell_id,
            "entity_type": "HARIS_CONFIGURED_LOGICAL_CELL",
            "provenance": provenance.value,
            "raw_congestion": "High" if cell_id == "T03" else "Low",
            "raw_congestion_evidence": copy.deepcopy(payload) if cell_id == "T03" else {
                "congestion_level": "Low", "confidence_level": 80,
            },
            "raw_congestion_observed_at": NOW,
            "reachability_summary": {"devices": cell_devices},
            "reachability_observed_at": NOW,
            "location_summary": None,
            "location_observed_at": None,
            "freshness": "FRESH", "haris_operational_state": "INCIDENT_OPEN",
            "updated_at": NOW + 2, "version": 0,
        }
        bundle.network_state.save(projection, 0)
    incident_id = "inc-phase7b"
    incident = {
        "incident_id": incident_id, "schema_version": 1,
        "correlation_key": "cell:T03", "primary_entity": "T03",
        "affected_entities": ["T03"],
        "affected_devices": [row["device_id"] for row in rows if row["cell_id"] == "T03"],
        "trigger_event_id": event.event_id, "trigger_provenance": provenance.value,
        "trigger_source_timestamp": NOW, "opened_at": NOW + 2,
        "updated_at": NOW + 2, "severity": "critical", "priority": "P1",
        "state": IncidentState.DETECTED.value, "plan_version": 0,
        "warden_decision": None, "verification_state": "PENDING",
        "recovery_state": "PENDING", "outcome": None, "closed_at": None,
        "version": 0, "trace_id": event.trace_id,
    }
    if field_intervention:
        incident.update({
            "field_intervention_required": True,
            "field_intervention_site": "T03",
            "field_intervention_skills": ["tower-inspection"],
            "field_intervention_reason": "Fixture site inspection required.",
            "field_intervention_evidence": {
                "source": "FIXTURE_SIMULATED", "kind": "site_power_demo",
            },
        })
    bundle.incidents.create_or_get_active(incident)
    row = {
        "outbox_id": "out-phase7b", "event_id": event.event_id,
        "event_type": "DURABLE_INCIDENT_READY",
        "payload": {"incident_id": incident_id}, "trace_id": event.trace_id,
        "created_at": NOW + 2,
    }
    bundle.outbox.append(row)
    return incident_id, row


class Phase7BCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.settings = _settings()
        self.client = NoProviderFixtureClient(self.settings)
        self.memory = _memory(self.settings, Path(self.temp.name))
        self.agent = HarisAgentSystem(self.client, memory=self.memory, settings=self.settings)
        self.bundle = InMemoryRepositoryBundle()
        self.metrics = RuntimeIngestionMetrics()

    def tearDown(self):
        self.temp.cleanup()

    def service(self, failure_hook=None):
        return DurableIncidentDecisionService(
            bundle=self.bundle, agent_system=self.agent, is_ready=lambda: True,
            metrics=self.metrics, failure_hook=failure_hook,
        )

    def context(self, incident_id):
        return asyncio.run(build_incident_reasoning_context(
            bundle=self.bundle, agent_system=self.agent, incident_id=incident_id,
        ))

    def run_service(self, row, failure_hook=None):
        return asyncio.run(self.service(failure_hook).handle_durable_incident_ready(row))


class DurableReasoningContextTests(Phase7BCase):
    def test_durable_incident_reload_ignores_signal_payload_authority(self):
        incident_id, row = _seed(self.bundle)
        poisoned = copy.deepcopy(row)
        poisoned["payload"].update({"confidence": 0, "actions": ["arbitrary"]})
        result = self.run_service(poisoned)
        self.assertEqual(result.incident_id, incident_id)
        self.assertEqual(result.status, "AUTHORIZED_PLAN")

    def test_bounded_context_construction(self):
        incident_id, _ = _seed(self.bundle)
        context = self.context(incident_id)
        self.assertLessEqual(len(context.devices), 64)
        self.assertLessEqual(len(context.network_state), 128)
        self.assertLessEqual(len(context.prior_memory), 3)
        self.assertEqual(context.affected_entities, ["T03"])

    def test_provenance_is_preserved(self):
        incident_id, _ = _seed(self.bundle)
        context = self.context(incident_id)
        self.assertEqual(context.provenance, "FIXTURE_SIMULATED")
        self.assertEqual(context.environmental_source, "FIXTURE")

    def test_missing_evidence_remains_unavailable_not_zero(self):
        incident_id, row = _seed(self.bundle, confidence=None)
        context = self.context(incident_id)
        self.assertFalse(context.reasoning_ready)
        self.assertIn("congestion_confidence", context.unavailable_evidence)
        self.assertIsNone(context.prediction["confidence"])
        result = self.run_service(row)
        self.assertEqual(result.status, "ESCALATED")
        self.assertEqual(result.action_count, 0)

    def test_iso_verification_timestamp_is_supported(self):
        incident_id, _ = _seed(self.bundle, field_intervention=True)
        self.bundle.verification.save({
            "verification_id": "verify-old", "incident_id": incident_id,
            "verification_type": "TRUSTED_DISPATCH", "number_verified": False,
            "recent_sim_swap": None, "verified_at": "2026-01-01T00:00:00Z",
        })
        self.bundle.verification.save({
            "verification_id": "verify-new", "incident_id": incident_id,
            "verification_type": "TRUSTED_DISPATCH", "number_verified": True,
            "recent_sim_swap": False, "verified_at": "2026-01-02T00:00:00+00:00",
        })
        context = self.context(incident_id)
        self.assertEqual(context.latest_verification["verification_id"], "verify-new")

    def test_bounded_memory_influence_is_maximum_point_zero_three(self):
        incident_id, _ = _seed(self.bundle)
        self.memory._incidents.append(IncidentMemory(
            incident_id="prior-verified", summary="network T03 storm",
            storm_type="sandstorm", peak_congestion_level="High",
            peak_confidence_level=90, affected_cells=["T03"],
            affected_devices=["critical-1"], actions=["qos"],
            executed_actions=["qos"], outcome="verified",
        ))
        context = self.context(incident_id)
        state = asyncio.run(self.agent.run_durable_reasoning(context.model_dump()))
        self.assertAlmostEqual(state["plan"]["confidence"], 0.89)
        self.assertLessEqual(len(context.prior_memory), 3)

    def test_persistence_integration_records_are_filtered(self):
        incident_id, _ = _seed(self.bundle)
        self.bundle.network_state.save({
            "entity_id": "PERSISTENCE-TEST-CELL", "entity_type": "TEST",
            "source_mode": "PERSISTENCE_INTEGRATION_TEST", "version": 0,
            "reachability_summary": {"devices": [{
                "device_id": "PERSISTENCE-TEST-DEVICE", "reachable": True,
                "tier": 1, "cell_id": "T03",
            }]},
        }, 0)
        context = self.context(incident_id)
        self.assertNotIn("PERSISTENCE-TEST-DEVICE", {d["device_id"] for d in context.devices})

    def test_context_reads_ownership_actions_verification_and_recovery(self):
        incident_id, _ = _seed(self.bundle)
        self.bundle.resource_ownership.acquire({
            "resource_key": "device:critical-1", "resource_type": "DEVICE",
            "owner_incident_id": incident_id, "acquired_at": NOW,
        })
        command = ActionCommand(
            incident_id=incident_id, command_type="QOD_PLAN",
            resource_key="device:critical-1", device_id="critical-1",
            plan_version=99, requested_at=NOW,
        )
        self.bundle.actions.create_or_get(command)
        self.bundle.verification.save({
            "verification_id": "verification-1", "incident_id": incident_id,
            "verification_type": "NETWORK_KPI", "created_at": NOW,
        })
        self.bundle.recovery.save({
            "recovery_id": "recovery-1", "incident_id": incident_id,
            "state": "PENDING",
        })
        context = self.context(incident_id)
        self.assertEqual(len(context.ownership), 1)
        self.assertEqual(len(context.nonterminal_actions), 1)
        self.assertEqual(context.recovery["recovery_id"], "recovery-1")


class DurableGraphAndWardenTests(Phase7BCase):
    def test_existing_langgraph_five_role_path_reaches_authorized_plan(self):
        incident_id, row = _seed(self.bundle)
        result = self.run_service(row)
        stages = [item["stage"] for item in result.reasoning_trace]
        self.assertEqual(result.status, "AUTHORIZED_PLAN")
        for stage in ("SENTINEL", "CARTOGRAPHER", "ACTUATOR_PLAN", "WARDEN"):
            self.assertIn(stage, stages)
        self.assertIn("provenance=FIXTURE_SIMULATED", str(result.reasoning_trace))
        self.assertEqual(self.bundle.incidents.get(incident_id)["state"], "APPROVED")

    def test_deterministic_candidate_allowlist_and_slice_not_operating(self):
        incident_id, _ = _seed(self.bundle)
        context = self.context(incident_id)
        state = asyncio.run(self.agent.run_durable_reasoning(context.model_dump()))
        self.assertTrue(all(value.startswith("candidate-") for value in state["plan"]["candidate_ids"]))
        self.assertNotIn("slice_attach", {item["kind"] for item in state["plan"]["actions"]})
        self.assertTrue(any(item["kind"] == "slice_attach" for item in state["plan"]["rejected_candidates"]))

    def test_invalid_llm_candidate_is_rejected(self):
        with self.assertRaises(ValueError):
            ReasoningRouter._validated_planner_result(
                '{"ranked_candidate_ids":["candidate-999"],"confidence_adjustment":0.0}',
                ["candidate-0"],
            )

    def test_llm_advisory_cannot_bypass_device_guard(self):
        actions = [
            {"kind": "qos", "device_id": f"d-{index}",
             "parameters": {"profile": "guaranteed", "duration_seconds": 300},
             "reason": "bounded"}
            for index in range(3)
        ]
        state = {
            "durable_planning_only": True,
            "durable_policy": {"incident_current": True, "incident_cost_total_usd": 0,
                               "cost_ceiling_usd": 5, "resource_conflicts": []},
            "plan": {"incident_id": "inc", "actions": actions, "confidence": 1.0,
                     "expected_cost_usd": 0, "expected_benefit": 1.0,
                     "blast_radius": 0.1, "approval_required": False,
                     "rationale": "advisory", "selected_device_ids": ["d-0", "d-1", "d-2"]},
            "trace": [], "events": [],
        }
        result = asyncio.run(self.agent._warden(state))
        self.assertFalse(result["warden"]["verified"])
        self.assertFalse(result["warden"]["safety_checks"]["unique_device_count_within_limit"])

    def test_confidence_threshold_escalates(self):
        self.agent.settings.guardrails.minimum_confidence = 0.90
        _incident_id, row = _seed(self.bundle)
        result = self.run_service(row)
        self.assertEqual(result.status, "ESCALATED")
        self.assertEqual(result.action_count, 0)

    def test_blast_radius_threshold_escalates(self):
        self.agent.settings.guardrails.human_approval_blast_radius = 0.40
        _incident_id, row = _seed(self.bundle)
        result = self.run_service(row)
        self.assertEqual(result.status, "ESCALATED")
        self.assertEqual(result.action_count, 0)

    def test_maximum_two_protected_devices(self):
        incident_id, _ = _seed(self.bundle, tier1=3, fleet=5)
        state = asyncio.run(self.agent.run_durable_reasoning(self.context(incident_id).model_dump()))
        self.assertEqual(len(state["plan"]["selected_device_ids"]), 2)

    def test_durable_cost_guard_blocks_over_budget(self):
        incident_id, row = _seed(self.bundle)
        self.bundle.cost_ledger.append({
            "ledger_id": "cost-existing", "incident_id": incident_id,
            "estimated_policy_cost": 4.0, "day_bucket": "2026-09-08",
        })
        result = self.run_service(row)
        self.assertEqual(result.status, "BLOCKED")
        self.assertEqual(result.action_count, 0)

    def test_resource_ownership_conflict_blocks(self):
        _incident_id, row = _seed(self.bundle)
        self.bundle.resource_ownership.acquire({
            "resource_key": "device:critical-1", "resource_type": "DEVICE",
            "owner_incident_id": "inc-other", "acquired_at": NOW,
        })
        result = self.run_service(row)
        self.assertEqual(result.status, "BLOCKED")
        self.assertEqual(result.action_count, 0)

    def test_recent_sim_swap_blocks_trusted_dispatch(self):
        incident_id, row = _seed(self.bundle, field_intervention=True)
        self.bundle.verification.save({
            "verification_id": "trust-recent-swap", "incident_id": incident_id,
            "verification_type": "TRUSTED_DISPATCH", "number_verified": True,
            "recent_sim_swap": True, "verified_at": NOW,
        })
        result = self.run_service(row)
        self.assertEqual(result.status, "BLOCKED")
        self.assertEqual(result.warden_decision, "BLOCK")

    def test_missing_trust_evidence_escalates_without_oauth(self):
        _incident_id, row = _seed(self.bundle, field_intervention=True)
        result = self.run_service(row)
        self.assertEqual(result.status, "ESCALATED")
        self.assertEqual(self.client.provider_calls, 0)

    def test_unsupported_capability_blocks(self):
        _incident_id, row = _seed(self.bundle)
        with patch.object(self.client, "action_safety_error", return_value="unsupported"):
            result = self.run_service(row)
        self.assertEqual(result.status, "BLOCKED")
        self.assertEqual(result.action_count, 0)

    def test_live_write_setting_does_not_grant_phase7b_execution(self):
        self.agent.settings.nac_mode = "live_write"
        _incident_id, row = _seed(self.bundle)
        result = self.run_service(row)
        self.assertEqual(result.status, "AUTHORIZED_PLAN")
        self.assertFalse(result.provider_execution_performed)
        self.assertEqual(self.client.provider_calls, 0)

    def test_provider_execution_is_never_called(self):
        _incident_id, row = _seed(self.bundle)
        result = self.run_service(row)
        self.assertGreater(result.action_count, 0)
        self.assertEqual(self.client.provider_calls, 0)
        for action in self.bundle.actions.pending_or_unknown():
            self.assertEqual(action.state, ActionState.READY)
            self.assertIsNone(action.provider_resource_id)


class DurableDecisionSafetyTests(Phase7BCase):
    def test_durable_action_plan_fields_and_state(self):
        incident_id, row = _seed(self.bundle)
        result = self.run_service(row)
        actions = self.bundle.actions.pending_or_unknown()
        self.assertEqual(len(actions), result.action_count)
        self.assertTrue(all(item.incident_id == incident_id for item in actions))
        self.assertTrue(all(item.plan_version == 1 for item in actions))
        self.assertTrue(all(item.preconditions["warden_decision"] == "ALLOW" for item in actions))
        self.assertTrue(all(item.preconditions["provider_execution_permitted"] is False for item in actions))

    def test_durable_action_plan_idempotency(self):
        _incident_id, row = _seed(self.bundle)
        first = self.run_service(row)
        second = self.run_service(row)
        self.assertTrue(second.duplicate)
        self.assertEqual(set(first.action_ids), set(second.action_ids))
        self.assertEqual(len(self.bundle.actions.pending_or_unknown()), first.action_count)

    def test_terminal_incident_is_skipped(self):
        incident_id, row = _seed(self.bundle)
        self.bundle.incidents.transition(
            incident_id, IncidentState.BLOCKED, actor="test", reason_code="terminal",
            trace_id="trace-phase7b", at=NOW + 3,
        )
        result = self.run_service(row)
        self.assertEqual(result.status, "TERMINAL_SKIPPED")
        self.assertEqual(result.action_count, 0)

    def test_stale_incident_change_during_reasoning_is_retryable(self):
        incident_id, row = _seed(self.bundle)

        async def stale(_context):
            self.bundle.incidents.transition(
                incident_id, IncidentState.EVALUATING, actor="other",
                reason_code="concurrent", trace_id="trace-phase7b", at=NOW + 3,
            )
            return {"plan": {}, "warden": {}, "trace": []}

        with patch.object(self.agent, "run_durable_reasoning", side_effect=stale):
            with self.assertRaises(DurableDecisionRetry):
                self.run_service(row)
        self.assertEqual(self.bundle.actions.pending_or_unknown(), [])

    def test_concurrent_decision_race_converges(self):
        _incident_id, row = _seed(self.bundle)
        service_a = self.service()
        service_b = self.service()

        async def race():
            return await asyncio.gather(
                service_a.handle_durable_incident_ready(copy.deepcopy(row)),
                service_b.handle_durable_incident_ready(copy.deepcopy(row)),
            )

        results = asyncio.run(race())
        self.assertEqual({item.status for item in results}, {"AUTHORIZED_PLAN"})
        self.assertEqual(sum(item.duplicate for item in results), 1)
        self.assertEqual(len(self.bundle.actions.pending_or_unknown()), results[0].action_count)

    def test_crash_before_plan_persistence_is_safe_to_retry(self):
        _incident_id, row = _seed(self.bundle)

        def fail(stage):
            if stage == "AFTER_REASONING_BEFORE_PLAN":
                raise RuntimeError("simulated crash")

        with self.assertRaises(RuntimeError):
            self.run_service(row, fail)
        self.assertEqual(self.bundle.actions.pending_or_unknown(), [])
        result = self.run_service(row)
        self.assertEqual(result.status, "AUTHORIZED_PLAN")

    def test_crash_after_plan_persistence_dedupes_on_retry(self):
        _incident_id, row = _seed(self.bundle)

        def fail(stage):
            if stage == "AFTER_PLAN_PERSISTENCE":
                raise RuntimeError("simulated crash")

        with self.assertRaises(RuntimeError):
            self.run_service(row, fail)
        action_ids = {item.command_id for item in self.bundle.actions.pending_or_unknown()}
        self.assertTrue(action_ids)
        result = self.run_service(row)
        self.assertEqual(set(result.action_ids), action_ids)
        self.assertEqual(len(self.bundle.actions.pending_or_unknown()), len(action_ids))

    def test_restart_with_same_repositories_does_not_duplicate_plan(self):
        _incident_id, row = _seed(self.bundle)
        first = self.run_service(row)
        restarted_agent = HarisAgentSystem(
            NoProviderFixtureClient(self.settings), memory=self.memory, settings=self.settings,
        )
        restarted = DurableIncidentDecisionService(
            bundle=self.bundle, agent_system=restarted_agent, is_ready=lambda: True,
            metrics=RuntimeIngestionMetrics(),
        )
        second = asyncio.run(restarted.handle_durable_incident_ready(row))
        self.assertTrue(second.duplicate)
        self.assertEqual(set(second.action_ids), set(first.action_ids))

    def test_duplicate_outbox_delivery_is_acknowledged_after_decision(self):
        _incident_id, _row = _seed(self.bundle)
        published = []
        consumer = DurableOutboxWakeupConsumer(
            bundle=self.bundle, event_bus=InMemoryEventBus(), is_ready=lambda: True,
            incident_ready=self.service().handle_durable_incident_ready,
            post_commit=lambda event: _append_async(published, event.event_id),
            metrics=self.metrics, owner="PHASE7B-TEST",
        )
        self.assertEqual(asyncio.run(consumer.process_once(limit=1)), 1)
        self.assertEqual(self.bundle.incidents.get("inc-phase7b")["outcome"], "AUTHORIZED_PLAN")
        self.assertEqual(published, ["evt-phase7b"])

    def test_outbox_is_not_acked_when_durable_decision_fails(self):
        _incident_id, _row = _seed(self.bundle)

        async def fail(_row):
            raise DurableDecisionRetry("retry")

        bus = InMemoryEventBus()
        consumer = DurableOutboxWakeupConsumer(
            bundle=self.bundle, event_bus=bus, is_ready=lambda: True,
            incident_ready=fail, metrics=self.metrics, owner="PHASE7B-FAIL",
        )
        self.assertEqual(asyncio.run(consumer.process_once(limit=1)), 0)
        self.assertEqual(bus.recent(), [])
        self.assertIsNone(self.bundle.outbox.unsent()[0].get("sent_at"))

    def test_postcommit_callback_observes_durable_boundary(self):
        _incident_id, _row = _seed(self.bundle)
        observed = []

        async def post_commit(_event):
            observed.append({
                "state": self.bundle.incidents.get("inc-phase7b")["state"],
                "actions": len(self.bundle.actions.pending_or_unknown()),
            })

        consumer = DurableOutboxWakeupConsumer(
            bundle=self.bundle, event_bus=InMemoryEventBus(), is_ready=lambda: True,
            incident_ready=self.service().handle_durable_incident_ready,
            post_commit=post_commit, metrics=self.metrics, owner="PHASE7B-POST",
        )
        asyncio.run(consumer.process_once(limit=1))
        self.assertEqual(observed[0]["state"], "APPROVED")
        self.assertGreater(observed[0]["actions"], 0)

    def test_sanitized_reasoning_trace_has_no_private_reasoning(self):
        _incident_id, row = _seed(self.bundle)
        result = self.run_service(row)
        rendered = str(result.reasoning_trace).lower()
        self.assertNotIn("chain_of_thought", rendered)
        self.assertNotIn("private_reasoning", rendered)
        self.assertNotIn("authorization_url", rendered)
        self.assertTrue(all(set(item) == {"stage", "message"} for item in result.reasoning_trace))

    def test_secret_fields_are_rejected_before_durable_plan(self):
        with self.assertRaises(ValueError):
            ActionCommand(
                incident_id="inc", command_type="QOD_PLAN", resource_key="device:x",
                device_id="x", plan_version=1, requested_at=NOW,
                parameters_safe={"oauth_state": "secret-value"},
            )

    def test_noc_projection_uses_authorized_plan_not_mitigating(self):
        _incident_id, row = _seed(self.bundle)
        self.run_service(row)
        core = DurablePlatformCore(
            events=self.bundle.events, network=self.bundle.network_state,
            incidents=self.bundle.incidents, actions=self.bundle.actions,
            ownership=self.bundle.resource_ownership,
            verifications=self.bundle.verification, recoveries=self.bundle.recovery,
            outbox=self.bundle.outbox, inbox=self.bundle.inbox,
            checkpoints=self.bundle.checkpoints, cost_ledger=self.bundle.cost_ledger,
        )
        core.reconstruct()
        snapshot = core.snapshot()
        self.assertEqual(snapshot["active_incidents"][0]["outcome"], "AUTHORIZED_PLAN")
        self.assertEqual(snapshot["active_incidents"][0]["state"], "APPROVED")
        self.assertTrue(all(item["state"] == ActionState.READY for item in snapshot["actions"]))
        self.assertEqual(self.agent.current_cycle_status["decision_status"], "AUTHORIZED_PLAN")

    def test_api_snapshot_exposes_only_postcommit_bounded_decision(self):
        import run_api

        _incident_id, row = _seed(self.bundle)
        previous = {
            "_durable_core": run_api._durable_core,
            "_repository_bundle": run_api._repository_bundle,
            "_system": run_api._system,
        }
        try:
            core = DurablePlatformCore(
                events=self.bundle.events, network=self.bundle.network_state,
                incidents=self.bundle.incidents, actions=self.bundle.actions,
                ownership=self.bundle.resource_ownership,
                verifications=self.bundle.verification, recoveries=self.bundle.recovery,
                outbox=self.bundle.outbox, inbox=self.bundle.inbox,
                checkpoints=self.bundle.checkpoints, cost_ledger=self.bundle.cost_ledger,
            )
            core.reconstruct()
            run_api._durable_core = core
            run_api._repository_bundle = self.bundle
            run_api._system = self.agent
            self.assertNotIn("latest_decision", run_api._authoritative_snapshot())
            self.run_service(row)
            decision = run_api._authoritative_snapshot()["latest_decision"]
            self.assertEqual(decision["status"], "AUTHORIZED_PLAN")
            self.assertFalse(decision["provider_execution_performed"])
        finally:
            for name, value in previous.items():
                setattr(run_api, name, value)


async def _append_async(target, value):
    target.append(value)


if __name__ == "__main__":
    unittest.main()
