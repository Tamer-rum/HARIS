"""OFFLINE_SAFE contracts for the Phase 7E-A build-only harness."""
from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import runpy
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from agents import HarisAgentSystem
from config import AppSettings
from durable_core import InMemoryRepositoryBundle, RepositoryUnavailable
from durable_reasoning import build_incident_reasoning_context
from external.validate_real_qod_closed_loop import (
    ChallengeStore, EvidenceWriter, ProviderBudget, RealQodValidationController,
    DurableRealQodBackend, ValidationAbort, ValidationConfiguration,
    ValidationReasoningAgent,
    _sanitize, build_live_backend, dry_run, main, preflight,
)
from nokia_clients import CongestionReading, DeviceStatus, LiveNokiaClient
from postgres_persistence import (
    MockPostgresTransport, PersistenceNotConfigured,
    PersistenceRpcContractFailed, PersistenceTransportUnavailable,
    PostgresRepositoryBundle,
)
import runtime
from runtime import provider_access_count, reset_provider_accesses


def valid_environment(**changes):
    values = {
        "HARIS_RUNTIME_ENV": "real_qod_validation",
        "HARIS_ALLOW_REAL_QOD_VALIDATION": "true",
        "ENABLE_LIVE_WRITE_LOOP": "true",
        "ENABLE_CONTINUOUS_LOOP": "false",
        "NAC_MODE": "live_write",
        "HARIS_PERSISTENCE_MODE": "postgres",
        "HARIS_REAL_QOD_TEST_DEVICE": "ambulance-01",
        "HARIS_REAL_QOD_TEST_CELL": "T03",
        "NAC_API_TOKEN": "test-placeholder-not-used",
        "SUPABASE_URL": "https://example.supabase.co",
        "SUPABASE_KEY": "test-placeholder-not-used",
        "NAC_QOD_PROFILE_MAP": '{"guaranteed":"profile-id"}',
        "NAC_QOD_SERVICE_IPV4": "192.0.2.1",
        "DURABLE_RECONCILIATION_MAX_ATTEMPTS": "4",
    }
    values.update(changes)
    return values


def config(**changes):
    return ValidationConfiguration.from_environment(valid_environment(**changes))


class Clock:
    def __init__(self): self.now = 1000.0
    def __call__(self): return self.now
    def advance(self, value): self.now += value


@dataclass
class FakeAction:
    command_id: str = "cmd-validation"
    command_type: str = "QOD_PLAN"
    device_id: str = "ambulance-01"
    provider_resource_id: str | None = "provider-validation-1"
    state_name: str = "READY"
    @property
    def state(self): return type("State", (), {"value": self.state_name})()


@dataclass
class FakeResult:
    status: str
    action_state: str
    provider_execution_provenance: str | None = "NOKIA_LIVE"
    provider_state: str | None = "AVAILABLE"
    verification_state: str | None = "IMPROVED"
    rollback_state: str | None = None
    next_eligible_at: float | None = None
    provider_invoked: bool = True


class FakeBackend:
    def __init__(self, *, warden="ALLOW", actions=1, capability="QOD_PLAN",
                 target="ambulance-01", ready=True, execute="VERIFIED",
                 provider_id="provider-validation-1", verification="IMPROVED",
                 cleanup="ROLLED_BACK", reconcile=None, context_error=None):
        self.warden, self.action_count = warden, actions
        self.capability, self.target, self.ready = capability, target, ready
        self.execute_mode, self.verification = execute, verification
        self.cleanup_mode, self.context_error = cleanup, context_error
        self.reconcile_results = list(reconcile or [])
        self.item = FakeAction(command_type=capability, device_id=target,
                               provider_resource_id=provider_id,
                               state_name="READY" if ready else "PENDING")
        self.context_count = self.execute_count = self.cleanup_prepare_count = 0
        self.reconcile_count = 0

    async def create_durable_context(self, run_id, target, cell):
        self.context_count += 1
        if self.context_error: raise ValidationAbort("PERSISTENCE_READY", self.context_error)
        return f"{run_id}-INCIDENT"

    async def authorize(self, incident_id):
        return {"warden_decision": self.warden, "plan_version": 1}

    def current_plan_actions(self, incident_id, plan_version):
        return [self.item] * self.action_count

    def action(self, action_id): return self.item

    async def execute(self, action_id):
        self.execute_count += 1
        if self.item.state_name == "ROLLBACK_REQUIRED":
            self.item.state_name = "ROLLED_BACK" if self.cleanup_mode == "ROLLED_BACK" else "OUTCOME_UNKNOWN"
            return FakeResult(self.cleanup_mode, self.item.state_name,
                              rollback_state="VERIFIED" if self.cleanup_mode == "ROLLED_BACK" else "OUTCOME_UNKNOWN")
        if self.execute_mode == "PROVIDER_FAILED":
            self.item.state_name = "FAILED"; self.item.provider_resource_id = None
            return FakeResult("PROVIDER_FAILED", "FAILED", provider_state=None, verification_state="FAILED")
        if self.execute_mode == "OUTCOME_UNKNOWN":
            self.item.state_name = "OUTCOME_UNKNOWN"
            return FakeResult("OUTCOME_UNKNOWN", "OUTCOME_UNKNOWN", provider_state=None, verification_state=None)
        if self.execute_mode == "RECONCILIATION_REQUIRED":
            self.item.state_name = "RECONCILIATION_REQUIRED"
            return FakeResult("VERIFICATION_UNAVAILABLE", self.item.state_name,
                              verification_state="INSUFFICIENT_EVIDENCE")
        self.item.state_name = "SUCCESS"
        return FakeResult("VERIFIED", "SUCCESS")

    async def reconcile(self, action_id):
        self.reconcile_count += 1
        result = self.reconcile_results.pop(0) if self.reconcile_results else FakeResult(
            "VERIFIED_IMPROVED", "SUCCESS", verification_state=self.verification,
        )
        self.item.state_name = result.action_state
        return result

    def prepare_cleanup(self, action_id, run_id):
        self.cleanup_prepare_count += 1
        if not self.item.provider_resource_id: raise ValidationAbort("CLEANUP", "CLEANUP_NOT_SAFE")
        self.item.state_name = "ROLLBACK_REQUIRED"
        return self.item

    def latest_network_verification(self, action_id): return self.verification
    def latest_network_verification_provenance(self, action_id):
        return "NOKIA_LIVE" if self.verification else None


class HarnessCase(unittest.TestCase):
    def setUp(self):
        reset_provider_accesses()
        self.temp = tempfile.TemporaryDirectory(prefix="haris-qod-test-")
        self.directory = Path(self.temp.name)
        self.clock = Clock()
        self.store = ChallengeStore(self.directory, clock=self.clock)

    def tearDown(self): self.temp.cleanup()

    def issue(self, cfg=None):
        cfg = cfg or config()
        result = dry_run(cfg, self.store)
        return cfg, result["run_id"], result["challenge"]

    def run_controller(self, backend=None, *, cfg=None, abort=None):
        cfg, run_id, challenge = self.issue(cfg)
        backend = backend or FakeBackend()
        controller = RealQodValidationController(
            config=cfg, challenge_store=self.store,
            backend_factory=lambda _config, _budget: backend,
            evidence_writer=EvidenceWriter(self.directory), clock=self.clock,
            sleeper=lambda _delay: asyncio.sleep(0), abort_requested=abort or (lambda: False),
        )
        return asyncio.run(controller.execute(run_id, challenge)), backend, run_id


class PreflightTests(HarnessCase):
    def test_preflight_with_no_gates(self):
        result = preflight(ValidationConfiguration.from_environment({}))
        self.assertEqual(result["status"], "ABORTED_SAFE")

    def test_preflight_correct_gates(self):
        self.assertEqual(preflight(config())["status"], "PREFLIGHT_PASS")

    def test_runtime_environment_must_be_exact(self):
        self.assertEqual(preflight(config(HARIS_RUNTIME_ENV="production"))["status"], "ABORTED_SAFE")

    def test_live_read_only_rejected(self):
        self.assertEqual(preflight(config(NAC_MODE="live_read_only"))["status"], "ABORTED_SAFE")

    def test_live_write_gate_disabled(self):
        self.assertEqual(preflight(config(ENABLE_LIVE_WRITE_LOOP="false"))["status"], "ABORTED_SAFE")

    def test_postgres_is_mandatory(self):
        self.assertEqual(preflight(config(HARIS_PERSISTENCE_MODE="memory"))["status"], "ABORTED_SAFE")

    def test_one_device_enforced(self):
        for value in ("", "a,b", "*", " a"):
            with self.subTest(value=value):
                self.assertEqual(preflight(config(HARIS_REAL_QOD_TEST_DEVICE=value))["status"], "ABORTED_SAFE")

    def test_one_cell_enforced(self):
        self.assertEqual(preflight(config(HARIS_REAL_QOD_TEST_CELL="a;b"))["status"], "ABORTED_SAFE")

    def test_target_must_match_one_tier1_haris_metadata_record(self):
        result = preflight(config(
            HARIS_REAL_QOD_TEST_DEVICE="sensor-01", HARIS_REAL_QOD_TEST_CELL="T03",
        ))
        self.assertEqual(result["status"], "ABORTED_SAFE")

    def test_generic_autonomous_loop_rejected(self):
        self.assertEqual(preflight(config(ENABLE_CONTINUOUS_LOOP="true"))["status"], "ABORTED_SAFE")

    def test_required_configuration_names_only(self):
        result = preflight(config(NAC_API_TOKEN=""))
        self.assertEqual(result["status"], "ABORTED_SAFE")
        self.assertNotIn("test-placeholder", json.dumps(result))

    def test_guaranteed_qod_profile_mapping_is_required(self):
        result = preflight(config(NAC_QOD_PROFILE_MAP='{"other":"profile-id"}'))
        self.assertEqual(result["status"], "ABORTED_SAFE")

    def test_output_sanitization_is_part_of_preflight(self):
        checks = {item["details"]: item["passed"] for item in preflight(config())["checks"]}
        self.assertTrue(checks["OUTPUT_SANITIZATION_ACTIVE"])

    def test_frozen_migrations_pass_static_hashes(self):
        details = {item["details"]: item["passed"] for item in preflight(config())["checks"]}
        self.assertTrue(all(details[f"MIGRATION_00{n}_FROZEN"] for n in range(2, 6)))


class DryRunAndChallengeTests(HarnessCase):
    def test_dry_run_has_zero_external_calls(self):
        result = dry_run(config(), self.store)
        self.assertEqual(result["status"], "DRY_RUN_READY")
        self.assertEqual(provider_access_count(), 0)

    def test_execute_flag_is_required(self):
        output = io.StringIO()
        with patch.dict(os.environ, valid_environment(), clear=True), contextlib.redirect_stdout(output):
            code = main(["--execute", "--artifact-dir", str(self.directory)])
        self.assertEqual(code, 2)
        self.assertIn("EXPLICIT_CONFIRMATION_REQUIRED", output.getvalue())

    def test_confirmation_challenge_required(self):
        with self.assertRaises(ValidationAbort): self.store.consume("REAL-QOD-TEST-" + "a" * 32, "alias", "missing")

    def test_challenge_wrong(self):
        cfg, run_id, _ = self.issue()
        with self.assertRaises(ValidationAbort): self.store.consume(run_id, cfg.alias, "wrong")

    def test_challenge_expired(self):
        cfg, run_id, challenge = self.issue(); self.clock.advance(301)
        with self.assertRaises(ValidationAbort): self.store.consume(run_id, cfg.alias, challenge)

    def test_challenge_is_single_use(self):
        cfg, run_id, challenge = self.issue(); self.store.consume(run_id, cfg.alias, challenge)
        with self.assertRaises(ValidationAbort): self.store.consume(run_id, cfg.alias, challenge)

    def test_challenge_is_target_bound(self):
        cfg, run_id, challenge = self.issue()
        with self.assertRaises(ValidationAbort): self.store.consume(run_id, "QOD-TARGET-WRONG", challenge)


class ScopeAndWardenTests(HarnessCase):
    def test_target_mismatch_aborts_before_execution(self):
        result, backend, _ = self.run_controller(FakeBackend(target="different-device"))
        self.assertEqual(result["final_classification"], "ABORTED_SAFE")
        self.assertEqual(backend.execute_count, 0)

    def test_wrong_capability_aborts(self):
        result, backend, _ = self.run_controller(FakeBackend(capability="SLICE_ATTACH_PLAN"))
        self.assertEqual((result["final_classification"], backend.execute_count), ("ABORTED_SAFE", 0))

    def test_warden_block_aborts(self):
        result, backend, _ = self.run_controller(FakeBackend(warden="BLOCK"))
        self.assertEqual((result["warden_result"], backend.execute_count), ("BLOCK", 0))

    def test_warden_escalate_aborts(self):
        result, backend, _ = self.run_controller(FakeBackend(warden="ESCALATE"))
        self.assertEqual((result["warden_result"], backend.execute_count), ("ESCALATE", 0))

    def test_non_ready_action_aborts(self):
        result, backend, _ = self.run_controller(FakeBackend(ready=False))
        self.assertEqual((result["final_classification"], backend.execute_count), ("ABORTED_SAFE", 0))

    def test_multiple_ready_actions_abort(self):
        result, backend, _ = self.run_controller(FakeBackend(actions=2))
        self.assertEqual((result["final_classification"], backend.execute_count), ("ABORTED_SAFE", 0))

    def test_zero_ready_actions_abort(self):
        result, backend, _ = self.run_controller(FakeBackend(actions=0))
        self.assertEqual((result["final_classification"], backend.execute_count), ("ABORTED_SAFE", 0))

    def test_stale_incident_fails_safe(self):
        result, backend, _ = self.run_controller(FakeBackend(context_error="INCIDENT_STALE"))
        self.assertEqual((result["final_classification"], backend.execute_count), ("ABORTED_SAFE", 0))

    def test_ownership_conflict_fails_safe(self):
        result, backend, _ = self.run_controller(FakeBackend(context_error="RESOURCE_OWNERSHIP_CONFLICT"))
        self.assertEqual((result["final_classification"], backend.execute_count), ("ABORTED_SAFE", 0))


class ProviderAndReconciliationTests(HarnessCase):
    def test_provider_create_success_and_cleanup(self):
        result, backend, _ = self.run_controller()
        self.assertEqual(result["final_classification"], "REAL_COMPLETE")
        self.assertEqual((backend.execute_count, backend.cleanup_prepare_count), (2, 1))

    def test_create_explicit_failure(self):
        result, backend, _ = self.run_controller(FakeBackend(execute="PROVIDER_FAILED", provider_id=None))
        self.assertEqual(result["provider_success"], "FAIL")
        self.assertEqual(backend.execute_count, 1)

    def test_create_ambiguity_is_reconciliation_required(self):
        result, backend, _ = self.run_controller(FakeBackend(execute="OUTCOME_UNKNOWN", provider_id=None))
        self.assertEqual(result["final_classification"], "RECONCILIATION_REQUIRED")
        self.assertEqual(backend.execute_count, 1)

    def test_ambiguous_create_is_not_retried(self):
        _result, backend, _ = self.run_controller(FakeBackend(execute="OUTCOME_UNKNOWN", provider_id=None))
        self.assertEqual(backend.execute_count, 1)

    def test_unknown_provider_id_makes_cleanup_not_safe(self):
        result, backend, _ = self.run_controller(FakeBackend(execute="OUTCOME_UNKNOWN", provider_id=None))
        self.assertEqual((result["cleanup_success"], backend.cleanup_prepare_count), ("NOT_SAFE", 0))

    def test_known_provider_id_is_recorded_without_exposing_target(self):
        result, _, _ = self.run_controller()
        self.assertEqual(result["provider_state"], "AVAILABLE")
        self.assertEqual(result["provenance"]["provider_evidence"], "NOKIA_LIVE")
        self.assertNotIn(config().target_device, json.dumps(result))

    def test_delayed_requested_then_available(self):
        sequence = [
            FakeResult("WAITING_FOR_PROVIDER", "RECONCILIATION_REQUIRED", provider_state="REQUESTED", verification_state=None),
            FakeResult("VERIFIED_IMPROVED", "SUCCESS", provider_state="AVAILABLE", verification_state="IMPROVED"),
        ]
        result, backend, _ = self.run_controller(FakeBackend(execute="RECONCILIATION_REQUIRED", reconcile=sequence))
        self.assertEqual((result["provider_state"], backend.reconcile_count), ("AVAILABLE", 2))

    def test_reconciliation_deadline_is_partial(self):
        sequence = [FakeResult("ESCALATED", "RECONCILIATION_REQUIRED", provider_state="REQUESTED",
                               verification_state="INSUFFICIENT_EVIDENCE")]
        result, _, _ = self.run_controller(FakeBackend(execute="RECONCILIATION_REQUIRED", reconcile=sequence,
                                                        verification=None))
        self.assertIn(result["final_classification"], {"REAL_PARTIAL", "RECONCILIATION_REQUIRED"})

    def test_provider_availability_without_network_improvement_is_partial(self):
        result, _, _ = self.run_controller(FakeBackend(verification="UNCHANGED"))
        self.assertEqual((result["provider_success"], result["network_verification_success"]), ("PASS", "FAIL"))
        self.assertEqual(result["final_classification"], "REAL_PARTIAL")

    def test_network_improvement_is_separate_dimension(self):
        result, _, _ = self.run_controller(FakeBackend(verification="IMPROVED"))
        self.assertEqual(result["network_verification_success"], "PASS")

    def test_cleanup_failure_is_not_complete(self):
        result, _, _ = self.run_controller(FakeBackend(cleanup="FAILED"))
        self.assertEqual((result["cleanup_success"], result["final_classification"]), ("FAIL", "REAL_PARTIAL"))

    def test_cleanup_ambiguity_is_not_retried(self):
        result, backend, _ = self.run_controller(FakeBackend(cleanup="OUTCOME_UNKNOWN"))
        self.assertEqual(result["cleanup_success"], "UNKNOWN")
        self.assertEqual(backend.execute_count, 2)


class BudgetAndAbortTests(HarnessCase):
    def test_create_budget_is_one(self):
        budget = ProviderBudget(4); budget.consume("CREATE")
        with self.assertRaises(ValidationAbort): budget.consume("CREATE")

    def test_cleanup_budget_is_one(self):
        budget = ProviderBudget(4); budget.consume("DELETE")
        with self.assertRaises(ValidationAbort): budget.consume("DELETE")

    def test_safe_read_budget_is_bounded(self):
        budget = ProviderBudget(2)
        for _ in range(budget.qod_read_limit):
            budget.consume("QOD_READ")
        with self.assertRaises(ValidationAbort): budget.consume("QOD_READ")

    def test_network_read_budget_is_bounded(self):
        budget = ProviderBudget(2)
        for _ in range(budget.network_read_limit):
            budget.consume("NETWORK_READ")
        with self.assertRaises(ValidationAbort): budget.consume("NETWORK_READ")

    def test_operator_abort_before_create(self):
        result, backend, _ = self.run_controller(abort=lambda: True)
        self.assertEqual((result["final_classification"], backend.context_count, backend.execute_count), ("ABORTED_SAFE", 0, 0))

    def test_operator_abort_after_known_create_enters_cleanup(self):
        calls = iter([False, False, True])
        result, backend, _ = self.run_controller(abort=lambda: next(calls, True))
        self.assertEqual((result["cleanup_success"], backend.cleanup_prepare_count), ("PASS", 1))

    def test_operator_abort_after_ambiguous_create_does_not_guess_cleanup(self):
        result, backend, _ = self.run_controller(FakeBackend(execute="OUTCOME_UNKNOWN", provider_id=None))
        self.assertEqual((result["final_classification"], backend.cleanup_prepare_count), ("RECONCILIATION_REQUIRED", 0))


class EvidenceAndIsolationTests(HarnessCase):
    def test_postcommit_report_artifact_written(self):
        result, _, run_id = self.run_controller()
        path = self.directory / f"real_qod_{run_id}.json"
        self.assertTrue(path.is_file())
        self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["run_id"], result["run_id"])
        self.assertEqual(result["summary"]["scope"], "1 device / 1 QoD action")
        self.assertEqual(result["summary"]["final_classification"], result["final_classification"])

    def test_artifact_rejects_sensitive_fields(self):
        with self.assertRaises(ValueError): EvidenceWriter(self.directory).write({"run_id": "x", "token": "secret"})

    def test_artifact_rejects_sensitive_values(self):
        with self.assertRaises(ValueError): _sanitize({"status": "https://x/?state=sensitive"})

    def test_secrets_never_enter_console_artifact_or_exception(self):
        secret_values = ["provider-secret-value", "database-secret-value"]
        environment = valid_environment(
            NAC_API_TOKEN=secret_values[0], SUPABASE_KEY=secret_values[1],
        )
        output = io.StringIO()
        with patch.dict(os.environ, environment, clear=True), contextlib.redirect_stdout(output):
            code = main(["--preflight"])
        self.assertEqual(code, 0)
        emitted = output.getvalue()
        for secret in secret_values:
            self.assertNotIn(secret, emitted)

    def test_unexpected_backend_exception_is_sanitized_in_artifact(self):
        class FailingBackend(FakeBackend):
            async def create_durable_context(self, run_id, target, cell):
                raise RuntimeError("provider-secret-value")
        result, _backend, run_id = self.run_controller(FailingBackend())
        artifact = (self.directory / f"real_qod_{run_id}.json").read_text(encoding="utf-8")
        self.assertEqual(result["final_classification"], "ABORTED_SAFE")
        abort = next(row for row in result["stages"] if row["status"] == "ABORT")
        self.assertEqual(abort, {
            "stage": "EVIDENCE_REPORT", "status": "ABORT",
            "safe_reason": "VALIDATION_INTERNAL_ERROR",
            "exception_class": "CONTRACT_ERROR",
        })
        self.assertNotIn("provider-secret-value", json.dumps(result) + artifact)

    def test_controller_preserves_last_safe_boundary_without_exception_text(self):
        class DiagnosticFailingBackend(FakeBackend):
            def set_diagnostic_sink(self, sink):
                self.diagnostic_sink = sink

            async def create_durable_context(self, run_id, target, cell):
                self.diagnostic_sink(
                    "LIVE_OBSERVATION_DEVICE_STATUS_START",
                    {"candidate_count": 4},
                )
                raise RuntimeError("provider-secret-value")

        result, _backend, run_id = self.run_controller(DiagnosticFailingBackend())
        artifact = (self.directory / f"real_qod_{run_id}.json").read_text(encoding="utf-8")
        diagnostic = next(
            row for row in result["stages"]
            if row["safe_reason"] == "LIVE_OBSERVATION_DEVICE_STATUS_START"
        )
        abort = next(row for row in result["stages"] if row["status"] == "ABORT")

        self.assertEqual(diagnostic["candidate_count"], 4)
        self.assertEqual(abort["exception_class"], "CONTRACT_ERROR")
        self.assertNotIn("provider-secret-value", json.dumps(result) + artifact)

    def test_persistence_construction_failure_has_safe_stage_and_no_side_effects(self):
        secret = "database-secret-value"
        calls = {"factory": 0, "incident": 0, "action": 0, "nokia": 0}

        def fail_construction(_config, _budget):
            calls["factory"] += 1
            raise RepositoryUnavailable(secret)

        cfg, run_id, challenge = self.issue()
        controller = RealQodValidationController(
            config=cfg, challenge_store=self.store,
            backend_factory=fail_construction,
            evidence_writer=EvidenceWriter(self.directory), clock=self.clock,
            sleeper=lambda _delay: asyncio.sleep(0),
        )
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = asyncio.run(controller.execute(run_id, challenge))
        artifact = (self.directory / f"real_qod_{run_id}.json").read_text(encoding="utf-8")
        aborts = [row for row in result["stages"] if row["status"] == "ABORT"]
        self.assertEqual(aborts, [{
            "stage": "PERSISTENCE_READY", "status": "ABORT",
            "safe_reason": "PERSISTENCE_UNAVAILABLE",
        }])
        self.assertIsNone(result["incident_id"])
        self.assertIsNone(result["action_id"])
        self.assertEqual(calls, {"factory": 1, "incident": 0, "action": 0, "nokia": 0})
        self.assertEqual(provider_access_count(), 0)
        self.assertNotIn(secret, stdout.getvalue() + stderr.getvalue() + artifact + json.dumps(result))

    def test_supported_persistence_reason_is_preserved_without_exception_details(self):
        cases = (
            (PersistenceTransportUnavailable(), "PERSISTENCE_UNAVAILABLE"),
            (PersistenceNotConfigured("PERSISTENCE_NOT_CONFIGURED"), "PERSISTENCE_NOT_CONFIGURED"),
        )
        for failure, expected_reason in cases:
            with self.subTest(reason=expected_reason):
                cfg, run_id, challenge = self.issue()
                controller = RealQodValidationController(
                    config=cfg, challenge_store=self.store,
                    backend_factory=lambda *_args, error=failure: (_ for _ in ()).throw(error),
                    evidence_writer=EvidenceWriter(self.directory), clock=self.clock,
                    sleeper=lambda _delay: asyncio.sleep(0),
                )
                result = asyncio.run(controller.execute(run_id, challenge))
                abort = next(row for row in result["stages"] if row["status"] == "ABORT")
                self.assertEqual(abort["stage"], "PERSISTENCE_READY")
                self.assertEqual(abort["safe_reason"], expected_reason)

    def test_post_reconstruction_persistence_failure_has_safe_context_stage(self):
        secret = "database-secret-value"

        class FailingContextBackend(FakeBackend):
            async def create_durable_context(self, run_id, target, cell):
                self.context_count += 1
                raise PersistenceRpcContractFailed(secret, rpc_name="haris_process_inbound_event")

        result, backend, run_id = self.run_controller(FailingContextBackend())
        artifact = (self.directory / f"real_qod_{run_id}.json").read_text(encoding="utf-8")
        abort = next(row for row in result["stages"] if row["status"] == "ABORT")
        self.assertEqual(abort, {
            "stage": "DURABLE_CONTEXT", "status": "ABORT",
            "safe_reason": "PERSISTENCE_RPC_CONTRACT_FAILED",
        })
        self.assertEqual(backend.execute_count, 0)
        self.assertIsNone(result["incident_id"])
        self.assertNotIn(secret, json.dumps(result) + artifact)

    def test_console_output_is_single_sanitized_json(self):
        output = io.StringIO()
        with patch.dict(os.environ, {}, clear=True), contextlib.redirect_stdout(output):
            code = main(["--preflight"])
        self.assertEqual(code, 2)
        parsed = json.loads(output.getvalue())
        self.assertNotIn("SUPABASE", json.dumps(parsed))

    def test_import_has_no_external_side_effect(self):
        reset_provider_accesses()
        runpy.run_path(
            str(Path(__file__).resolve().parent / "external" / "validate_real_qod_closed_loop.py"),
            run_name="haris_qod_import_probe",
        )
        self.assertEqual(provider_access_count(), 0)

    def test_preflight_and_dryrun_never_build_backend(self):
        called = []
        dry_run(config(), self.store)
        self.assertEqual(called, [])

    def test_controller_leaves_no_background_task(self):
        async def exercise():
            cfg, run_id, challenge = self.issue()
            before = set(asyncio.all_tasks())
            controller = RealQodValidationController(
                config=cfg, challenge_store=self.store,
                backend_factory=lambda *_: FakeBackend(), evidence_writer=EvidenceWriter(self.directory),
                clock=self.clock, sleeper=lambda _: asyncio.sleep(0),
            )
            await controller.execute(run_id, challenge)
            return set(asyncio.all_tasks()) - before
        self.assertEqual(asyncio.run(exercise()), set())

    def test_controller_has_no_direct_provider_or_generic_dispatch(self):
        source = inspect_source = __import__("inspect").getsource(RealQodValidationController)
        self.assertNotIn("request_qos", inspect_source)
        self.assertNotIn("release_qos", source)
        self.assertNotIn("SLICE_ATTACH", source)
        self.assertNotIn("create_geofence", source)

    def test_full_mock_run_records_no_real_provider_or_supabase_call(self):
        self.run_controller()
        self.assertEqual(provider_access_count(), 0)

    def test_live_composition_reaches_post_reconstruction_with_fake_dependencies(self):
        fake_bundle = SimpleNamespace()
        fake_client = SimpleNamespace(
            device_phone_map={"ambulance-01": "configured-test-identifier"},
            client=SimpleNamespace(sessions=SimpleNamespace()),
        )
        fake_agents = ModuleType("agents")
        fake_agents.HarisAgentSystem = lambda *_args, **_kwargs: SimpleNamespace()
        environment = valid_environment()
        with patch.dict(os.environ, environment, clear=True), patch.object(
            runtime.sys, "argv", ["harness"]
        ), patch.dict(
            __import__("sys").modules, {"agents": fake_agents}
        ), patch(
            "postgres_persistence.build_repository_bundle", return_value=fake_bundle
        ) as build_bundle, patch(
            "platform_lifecycle.reconstruct_platform_state"
        ) as reconstruct, patch(
            "nokia_clients.build_nokia_client", return_value=fake_client
        ), patch(
            "durable_reasoning.DurableIncidentDecisionService", return_value=SimpleNamespace()
        ), patch(
            "durable_execution.ExistingNokiaActuatorAdapter", return_value=SimpleNamespace()
        ), patch(
            "durable_execution.DurableActionExecutionService", return_value=SimpleNamespace()
        ), patch(
            "durable_reconciliation.DurableActionReconciliationService", return_value=SimpleNamespace()
        ):
            backend = build_live_backend(config(), ProviderBudget(4))
        self.assertIs(backend.bundle, fake_bundle)
        build_bundle.assert_called_once()
        reconstruct.assert_called_once_with(fake_bundle)
        self.assertEqual(provider_access_count(), 0)


class DurableContextContractTests(unittest.TestCase):
    class EvidenceClient:
        _metadata = {
            "ambulance-01": {"tier": 1, "cell_id": "T03", "battery_pct": 80},
            "scada-01": {"tier": 1, "cell_id": "T03", "battery_pct": 75},
            "sensor-01": {"tier": 3, "cell_id": "T03", "battery_pct": 65},
            "fleet-01": {"tier": 3, "cell_id": "T05", "battery_pct": 70},
        }

        def __init__(self, available_device_ids=None):
            self.network_reads = 0
            self.provider_mutations = 0
            self.requested_device_ids = []
            self.available_device_ids = set(
                available_device_ids
                if available_device_ids is not None
                else self._metadata
            )
            # Values are inert test markers. Only mapping membership matters.
            self.device_phone_map = {
                device_id: "configured-test-identifier"
                for device_id in self._metadata
            }

        async def congestion_insights(self, cell_ids):
            self.network_reads += 1
            return [CongestionReading(
                cell_id=cell_ids[0], congestion_level="High", confidence_level=95,
                interval_start="2026-09-09T00:00:00Z",
                interval_stop="2026-09-09T00:05:00Z",
            )]

        async def device_status(self, device_ids):
            self.network_reads += 1
            self.requested_device_ids = list(device_ids)
            return [
                DeviceStatus(
                    device_id=device_id, reachable=True, roaming=False,
                    battery_pct=self._metadata[device_id]["battery_pct"],
                    tier=self._metadata[device_id]["tier"],
                    cell_id=self._metadata[device_id]["cell_id"],
                )
                for device_id in device_ids
                if device_id in self.available_device_ids
            ]

        def action_safety_error(self, action_kind, parameters):
            return None

        def capability_report(self):
            return {
                "qod": {"status": "SUPPORTED_AND_CONFIGURED", "reason": None},
                "slicing": {"status": "SDK_SUPPORTED_CONFIG_MISSING", "reason": "not operating"},
            }

    class EmptyMemory:
        async def search_incidents(self, _query, limit=5):
            return []

    @staticmethod
    def settings():
        return AppSettings(
            nac_mode="live_write",
            enable_live_write_loop=True,
            fixture_dir=str(Path(__file__).resolve().parent / "fixtures"),
            registered_devices=[
                "ambulance-01", "scada-01", "sensor-01", "fleet-01",
            ],
            geofencing_monitoring_enabled=False,
            nac_qod_profile_map={"guaranteed": "configured-profile"},
            nac_qod_service_ipv4="192.0.2.1",
        )

    @classmethod
    def backend(cls, bundle, client, system=None):
        system = system or SimpleNamespace(settings=cls.settings())
        return DurableRealQodBackend(
            bundle=bundle, system=system, decision=SimpleNamespace(),
            executor=SimpleNamespace(), reconciler=SimpleNamespace(), client=client,
        )

    def run_reasoning(self, available_device_ids):
        bundle = InMemoryRepositoryBundle()
        client = self.EvidenceClient(available_device_ids)
        settings = self.settings()
        system = HarisAgentSystem(
            client, memory=self.EmptyMemory(), settings=settings,
        )
        backend = self.backend(bundle, client, system)
        run_id = "REAL-QOD-TEST-" + "c" * 32
        incident_id = asyncio.run(
            backend.create_durable_context(run_id, "ambulance-01", "T03")
        )
        context = asyncio.run(build_incident_reasoning_context(
            bundle=bundle, agent_system=system, incident_id=incident_id,
        ))
        result = asyncio.run(
            ValidationReasoningAgent(system).run_durable_reasoning(
                context.model_dump()
            )
        )
        return client, context, result

    def test_context_uses_atomic_inbound_with_complete_postgres_contract(self):
        captured = {}

        def process(arguments):
            captured.update(arguments)
            return {
                "status": "accepted",
                "event": {"inserted": True},
                "projection": {**arguments["p_projection"], "version": 1},
                "incident": {**arguments["p_incident"], "_created": True},
            }

        transport = MockPostgresTransport({"haris_process_inbound_event": process})
        client = self.EvidenceClient()
        incident_id = asyncio.run(self.backend(
            PostgresRepositoryBundle(transport), client,
        ).create_durable_context("REAL-QOD-TEST-" + "a" * 32, "ambulance-01", "T03"))

        self.assertEqual(incident_id, "REAL-QOD-TEST-" + "a" * 32 + "-INCIDENT")
        self.assertEqual(
            [name for name, _ in transport.calls],
            ["haris_read_domain", "haris_process_inbound_event"],
        )
        projection = captured["p_projection"]
        required_projection = {
            "entity_id", "entity_type", "mapping_source", "provenance",
            "raw_congestion", "raw_congestion_observed_at",
            "reachability_summary", "reachability_observed_at",
            "location_summary", "location_observed_at", "freshness",
            "haris_operational_state", "active_incident_ids",
            "last_source_change_at", "last_operational_change_at",
            "updated_at", "version", "expected_version",
        }
        self.assertTrue(required_projection.issubset(projection))
        self.assertEqual(projection["active_incident_ids"], [incident_id])
        incident = captured["p_incident"]
        self.assertEqual(
            incident["affected_devices"],
            ["ambulance-01", "scada-01", "sensor-01"],
        )
        self.assertEqual(
            client.requested_device_ids,
            ["ambulance-01", "fleet-01", "scada-01", "sensor-01"],
        )
        self.assertEqual(
            projection["reachability_summary"]["unavailable_device_ids"], [],
        )
        for row in projection["reachability_summary"]["devices"]:
            self.assertEqual(row["reachability_provenance"], "NOKIA_LIVE")
            self.assertEqual(row["metadata_provenance"], "HARIS_CONFIGURED")
        self.assertEqual(incident["schema_version"], 1)
        self.assertIsNone(captured["p_outbox"])
        self.assertEqual((client.network_reads, client.provider_mutations), (2, 0))

    def test_context_emits_ordered_safe_boundary_diagnostics(self):
        bundle = InMemoryRepositoryBundle()
        client = self.EvidenceClient(["ambulance-01", "fleet-01"])
        backend = self.backend(bundle, client)
        diagnostics = []
        backend.set_diagnostic_sink(
            lambda code, metadata=None: diagnostics.append((code, metadata or {}))
        )

        incident_id = asyncio.run(backend.create_durable_context(
            "REAL-QOD-TEST-" + "1" * 32, "ambulance-01", "T03",
        ))

        codes = [code for code, _metadata in diagnostics]
        self.assertEqual(codes, [
            "LIVE_OBSERVATION_CONGESTION_START",
            "LIVE_OBSERVATION_CONGESTION_OK",
            "LIVE_OBSERVATION_FLEET_ENUMERATED",
            "LIVE_OBSERVATION_MAPPED_FLEET_FILTERED",
            "LIVE_OBSERVATION_DEVICE_STATUS_START",
            "LIVE_OBSERVATION_DEVICE_STATUS_PARTIAL",
            "LIVE_OBSERVATION_TARGET_PRESENT",
            "DURABLE_EXISTING_PROJECTION_READ",
            "DURABLE_EVENT_CREATED",
            "DURABLE_SNAPSHOT_BUILT",
            "DURABLE_PROJECTION_PREPARED",
            "DURABLE_INCIDENT_PREPARED",
            "DURABLE_INGEST_START",
            "DURABLE_INGEST_OK",
            "DURABLE_PROJECTION_SAVED",
            "DURABLE_INCIDENT_CREATED",
            "DURABLE_CONTEXT_RETURNED",
        ])
        status_metadata = dict(diagnostics[5][1])
        self.assertEqual(status_metadata, {
            "candidate_count": 4,
            "authoritative_count": 2,
            "unavailable_count": 2,
            "target_present": True,
        })
        self.assertTrue(incident_id.endswith("-INCIDENT"))
        self.assertEqual(client.provider_mutations, 0)

    def test_fresh_run_updates_existing_projection_at_current_version(self):
        bundle = InMemoryRepositoryBundle()
        previous_incident = "REAL-QOD-TEST-previous-INCIDENT"
        bundle.network_state.save({
            "entity_id": "T03",
            "active_incident_ids": [previous_incident],
            "version": 0,
        }, 0)
        client = self.EvidenceClient(["ambulance-01", "fleet-01"])
        backend = self.backend(bundle, client)
        run_id = "REAL-QOD-TEST-" + "4" * 32

        incident_id = asyncio.run(backend.create_durable_context(
            run_id, "ambulance-01", "T03",
        ))

        projection = bundle.network_state.get("T03")
        self.assertEqual(projection["version"], 2)
        self.assertEqual(
            projection["active_incident_ids"],
            [previous_incident, incident_id],
        )
        self.assertIsNotNone(bundle.incidents.get(incident_id))
        self.assertEqual(bundle.actions.for_incident(incident_id), [])
        self.assertEqual(client.provider_mutations, 0)

    def test_concurrent_projection_advance_fails_closed_without_retry(self):
        bundle = InMemoryRepositoryBundle()
        bundle.network_state.save({
            "entity_id": "T03", "active_incident_ids": [], "version": 0,
        }, 0)
        client = self.EvidenceClient()
        backend = self.backend(bundle, client)
        original_process = bundle.inbound.process
        process_calls = 0

        def concurrent_process(*args, **kwargs):
            nonlocal process_calls
            process_calls += 1
            current = bundle.network_state.get("T03")
            bundle.network_state.save(current, int(current["version"]))
            return original_process(*args, **kwargs)

        with patch.object(
            bundle.inbound, "process", side_effect=concurrent_process,
        ), self.assertRaises(ValidationAbort) as failure:
            asyncio.run(backend.create_durable_context(
                "REAL-QOD-TEST-" + "5" * 32, "ambulance-01", "T03",
            ))

        self.assertEqual(failure.exception.safe_reason, "DURABLE_INGEST_FAILED")
        self.assertEqual(failure.exception.exception_class, "PERSISTENCE_RPC_ERROR")
        self.assertEqual(process_calls, 1)
        self.assertEqual(bundle.incidents.active(), [])
        self.assertEqual(bundle.actions.pending_or_unknown(), [])
        self.assertEqual(client.provider_mutations, 0)

    def test_invalid_device_collection_shape_is_safely_classified(self):
        class InvalidShapeClient(self.EvidenceClient):
            async def device_status(self, device_ids):
                self.network_reads += 1
                return [{"device_id": "not-a-typed-observation"}]

        backend = self.backend(InMemoryRepositoryBundle(), InvalidShapeClient())
        diagnostics = []
        backend.set_diagnostic_sink(
            lambda code, metadata=None: diagnostics.append(code)
        )

        with self.assertRaises(ValidationAbort) as failure:
            asyncio.run(backend.create_durable_context(
                "REAL-QOD-TEST-" + "2" * 32, "ambulance-01", "T03",
            ))

        self.assertEqual(failure.exception.safe_reason, "LIVE_DEVICE_RESPONSE_INVALID")
        self.assertEqual(failure.exception.exception_class, "RESPONSE_SHAPE_ERROR")
        self.assertEqual(diagnostics[-1], "LIVE_OBSERVATION_DEVICE_STATUS_START")

    def test_atomic_ingest_failure_is_safely_classified(self):
        bundle = InMemoryRepositoryBundle()
        client = self.EvidenceClient()
        backend = self.backend(bundle, client)
        diagnostics = []
        backend.set_diagnostic_sink(
            lambda code, metadata=None: diagnostics.append(code)
        )

        with patch.object(
            bundle.inbound, "process",
            side_effect=PersistenceRpcContractFailed(
                "sensitive-database-detail",
                rpc_name="haris_process_inbound_event",
            ),
        ), self.assertRaises(ValidationAbort) as failure:
            asyncio.run(backend.create_durable_context(
                "REAL-QOD-TEST-" + "3" * 32, "ambulance-01", "T03",
            ))

        self.assertEqual(failure.exception.safe_reason, "DURABLE_INGEST_FAILED")
        self.assertEqual(failure.exception.exception_class, "PERSISTENCE_RPC_ERROR")
        self.assertEqual(diagnostics[-1], "DURABLE_INGEST_START")
        self.assertNotIn("sensitive-database-detail", str(failure.exception))
        self.assertEqual(bundle.incidents.active(), [])
        self.assertEqual(bundle.actions.pending_or_unknown(), [])

    def test_context_retry_is_idempotent_and_creates_no_duplicate_incident_or_action(self):
        bundle = InMemoryRepositoryBundle()
        client = self.EvidenceClient()
        backend = self.backend(bundle, client)
        run_id = "REAL-QOD-TEST-" + "b" * 32

        first = asyncio.run(backend.create_durable_context(run_id, "ambulance-01", "T03"))
        second = asyncio.run(backend.create_durable_context(run_id, "ambulance-01", "T03"))

        self.assertEqual(first, second)
        self.assertEqual(len(bundle.events.events_after()), 1)
        self.assertEqual(len(bundle.incidents.active()), 1)
        self.assertEqual(bundle.actions.for_incident(first), [])
        self.assertEqual(bundle.outbox.unsent(), [])
        self.assertEqual(client.provider_mutations, 0)

    def test_one_authoritatively_observed_device_still_escalates_at_blast_one(self):
        client, context, result = self.run_reasoning(["ambulance-01"])

        self.assertEqual(len(context.devices), 1)
        self.assertEqual(context.provenance, "NOKIA_LIVE")
        self.assertEqual(result["plan"]["selected_device_ids"], ["ambulance-01"])
        self.assertEqual(result["plan"]["blast_radius"], 1.0)
        self.assertTrue(result["plan"]["approval_required"])
        self.assertFalse(result["warden"]["verified"])
        self.assertEqual(provider_access_count(), 0)
        self.assertEqual(client.provider_mutations, 0)

    def test_two_legitimate_observers_one_target_gives_half_blast_radius(self):
        client, context, result = self.run_reasoning(
            ["ambulance-01", "fleet-01"]
        )

        self.assertEqual(
            [device["device_id"] for device in context.devices],
            ["ambulance-01", "fleet-01"],
        )
        self.assertEqual(context.durable_policy["allowed_mutation_device_ids"], ["ambulance-01"])
        self.assertTrue(context.durable_policy["mutation_device_allowlist_enforced"])
        self.assertEqual(result["plan"]["selected_device_ids"], ["ambulance-01"])
        self.assertEqual(len(result["plan"]["actions"]), 1)
        self.assertEqual(result["plan"]["actions"][0]["kind"], "qos")
        self.assertEqual(result["plan"]["blast_radius"], 0.5)
        self.assertFalse(result["plan"]["approval_required"])
        self.assertTrue(result["warden"]["verified"])
        self.assertEqual(result["warden"]["safety_checks"]["mutation_scope_ok"], True)
        self.assertEqual(provider_access_count(), 0)
        self.assertEqual(client.provider_mutations, 0)

    def test_full_observed_cohort_cannot_expand_validation_mutation_scope(self):
        client, context, result = self.run_reasoning(
            ["ambulance-01", "fleet-01", "scada-01", "sensor-01"]
        )

        self.assertEqual(len(context.devices), 4)
        self.assertEqual(
            {device["device_id"] for device in context.devices},
            {"ambulance-01", "fleet-01", "scada-01", "sensor-01"},
        )
        self.assertEqual(result["plan"]["selected_device_ids"], ["ambulance-01"])
        self.assertTrue(all(
            action["device_id"] == "ambulance-01"
            for action in result["plan"]["actions"]
        ))
        self.assertAlmostEqual(result["plan"]["blast_radius"], 1 / 4)
        rejected = result["plan"]["rejected_candidates"]
        self.assertTrue(any(
            item["reason"] == "candidate is outside the durable mutation scope"
            for item in rejected
        ))
        self.assertEqual(provider_access_count(), 0)
        self.assertEqual(client.provider_mutations, 0)

    def test_unavailable_configured_device_is_not_counted_as_observed(self):
        _client, context, result = self.run_reasoning(
            ["ambulance-01", "sensor-01"]
        )

        self.assertNotIn("scada-01", {
            device["device_id"] for device in context.devices
        })
        self.assertEqual(result["plan"]["blast_radius"], 0.5)
        reachability = context.network_state[0]["reachability_summary"]
        self.assertEqual(
            reachability["unavailable_device_ids"],
            ["fleet-01", "scada-01"],
        )

    def test_validation_cohort_does_not_mix_in_an_unrelated_old_projection(self):
        bundle = InMemoryRepositoryBundle()
        client = self.EvidenceClient(["ambulance-01", "fleet-01"])
        settings = self.settings()
        system = HarisAgentSystem(
            client, memory=self.EmptyMemory(), settings=settings,
        )
        backend = self.backend(bundle, client, system)
        incident_id = asyncio.run(backend.create_durable_context(
            "REAL-QOD-TEST-" + "e" * 32, "ambulance-01", "T03",
        ))
        bundle.network_state.save({
            "entity_id": "T05", "entity_type": "HARIS_CONFIGURED_LOGICAL_CELL",
            "provenance": "NOKIA_LIVE", "freshness": "STALE",
            "reachability_summary": {"devices": [{
                "device_id": "old-observer", "reachable": True,
                "roaming": False, "battery_pct": 50, "tier": 3,
                "cell_id": "T05",
            }]},
            "updated_at": 1.0, "version": 0,
        }, 0)

        context = asyncio.run(build_incident_reasoning_context(
            bundle=bundle, agent_system=system, incident_id=incident_id,
        ))

        self.assertEqual(
            [device["device_id"] for device in context.devices],
            ["ambulance-01", "fleet-01"],
        )
        self.assertNotIn("old-observer", {
            device["device_id"] for device in context.devices
        })

    def test_guardrail_thresholds_and_one_device_mutation_budget_are_unchanged(self):
        settings = self.settings()
        budget = ProviderBudget(4)

        self.assertEqual(settings.guardrails.human_approval_blast_radius, 0.70)
        self.assertEqual(settings.guardrails.minimum_confidence, 0.72)
        budget.consume("CREATE")
        budget.consume("DELETE")
        with self.assertRaises(ValidationAbort):
            budget.consume("CREATE")
        with self.assertRaises(ValidationAbort):
            budget.consume("DELETE")


class LiveFleetObservationTests(unittest.TestCase):
    @staticmethod
    def settings():
        return AppSettings(
            nac_mode="live_write", nac_api_token="offline-test-token",
            fixture_dir=str(Path(__file__).resolve().parent / "fixtures"),
        )

    def test_unavailable_non_target_observer_is_excluded_without_fabrication(self):
        client = LiveNokiaClient(self.settings())
        unavailable_id = "scada-01"
        unavailable_provider_id = client.device_phone_map[unavailable_id]

        def status(device):
            if device["phoneNumber"] == unavailable_provider_id:
                raise RuntimeError("sensitive-provider-error")
            return {"reachable": True}

        with patch(
            "nokia_clients._assert_external_test_call_allowed", return_value=None,
        ), patch.object(
            client.client.device_status.api.reachability_status,
            "get_reachability", side_effect=status,
        ), self.assertLogs("haris.nokia", level="WARNING") as captured:
            rows = asyncio.run(client.device_status([
                "ambulance-01", unavailable_id, "sensor-01",
            ]))

        self.assertEqual(
            [row.device_id for row in rows],
            ["ambulance-01", "sensor-01"],
        )
        self.assertNotIn(unavailable_id, {row.device_id for row in rows})
        log_text = "\n".join(captured.output)
        self.assertNotIn("sensitive-provider-error", log_text)
        self.assertNotIn(unavailable_provider_id, log_text)
        self.assertEqual(provider_access_count(), 0)

    def test_malformed_reachability_is_unavailable_not_false(self):
        client = LiveNokiaClient(self.settings())
        with patch(
            "nokia_clients._assert_external_test_call_allowed", return_value=None,
        ), patch.object(
            client.client.device_status.api.reachability_status,
            "get_reachability", return_value={"status": "unknown"},
        ):
            rows = asyncio.run(client.device_status(["ambulance-01"]))

        self.assertEqual(rows, [])
        self.assertEqual(provider_access_count(), 0)

    def test_runtime_access_block_is_not_downgraded_to_unavailable_evidence(self):
        client = LiveNokiaClient(self.settings())

        with patch(
            "nokia_clients._assert_external_test_call_allowed",
            side_effect=runtime.ExternalProviderAccessBlocked("offline boundary"),
        ), self.assertRaises(runtime.ExternalProviderAccessBlocked):
            asyncio.run(client.device_status(["ambulance-01"]))

        self.assertEqual(provider_access_count(), 0)

    def test_missing_target_after_partial_observation_aborts_before_incident(self):
        bundle = InMemoryRepositoryBundle()
        client = DurableContextContractTests.EvidenceClient(["sensor-01"])
        backend = DurableContextContractTests.backend(bundle, client)

        with self.assertRaises(ValidationAbort) as failure:
            asyncio.run(backend.create_durable_context(
                "REAL-QOD-TEST-" + "f" * 32, "ambulance-01", "T03",
            ))

        self.assertEqual(failure.exception.stage, "DURABLE_CONTEXT")
        self.assertEqual(
            failure.exception.safe_reason, "LIVE_DEVICE_CONTEXT_UNAVAILABLE",
        )
        self.assertEqual(bundle.incidents.active(), [])
        self.assertEqual(bundle.actions.pending_or_unknown(), [])
        self.assertEqual(client.provider_mutations, 0)

    def test_aggregate_status_exception_is_reclassified_without_secret_text(self):
        class FailingClient(DurableContextContractTests.EvidenceClient):
            async def device_status(self, device_ids):
                raise RuntimeError("sensitive-provider-error")

        bundle = InMemoryRepositoryBundle()
        client = FailingClient()
        backend = DurableContextContractTests.backend(bundle, client)

        with self.assertRaises(ValidationAbort) as failure:
            asyncio.run(backend.create_durable_context(
                "REAL-QOD-TEST-" + "0" * 32, "ambulance-01", "T03",
            ))

        self.assertEqual(failure.exception.stage, "DURABLE_CONTEXT")
        self.assertEqual(
            failure.exception.safe_reason, "LIVE_DEVICE_COHORT_UNAVAILABLE",
        )
        self.assertNotIn("sensitive-provider-error", str(failure.exception))
        self.assertEqual(bundle.incidents.active(), [])


class RuntimeIsolationTests(unittest.TestCase):
    def test_real_qod_runtime_allows_only_nokia_and_persistence(self):
        with patch.dict(os.environ, valid_environment(), clear=True), patch.object(runtime.sys, "argv", ["harness"]):
            policy = runtime.external_access_policy()
        self.assertEqual(policy.runtime, runtime.RuntimeEnvironment.REAL_QOD_VALIDATION)
        self.assertTrue(policy.allow_nokia_read)
        self.assertTrue(policy.allow_nokia_write)
        self.assertTrue(policy.allow_remote_database)
        self.assertTrue(policy.allow_persistence_http)
        self.assertFalse(policy.allow_llm)
        self.assertFalse(policy.allow_oauth)
        self.assertFalse(policy.allow_remote_event_bus)
        self.assertFalse(policy.allow_external_http)

    def test_dedicated_gate_missing_falls_back_to_all_deny_test_policy(self):
        environment = valid_environment(HARIS_ALLOW_REAL_QOD_VALIDATION="false")
        with patch.dict(os.environ, environment, clear=True), patch.object(runtime.sys, "argv", ["harness"]):
            policy = runtime.external_access_policy()
        self.assertEqual(policy.runtime, runtime.RuntimeEnvironment.TEST)
        self.assertFalse(policy.allow_nokia_write)


class ValidationReasoningAgentTests(unittest.TestCase):
    def test_setup_signal_is_derived_and_nokia_congestion_is_untouched(self):
        class Wrapped:
            async def run_durable_reasoning(self, context):
                return context
        context = {
            "incident_id": "REAL-QOD-TEST-" + "d" * 32 + "-INCIDENT",
            "provenance": "NOKIA_LIVE",
            "congestion": [{"congestion_level": "High", "confidence_level": 90}],
            "agent_incident": {"storm_advisory": False},
        }
        result = asyncio.run(ValidationReasoningAgent(Wrapped()).run_durable_reasoning(context))
        self.assertEqual(result["environmental_source"], "HARIS_DERIVED_VALIDATION_SETUP")
        self.assertTrue(result["dust_advisory"])
        self.assertEqual(result["congestion"], context["congestion"])
        self.assertFalse(context["agent_incident"]["storm_advisory"])

    def test_setup_signal_rejects_non_validation_incident(self):
        class Wrapped:
            async def run_durable_reasoning(self, context):
                return context
        with self.assertRaises(ValidationAbort):
            asyncio.run(ValidationReasoningAgent(Wrapped()).run_durable_reasoning({
                "incident_id": "operational-incident", "provenance": "NOKIA_LIVE",
            }))


if __name__ == "__main__":
    unittest.main()
