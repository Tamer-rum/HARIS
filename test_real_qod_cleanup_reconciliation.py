"""OFFLINE_SAFE tests for the Phase 7E reconciliation-only operator path."""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from durable_core import (
    ActionCommand, ActionState, InMemoryRepositoryBundle, IncidentState,
    RecoveryState, RepositoryUnavailable,
)
from durable_execution import ActionVerificationResult, DurableActionExecutionService
from durable_reconciliation import DurableActionReconciliationService
from external import reconcile_real_qod_cleanup as operator
from runtime_events import RuntimeIngestionMetrics
from test_durable_execution import MockProvider, NOW, seed, settings


class CleanupReadAdapter(MockProvider):
    def __init__(self, bundle, outcome="VERIFIED_ROLLBACK"):
        super().__init__(bundle)
        self.outcome = outcome
        self.rollback_reads = 0

    async def execute(self, _action):
        raise AssertionError("reconciliation-only path must not CREATE")

    async def rollback(self, _action, _provider_resource_id):
        raise AssertionError("reconciliation-only path must not DELETE")

    async def verify_rollback(self, _action, _provider_resource_id):
        self.rollback_reads += 1
        if self.outcome == "ERROR":
            raise RuntimeError("https://provider.invalid/resource?token=SECRET")
        return ActionVerificationResult(
            outcome=self.outcome,
            reason=(
                "provider_resource_inactive"
                if self.outcome == "VERIFIED_ROLLBACK"
                else "live_rollback_not_yet_terminal"
            ),
            provenance="NOKIA_LIVE",
            evidence={"resource_inactive": self.outcome == "VERIFIED_ROLLBACK"},
        )


class ReconciliationOnlyServiceTests(unittest.TestCase):
    run_id = "REAL-QOD-TEST-offline"
    incident_id = f"{run_id}-INCIDENT"

    def setUp(self):
        self.bundle = InMemoryRepositoryBundle()
        self.original = seed(
            self.bundle, action_state=ActionState.ROLLBACK_REQUIRED,
            incident_id=self.incident_id,
        )
        current = self.bundle.actions.get(self.original.command_id)
        current.provider_resource_id = "opaque-provider-resource"
        self.original = self.bundle.actions.update(current, current.version)
        incident = self.bundle.incidents.get(self.incident_id)
        for state in (IncidentState.MITIGATING, IncidentState.VERIFYING, IncidentState.RECOVERING):
            incident = self.bundle.incidents.transition(
                self.incident_id, state, actor="TEST", reason_code="TEST",
                trace_id="offline", at=NOW + 1,
            )
        self.bundle.resource_ownership.acquire({
            "resource_key": self.original.resource_key,
            "resource_type": "DEVICE", "owner_incident_id": self.incident_id,
            "acquired_at": NOW, "lease_started_at": NOW,
            "lease_expires_at": NOW + 300,
            "provider_resource_id": "opaque-provider-resource",
        })
        cleanup = ActionCommand(
            incident_id=self.incident_id, command_type="QOD_RELEASE",
            resource_key=self.original.resource_key, device_id=self.original.device_id,
            plan_version=self.original.plan_version, requested_at=NOW + 1,
            parameters_safe={"original_command_id": self.original.command_id},
            preconditions={"phase": "7C_ROLLBACK", "provider_resource_id_known": True},
            state=ActionState.RECONCILIATION_REQUIRED,
        )
        self.cleanup = self.bundle.actions.create_or_get(cleanup)[0]
        self.bundle.recovery.save({
            "recovery_id": f"recovery-{self.incident_id}",
            "incident_id": self.incident_id,
            "resource_keys": [self.original.resource_key],
            "state": RecoveryState.PARTIAL.value,
            "started_at": NOW, "completed_at": None,
            "failure_reason": "rollback_verification_unavailable",
        })
        self.runtime_settings = settings(
            durable_reconciliation_max_attempts=3,
            durable_reconciliation_base_backoff_seconds=1,
            durable_reconciliation_max_backoff_seconds=2,
            durable_reconciliation_deadline_seconds=120,
        )

    def service(self, outcome="VERIFIED_ROLLBACK"):
        adapter = CleanupReadAdapter(self.bundle, outcome)
        metrics = RuntimeIngestionMetrics()
        executor = DurableActionExecutionService(
            bundle=self.bundle, adapter=adapter, settings=self.runtime_settings,
            is_ready=lambda: True, metrics=metrics,
        )
        reconciler = DurableActionReconciliationService(
            bundle=self.bundle, adapter=adapter, settings=self.runtime_settings,
            execution_service=executor, is_ready=lambda: True, metrics=metrics,
        )
        return reconciler, adapter

    def reconcile(self, outcome="VERIFIED_ROLLBACK"):
        service, adapter = self.service(outcome)
        result = asyncio.run(service.reconcile_validation_cleanup(
            self.original.command_id, self.run_id,
        ))
        return result, adapter

    def test_terminal_readback_finalizes_existing_cleanup_without_mutation(self):
        result, adapter = self.reconcile()
        self.assertEqual(result.status, "CLEANUP_VERIFIED")
        self.assertEqual(adapter.rollback_reads, 1)
        self.assertEqual(adapter.execute_count, 0)
        self.assertEqual(adapter.rollback_count, 0)
        self.assertEqual(self.bundle.actions.get(self.original.command_id).state, ActionState.ROLLED_BACK)
        self.assertEqual(self.bundle.actions.get(self.cleanup.command_id).state, ActionState.ROLLED_BACK)
        self.assertEqual(self.bundle.incidents.get(self.incident_id)["state"], IncidentState.RESOLVED.value)
        self.assertEqual(self.bundle.recovery.for_incident(self.incident_id)["state"], RecoveryState.COMPLETE.value)
        self.assertIsNone(self.bundle.resource_ownership.get_active(self.original.resource_key))

    def test_active_readback_preserves_reconciliation_required(self):
        result, adapter = self.reconcile("VERIFICATION_UNAVAILABLE")
        self.assertEqual((result.status, result.provider_state), ("RESOURCE_STILL_ACTIVE", "ACTIVE"))
        self.assertEqual(adapter.rollback_reads, 1)
        self.assertEqual(self.bundle.actions.get(self.cleanup.command_id).state, ActionState.RECONCILIATION_REQUIRED)
        self.assertEqual(self.bundle.incidents.get(self.incident_id)["state"], IncidentState.RECOVERING.value)

    def test_provider_error_remains_reconciliation_required_and_sanitized(self):
        captured_logs = []

        class CaptureHandler(logging.Handler):
            def emit(self, record):
                captured_logs.append(record.getMessage())

        root_logger = logging.getLogger()
        prior_level = root_logger.level
        handler = CaptureHandler()
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.DEBUG)
        try:
            result, adapter = self.reconcile("ERROR")
        finally:
            root_logger.removeHandler(handler)
            root_logger.setLevel(prior_level)
        self.assertEqual(result.status, "RECONCILIATION_REQUIRED")
        self.assertEqual(adapter.rollback_reads, 1)
        durable_records = self.bundle.verification.for_incident(self.incident_id)
        rendered = json.dumps({
            "result": result.public(), "durable": durable_records,
            "logs": captured_logs,
        })
        self.assertNotIn("SECRET", rendered)
        self.assertNotIn("provider.invalid", rendered)

    def test_verified_cleanup_replay_does_not_read_provider_again(self):
        first, adapter = self.reconcile()
        self.assertEqual(first.status, "CLEANUP_VERIFIED")
        service = DurableActionReconciliationService(
            bundle=self.bundle, adapter=adapter, settings=self.runtime_settings,
            execution_service=SimpleNamespace(), is_ready=lambda: True,
        )
        second = asyncio.run(service.reconcile_validation_cleanup(
            self.original.command_id, self.run_id,
        ))
        self.assertEqual((second.status, second.duplicate), ("CLEANUP_VERIFIED", True))
        self.assertEqual(adapter.rollback_reads, 1)

    def test_crash_after_verified_readback_resumes_without_another_get(self):
        self.bundle.verification.save({
            "verification_id": f"reconcile-cleanup-{self.cleanup.command_id}-1",
            "incident_id": self.incident_id, "evidence_event_ids": [],
            "verification_type": "ROLLBACK_RECONCILIATION",
            "state": "IMPROVED", "started_at": NOW, "updated_at": NOW,
            "reason": "provider_resource_inactive",
            "source_provenance": "NOKIA_LIVE",
            "result": {
                "command_id": self.cleanup.command_id, "attempt_number": 1,
                "outcome": "VERIFIED_ROLLBACK", "resource_inactive": True,
            },
        })
        result, adapter = self.reconcile("ERROR")
        self.assertEqual((result.status, result.duplicate), ("CLEANUP_VERIFIED", True))
        self.assertEqual(adapter.rollback_reads, 0)
        self.assertEqual(self.bundle.actions.get(self.cleanup.command_id).state, ActionState.ROLLED_BACK)

    def test_ready_cleanup_is_blocked_without_read_or_delete(self):
        current = self.bundle.actions.get(self.cleanup.command_id)
        current.state = ActionState.READY
        self.bundle.actions.update(current, current.version)
        result, adapter = self.reconcile()
        self.assertEqual((result.status, result.reason), ("BLOCKED_SAFE", "cleanup_not_previously_attempted"))
        self.assertEqual(adapter.rollback_reads, 0)
        self.assertEqual(adapter.rollback_count, 0)

    def test_foreign_binding_fails_closed_before_provider_read(self):
        current = self.bundle.actions.get(self.cleanup.command_id)
        current.parameters_safe = {"original_command_id": "foreign-action"}
        self.bundle.actions.update(current, current.version)
        service, adapter = self.service()
        with self.assertRaises(RepositoryUnavailable):
            asyncio.run(service.reconcile_validation_cleanup(
                self.original.command_id, self.run_id,
            ))
        self.assertEqual(adapter.rollback_reads, 0)

    def test_attempt_limit_prevents_another_provider_read(self):
        for attempt in range(1, 4):
            self.bundle.verification.save({
                "verification_id": f"reconcile-cleanup-{self.cleanup.command_id}-{attempt}",
                "incident_id": self.incident_id, "evidence_event_ids": [],
                "verification_type": "ROLLBACK_RECONCILIATION",
                "state": "INSUFFICIENT_EVIDENCE", "started_at": NOW,
                "updated_at": NOW, "reason": "rollback_readback_unavailable",
                "source_provenance": "UNAVAILABLE",
                "result": {"command_id": self.cleanup.command_id, "attempt_number": attempt},
            })
        result, adapter = self.reconcile()
        self.assertEqual(result.status, "RECONCILIATION_REQUIRED")
        self.assertEqual(adapter.rollback_reads, 0)


class OperatorBoundaryTests(unittest.TestCase):
    def test_reconciliation_operator_requires_live_read_context_not_write_gate(self):
        read_only = settings(
            "live_read_only", False, nac_api_token="opaque-test-token",
        )
        fixture = settings("fixture", False, nac_api_token="opaque-test-token")
        self.assertTrue(operator._live_read_context(read_only))
        self.assertFalse(read_only.enable_live_write_loop)
        self.assertFalse(operator._live_read_context(fixture))

    def test_read_only_nokia_boundary_allows_one_get_and_no_mutations(self):
        class Sessions:
            def get(self, resource_id):
                return {"status": "AVAILABLE", "resource": bool(resource_id)}

        client = operator._ReadOnlyNokiaClient(
            SimpleNamespace(client=SimpleNamespace(sessions=Sessions()))
        )
        self.assertEqual(client.sessions.get("opaque")["status"], "AVAILABLE")
        with self.assertRaises(operator.ReconciliationAbort):
            client.sessions.get("opaque")
        with self.assertRaises(operator.ReconciliationAbort):
            asyncio.run(client.request_qos())
        with self.assertRaises(operator.ReconciliationAbort):
            asyncio.run(client.release_qos())

    def test_artifact_reference_is_bounded_and_output_omits_identifiers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact_dir = root / "artifacts" / "validation"
            artifact_dir.mkdir(parents=True)
            artifact = artifact_dir / "real_qod_REAL-QOD-TEST-safe.json"
            artifact.write_text(json.dumps({
                "run_id": "REAL-QOD-TEST-safe",
                "action_id": "sensitive-action-reference",
            }), encoding="utf-8")
            with patch.object(operator, "PROJECT_ROOT", root):
                run_id, action_id = operator._reference(artifact)
            self.assertEqual(run_id, "REAL-QOD-TEST-safe")
            self.assertEqual(action_id, "sensitive-action-reference")
            emitted = {
                "RECONCILIATION_STATUS": "RECONCILIATION_REQUIRED",
                "provider_mutation_performed": False,
            }
            self.assertNotIn(run_id, json.dumps(emitted))
            self.assertNotIn(action_id, json.dumps(emitted))

    def test_durable_view_is_sanitized_and_read_only(self):
        bundle = InMemoryRepositoryBundle()
        run_id = "REAL-QOD-TEST-view"
        action = seed(
            bundle, action_state=ActionState.ROLLBACK_REQUIRED,
            incident_id=f"{run_id}-INCIDENT",
        )
        current = bundle.actions.get(action.command_id)
        current.provider_resource_id = "provider-secret-identifier"
        bundle.actions.update(current, current.version)
        before_version = bundle.actions.get(action.command_id).version
        view = operator._durable_view(bundle, run_id, action.command_id)
        rendered = json.dumps(view)
        self.assertTrue(view["provider_resource_id_known"])
        self.assertNotIn("provider-secret-identifier", rendered)
        self.assertNotIn(action.command_id, rendered)
        self.assertEqual(bundle.actions.get(action.command_id).version, before_version)


if __name__ == "__main__":
    unittest.main()
