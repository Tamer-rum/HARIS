import contextlib
import importlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import external.validate_persistence_concurrency as validator


RUN_ID = "a" * 32


class FakeProcess:
    instances = []

    def __init__(self, args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.returncode = 0
        self.worker = args[args.index("--worker-id") + 1]
        self.gate = Path(args[args.index("--gate-file") + 1])
        self.ready = Path(args[args.index("--ready-file") + 1])
        self.ready.touch()
        self.__class__.instances.append(self)

    def poll(self):
        return None

    def communicate(self, timeout=None):
        if timeout is None or not self.gate.exists():
            raise AssertionError("workers were not released through the shared start barrier")
        payload = {
            "worker": self.worker,
            "created": self.worker == "A",
            "incident_id": f"PERSISTENCE-TEST-{RUN_ID}-INC-CANONICAL",
        }
        return json.dumps(payload), ""


class PersistenceConcurrencyValidatorTests(unittest.TestCase):
    def test_import_has_no_network_or_transport_construction(self):
        with patch("socket.socket.connect", side_effect=AssertionError("network")):
            reloaded = importlib.reload(validator)
        self.assertIs(reloaded, validator)
        self.assertNotIn("httpx", validator.__dict__)

    def test_gates_fail_closed_before_controller_work(self):
        for environment, reason in (
            ({}, "PERSISTENCE_INTEGRATION_NOT_AUTHORIZED"),
            ({"HARIS_RUNTIME_ENV": "persistence_integration"}, "PERSISTENCE_INTEGRATION_NOT_AUTHORIZED"),
            ({"HARIS_RUNTIME_ENV": "persistence_integration", "HARIS_ALLOW_PERSISTENCE_INTEGRATION": "true"}, "PERSISTENCE_NOT_CONFIGURED"),
            ({"HARIS_RUNTIME_ENV": "persistence_integration", "HARIS_ALLOW_PERSISTENCE_INTEGRATION": "true", "HARIS_PERSISTENCE_MODE": "postgres"}, "PERSISTENCE_NOT_CONFIGURED"),
            ({"HARIS_RUNTIME_ENV": "persistence_integration", "HARIS_ALLOW_PERSISTENCE_INTEGRATION": "true", "HARIS_PERSISTENCE_MODE": "postgres", "SUPABASE_URL": "http://bad.example", "SUPABASE_KEY": "secret-placeholder"}, "PERSISTENCE_HOST_NOT_ALLOWED"),
        ):
            with self.subTest(reason=reason):
                allowed, actual, hostname = validator.integration_gate(environment)
                self.assertFalse(allowed)
                self.assertEqual(actual, reason)
                self.assertIsNone(hostname)

    def test_main_fails_before_bundle_or_network_when_unauthorized(self):
        output = io.StringIO()
        with patch.dict(os.environ, {}, clear=True), patch.object(
            validator, "_bundle", side_effect=AssertionError("bundle must remain lazy")
        ), contextlib.redirect_stdout(output):
            code = validator.main([])
        self.assertEqual(code, 3)
        lines = output.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        payload = json.loads(lines[0])
        self.assertEqual(payload["safe_reason"], "PERSISTENCE_INTEGRATION_NOT_AUTHORIZED")

    def test_run_id_and_claim_owners_are_canonical(self):
        first, second = validator.new_run_id(), validator.new_run_id()
        self.assertRegex(first, r"^[0-9a-f]{32}$")
        self.assertRegex(second, r"^[0-9a-f]{32}$")
        self.assertNotEqual(first, second)
        self.assertEqual(validator.claim_owner(RUN_ID, "A"), f"PERSISTENCE-TEST-{RUN_ID}-WORKER-A")
        self.assertEqual(validator.claim_owner(RUN_ID, "B"), f"PERSISTENCE-TEST-{RUN_ID}-WORKER-B")
        with self.assertRaises(validator.ConcurrencyValidationFailure):
            validator.claim_owner(RUN_ID, "other")

    def test_child_environment_is_minimal_and_strips_provider_configuration(self):
        source = {
            "PATH": "safe-path", "HARIS_RUNTIME_ENV": "persistence_integration",
            "HARIS_ALLOW_PERSISTENCE_INTEGRATION": "true", "HARIS_PERSISTENCE_MODE": "postgres",
            "SUPABASE_URL": "https://project.supabase.co", "SUPABASE_KEY": "secret-placeholder",
            "NAC_API_TOKEN": "forbidden", "CAMARA_API_KEY": "forbidden",
            "OAUTH_CLIENT_SECRET": "forbidden", "GEMINI_API_KEY": "forbidden",
            "GROQ_API_KEY": "forbidden", "OPENROUTER_API_KEY": "forbidden",
            "COHERE_API_KEY": "forbidden", "MISTRAL_API_KEY": "forbidden",
            "REDIS_URL": "forbidden", "WEATHER_API_KEY": "forbidden",
        }
        child = validator.sanitized_child_environment(source)
        self.assertEqual(set(child), {
            "PATH", "PYTHONPATH", "HARIS_RUNTIME_ENV",
            "HARIS_ALLOW_PERSISTENCE_INTEGRATION", "HARIS_PERSISTENCE_MODE",
            "SUPABASE_URL", "SUPABASE_KEY",
        })
        self.assertEqual(child["PYTHONPATH"], str(validator.PROJECT_ROOT))
        self.assertFalse(set(source) - validator._PROCESS_ENV - validator._PERSISTENCE_ENV & set(child))

    def test_worker_pair_constructs_two_independent_processes_before_release(self):
        FakeProcess.instances = []
        with tempfile.TemporaryDirectory() as temporary:
            results = validator.run_worker_pair(
                "incident", RUN_ID, Path(temporary), popen_factory=FakeProcess,
            )
        self.assertEqual(len(FakeProcess.instances), 2)
        self.assertIsNot(FakeProcess.instances[0], FakeProcess.instances[1])
        self.assertIsNot(FakeProcess.instances[0].kwargs["env"], FakeProcess.instances[1].kwargs["env"])
        self.assertEqual([row["worker"] for row in results], ["A", "B"])

    def test_success_contract_contains_only_required_final_fields(self):
        self.assertEqual(validator._SUCCESS_KEYS, {
            "status", "concurrent_incident_singleton", "incident_id_convergence",
            "duplicate_active_incidents", "resource_single_owner",
            "resource_conflict_enforced", "active_resource_owners",
            "event_idempotency", "duplicate_events", "inbox_idempotency",
            "action_idempotency", "duplicate_actions", "optimistic_version_conflict",
            "stale_write_rejected", "outbox_claim_exclusive", "outbox_claim_overlap",
            "outbox_namespace_isolation", "crash_replay_safe",
            "sent_action_outcome_unknown", "reconciliation_required",
            "no_action_resend", "no_false_success", "run_id",
        })

    def test_incident_convergence_evaluator(self):
        result = validator.evaluate_incident([
            {"created": True, "incident_id": "one"},
            {"created": False, "incident_id": "one"},
        ], 1)
        self.assertEqual(result["duplicate_active_incidents"], 0)
        with self.assertRaises(validator.ConcurrencyValidationFailure):
            validator.evaluate_incident([
                {"created": True, "incident_id": "one"},
                {"created": True, "incident_id": "two"},
            ], 2)

    def test_resource_conflict_evaluator(self):
        result = validator.evaluate_resource([
            {"outcome": "ACQUIRED"}, {"outcome": "RESOURCE_CONFLICT"},
        ], 1)
        self.assertEqual(result["active_resource_owners"], 1)
        with self.assertRaises(validator.ConcurrencyValidationFailure):
            validator.evaluate_resource([{"outcome": "ACQUIRED"}, {"outcome": "ACQUIRED"}], 2)

    def test_event_idempotency_evaluator(self):
        self.assertEqual(
            validator.evaluate_event([{"inserted": True}, {"inserted": False}], True)["duplicate_events"], 0,
        )
        with self.assertRaises(validator.ConcurrencyValidationFailure):
            validator.evaluate_event([{"inserted": True}, {"inserted": True}], True)

    def test_inbox_idempotency_evaluator(self):
        result = validator.evaluate_inbox(
            [{"outcome": "accepted"}, {"outcome": "duplicate"}],
            event_exists=True, incident_exists=True, projection_exists=True, transition_count=1,
        )
        self.assertEqual(result["inbox_idempotency"], "PASS")
        with self.assertRaises(validator.ConcurrencyValidationFailure):
            validator.evaluate_inbox(
                [{"outcome": "accepted"}, {"outcome": "duplicate"}],
                event_exists=True, incident_exists=True, projection_exists=True, transition_count=2,
            )

    def test_action_idempotency_evaluator(self):
        result = validator.evaluate_action([
            {"created": True, "command_id": "canonical"},
            {"created": False, "command_id": "canonical"},
        ], True)
        self.assertEqual(result["duplicate_actions"], 0)
        with self.assertRaises(validator.ConcurrencyValidationFailure):
            validator.evaluate_action([
                {"created": True, "command_id": "a"},
                {"created": False, "command_id": "b"},
            ], True)

    def test_version_conflict_evaluator(self):
        result = validator.evaluate_version([
            {"outcome": "UPDATED"}, {"outcome": "VERSION_CONFLICT"},
        ], 2)
        self.assertEqual(result["stale_write_rejected"], "PASS")
        with self.assertRaises(validator.ConcurrencyValidationFailure):
            validator.evaluate_version([{"outcome": "UPDATED"}, {"outcome": "UPDATED"}], 3)

    def test_outbox_disjoint_and_exact_run_evaluator(self):
        ns = validator.namespace(RUN_ID)
        expected = {f"{ns}-OUT-1", f"{ns}-OUT-2"}
        result = validator.evaluate_outbox([
            {"claimed_ids": [f"{ns}-OUT-1"], "event_ids": [f"{ns}-EVT-1"]},
            {"claimed_ids": [f"{ns}-OUT-2"], "event_ids": [f"{ns}-EVT-2"]},
        ], RUN_ID, expected)
        self.assertEqual(result["outbox_claim_overlap"], 0)
        self.assertEqual(result["outbox_namespace_isolation"], "PASS")
        with self.assertRaises(validator.ConcurrencyValidationFailure):
            validator.evaluate_outbox([
                {"claimed_ids": [f"{ns}-OUT-1"], "event_ids": [f"{ns}-EVT-1"]},
                {"claimed_ids": [f"{ns}-OUT-1"], "event_ids": ["operational-event"]},
            ], RUN_ID, expected)

    def test_crash_replay_evaluator_never_reports_success_or_resend(self):
        replay = {
            "worker": "REPLAY", "stored_state": "SENT", "runtime_state": "OUTCOME_UNKNOWN",
            "reconciliation_required": True, "provider_called": False, "resent": False,
        }
        result = validator.evaluate_crash(replay, validator.CRASH_EXIT_CODE)
        self.assertEqual(result["no_false_success"], "PASS")
        self.assertEqual(result["no_action_resend"], "PASS")
        with self.assertRaises(validator.ConcurrencyValidationFailure):
            validator.evaluate_crash({**replay, "provider_called": True}, validator.CRASH_EXIT_CODE)

    def test_safe_failure_has_only_allowlisted_fields_and_no_secret_text(self):
        secret = "never-print-this-secret"
        raw = RuntimeError(f"Authorization: Bearer {secret} apikey={secret}")
        payload = validator.safe_failure_payload(raw, run_id=RUN_ID)
        self.assertLessEqual(set(payload), {
            "status", "safe_reason", "test_case", "worker", "rpc_name",
            "http_status", "expected_result", "actual_result_type", "run_id",
        })
        rendered = json.dumps(payload)
        self.assertNotIn(secret, rendered)
        self.assertNotIn("Authorization", rendered)
        self.assertNotIn("apikey", rendered)
        self.assertEqual(str(validator.ConcurrencyValidationFailure("PERSISTENCE_UNAVAILABLE")), "PERSISTENCE_UNAVAILABLE")

    def test_worker_failure_parser_never_forwards_stderr_or_unknown_fields(self):
        secret = "never-forward-this"
        completed = subprocess.CompletedProcess(
            ["worker"], 3,
            json.dumps({
                "status": "PERSISTENCE_CONCURRENCY_VALIDATION_FAILED",
                "safe_reason": "PERSISTENCE_RPC_CONTRACT_FAILED",
                "test_case": "OUTBOX_CLAIM", "worker": "A",
                "rpc_name": "haris_claim_outbox", "expected_result": "disjoint_current_run_claims",
                "actual_result_type": "array", "unsafe": secret,
            }),
            f"raw upstream {secret}",
        )
        with self.assertRaises(validator.ConcurrencyValidationFailure) as caught:
            validator._parse_child(completed, test_case="OUTBOX_CLAIM", worker="A", run_id=RUN_ID)
        self.assertNotIn(secret, str(caught.exception))

    def test_operational_filtering_check_accepts_filtered_views_and_rejects_leaks(self):
        ids = validator._ids(RUN_ID)

        class Network:
            def __init__(self, rows): self.rows = rows
            def load_all(self): return self.rows

        class Incidents:
            def __init__(self, rows): self.rows = rows
            def active(self): return self.rows

        class Bundle:
            network_state = Network({})
            incidents = Incidents([])

        validator._verify_operational_filtering(Bundle(), ids)
        Bundle.network_state = Network({ids["version_entity"]: {}})
        with self.assertRaises(validator.ConcurrencyValidationFailure):
            validator._verify_operational_filtering(Bundle(), ids)

    def test_validator_uses_production_composition_and_no_provider_execution(self):
        source = Path("external/validate_persistence_concurrency.py").read_text(encoding="utf-8")
        self.assertIn("build_persistence_transport", source)
        self.assertIn("PostgresRepositoryBundle", source)
        self.assertNotIn("httpx.Client", source)
        self.assertNotIn("requests.", source)
        self.assertIn('"provider_call_permitted": False', source)
        self.assertIn('"provider_called": False', source)

    def test_frozen_migration_hashes_002_through_005(self):
        import hashlib
        expected = {
            "002_haris_durable_event_incident_core.sql": "7afe3fb2b16def35d6b73a0ec5d17ee95f0462de9d64523d7ef5c231e9dead1f",
            "003_haris_conflict_status_alignment.sql": "8ef41c1d77eb4a44afc463cc4d618a56733bb83e12d3a5975ea6fb408b0e2122",
            "004_haris_outbox_claim_isolation.sql": "bd898186e26b60c8807a588760779c1a86d73c136b81089206f228427a5210d5",
            "005_haris_outbox_per_run_isolation.sql": "4f18b170281c78cf4611085633d31b24ebbdedd9a85f5e6d022e73e9a26a1faf",
        }
        root = Path("supabase/migrations")
        for name, digest in expected.items():
            self.assertEqual(hashlib.sha256((root / name).read_bytes()).hexdigest(), digest)


if __name__ == "__main__":
    unittest.main()
