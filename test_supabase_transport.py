import contextlib
import io
import json
import os
import re
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

import runtime
from config import AppSettings
from durable_core import RepositoryUnavailable, ResourceAlreadyOwned, VersionConflict
from postgres_persistence import (
    PersistenceAuthenticationFailed, PersistenceHostNotAllowed,
    PersistenceNetworkBlocked,
    PersistenceNotConfigured, PersistenceResponseInvalid,
    PersistenceRpcContractFailed, PersistenceSchemaNotReady,
    PersistenceTransportUnavailable,
    MockPostgresTransport, PostgresRepositoryBundle, build_persistence_transport,
)
from runtime import RuntimeEnvironment
from supabase_transport import (
    HARIS_RPC_ALLOWLIST, HARIS_RPC_RESPONSE_SHAPES, SupabaseRpcTransport,
    authorize_persistence_network,
)


PROJECT_URL = "https://project.supabase.co"
DUMMY_KEY = "test-only-placeholder-key"


class SupabaseTransportTests(unittest.TestCase):
    def make_transport(self, handler, *, authorizer=lambda _hostname: None):
        client = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
        self.addCleanup(client.close)
        return SupabaseRpcTransport(
            PROJECT_URL, DUMMY_KEY, expected_hostname="project.supabase.co",
            client=client, network_authorizer=authorizer,
        )

    def test_allowlist_exactly_matches_migration_rpc_contract(self):
        sql = Path("supabase/migrations/002_haris_durable_event_incident_core.sql").read_text(encoding="utf-8")
        migration_rpcs = set(re.findall(r"create\s+or\s+replace\s+function\s+public\.(haris_[a-z_]+)", sql, re.I))
        self.assertEqual(HARIS_RPC_ALLOWLIST, migration_rpcs)
        self.assertEqual(set(HARIS_RPC_RESPONSE_SHAPES),HARIS_RPC_ALLOWLIST)
        self.assertTrue(set(HARIS_RPC_RESPONSE_SHAPES.values()) <= {"boolean","object","array","null","null_or_object"})

    def test_unknown_rpc_is_rejected_before_authorizer_or_http(self):
        activity = []
        transport = self.make_transport(
            lambda _request: activity.append("http") or httpx.Response(200, json={}),
            authorizer=lambda _host: activity.append("authorized"),
        )
        with self.assertRaises(PersistenceRpcContractFailed) as raised:
            transport.rpc("arbitrary_sql_or_rpc", {})
        self.assertEqual(str(raised.exception),"PERSISTENCE_RPC_CONTRACT_FAILED")
        self.assertEqual(activity, [])

    def test_exact_https_supabase_project_url_and_hostname_are_required(self):
        invalid = (
            "http://project.supabase.co", "https://supabase.co",
            "https://other.example", "https://user:pass@project.supabase.co",
            "https://project.supabase.co/rest", "https://project.supabase.co?x=1",
            "https://project.supabase.co/#fragment",
        )
        for candidate in invalid:
            with self.subTest(candidate=candidate), self.assertRaises(PersistenceNotConfigured):
                SupabaseRpcTransport(candidate, DUMMY_KEY, expected_hostname="project.supabase.co")
        with self.assertRaises(PersistenceNotConfigured):
            SupabaseRpcTransport(PROJECT_URL, DUMMY_KEY, expected_hostname="different.supabase.co")

    def test_success_dict_list_and_empty_responses_are_parsed(self):
        responses = iter((
            httpx.Response(200, json={"sequence": 7}),
            httpx.Response(200, json=[{"entity_id": "T03"}]),
            httpx.Response(204),
        ))
        transport = self.make_transport(lambda _request: next(responses))
        self.assertEqual(transport.rpc("haris_event_sequence", {}), {"sequence": 7})
        self.assertEqual(transport.rpc("haris_read_domain", {}), [{"entity_id": "T03"}])
        self.assertIsNone(transport.rpc("haris_ack_outbox", {}))

    def test_only_declared_boolean_rpc_accepts_scalar_boolean(self):
        claim = self.make_transport(lambda _request: httpx.Response(200, json=True))
        self.assertIs(claim.rpc("haris_claim_inbox", {}), True)
        unrelated = self.make_transport(lambda _request: httpx.Response(200, json=True))
        with self.assertRaises(PersistenceResponseInvalid):
            unrelated.rpc("haris_read_domain", {})

    def test_auth_and_schema_statuses_have_explicit_safe_types(self):
        cases = (
            (401, PersistenceAuthenticationFailed, "PERSISTENCE_AUTH_FAILED"),
            (403, PersistenceAuthenticationFailed, "PERSISTENCE_AUTH_FAILED"),
            (404, PersistenceSchemaNotReady, "PERSISTENCE_SCHEMA_NOT_READY"),
        )
        for status, expected, reason in cases:
            body = {"message": "sensitive upstream details must not escape"}
            transport = self.make_transport(lambda _request, s=status: httpx.Response(s, json=body))
            with self.subTest(status=status), self.assertRaises(expected) as raised:
                transport.rpc("haris_read_domain", {})
            self.assertEqual(str(raised.exception), reason)
            self.assertEqual(raised.exception.http_status,status)
            self.assertNotIn("sensitive", str(raised.exception))

    def test_version_and_resource_conflicts_map_to_domain_errors(self):
        versioned_rpcs=("haris_process_inbound_event","haris_write_network_state","haris_update_incident","haris_save_action","haris_transition_incident")
        resource_rpcs=("haris_acquire_resource","haris_release_resource","haris_ack_outbox","haris_fail_outbox")
        for rpc_name in versioned_rpcs:
            version = self.make_transport(lambda _request: httpx.Response(409, json={"message":"private"}))
            with self.subTest(rpc=rpc_name),self.assertRaises(VersionConflict):
                version.rpc(rpc_name,{})
        for rpc_name in resource_rpcs:
            resource = self.make_transport(lambda _request: httpx.Response(409, json={"message":"private"}))
            with self.subTest(rpc=rpc_name),self.assertRaises(ResourceAlreadyOwned):
                resource.rpc(rpc_name,{})
        for rpc_name in versioned_rpcs+resource_rpcs:
            malformed = self.make_transport(lambda _request: httpx.Response(400, json={"message":"private"}))
            with self.subTest(rpc=rpc_name,status=400),self.assertRaises(PersistenceRpcContractFailed):
                malformed.rpc(rpc_name,{})
        generic = self.make_transport(lambda _request: httpx.Response(409, json={"message":"private"}))
        with self.assertRaises(RepositoryUnavailable) as raised:
            generic.rpc("haris_append_outbox", {})
        self.assertEqual(str(raised.exception), "persistence_conflict")

    def test_redirect_rate_limit_and_upstream_failures_fail_closed(self):
        for status in (301, 302, 307, 308):
            transport = self.make_transport(lambda _request, s=status: httpx.Response(s, headers={"location":"https://attacker.invalid"}))
            with self.subTest(status=status), self.assertRaises(PersistenceHostNotAllowed) as raised:
                transport.rpc("haris_read_domain", {})
            self.assertEqual(str(raised.exception), "PERSISTENCE_HOST_NOT_ALLOWED")
            self.assertEqual(raised.exception.http_status,status)
        for status in (429, 500, 503):
            transport = self.make_transport(lambda _request, s=status: httpx.Response(s, headers={"location":"https://attacker.invalid"}))
            with self.subTest(status=status), self.assertRaises(PersistenceTransportUnavailable) as raised:
                transport.rpc("haris_read_domain", {})
            self.assertEqual(str(raised.exception), "PERSISTENCE_UNAVAILABLE")
            self.assertEqual(raised.exception.http_status,status)

    def test_timeout_dns_tls_and_connection_errors_are_safe(self):
        failures = (
            lambda request: (_ for _ in ()).throw(httpx.ReadTimeout("private timeout URL", request=request)),
            lambda request: (_ for _ in ()).throw(httpx.ConnectError("private DNS detail", request=request)),
            lambda request: (_ for _ in ()).throw(httpx.ConnectError("private TLS detail", request=request)),
        )
        for failure in failures:
            transport = self.make_transport(failure)
            with self.assertRaises(PersistenceTransportUnavailable) as raised:
                transport.rpc("haris_read_domain", {})
            self.assertEqual(str(raised.exception), "PERSISTENCE_UNAVAILABLE")

    def test_malformed_and_oversized_json_fail_closed(self):
        malformed = self.make_transport(lambda _request: httpx.Response(200, content=b"not-json"))
        with self.assertRaises(PersistenceResponseInvalid) as raised:
            malformed.rpc("haris_read_domain", {})
        self.assertEqual(str(raised.exception), "PERSISTENCE_RESPONSE_INVALID")
        scalar = self.make_transport(lambda _request: httpx.Response(200, json="unexpected scalar"))
        with self.assertRaises(PersistenceResponseInvalid) as raised:
            scalar.rpc("haris_read_domain", {})
        self.assertEqual(str(raised.exception), "PERSISTENCE_RESPONSE_INVALID")
        oversized = self.make_transport(lambda _request: httpx.Response(200, content=b"x" * (4 * 1024 * 1024 + 1)))
        with self.assertRaises(PersistenceResponseInvalid) as raised:
            oversized.rpc("haris_read_domain", {})
        self.assertEqual(str(raised.exception), "PERSISTENCE_RESPONSE_INVALID")

    def test_headers_are_internal_and_request_target_is_fixed(self):
        captured = []
        def handler(request):
            captured.append(request)
            return httpx.Response(200, json=[])
        transport = self.make_transport(handler)
        transport.rpc("haris_read_domain", {"p_kind":"network","p_limit":1})
        request = captured[0]
        self.assertEqual(request.url, httpx.URL(f"{PROJECT_URL}/rest/v1/rpc/haris_read_domain"))
        self.assertEqual(request.headers["apikey"], DUMMY_KEY)
        self.assertEqual(request.headers["authorization"], f"Bearer {DUMMY_KEY}")
        self.assertNotIn(DUMMY_KEY, request.content.decode())

    def test_network_write_request_keys_and_jsonb_object_response(self):
        captured=[]
        def handler(request):
            payload=json.loads(request.content)
            captured.append(payload)
            return httpx.Response(200,json={**payload["p_record"],"version":1})
        transport=self.make_transport(handler)
        record={
            "entity_id":"PERSISTENCE-TEST-CELL-http", "entity_type":"HARIS_CONFIGURED_LOGICAL_CELL",
            "mapping_source":"PERSISTENCE_INTEGRATION_TEST", "provenance":"HARIS_DERIVED",
            "raw_congestion":"High", "raw_congestion_observed_at":1.0,
            "reachability_summary":None, "reachability_observed_at":None,
            "location_summary":None, "location_observed_at":None, "freshness":"FRESH",
            "haris_operational_state":"INCIDENT_OPEN", "active_incident_ids":[],
            "last_source_change_at":2.0, "last_operational_change_at":3.0,
            "version":0, "updated_at":4.0,
        }
        saved=PostgresRepositoryBundle(transport).network_state.save(record,0)
        self.assertEqual(set(captured[0]),{"p_record","p_expected_version"})
        self.assertEqual(set(captured[0]["p_record"]),set(record))
        self.assertIsInstance(captured[0]["p_expected_version"],int)
        for field in ("raw_congestion_observed_at","last_source_change_at","last_operational_change_at","updated_at"):
            self.assertIsInstance(captured[0]["p_record"][field],str)
            self.assertTrue(captured[0]["p_record"][field].endswith("Z"))
            self.assertEqual(saved[field],record[field])
        self.assertIsNone(captured[0]["p_record"]["reachability_observed_at"])
        self.assertIsNone(captured[0]["p_record"]["location_observed_at"])

    def test_incident_create_request_keys_and_jsonb_object_response(self):
        captured=[]
        def handler(request):
            payload=json.loads(request.content)
            captured.append(payload)
            return httpx.Response(200,json={**payload["p_record"],"_created":True})
        transport=self.make_transport(handler)
        incident={
            "incident_id":"PERSISTENCE-TEST-INC-http", "schema_version":1,
            "correlation_key":"persistence-integration:http", "primary_entity":"PERSISTENCE-TEST-CELL-http",
            "affected_entities":["PERSISTENCE-TEST-CELL-http"], "affected_devices":[],
            "trigger_event_id":"PERSISTENCE-TEST-EVT-http", "trigger_provenance":"PERSISTENCE_INTEGRATION_TEST",
            "trigger_source_timestamp":1.0, "opened_at":2.0, "updated_at":3.0,
            "severity":"test", "priority":"PERSISTENCE_TEST", "state":"DETECTED",
            "plan_version":1, "warden_decision":None, "verification_state":"PENDING",
            "recovery_state":"PENDING", "outcome":None, "closed_at":None,
            "version":0, "trace_id":"PERSISTENCE-TEST-TRACE-http",
        }
        stored,created=PostgresRepositoryBundle(transport).incidents.create_or_get_active(incident)
        self.assertEqual(set(captured[0]),{"p_record"})
        self.assertEqual(set(captured[0]["p_record"]),set(incident))
        for field in ("trigger_source_timestamp","opened_at","updated_at"):
            self.assertIsInstance(captured[0]["p_record"][field],str)
            self.assertTrue(captured[0]["p_record"][field].endswith("Z"))
            self.assertEqual(stored[field],incident[field])
        self.assertIsNone(captured[0]["p_record"]["closed_at"])
        self.assertTrue(created)

    def test_secrets_are_rejected_before_http_and_never_rendered(self):
        calls = []
        transport = self.make_transport(lambda _request: calls.append(True) or httpx.Response(200, json=[]))
        for arguments in ({"access_token":"x"},{"nested":{"oauth_state":"x"}},{"phone_number":"+99999991000"}):
            with self.assertRaises(PersistenceRpcContractFailed):
                transport.rpc("haris_read_domain", arguments)
        self.assertEqual(calls, [])
        rendered = repr(transport)
        self.assertNotIn(DUMMY_KEY, rendered)
        self.assertNotIn(PROJECT_URL, rendered)

    def test_safe_errors_emit_no_secret_or_upstream_body(self):
        upstream_secret = "upstream-sensitive-material"
        transport = self.make_transport(lambda _request: httpx.Response(500, text=upstream_secret))
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            with self.assertRaises(PersistenceTransportUnavailable) as raised:
                transport.rpc("haris_read_domain", {})
        combined = stdout.getvalue() + stderr.getvalue() + str(raised.exception)
        for sensitive in (DUMMY_KEY, upstream_secret, PROJECT_URL):
            self.assertNotIn(sensitive, combined)

    def test_default_network_gate_blocks_test_before_http(self):
        calls = []
        transport = self.make_transport(
            lambda _request: calls.append(True) or httpx.Response(200, json=[]),
            authorizer=__import__("supabase_transport").authorize_persistence_network,
        )
        with self.assertRaises(PersistenceTransportUnavailable):
            transport.rpc("haris_read_domain", {})
        self.assertEqual(calls, [])

    def test_read_only_preflight_uses_one_bounded_nonmatching_read(self):
        captured = []
        transport = self.make_transport(lambda request: captured.append(request) or httpx.Response(200, json=[]))
        self.assertEqual(transport.preflight(), [])
        self.assertEqual(len(captured), 1)
        self.assertTrue(str(captured[0].url).endswith("/rpc/haris_read_domain"))
        payload = captured[0].read().decode()
        self.assertIn('"p_limit":1', payload)
        self.assertIn("PERSISTENCE-PREFLIGHT-NO-MATCH", payload)

    def test_production_composition_builds_real_transport_without_http(self):
        configured = AppSettings(
            nac_mode="fixture", fixture_dir="fixtures", haris_persistence_mode="postgres",
            supabase_url=PROJECT_URL, supabase_key=DUMMY_KEY,
        )
        transport = build_persistence_transport(configured, RuntimeEnvironment.PRODUCTION)
        self.addCleanup(transport.close)
        self.assertIsInstance(transport, SupabaseRpcTransport)
        self.assertIsNone(transport._client)

    def test_real_qod_runtime_constructs_injected_postgres_transport_without_network(self):
        configured = AppSettings(
            nac_mode="live_write", enable_live_write_loop=True,
            haris_persistence_mode="postgres", supabase_url=PROJECT_URL,
            supabase_key=DUMMY_KEY,
        )
        fake = MockPostgresTransport()
        environment = {
            "HARIS_RUNTIME_ENV": "real_qod_validation",
            "HARIS_ALLOW_REAL_QOD_VALIDATION": "true",
            "HARIS_PERSISTENCE_MODE": "postgres",
            "SUPABASE_URL": PROJECT_URL,
        }
        with patch.dict(os.environ, environment, clear=True), patch.object(
            runtime.sys, "argv", ["harness"]
        ):
            built = build_persistence_transport(
                configured, RuntimeEnvironment.REAL_QOD_VALIDATION,
                transport_factory=lambda _settings, _hostname: fake,
            )
        self.assertIs(built, fake)
        self.assertEqual(fake.calls, [])

    def test_real_qod_persistence_network_gate_requires_dedicated_opt_in(self):
        base = {
            "HARIS_RUNTIME_ENV": "real_qod_validation",
            "HARIS_PERSISTENCE_MODE": "postgres",
            "SUPABASE_URL": PROJECT_URL,
        }
        with patch.dict(os.environ, base, clear=True), patch.object(
            runtime.sys, "argv", ["harness"]
        ), self.assertRaises(PersistenceNetworkBlocked):
            authorize_persistence_network("project.supabase.co")

        allowed = {**base, "HARIS_ALLOW_REAL_QOD_VALIDATION": "true"}
        with patch.dict(os.environ, allowed, clear=True), patch.object(
            runtime.sys, "argv", ["harness"]
        ), patch.object(runtime, "record_provider_access") as recorded:
            authorize_persistence_network("project.supabase.co")
        recorded.assert_called_once_with("supabase_rpc")

    def test_persistence_integration_network_gate_is_unchanged(self):
        environment = {
            "HARIS_RUNTIME_ENV": "persistence_integration",
            "HARIS_ALLOW_PERSISTENCE_INTEGRATION": "true",
            "HARIS_PERSISTENCE_MODE": "postgres",
            "SUPABASE_URL": PROJECT_URL,
        }
        with patch.dict(os.environ, environment, clear=True), patch.object(
            runtime.sys, "argv", ["harness"]
        ), patch.object(runtime, "record_provider_access") as recorded:
            authorize_persistence_network("project.supabase.co")
        recorded.assert_called_once_with("supabase_rpc")

    def test_offline_wrong_mode_and_wrong_host_remain_denied(self):
        valid = {
            "HARIS_RUNTIME_ENV": "real_qod_validation",
            "HARIS_ALLOW_REAL_QOD_VALIDATION": "true",
            "HARIS_PERSISTENCE_MODE": "postgres",
            "SUPABASE_URL": PROJECT_URL,
        }
        denied_environments = (
            {**valid, "HARIS_RUNTIME_ENV": "test", "HARIS_OFFLINE_TESTS": "true"},
            {**valid, "HARIS_PERSISTENCE_MODE": "memory"},
        )
        for environment in denied_environments:
            with self.subTest(environment=environment["HARIS_RUNTIME_ENV"]), patch.dict(
                os.environ, environment, clear=True
            ), patch.object(runtime.sys, "argv", ["harness"]), self.assertRaises(
                PersistenceNetworkBlocked
            ):
                authorize_persistence_network("project.supabase.co")

        with patch.dict(os.environ, valid, clear=True), patch.object(
            runtime.sys, "argv", ["harness"]
        ), self.assertRaises(PersistenceHostNotAllowed):
            authorize_persistence_network("different.supabase.co")


if __name__ == "__main__":
    unittest.main()
