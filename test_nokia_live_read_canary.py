import asyncio
import contextlib
import io
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

from external import nokia_live_read_canary as canary
import nokia_clients
from config import AppSettings
from runtime import RuntimeEnvironment


def valid_environment():
    return {
        "HARIS_RUNTIME_ENV": "external_integration",
        "HARIS_ALLOW_EXTERNAL_TESTS": "true",
        "NAC_MODE": "live_read_only",
        "NOKIA_OBSERVATION_ENABLED": "false",
        "ENABLE_CONTINUOUS_LOOP": "false",
        "ENABLE_LIVE_WRITE_LOOP": "false",
        "NAC_API_TOKEN": "test-only-placeholder",
    }


def valid_settings():
    return SimpleNamespace(
        nac_mode="live_read_only", nokia_observation_enabled=False,
        enable_continuous_loop=False, enable_live_write_loop=False,
        nac_api_token=object(),
    )


_DEFAULT_PAYLOAD = object()


class FakeClient:
    def __init__(self, failures=None, payload=_DEFAULT_PAYLOAD):
        self.calls = []
        self.failures = failures or {}
        self.payload = [object()] if payload is _DEFAULT_PAYLOAD else payload

    async def _read(self, name, target):
        self.calls.append((name, tuple(target)))
        failure = self.failures.get(name)
        if failure:
            raise failure
        return self.payload

    async def congestion_insights(self, value): return await self._read("congestion", value)
    async def device_status(self, value): return await self._read("reachability", value)
    async def location_retrieval(self, value): return await self._read("location", value)

    async def request_qos(self, *args): raise AssertionError("mutation reached")
    async def release_qos(self, *args): raise AssertionError("mutation reached")
    async def attach_slice(self, *args): raise AssertionError("mutation reached")
    async def detach_slice(self, *args): raise AssertionError("mutation reached")
    async def create_geofence(self, *args): raise AssertionError("mutation reached")
    async def delete_geofence(self, *args): raise AssertionError("mutation reached")


class NokiaLiveReadCanaryTests(unittest.TestCase):
    def run_it(self, environment=None, client=None, settings_factory=valid_settings):
        client = client or FakeClient()
        lines, code = asyncio.run(canary.run_canary(
            environment=valid_environment() if environment is None else environment,
            settings_factory=settings_factory,
            client_factory=lambda _settings: client,
        ))
        return lines, code, client

    def test_every_activation_guard_fails_before_client_or_provider(self):
        invalid = {
            "HARIS_RUNTIME_ENV": "production",
            "HARIS_ALLOW_EXTERNAL_TESTS": "false",
            "NAC_MODE": "fixture",
            "NOKIA_OBSERVATION_ENABLED": "true",
            "ENABLE_CONTINUOUS_LOOP": "true",
            "ENABLE_LIVE_WRITE_LOOP": "true",
            "NAC_API_TOKEN": "",
        }
        for key, value in invalid.items():
            with self.subTest(key=key):
                environment = valid_environment()
                environment[key] = value
                constructions = []
                lines, code = asyncio.run(canary.run_canary(
                    environment=environment,
                    settings_factory=lambda: constructions.append("settings"),
                    client_factory=lambda _: constructions.append("client"),
                ))
                self.assertEqual(code, 2)
                self.assertEqual(constructions, [])
                self.assertIn("EXPLICIT_READS_ATTEMPTED=0", lines)

    def test_settings_guard_rechecks_effective_configuration(self):
        bad = valid_settings()
        bad.enable_live_write_loop = True
        lines, code, client = self.run_it(settings_factory=lambda: bad)
        self.assertEqual(code, 2)
        self.assertEqual(client.calls, [])
        self.assertIn("PROVIDER_MUTATIONS=0", lines)

    def test_success_calls_exactly_three_allowlisted_reads(self):
        lines, code, client = self.run_it()
        self.assertEqual(code, 0)
        self.assertEqual(client.calls, [
            ("congestion", ("T03",)),
            ("reachability", ("ambulance-01",)),
            ("location", ("ambulance-01",)),
        ])
        self.assertIn("EXPLICIT_READS_ATTEMPTED=3", lines)
        for capability in ("CONGESTION", "REACHABILITY", "LOCATION"):
            self.assertIn(f"{capability}=SUCCESS", lines)
            self.assertIn(f"{capability}_PROVENANCE=NOKIA_LIVE", lines)

    def test_budget_and_source_exclude_mutation_and_runtime_composition(self):
        with open(canary.__file__, encoding="utf-8") as handle:
            source = handle.read().lower()
        self.assertEqual(canary.MAX_EXPLICIT_READS, 3)
        for forbidden in (
            "durableactionexecutionservice", "observationstore", "harisscheduler",
            "runtimeeventingestor", "durableoutboxwakeupconsumer", "harisagentsystem",
            "langgraph", "crewai", "gemini", "groq", "public_dust_feed_url",
            "request_qos", "release_qos", "attach_slice", "detach_slice",
            "create_geofence", "delete_geofence", "supabase", "postgres",
        ):
            self.assertNotIn(forbidden, source)

    def test_capability_failure_can_continue_but_never_exceeds_budget(self):
        client = FakeClient(failures={"congestion": TimeoutError("private timeout detail")})
        lines, code, client = self.run_it(client=client)
        self.assertEqual(code, 0)
        self.assertEqual(len(client.calls), 3)
        self.assertIn("CONGESTION=TIMEOUT", lines)
        self.assertNotIn("CONGESTION_PROVENANCE=NOKIA_LIVE", lines)
        self.assertIn("REACHABILITY=SUCCESS", lines)

    def test_exception_taxonomy_is_symbolic_and_value_blind(self):
        cases = (
            (type("AuthenticationException", (Exception,), {})(), "UNAUTHORIZED"),
            (type("RateLimitError", (Exception,), {})(), "RATE_LIMITED"),
            (TimeoutError(), "TIMEOUT"),
            (type("NotFound", (Exception,), {})(), "UNSUPPORTED_IDENTITY"),
            (type("APIConnectionError", (Exception,), {})(), "UNAVAILABLE"),
            (type("ServiceError", (Exception,), {})(), "UNAVAILABLE"),
            (type("APIError", (Exception,), {})(), "ERROR_SANITIZED"),
            (RuntimeError("provider-secret-body"), "ERROR_SANITIZED"),
        )
        for exception, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(canary.classify_exception(exception), expected)

    def test_http_status_taxonomy_is_sanitized(self):
        for status, expected in ((401, "UNAUTHORIZED"), (403, "UNAUTHORIZED"),
                                 (404, "UNSUPPORTED_IDENTITY"), (429, "RATE_LIMITED"),
                                 (503, "UNAVAILABLE"), (500, "ERROR_SANITIZED")):
            exc = type("ProviderError", (Exception,), {"status_code": status})("raw-private-body")
            self.assertEqual(canary.classify_exception(exc), expected)

    def test_empty_or_malformed_read_is_unavailable_without_provenance(self):
        for payload in ([], {}, None, "raw-provider-value"):
            with self.subTest(payload=type(payload).__name__):
                lines, _, _ = self.run_it(client=FakeClient(payload=payload))
                self.assertIn("CONGESTION=UNAVAILABLE", lines)
                self.assertNotIn("CONGESTION_PROVENANCE=NOKIA_LIVE", lines)

    def test_raw_results_coordinates_identifiers_and_exceptions_never_render(self):
        private = "PRIVATE_PROVIDER_BODY_LATITUDE_LONGITUDE_PHONE_MSISDN"
        client = FakeClient(failures={"location": RuntimeError(private)})
        lines, _, _ = self.run_it(client=client)
        output = "\n".join(lines)
        self.assertNotIn(private, output)
        for forbidden in ("latitude", "longitude", "+999", "msisdn", "authorization", "token"):
            self.assertNotIn(forbidden, output.lower())

    def test_output_contract_always_reports_zero_mutation_and_persistence(self):
        for environment in (valid_environment(), {}):
            lines, _, _ = self.run_it(environment=environment)
            self.assertIn("PROVIDER_MUTATIONS=0", lines)
            self.assertIn("PERSISTENCE_WRITES=0", lines)
            attempted = [line for line in lines if line.startswith("EXPLICIT_READS_ATTEMPTED=")]
            self.assertEqual(len(attempted), 1)
            self.assertIn(int(attempted[0].split("=", 1)[1]), range(4))

    def test_main_prints_only_sanitized_lines(self):
        original = canary.run_canary
        async def fake_run():
            return ["CONGESTION=SUCCESS", "CONGESTION_PROVENANCE=NOKIA_LIVE",
                    "EXPLICIT_READS_ATTEMPTED=1", "PROVIDER_MUTATIONS=0",
                    "PERSISTENCE_WRITES=0"], 0
        canary.run_canary = fake_run
        try:
            stream = io.StringIO()
            with contextlib.redirect_stdout(stream), contextlib.redirect_stderr(stream):
                self.assertEqual(canary.main(), 0)
            self.assertNotIn("PRIVATE", stream.getvalue())
        finally:
            canary.run_canary = original


class NokiaLiveReadCanaryEndpointTests(unittest.TestCase):
    def setUp(self):
        nokia_clients._live_read_canary_executed = False
        nokia_clients.operational_rate_limiter.clear()
        self.token = "test-only-operational-token"
        self.settings = AppSettings(
            nac_mode="fixture",
            haris_operational_api_token=self.token,
            nac_api_token="test-only-nokia-placeholder",
            nokia_observation_enabled=False,
            enable_continuous_loop=False,
            enable_live_write_loop=False,
        )

    def _request(self, *, authorization=None, client=None):
        headers = {"Authorization": authorization} if authorization else {}
        fake = client or FakeClient()
        with patch("nokia_clients.get_settings", return_value=self.settings), \
                patch("nokia_clients.runtime_environment", return_value=RuntimeEnvironment.PRODUCTION), \
                patch("nokia_clients.LiveNokiaClient", return_value=fake), \
                TestClient(nokia_clients.app) as api:
            response = api.post("/api/nac/admin/live-read-canary", headers=headers)
        return response, fake

    def test_endpoint_requires_existing_operational_authentication(self):
        missing, client = self._request()
        self.assertEqual(missing.status_code, 401)
        self.assertEqual(client.calls, [])
        invalid, client = self._request(authorization="Bearer invalid")
        self.assertEqual(invalid.status_code, 403)
        self.assertEqual(client.calls, [])

    def test_valid_auth_runs_only_three_reads_and_returns_sanitized_contract(self):
        response, client = self._request(authorization=f"Bearer {self.token}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(client.calls, [
            ("congestion", ("T03",)),
            ("reachability", ("ambulance-01",)),
            ("location", ("ambulance-01",)),
        ])
        body = response.json()
        self.assertEqual(body["explicit_reads"], 3)
        self.assertEqual(body["provider_mutations"], 0)
        for name in ("congestion", "reachability", "location"):
            self.assertEqual(body[name], {"classification": "SUCCESS", "provenance": "NOKIA_LIVE"})
        self.assertNotIn("test-only", response.text)
        self.assertNotIn("latitude", response.text.lower())

    def test_timeout_and_unauthorized_are_classified_without_raw_error(self):
        private = "raw-provider-token-coordinate"
        client = FakeClient(failures={
            "congestion": TimeoutError(private),
            "reachability": type("AuthenticationException", (Exception,), {})(private),
        })
        response, client = self._request(
            authorization=f"Bearer {self.token}", client=client,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["congestion"]["classification"], "TIMEOUT")
        self.assertEqual(response.json()["reachability"]["classification"], "UNAUTHORIZED")
        self.assertNotIn(private, response.text)

    def test_endpoint_is_process_local_one_shot(self):
        first, client = self._request(authorization=f"Bearer {self.token}")
        second, _ = self._request(authorization=f"Bearer {self.token}", client=client)
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 409)
        self.assertEqual(len(client.calls), 3)


if __name__ == "__main__":
    unittest.main()
