"""Offline regression coverage for the Phase 8A operational API boundary."""
from __future__ import annotations

import asyncio
import logging
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
import httpx

import app as streamlit_app
import nokia_clients
import run_api
from api_security import (
    BoundedRateLimiter, OperationalAuthError, RouteClass,
    authenticate_bearer, classify_route, operational_rate_limiter,
    redact_provider_identifiers,
)
from config import AppSettings
from nokia_clients import create_fastapi_app
from platform_events import NokiaCongestionWebhook
from runtime import RuntimeEnvironment


TOKEN = "phase-8a-test-operational-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


class ApiSecurityTests(unittest.TestCase):
    def setUp(self):
        operational_rate_limiter.clear()
        nokia_clients.autonomous_demo_idempotency.clear()
        self.settings = AppSettings(
            nac_mode="fixture", haris_operational_api_token=TOKEN,
            haris_backend_api_token=TOKEN, gemini_api_key=None,
            groq_api_key=None,
        )

    def test_route_inventory_classification(self):
        self.assertEqual(classify_route("GET", "/api/nac/health"), RouteClass.PUBLIC_HEALTH)
        self.assertEqual(classify_route("GET", "/api/platform/health"), RouteClass.PUBLIC_HEALTH)
        self.assertEqual(classify_route("GET", "/api/v1/noc/snapshot"), RouteClass.OPERATIONAL_READ)
        self.assertEqual(classify_route("GET", "/api/nac/incidents/id-1"), RouteClass.OPERATIONAL_READ)
        self.assertEqual(classify_route("POST", "/api/nac/autonomous/run"), RouteClass.CONTROL_IDENTITY)
        self.assertEqual(classify_route("DELETE", "/api/nac/qos/opaque"), RouteClass.FIXTURE_MUTATION)
        self.assertEqual(classify_route("POST", "/api/events/nokia/congestion"), RouteClass.PROVIDER_CALLBACK)

    def test_public_health_and_protected_http_authentication(self):
        with patch("nokia_clients.get_settings", return_value=self.settings), TestClient(nokia_clients.app) as client:
            self.assertEqual(client.get("/api/nac/health").status_code, 200)
            self.assertEqual(client.get("/api/nac/mode").status_code, 401)
            self.assertEqual(client.get("/api/nac/mode", headers={"Authorization": "Bearer wrong"}).status_code, 403)
            accepted = client.get("/api/nac/mode", headers=AUTH)
        self.assertEqual(accepted.status_code, 200)

    def test_missing_backend_expected_token_fails_closed_before_handler(self):
        no_token = self.settings.model_copy(update={"haris_operational_api_token": None})
        with patch("nokia_clients.get_settings", return_value=no_token), patch("nokia_clients.get_api_client") as provider, TestClient(nokia_clients.app) as client:
            response = client.get("/api/nac/capabilities", headers=AUTH)
        self.assertEqual(response.status_code, 503)
        provider.assert_not_called()

    def test_authentication_failure_cannot_reach_provider_or_handler(self):
        with patch("nokia_clients.get_settings", return_value=self.settings), patch("nokia_clients.get_api_client") as provider, TestClient(nokia_clients.app) as client:
            response = client.post("/api/nac/device-status", json=["device-1"])
        self.assertEqual(response.status_code, 401)
        provider.assert_not_called()

    def test_websocket_requires_valid_bearer(self):
        with patch("nokia_clients.get_settings", return_value=self.settings), patch("run_api.get_settings", return_value=self.settings), TestClient(run_api.app) as client:
            with self.assertRaises(WebSocketDisconnect) as missing:
                with client.websocket_connect("/ws/noc"):
                    pass
            self.assertEqual(missing.exception.code, 4401)
            with self.assertRaises(WebSocketDisconnect) as invalid:
                with client.websocket_connect("/ws/noc", headers={"Authorization": "Bearer wrong"}):
                    pass
            self.assertEqual(invalid.exception.code, 4403)
            with client.websocket_connect("/ws/noc", headers=AUTH) as websocket:
                self.assertIn(websocket.receive_json()["type"], {"snapshot", "platform_state"})

    def test_provider_identifier_redaction_is_recursive_and_preserves_logical_ids(self):
        payload = {
            "incident_id": "incident-1",
            "provider_resource_id": "provider-secret",
            "nested": [{"provider_session_id": "session-secret", "cell_id": "T03"}],
            "tuple": ({"subscription_id": "subscription-secret"},),
            "credentials": {
                "access_token": "never-expose-access-token",
                "client_secret": "never-expose-client-secret",
                "phone_number": "+99999991000",
            },
            "error": "request failed at /verify?code=oauth-code&state=oauth-state",
        }
        redacted = redact_provider_identifiers(payload)
        rendered = str(redacted)
        self.assertNotIn("provider-secret", rendered)
        self.assertNotIn("session-secret", rendered)
        self.assertNotIn("subscription-secret", rendered)
        self.assertNotIn("never-expose-access-token", rendered)
        self.assertNotIn("never-expose-client-secret", rendered)
        self.assertNotIn("+99999991000", rendered)
        self.assertNotIn("oauth-code", rendered)
        self.assertNotIn("oauth-state", rendered)
        self.assertEqual(redacted["incident_id"], "incident-1")
        self.assertEqual(redacted["nested"][0]["cell_id"], "T03")
        hidden_handoff = redact_provider_identifiers(
            {"authorization_url": "https://consent.invalid/?code=secret&state=opaque"}
        )
        self.assertEqual(hidden_handoff["authorization_url"], "[REDACTED]")
        handoff = redact_provider_identifiers(
            {"authorization_url": "https://consent.invalid/?state=opaque"},
            allow_authorization_url=True,
        )
        self.assertEqual(handoff["authorization_url"], "https://consent.invalid/?state=opaque")

    def test_websocket_delta_redacts_provider_identifier(self):
        class Socket:
            def __init__(self):
                self.sent = []

            async def send_json(self, value):
                self.sent.append(value)

        socket = Socket()
        run_api._websocket_clients.add(socket)
        try:
            asyncio.run(run_api._broadcast({
                "type": "network.state.changed",
                "data": {"action_id": "action-1", "provider_resource_id": "never-expose"},
            }))
        finally:
            run_api._websocket_clients.discard(socket)
        self.assertEqual(socket.sent[0]["data"]["action_id"], "action-1")
        self.assertNotIn("never-expose", str(socket.sent))

    def test_http_response_redaction_runs_at_boundary(self):
        api = create_fastapi_app()

        @api.get("/api/v1/noc/snapshot")
        async def synthetic_snapshot():
            return {
                "action_id": "action-1", "provider_resource_id": "never-expose",
                "nested": [{"provider_subscription_id": "also-secret"}],
                "access_token": "response-token-secret",
                "diagnostic": "Bearer embedded-secret-value",
            }

        with patch("nokia_clients.get_settings", return_value=self.settings), TestClient(api) as client:
            response = client.get("/api/v1/noc/snapshot", headers=AUTH)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["action_id"], "action-1")
        self.assertNotIn("never-expose", response.text)
        self.assertNotIn("also-secret", response.text)
        self.assertNotIn("response-token-secret", response.text)
        self.assertNotIn("embedded-secret-value", response.text)

    def test_callback_shared_secret_uses_constant_time_comparison(self):
        webhook_settings = AppSettings(
            nac_mode="fixture", haris_operational_api_token=TOKEN,
            nokia_event_webhook_secret="provider-event-secret",
            gemini_api_key=None, groq_api_key=None,
        )
        event = NokiaCongestionWebhook(event_timestamp=123.0, cell_id="T03", congestion_level="High")
        with patch("run_api.get_settings", return_value=webhook_settings), patch("run_api.hmac.compare_digest", return_value=False) as compare:
            with self.assertRaises(HTTPException) as denied:
                asyncio.run(run_api.ingest_nokia_congestion(event, x_haris_event_secret="wrong"))
        self.assertEqual(denied.exception.status_code, 401)
        compare.assert_called_once()

    def test_geofence_callback_fails_closed_without_proven_provider_auth(self):
        with TestClient(nokia_clients.app) as client:
            response = client.post("/api/nac/callbacks/nokia/geofence", json={"type": "org.camaraproject.geofencing-subscriptions.v0.area-entered"})
        self.assertEqual(response.status_code, 503)

    def test_rate_limiter_is_bounded_and_recovers_after_window(self):
        limiter = BoundedRateLimiter(limit=2, window_seconds=10, max_keys=2)
        self.assertTrue(limiter.allow("p", RouteClass.OPERATIONAL_READ, "127.0.0.1", now=0))
        self.assertTrue(limiter.allow("p", RouteClass.OPERATIONAL_READ, "127.0.0.1", now=1))
        self.assertFalse(limiter.allow("p", RouteClass.OPERATIONAL_READ, "127.0.0.1", now=2))
        self.assertTrue(limiter.allow("p", RouteClass.OPERATIONAL_READ, "127.0.0.1", now=11))
        limiter.allow("other", RouteClass.OPERATIONAL_READ, "127.0.0.2", now=11)
        limiter.allow("third", RouteClass.OPERATIONAL_READ, "127.0.0.3", now=11)
        self.assertLessEqual(len(limiter._entries), 2)

    def test_rate_limit_rejection_is_sanitized(self):
        with patch("nokia_clients.get_settings", return_value=self.settings), patch.object(operational_rate_limiter, "allow", return_value=False), TestClient(nokia_clients.app) as client:
            response = client.get("/api/nac/mode", headers=AUTH)
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.json(), {"detail": "Operational API rate limit exceeded."})
        self.assertNotIn(TOKEN, response.text)

    def test_fixture_live_mutation_guard_is_unchanged(self):
        live_settings = self.settings.model_copy(update={"nac_mode": "live_read_only"})
        fake_client = MagicMock()
        fake_client.settings = live_settings
        with patch("nokia_clients.get_settings", return_value=live_settings), patch("nokia_clients.get_api_client", return_value=fake_client), TestClient(nokia_clients.app) as client:
            response = client.post("/api/nac/qos", headers=AUTH, json={"device_id": "device-1", "profile": "guaranteed", "duration_seconds": 300})
        self.assertEqual(response.status_code, 403)
        fake_client.request_qos.assert_not_called()

    def test_production_documentation_is_disabled(self):
        with patch("nokia_clients.runtime_environment", return_value=RuntimeEnvironment.PRODUCTION), TestClient(nokia_clients.app) as client:
            self.assertEqual(client.get("/docs").status_code, 404)
            self.assertEqual(client.get("/redoc").status_code, 404)
            self.assertEqual(client.get("/openapi.json").status_code, 404)

    def test_streamlit_backend_request_requires_and_sends_only_configured_token(self):
        missing = self.settings.model_copy(update={"haris_backend_url": "https://backend.invalid", "haris_backend_api_token": None})
        with patch.object(streamlit_app, "settings", missing), patch("httpx.AsyncClient") as http:
            with self.assertRaises(streamlit_app.BackendAuthenticationConfigurationError):
                asyncio.run(streamlit_app.backend_request("GET", "/api/nac/mode"))
        http.assert_not_called()

        configured = self.settings.model_copy(update={"haris_backend_url": "https://backend.invalid"})
        response = MagicMock(status_code=200)
        response.json.return_value = {"mode": "fixture"}
        response.raise_for_status.return_value = None
        async_client = AsyncMock()
        async_client.__aenter__.return_value.request.return_value = response
        async_client.__aexit__.return_value = None
        with patch.object(streamlit_app, "settings", configured), patch("httpx.AsyncClient", return_value=async_client):
            result = asyncio.run(streamlit_app.backend_request("GET", "/api/nac/mode"))
        self.assertEqual(result, {"mode": "fixture"})
        kwargs = async_client.__aenter__.return_value.request.await_args.kwargs
        self.assertEqual(kwargs["headers"], AUTH)

    def test_streamlit_backend_request_cannot_override_authorization(self):
        configured = self.settings.model_copy(update={"haris_backend_url": "https://backend.invalid"})
        response = MagicMock(status_code=200)
        response.json.return_value = {"ok": True}
        response.raise_for_status.return_value = None
        async_client = AsyncMock()
        async_client.__aenter__.return_value.request.return_value = response
        async_client.__aexit__.return_value = None
        with patch.object(streamlit_app, "settings", configured), patch("httpx.AsyncClient", return_value=async_client):
            asyncio.run(streamlit_app.backend_request(
                "POST", "/api/nac/autonomous/run",
                extra_headers={"Authorization": "Bearer attacker", "Idempotency-Key": "opaque-request-key-1234"},
            ))
        headers = async_client.__aenter__.return_value.request.await_args.kwargs["headers"]
        self.assertEqual(headers["Authorization"], f"Bearer {TOKEN}")
        self.assertEqual(headers["Idempotency-Key"], "opaque-request-key-1234")

    def test_streamlit_accepts_only_allowlisted_field_diagnostic(self):
        configured = self.settings.model_copy(update={"haris_backend_url": "https://backend.invalid"})
        response = MagicMock(status_code=500)
        response.json.return_value = {
            "status": "ERROR", "error": "FIELD_INTERVENTION_INTERNAL_ERROR",
            "stage": "FIELD_CYCLE_EXECUTION", "detail": "must-not-be-rendered",
        }
        async_client = AsyncMock()
        async_client.__aenter__.return_value.request.return_value = response
        async_client.__aexit__.return_value = None
        with patch.object(streamlit_app, "settings", configured), \
                patch("httpx.AsyncClient", return_value=async_client), \
                self.assertRaises(streamlit_app.BackendSafeDiagnosticError) as raised:
            asyncio.run(streamlit_app.backend_request(
                "POST", "/api/nac/autonomous/field-intervention-demo"
            ))
        self.assertEqual(raised.exception.stage, "FIELD_CYCLE_EXECUTION")
        self.assertNotIn("must-not-be-rendered", str(raised.exception))

    def test_streamlit_field_transport_failures_are_safely_classified(self):
        configured = self.settings.model_copy(update={"haris_backend_url": "https://backend.invalid"})
        expected = {
            401: "AUTHENTICATION_REQUIRED", 403: "ACCESS_DENIED",
            409: "REQUEST_IN_PROGRESS", 422: "REQUEST_INVALID",
            429: "RATE_LIMITED", 500: "BACKEND_INTERNAL_ERROR",
            502: "BACKEND_DEPENDENCY_UNAVAILABLE", 503: "BACKEND_NOT_READY",
        }
        for status, category in expected.items():
            response = MagicMock(status_code=status)
            response.json.return_value = {}
            async_client = AsyncMock()
            async_client.__aenter__.return_value.request.return_value = response
            async_client.__aexit__.return_value = None
            with self.subTest(status=status), patch.object(streamlit_app, "settings", configured), \
                    patch("httpx.AsyncClient", return_value=async_client), \
                    self.assertRaises(streamlit_app.BackendOperationalError) as raised:
                asyncio.run(streamlit_app.backend_request(
                    "POST", "/api/nac/autonomous/field-intervention-demo"
                ))
            self.assertEqual(raised.exception.category, category)

        for error, category in (
            (httpx.ReadTimeout("timed out"), "BACKEND_TIMEOUT"),
            (httpx.ConnectError("unreachable"), "BACKEND_UNAVAILABLE"),
        ):
            async_client = AsyncMock()
            async_client.__aenter__.return_value.request.side_effect = error
            async_client.__aexit__.return_value = None
            with self.subTest(category=category), patch.object(streamlit_app, "settings", configured), \
                    patch("httpx.AsyncClient", return_value=async_client), \
                    self.assertRaises(streamlit_app.BackendOperationalError) as raised:
                asyncio.run(streamlit_app.backend_request(
                    "POST", "/api/nac/autonomous/field-intervention-demo"
                ))
            self.assertEqual(raised.exception.category, category)
            self.assertNotIn(str(error), str(raised.exception))

    def test_streamlit_rejects_empty_or_malformed_success_response(self):
        configured = self.settings.model_copy(update={"haris_backend_url": "https://backend.invalid"})
        for returned in (ValueError("not json"), ["not", "an", "object"]):
            response = MagicMock(status_code=200)
            response.json.side_effect = returned if isinstance(returned, Exception) else None
            if not isinstance(returned, Exception):
                response.json.return_value = returned
            async_client = AsyncMock()
            async_client.__aenter__.return_value.request.return_value = response
            async_client.__aexit__.return_value = None
            with patch.object(streamlit_app, "settings", configured), \
                    patch("httpx.AsyncClient", return_value=async_client), \
                    self.assertRaises(streamlit_app.BackendOperationalError) as raised:
                asyncio.run(streamlit_app.backend_request(
                    "POST", "/api/nac/autonomous/field-intervention-demo"
                ))
            self.assertEqual(raised.exception.category, "BACKEND_RESPONSE_INVALID")

    def test_isolated_autonomous_endpoint_is_idempotent_and_fixture_only(self):
        system = MagicMock()
        system.settings = self.settings
        system.client = nokia_clients.FixtureNokiaClient(self.settings)
        system.run_cycle = AsyncMock()
        system.current_cycle_status = {
            "execution_context": "ISOLATED_FIXTURE_DEMO", "provenance": "SIMULATED",
            "warden": {"verified": True},
        }
        system._supervisory_safe.side_effect = lambda value: value
        headers = {**AUTH, "Idempotency-Key": "opaque-demo-request-12345"}
        with patch("nokia_clients.get_settings", return_value=self.settings), patch("nokia_clients._authoritative_haris_system", return_value=system), TestClient(nokia_clients.app) as client:
            first = client.post("/api/nac/autonomous/run", headers=headers)
            replay = client.post("/api/nac/autonomous/run", headers=headers)
        self.assertEqual(first.status_code, 200)
        self.assertEqual(replay.json(), first.json())
        system.run_cycle.assert_awaited_once_with(dust_advisory=True, isolated_fixture_demo=True)
        self.assertFalse(first.json()["durable_domain_write"])
        self.assertFalse(first.json()["durable_history_write"])

        non_fixture_adapter = MagicMock()
        system.client = non_fixture_adapter
        with patch("nokia_clients.get_settings", return_value=self.settings), patch("nokia_clients._authoritative_haris_system", return_value=system), TestClient(nokia_clients.app) as client:
            rejected = client.post(
                "/api/nac/autonomous/run",
                headers={**AUTH, "Idempotency-Key": "another-demo-request-123"},
            )
        self.assertEqual(rejected.status_code, 503)

    def test_isolated_autonomous_in_progress_duplicate_fails_closed(self):
        guard = nokia_clients._AutonomousDemoIdempotency(ttl_seconds=30, max_entries=2)

        async def exercise():
            self.assertIsNone(await guard.begin("opaque-concurrent-key-123"))
            with self.assertRaises(HTTPException) as duplicate:
                await guard.begin("opaque-concurrent-key-123")
            return duplicate.exception.status_code

        self.assertEqual(asyncio.run(exercise()), 409)

    def test_secret_never_appears_in_auth_error_or_logs(self):
        with self.assertLogs("haris.security-test", level="WARNING") as captured:
            logging.getLogger("haris.security-test").warning("authentication denied")
            with self.assertRaises(OperationalAuthError) as denied:
                authenticate_bearer(self.settings.haris_operational_api_token, "Bearer invalid")
        self.assertNotIn(TOKEN, str(denied.exception))
        self.assertNotIn(TOKEN, " ".join(captured.output))


if __name__ == "__main__":
    unittest.main(verbosity=2)
