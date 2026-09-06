"""Mock-only tests for consent-bound pending dispatch continuation."""
import asyncio
import time
import unittest
from unittest.mock import AsyncMock, patch

from agents import HarisAgentSystem
from config import AppSettings
from dispatch import DispatchAttempt, PendingDispatchStore, frontend_consent_tokens, pending_dispatches, trusted_dispatch_history
from fastapi import HTTPException
from fastapi.testclient import TestClient
from memory import IncidentMemory, MemoryStore
from nokia_clients import app as api_app, number_verification_callback, number_verification_states, register_dispatch_resume_handler, register_dispatch_system_factory, register_dispatch_verification_failure_handler, verified_identities
from nokia_clients import FixtureNokiaClient


class DispatchContinuationTests(unittest.TestCase):
    def setUp(self):
        pending_dispatches._items = {}
        frontend_consent_tokens._tokens = {}
        number_verification_states._pending = {}
        trusted_dispatch_history._attempts = []
        verified_identities._verified_at = {}

    def test_pending_dispatch_binding_expiry_and_single_use(self):
        store = PendingDispatchStore()
        pending = store.create(incident_id="i1", engineer_id="e1", phone_number="+99999991000", site="T03", intervention_type="physical", ttl_seconds=300)
        store.bind_oauth_state(pending.pending_id, "state-a")
        with self.assertRaises(ValueError): store.consume_for_resume(pending_id=pending.pending_id, engineer_id="other", phone_number="+99999991000", oauth_state="state-a")
        item = store.consume_for_resume(pending_id=pending.pending_id, engineer_id="e1", phone_number="+99999991000", oauth_state="state-a")
        self.assertEqual(item.incident_id, "i1")
        with self.assertRaises(ValueError): store.consume_for_resume(pending_id=pending.pending_id, engineer_id="e1", phone_number="+99999991000", oauth_state="state-a")
        expired = store.create(incident_id="i2", engineer_id="e2", phone_number="+99999991001", site="T03", intervention_type="physical", ttl_seconds=1)
        expired.expires_at = time.time() - 1; store._items[expired.pending_id] = expired
        with self.assertRaises(ValueError): store.consume_for_resume(pending_id=expired.pending_id, engineer_id="e2", phone_number="+99999991001", oauth_state="x")

    def test_callback_resumes_exact_bound_pending_once(self):
        pending = pending_dispatches.create(incident_id="incident-a", engineer_id="eng-a", phone_number="+99999991000", site="T03", intervention_type="physical", ttl_seconds=300)
        state = number_verification_states.create(pending.phone_number, dispatch_pending_id=pending.pending_id, engineer_id=pending.engineer_id)
        pending_dispatches.bind_oauth_state(pending.pending_id, state)
        handler = AsyncMock()
        register_dispatch_resume_handler(handler)
        class Device:
            def verify_number(self, **_): return True
        class Devices:
            def get(self, **_): return Device()
        class Client:
            def __init__(self, **_): self.devices = Devices()
        settings = AppSettings(nac_mode="fixture", nac_api_token="test", trusted_dispatch_verification_ttl_seconds=300)
        with patch("nokia_clients.get_settings", return_value=settings), patch("network_as_code.NetworkAsCodeClient", Client):
            result = asyncio.run(number_verification_callback(code="opaque-code", state=state))
        self.assertEqual(result["status"], "verified")
        handler.assert_awaited_once()
        self.assertEqual(handler.await_args.args[0].incident_id, "incident-a")
        with self.assertRaises(Exception):
            asyncio.run(number_verification_callback(code="opaque-code", state=state))

    def test_verified_false_isolated_failure_handler_not_invalid_state(self):
        pending = pending_dispatches.create(incident_id="incident-false", engineer_id="eng-a", phone_number="+99999991000", site="T03", intervention_type="physical", ttl_seconds=300)
        state = number_verification_states.create(pending.phone_number, dispatch_pending_id=pending.pending_id, engineer_id=pending.engineer_id)
        pending_dispatches.bind_oauth_state(pending.pending_id, state)
        failure = AsyncMock(); register_dispatch_verification_failure_handler(failure)
        class Device:
            def verify_number(self, **_): return False
        class Devices:
            def get(self, **_): return Device()
        class Client:
            def __init__(self, **_): self.devices = Devices()
        settings = AppSettings(nac_mode="fixture", nac_api_token="test")
        with patch("nokia_clients.get_settings", return_value=settings), patch("network_as_code.NetworkAsCodeClient", Client):
            response = asyncio.run(number_verification_callback(code="opaque", state=state))
        self.assertEqual(response["status"], "not_verified")
        failure.assert_awaited_once()
        self.assertEqual(failure.await_args.args[0].engineer_id, "eng-a")

    def test_verified_false_creates_distinct_fallback_pending_flow(self):
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", nac_api_token="test", nac_number_verification_redirect_uri="https://callback.example", trusted_dispatch_max_attempts=2, gemini_api_key=None, groq_api_key=None)
        system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
        pending = pending_dispatches.create(incident_id="i-false-fallback", engineer_id="eng-demo-01", phone_number="+99999991000", site="T03", intervention_type="physical", ttl_seconds=60)
        with patch("agents.start_number_verification_for_dispatch", new=AsyncMock(return_value={"authorization_url":"https://nokia.example/consent-b", "expires_in_seconds":"300"})) as start:
            asyncio.run(system._handle_number_verification_failure(pending))
        next_pending = start.await_args.args[0]
        self.assertNotEqual(next_pending.pending_id, pending.pending_id)
        self.assertNotEqual(next_pending.phone_number, pending.phone_number)
        self.assertEqual(system.current_dispatch_status["engineer_id"], "eng-demo-02")
        self.assertEqual(pending_dispatches.get(pending.pending_id).status, "BLOCKED")
        self.assertEqual([item.engineer_id for item in trusted_dispatch_history.for_incident("i-false-fallback")], ["eng-demo-01"])

    def test_field_intervention_automatically_starts_verification(self):
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", nac_api_token="test", nac_number_verification_redirect_uri="https://callback.example", gemini_api_key=None, groq_api_key=None)
        system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
        state = {"cycle_id": "i-start", "incident": {"incident_id": "i-start", "affected_cells": ["T03"]}, "field_intervention_site": "T03", "field_intervention_skills": ["tower-inspection"]}
        with patch("agents.start_number_verification_for_dispatch", new=AsyncMock(return_value={"authorization_url": "https://nokia.example/auth", "expires_in_seconds": "300"})) as start:
            result = asyncio.run(system._evaluate_field_intervention(state))
        start.assert_awaited_once()
        self.assertEqual(result["status"], "WAITING_FOR_IDENTITY_VERIFICATION")
        self.assertNotIn("authorization_url", result)
        self.assertEqual(system.dispatch_authorization_url, "https://nokia.example/auth")

    def test_unknown_pending_and_oauth_binding_mismatch_fail_closed(self):
        store = PendingDispatchStore()
        with self.assertRaises(ValueError):
            store.consume_for_resume(pending_id="unknown", engineer_id="e", phone_number="+999", oauth_state="state")
        pending = store.create(incident_id="i", engineer_id="e", phone_number="+99999991000", site="T03", intervention_type="physical", ttl_seconds=60)
        store.bind_oauth_state(pending.pending_id, "right-state")
        with self.assertRaises(ValueError):
            store.consume_for_resume(pending_id=pending.pending_id, engineer_id="e", phone_number="+99999991000", oauth_state="wrong-state")

    def test_callback_rejects_unknown_dispatch_without_resuming_handler(self):
        state = number_verification_states.create("+99999991000", dispatch_pending_id="unknown", engineer_id="eng")
        handler = AsyncMock(); register_dispatch_resume_handler(handler)
        class Device:
            def verify_number(self, **_): return True
        class Devices:
            def get(self, **_): return Device()
        class Client:
            def __init__(self, **_): self.devices = Devices()
        settings = AppSettings(nac_mode="fixture", nac_api_token="test")
        with patch("nokia_clients.get_settings", return_value=settings), patch("network_as_code.NetworkAsCodeClient", Client):
            with self.assertRaises(HTTPException): asyncio.run(number_verification_callback(code="opaque", state=state))
        handler.assert_not_awaited()

    def test_resume_continues_sim_swap_and_blocks_or_allows(self):
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", nac_api_token="test", gemini_api_key=None, groq_api_key=None)
        system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
        pending = pending_dispatches.create(incident_id="i-sim", engineer_id="eng-demo-01", phone_number="+99999991000", site="T03", intervention_type="physical", ttl_seconds=60)
        with patch("agents.evaluate_trusted_dispatch_phone", new=AsyncMock(return_value={"decision":"ALLOW", "number_verified":True, "recent_sim_swap":False, "reason":"clean"})):
            asyncio.run(system._resume_pending_dispatch(pending))
        self.assertEqual(system.current_dispatch_status["status"], "APPROVED")
        self.assertEqual(trusted_dispatch_history.for_incident("i-sim")[-1].sim_swap_status, "NO_RECENT_SWAP")

    def test_recent_swap_falls_back_and_attempt_limit_fails_closed(self):
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", nac_api_token="test", nac_number_verification_redirect_uri="https://callback.example", trusted_dispatch_max_attempts=1, gemini_api_key=None, groq_api_key=None)
        system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
        pending = pending_dispatches.create(incident_id="i-limit", engineer_id="eng-demo-01", phone_number="+99999991000", site="T03", intervention_type="physical", ttl_seconds=60)
        with patch("agents.evaluate_trusted_dispatch_phone", new=AsyncMock(return_value={"decision":"BLOCK", "number_verified":True, "recent_sim_swap":True, "reason":"Recent SIM swap detected."})), patch("agents.start_number_verification_for_dispatch", new=AsyncMock()) as start:
            asyncio.run(system._resume_pending_dispatch(pending))
        start.assert_not_awaited()
        self.assertEqual(system.current_dispatch_status["status"], "MANUAL_INTERVENTION_REQUIRED")
        self.assertEqual(trusted_dispatch_history.for_incident("i-limit")[-1].final_dispatch_status, "BLOCKED")

    def test_blocked_engineer_automatically_starts_next_eligible_engineer(self):
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", nac_api_token="test", nac_number_verification_redirect_uri="https://callback.example", trusted_dispatch_max_attempts=2, gemini_api_key=None, groq_api_key=None)
        system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
        pending = pending_dispatches.create(incident_id="i-fallback", engineer_id="eng-demo-01", phone_number="+99999991000", site="T03", intervention_type="physical", ttl_seconds=60)
        with patch("agents.evaluate_trusted_dispatch_phone", new=AsyncMock(return_value={"decision":"BLOCK", "number_verified":True, "recent_sim_swap":True, "reason":"Recent SIM swap detected."})), patch("agents.start_number_verification_for_dispatch", new=AsyncMock(return_value={"authorization_url":"https://nokia.example/auth", "expires_in_seconds":"300"})) as start:
            asyncio.run(system._resume_pending_dispatch(pending))
        start.assert_awaited_once()
        self.assertNotEqual(start.await_args.args[0].pending_id, pending.pending_id)
        self.assertNotEqual(start.await_args.args[0].phone_number, pending.phone_number)
        self.assertEqual(system.current_dispatch_status["engineer_id"], "eng-demo-02")
        self.assertEqual(system.current_dispatch_status["status"], "WAITING_FOR_IDENTITY_VERIFICATION")

    def test_history_audit_and_sensitive_values_are_separated(self):
        attempt = DispatchAttempt(incident_id="i-audit", engineer_id="eng", masked_phone_number="***1000", site="T03", intervention_type="physical", verification_status="VERIFIED", reason="clean", final_dispatch_status="APPROVED")
        trusted_dispatch_history.record(attempt)
        self.assertEqual(trusted_dispatch_history.for_incident("i-audit")[0].masked_phone_number, "***1000")
        store = MemoryStore(AppSettings(nac_mode="fixture", fixture_dir="fixtures", gemini_api_key=None, groq_api_key=None)); store._incidents = []; store._save_local = lambda: None
        record = IncidentMemory(incident_id="i-audit", summary="dispatch completed", storm_type="sandstorm", peak_congestion_level="High", peak_confidence_level=90, affected_cells=["T03"], affected_devices=[], actions=[], executed_actions=[], outcome="verified", audit={"trusted_dispatch":{"status":"APPROVED"}, "dispatch_history":[attempt.model_dump()]})
        asyncio.run(store.remember_incident(record))
        view = store.normalized_view(store.recent_incidents()[0])
        rendered = str(view)
        self.assertIn("APPROVED", rendered)
        for secret in ("https://nokia.example/auth", "opaque-oauth-code", "raw-state", "test-token", "+99999991000"):
            self.assertNotIn(secret, rendered)

    def test_backend_authority_uses_one_time_consent_handoff_not_public_status(self):
        class BackendSystem:
            settings = AppSettings(nac_mode="fixture")
            dispatch_authorization_url = "https://nokia.example/consent"
            current_cycle_status = {
                "final_status": "waiting_for_identity_verification",
                "trusted_dispatch": {"pending_id": "pending-1", "incident_id": "incident-1", "status": "WAITING_FOR_IDENTITY_VERIFICATION", "masked_phone_number": "***1000"},
            }
            current_dispatch_status = current_cycle_status["trusted_dispatch"]
            current_supervisory_status = {
                "cycle": current_cycle_status,
                "active_incident": {"incident_id": "incident-1"},
                "dispatch_history": [{"engineer_id": "eng-demo-01", "masked_phone_number": "***1000", "verification_status": "WAITING_FOR_IDENTITY_VERIFICATION"}],
                "audit": {"chain": {"valid": True, "records": 1}, "records": [{"cycle_id": "cycle-1", "incident_id": "incident-1", "outcome": "waiting_for_identity_verification"}]},
            }
            def __init__(self): self.calls = 0
            async def run_field_intervention_demo(self): self.calls += 1
        backend = BackendSystem()
        register_dispatch_system_factory(lambda: backend)
        with TestClient(api_app) as client:
            response = client.post("/api/nac/autonomous/field-intervention-demo")
            status = client.get("/api/nac/autonomous/status")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(backend.calls, 1)
        self.assertEqual(response.json()["cycle"]["trusted_dispatch"]["masked_phone_number"], "***1000")
        self.assertNotIn("authorization_url", response.json()["cycle"])
        self.assertNotIn("authorization_url", status.json())
        self.assertNotIn("consent_action_token", status.json())
        self.assertEqual(status.json()["active_incident"]["incident_id"], "incident-1")
        self.assertEqual(status.json()["dispatch_history"][0]["masked_phone_number"], "***1000")
        token = response.json()["consent_action_token"]
        workflow = response.json()["workflow_session_token"]
        with TestClient(api_app) as client:
            handoff = client.post("/api/nac/autonomous/consent-action", json={"action_token": token})
            replay = client.post("/api/nac/autonomous/consent-action", json={"action_token": token})
            wrong = client.post("/api/nac/autonomous/consent-action", json={"action_token": "x" * 32})
            refreshed = client.post("/api/nac/autonomous/consent-action-token", json={"workflow_session_token": workflow})
        self.assertEqual(handoff.status_code, 200)
        self.assertEqual(handoff.json()["authorization_url"], "https://nokia.example/consent")
        self.assertEqual(replay.status_code, 403)
        self.assertEqual(wrong.status_code, 403)
        self.assertEqual(refreshed.status_code, 200)

    def test_backend_supervisory_status_keeps_waiting_incident_and_audits_callback_transition(self):
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", nac_api_token="test", gemini_api_key=None, groq_api_key=None)
        memory = MemoryStore(settings); memory._incidents = []; memory._save_local = lambda: None
        system = HarisAgentSystem(FixtureNokiaClient(settings), memory=memory, settings=settings)
        with patch("agents.start_number_verification_for_dispatch", new=AsyncMock(return_value={"authorization_url": "https://nokia.example/initial", "expires_in_seconds": "300"})):
            initial = asyncio.run(system.run_field_intervention_demo())
        incident_id = initial["incident"]["incident_id"]
        pending = pending_dispatches.create(incident_id=incident_id, engineer_id="eng-demo-01", phone_number="+99999991000", site="T03", intervention_type="physical", ttl_seconds=60)
        system._latest_dispatch = {"pending_id": pending.pending_id, "incident_id": incident_id, "engineer_id": "eng-demo-01", "masked_phone_number": "***1000", "status": "WAITING_FOR_IDENTITY_VERIFICATION", "reason": "Fresh Number Verification consent is required."}
        waiting = system.current_supervisory_status
        self.assertEqual(waiting["active_incident"]["incident_id"], incident_id)
        self.assertEqual(waiting["cycle"]["trusted_dispatch"]["incident_id"], incident_id)
        before = len(waiting["audit"]["records"])
        with patch("agents.evaluate_trusted_dispatch_phone", new=AsyncMock(return_value={"decision": "BLOCK", "number_verified": True, "recent_sim_swap": True, "reason": "Recent SIM swap detected."})), patch("agents.start_number_verification_for_dispatch", new=AsyncMock(return_value={"authorization_url": "https://nokia.example/consent", "expires_in_seconds": "300"})):
            asyncio.run(system._resume_pending_dispatch(pending))
        after = system.current_supervisory_status
        self.assertEqual(after["cycle"]["trusted_dispatch"]["engineer_id"], "eng-demo-02")
        self.assertEqual(after["cycle"]["trusted_dispatch"]["status"], "WAITING_FOR_IDENTITY_VERIFICATION")
        self.assertGreater(len(after["audit"]["records"]), before)
        self.assertTrue(after["audit"]["chain"]["valid"])
        rendered = str(after)
        for secret in ("https://nokia.example/consent", "+99999991000", "oauth_state", "access_token"):
            self.assertNotIn(secret, rendered)


if __name__ == "__main__": unittest.main(verbosity=2)
