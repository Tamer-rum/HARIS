"""Mock-only tests for consent-bound pending dispatch continuation."""
import asyncio
import time
import unittest
from unittest.mock import AsyncMock, patch

from agents import HarisAgentSystem
from config import AppSettings
from dispatch import DispatchAttempt, PendingDispatchStore, pending_dispatches, trusted_dispatch_history
from fastapi import HTTPException
from memory import IncidentMemory, MemoryStore
from nokia_clients import number_verification_callback, number_verification_states, register_dispatch_resume_handler, verified_identities
from nokia_clients import FixtureNokiaClient


class DispatchContinuationTests(unittest.TestCase):
    def setUp(self):
        pending_dispatches._items = {}
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

    def test_field_intervention_automatically_starts_verification(self):
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", nac_api_token="test", nac_number_verification_redirect_uri="https://callback.example", gemini_api_key=None, groq_api_key=None)
        system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
        state = {"cycle_id": "i-start", "incident": {"incident_id": "i-start", "affected_cells": ["T03"]}, "field_intervention_site": "T03", "field_intervention_skills": ["tower-inspection"]}
        with patch("agents.start_number_verification_for_dispatch", new=AsyncMock(return_value={"authorization_url": "https://nokia.example/auth", "expires_in_seconds": "300"})) as start:
            result = asyncio.run(system._evaluate_field_intervention(state))
        start.assert_awaited_once()
        self.assertEqual(result["status"], "WAITING_FOR_IDENTITY_VERIFICATION")
        self.assertIn("authorization_url", result)

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
        self.assertEqual(system.latest_dispatch["status"], "APPROVED")
        self.assertEqual(trusted_dispatch_history.for_incident("i-sim")[-1].sim_swap_status, "NO_RECENT_SWAP")

    def test_recent_swap_falls_back_and_attempt_limit_fails_closed(self):
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", nac_api_token="test", nac_number_verification_redirect_uri="https://callback.example", trusted_dispatch_max_attempts=1, gemini_api_key=None, groq_api_key=None)
        system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
        pending = pending_dispatches.create(incident_id="i-limit", engineer_id="eng-demo-01", phone_number="+99999991000", site="T03", intervention_type="physical", ttl_seconds=60)
        with patch("agents.evaluate_trusted_dispatch_phone", new=AsyncMock(return_value={"decision":"BLOCK", "number_verified":True, "recent_sim_swap":True, "reason":"Recent SIM swap detected."})), patch("agents.start_number_verification_for_dispatch", new=AsyncMock()) as start:
            asyncio.run(system._resume_pending_dispatch(pending))
        start.assert_not_awaited()
        self.assertEqual(system.latest_dispatch["status"], "MANUAL_INTERVENTION_REQUIRED")
        self.assertEqual(trusted_dispatch_history.for_incident("i-limit")[-1].final_dispatch_status, "BLOCKED")

    def test_blocked_engineer_automatically_starts_next_eligible_engineer(self):
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", nac_api_token="test", nac_number_verification_redirect_uri="https://callback.example", trusted_dispatch_max_attempts=2, gemini_api_key=None, groq_api_key=None)
        system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
        pending = pending_dispatches.create(incident_id="i-fallback", engineer_id="eng-demo-01", phone_number="+99999991000", site="T03", intervention_type="physical", ttl_seconds=60)
        with patch("agents.evaluate_trusted_dispatch_phone", new=AsyncMock(return_value={"decision":"BLOCK", "number_verified":True, "recent_sim_swap":True, "reason":"Recent SIM swap detected."})), patch("agents.start_number_verification_for_dispatch", new=AsyncMock(return_value={"authorization_url":"https://nokia.example/auth", "expires_in_seconds":"300"})) as start:
            asyncio.run(system._resume_pending_dispatch(pending))
        start.assert_awaited_once()
        self.assertEqual(system.latest_dispatch["engineer_id"], "eng-demo-02")
        self.assertEqual(system.latest_dispatch["status"], "WAITING_FOR_IDENTITY_VERIFICATION")

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


if __name__ == "__main__": unittest.main(verbosity=2)
