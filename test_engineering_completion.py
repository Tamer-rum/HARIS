"""Focused tests for forecasting, memory influence, API metadata, and scheduling."""
import asyncio
import json
from unittest.mock import patch
import unittest

from fastapi.testclient import TestClient

from agents import HarisAgentSystem, Incident, ReasoningRouter
from playbooks import Action
from config import AppSettings
from memory import IncidentMemory, MemoryStore
from nokia_clients import CongestionReading, FixtureNokiaClient, app
from prediction import RiskForecaster
from scheduler import HarisScheduler


class EngineeringCompletionTests(unittest.TestCase):
    @staticmethod
    def reading(cell_id, level, confidence):
        return CongestionReading(
            cell_id=cell_id, congestion_level=level, confidence_level=confidence,
            interval_start="2026-01-01T00:00:00Z", interval_stop="2026-01-01T00:05:00Z",
        )

    def test_rising_categorical_congestion_with_dust_increases_risk(self):
        forecast = RiskForecaster()
        forecast.predict([self.reading("T03", "Low", 80)], False)
        result = forecast.predict([self.reading("T03", "Medium", 80)], True)
        self.assertEqual(result.predicted_risk_level, "High")
        self.assertGreaterEqual(result.degradation_probability, 0.70)

    def test_stable_and_missing_evidence_are_conservative(self):
        forecast = RiskForecaster()
        stable = forecast.predict([self.reading("T01", "None", 90)], False)
        missing = forecast.predict([], False)
        self.assertEqual(stable.predicted_risk_level, "Low")
        self.assertLess(missing.confidence, stable.confidence)

    def test_prior_verified_corridor_incident_changes_triage_confidence(self):
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", gemini_api_key=None, groq_api_key=None)
        memory = MemoryStore(settings)
        memory._incidents = [IncidentMemory(
            incident_id="prior-t03", summary="sandstorm T03 verified", storm_type="sandstorm",
            peak_congestion_level="High", peak_confidence_level=90,
            affected_cells=["T03"], affected_devices=["ambulance-01"],
            actions=["qos"], executed_actions=["qos"], outcome="verified",
        )]
        async def no_write(_incident):
            return None
        memory.remember_incident = no_write
        system = HarisAgentSystem(FixtureNokiaClient(settings), memory=memory, settings=settings)
        state = asyncio.run(system.graph.ainvoke({"cycle_id": "memory-test", "dust_advisory": True, "trace": []}))
        self.assertTrue(state["memory_context"])
        self.assertIn("prior-t03", state["plan"]["rationale"])
        self.assertGreaterEqual(state["plan"]["confidence"], 0.89)

    def test_deployment_metadata_endpoints(self):
        with TestClient(app) as client:
            self.assertEqual(client.get("/api/nac/health").status_code, 200)
            self.assertEqual(client.get("/api/nac/mode").status_code, 200)
            self.assertEqual(client.get("/api/nac/capabilities").status_code, 200)
            self.assertEqual(client.post("/api/nac/callbacks/nokia/geofence", json={"type": "org.camaraproject.geofencing-subscriptions.v0.area-entered"}).status_code, 200)
            self.assertEqual(client.post("/api/nac/callbacks/nokia/geofence", json={"type": "unexpected"}).status_code, 422)

    def test_scheduler_prevents_duplicate_start_and_survives_cycle_failure(self):
        class FakeSystem:
            def __init__(self): self.calls = 0
            async def run_cycle(self, dust_advisory=True):
                self.calls += 1
                if self.calls == 1: raise RuntimeError("expected test failure")
                return {"cycle_id": str(self.calls)}
        async def exercise():
            settings = AppSettings(nac_mode="fixture", cycle_seconds=1, gemini_api_key=None, groq_api_key=None)
            scheduler = HarisScheduler(FakeSystem(), settings)
            self.assertTrue(await scheduler.start())
            self.assertFalse(await scheduler.start())
            await asyncio.sleep(1.1)
            await scheduler.stop()
            self.assertGreaterEqual(scheduler.cycles_completed, 1)
        asyncio.run(exercise())

    def test_environment_source_fixture_cached_and_unavailable(self):
        system = HarisAgentSystem(FixtureNokiaClient(AppSettings(nac_mode="fixture", fixture_dir="fixtures", gemini_api_key=None, groq_api_key=None)), settings=AppSettings(nac_mode="fixture", fixture_dir="fixtures", gemini_api_key=None, groq_api_key=None))
        self.assertEqual(asyncio.run(system._dust_advisory(True))[1], "FIXTURE")
        self.assertEqual(asyncio.run(system._dust_advisory(None))[1], "UNAVAILABLE")
        system._cached_environment = True
        system.settings.public_dust_feed_url = "http://invalid"
        self.assertEqual(asyncio.run(system._dust_advisory(None))[1], "CACHED")

    def test_environment_source_live(self):
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", public_dust_feed_url="https://weather.example", gemini_api_key=None, groq_api_key=None)
        system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
        class Response:
            def raise_for_status(self): pass
            def json(self): return {"dust_advisory": True}
        class Client:
            def __init__(self, **_): pass
            async def __aenter__(self): return self
            async def __aexit__(self, *_): pass
            async def get(self, _): return Response()
        with patch("httpx.AsyncClient", Client):
            self.assertEqual(asyncio.run(system._dust_advisory(None))[1], "LIVE")

    def test_old_audit_normalization_does_not_mutate_source(self):
        raw = {"incident_id": "old", "outcome": "verified", "affected_cells": ["T03"]}
        view = MemoryStore.normalized_view(raw)
        self.assertEqual(view["cycle_id"], "N/A")
        self.assertIsNone(view["prediction"])
        self.assertNotIn("audit", raw)

    def test_audit_hash_chain_detects_tampering(self):
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", gemini_api_key=None, groq_api_key=None)
        store = MemoryStore(settings)
        store._incidents = []
        async def add(identifier):
            await store.remember_incident(IncidentMemory(incident_id=identifier, summary=identifier, storm_type="sandstorm", peak_congestion_level="High", peak_confidence_level=90, affected_cells=["T03"], affected_devices=[], actions=[], executed_actions=[], outcome="verified"))
        asyncio.run(add("one")); asyncio.run(add("two"))
        self.assertTrue(store.verify_audit_chain()["valid"])
        store._incidents[0].summary = "tampered"
        self.assertFalse(store.verify_audit_chain()["valid"])

    def test_streamlit_system_memory_accepts_empty_and_legacy_audit_chain(self):
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", gemini_api_key=None, groq_api_key=None)
        # Same HarisAgentSystem/MemoryStore construction used by app.get_system().
        system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
        system.memory._incidents = []
        self.assertEqual(system.memory.verify_audit_chain(), {"valid": True, "records": 0})
        system.memory._incidents = [IncidentMemory(incident_id="legacy", summary="legacy", storm_type="sandstorm", peak_congestion_level="High", peak_confidence_level=1, affected_cells=[], affected_devices=[], actions=[], executed_actions=[], outcome="verified")]
        self.assertEqual(system.memory.verify_audit_chain()["reason"], "legacy_record_without_hash")

    def test_streamlit_factory_memory_exposes_audit_chain(self):
        # app.get_system constructs the production Streamlit dependency chain.
        import app
        system = app.get_system()
        self.assertTrue(hasattr(system.memory, "verify_audit_chain"))
        system.memory._incidents = []
        self.assertEqual(system.memory.verify_audit_chain(), {"valid": True, "records": 0})

    def test_streamlit_system_exposes_safe_policy_and_dispatch_accessors(self):
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", gemini_api_key=None, groq_api_key=None)
        system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
        self.assertTrue(system.geofencing_monitoring_enabled)
        system.set_geofencing_monitoring(False)
        self.assertFalse(system.geofencing_monitoring_enabled)
        self.assertEqual(system.current_dispatch_status, {})
        system._latest_dispatch = {"status": "WAITING_FOR_IDENTITY_VERIFICATION", "masked_phone_number": "***1000", "authorization_url": "https://private.example/state"}
        self.assertEqual(system.current_dispatch_status["status"], "WAITING_FOR_IDENTITY_VERIFICATION")
        self.assertNotIn("authorization_url", system.current_dispatch_status)
        self.assertEqual(system.dispatch_authorization_url, "https://private.example/state")

    def test_streamlit_five_sections_render_with_empty_status(self):
        import app
        # Bare-mode rendering exercises the same dependencies used by Streamlit;
        # no endpoint or Nokia operation is invoked by these presentation calls.
        app.render_overview(None)
        app.render_network_intelligence(None)
        app.render_controls()
        app.render_decision_engine(None)
        app.render_impact(None)
        app.render_playbook_and_feed(None)
        app.render_trusted_dispatch(None)
        app.render_history_audit(None)

    def test_streamlit_nullable_backend_status_is_authoritative_and_safe(self):
        import app
        class FakeMemory:
            def recent_incidents(self): return []
            def verify_audit_chain(self): return {"valid": True, "records": 0}
            def normalized_view(self, item): return item
        class FakeSystem:
            geofencing_monitoring_enabled = True
            current_dispatch_status = {}
            dispatch_authorization_url = None
            memory = FakeMemory()
            class client:
                name = "fixture"
                @staticmethod
                def capability_report(): return {}
            def set_geofencing_monitoring(self, _enabled): pass
        cycle = {
            "final_status": None, "incident": None, "prediction": None,
            "warden": None, "plan": None, "execution": None,
            "verification": None, "rollback": None, "learning": None,
            "events": None, "trace": None, "active_playbook": None,
            "trusted_dispatch": {
                "status": "WAITING_FOR_IDENTITY_VERIFICATION",
                "incident_id": "inc-wait", "engineer_id": "eng-demo-02",
                "masked_phone_number": "***1001",
            },
        }
        supervisory = {
            "haris_state": "WAITING_FOR_IDENTITY_VERIFICATION",
            "active_incident": {"incident_id": "inc-wait"},
            "dispatch_history": [],
            "audit": {"chain": {"valid": True, "records": 0}, "records": []},
        }
        fake = FakeSystem()
        with patch.object(app, "get_system", return_value=fake), patch.object(app.st, "caption") as caption:
            app.render_status_bar(cycle, supervisory)
            rendered = " ".join(str(call.args[0]) for call in caption.call_args_list if call.args)
            self.assertIn("WAITING_FOR_IDENTITY_VERIFICATION", rendered)
            self.assertIn("inc-wait", rendered)
            # Exercise all five supervisory sections with the same nullable
            # backend payload; none may assume a completed cycle.
            app.render_overview(cycle, supervisory)
            app.render_network_intelligence(cycle)
            app.render_controls()
            app.render_decision_engine(cycle)
            app.render_impact(cycle)
            app.render_playbook_and_feed(cycle)
            app.render_trusted_dispatch(cycle, supervisory)
            app.render_history_audit(cycle, supervisory)
        self.assertEqual(app.authoritative_haris_state({}, {}), "READY")
        self.assertEqual(app.authoritative_haris_state({"final_status": "mitigated"}, {}), "MITIGATED")
        self.assertEqual(app.authoritative_haris_state({"final_status": "rolled_back_safely"}, {}), "ROLLED_BACK_SAFELY")

    def test_overview_notifications_require_dispatch_evidence_and_pending_semantics(self):
        import app
        self.assertEqual(app.overview_notifications({}, {}), [])
        cycle = {
            "final_status": "waiting_for_identity_verification",
            "trusted_dispatch": {
                "status": "WAITING_FOR_IDENTITY_VERIFICATION",
                "fallback_from": "eng-demo-01",
            },
        }
        supervisory = {
            "dispatch_history": [{
                "engineer_id": "eng-demo-01", "sim_swap_status": "RECENT_SWAP",
                "warden_decision": "BLOCK",
            }],
        }
        notices = app.overview_notifications(cycle, supervisory)
        self.assertEqual([item[1] for item in notices], ["SECURITY ALERT", "AUTOMATIC FALLBACK", "ACTION REQUIRED"])
        self.assertIn("Recent SIM swap", notices[0][2])
        self.assertIn("Awaiting secure Nokia", notices[-1][2])

    def test_api_startup_is_lazy_when_scheduler_disabled(self):
        import run_api
        run_api._system = None
        run_api._scheduler = None
        run_api._scheduler_task = None
        settings = AppSettings(nac_mode="fixture", enable_continuous_loop=False, gemini_api_key=None, groq_api_key=None)
        with patch("run_api.get_settings", return_value=settings), patch("run_api.HarisAgentSystem") as system:
            asyncio.run(run_api.start_haris_scheduler())
        system.assert_not_called()

    def test_reasoning_router_mocked_success_malformed_and_failure_fallback(self):
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", gemini_api_key=None, groq_api_key=None)
        router = ReasoningRouter(settings)
        incident = Incident(storm_advisory=True, peak_congestion_level="High", peak_confidence_level=90, affected_cells=["T03"], affected_devices=["ambulance-01"], severity="critical")
        class Good:
            async def ainvoke(self, _): return type("R", (), {"content": json.dumps({"confidence": .9, "benefit": .8, "rationale": "mock"})})()
        class Bad:
            async def ainvoke(self, _): raise TimeoutError()
        router.gemini = Good()
        self.assertTrue(asyncio.run(router.assess(incident, [], []))["ai_planner_used"])
        router.gemini = Bad()
        self.assertTrue(asyncio.run(router.assess(incident, [], []))["fallback_used"])
        router.gemini = None
        router.groq = Good()
        self.assertTrue(asyncio.run(router.assess(incident, [], []))["ai_planner_used"])
        router.groq = Bad()
        self.assertTrue(asyncio.run(router.assess(incident, [], []))["fallback_used"])

    def test_crewai_mocked_success_filters_unauthorized_and_failure_falls_back(self):
        settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures", gemini_api_key=None, groq_api_key=None)
        system = HarisAgentSystem(FixtureNokiaClient(settings), settings=settings)
        system.crewai_agents = {"TRIAGE": object(), "WARDEN": object()}
        incident = Incident(storm_advisory=True, peak_congestion_level="High", peak_confidence_level=90, affected_cells=["T03"], affected_devices=["ambulance-01"], severity="critical")
        class FakeCrew:
            def __init__(self, **_): pass
            def kickoff(self): return json.dumps({"recommended_action_order": ["qos", "unauthorized"], "confidence_modifier": .01})
        with patch("agents.Task", lambda **_: object()), patch("agents.Crew", FakeCrew):
            advisory = asyncio.run(system._crew_advisory(incident, [Action("qos", "ambulance-01", {}, "test")], []))
        self.assertTrue(advisory["used"])
        self.assertEqual(advisory["advisory"]["recommended_action_order"], ["qos"])
        class BrokenCrew:
            def __init__(self, **_): pass
            def kickoff(self): raise RuntimeError("mock failure")
        with patch("agents.Task", lambda **_: object()), patch("agents.Crew", BrokenCrew):
            self.assertTrue(asyncio.run(system._crew_advisory(incident, [], []))["fallback"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
