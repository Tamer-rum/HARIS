import asyncio
import math
import unittest
from pathlib import Path

import app
from config import AppSettings
from memory import IncidentMemory, MemoryStore, SqliteHistoryRepository


class FinalFrontendPolishTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = Path("app.py").read_text(encoding="utf-8")

    def test_01_operational_cards_use_shared_content_structure(self):
        markup = app.operational_card("Backend Health", "HEALTHY", "All services operational", icon="server")
        self.assertIn('class="ops-card ', markup)
        self.assertIn('class="ops-card-content"', markup)
        self.assertIn("height:180px", self.source)
        self.assertIn("min-height:180px", self.source)

    def test_02_operational_decoration_is_below_text_content(self):
        self.assertIn(".ops-card-content { position:relative; z-index:2", self.source)
        self.assertIn(".ops-card-motif { z-index:0", self.source)

    def test_03_capability_cards_have_no_bottom_haze(self):
        start = self.source.index("/* Capability cards retain")
        block = self.source[start:self.source.index("/* Streamlit toggle variants", start)]
        self.assertIn(".capability-card::before { content:none !important", block)
        self.assertIn(".capability-card { box-shadow:none !important", block)
        self.assertNotIn(".capability-card:hover::before", block)

    def test_04_geofence_toggle_has_no_red_theme_rule(self):
        start = self.source.index("/* Streamlit toggle variants")
        block = self.source[start:self.source.index(".footer", start)]
        self.assertIn("rgba(65,75,85,.45)", block)
        self.assertIn("rgba(10,75,110,.48)", block)
        self.assertNotIn("#ff4d", block.lower())

    def test_05_history_compatibility_falls_back_for_plain_memory(self):
        class PlainMemory:
            pass
        self.assertEqual(app.history_storage_status(PlainMemory()), {
            "backend": "memory", "durable": False, "available": True, "status": "PROCESS_LOCAL",
        })

    def test_06_process_local_memory_status_is_truthful(self):
        store = MemoryStore(AppSettings(nac_mode="fixture", fixture_dir="fixtures"))
        self.assertEqual(store.persistence_status["backend"], "memory")
        self.assertFalse(store.persistence_status["durable"])
        self.assertTrue(store.persistence_status["available"])

    def test_07_persistent_adapter_status_is_safe(self):
        repository = SqliteHistoryRepository(":memory:")
        store = MemoryStore(AppSettings(nac_mode="fixture", fixture_dir="fixtures"), history_repository=repository)
        self.assertEqual(store.persistence_status, {
            "backend": "sqlite", "durable": True, "available": True, "status": "DURABLE",
        })

    def test_08_audit_hash_validation_semantics_remain_intact(self):
        store = MemoryStore(AppSettings(nac_mode="fixture", fixture_dir="fixtures"))
        store._incidents = []
        store._save_local_policies = lambda: None
        record = IncidentMemory(incident_id="audit", cycle_id="audit-cycle", summary="audit", storm_type="sandstorm",
            peak_congestion_level="High", peak_confidence_level=90, affected_cells=["T03"], affected_devices=[],
            actions=[], executed_actions=[], outcome="mitigated")
        asyncio.run(store.remember_incident(record))
        self.assertTrue(store.verify_audit_chain()["valid"])
        store._incidents[0].outcome = "tampered"
        self.assertFalse(store.verify_audit_chain()["valid"])

    def test_09_deployed_autonomous_run_uses_backend_authority_with_local_fallback(self):
        controls_start = self.source.index("def render_controls()")
        controls = self.source[controls_start:self.source.index("def render_", controls_start + 4)]
        self.assertIn('backend_request("POST", "/api/nac/autonomous/run")', controls)
        self.assertIn("if settings.haris_backend_url:", controls)
        self.assertIn("result = payload[\"cycle\"]", controls)
        self.assertIn("get_system().run_cycle(", controls)

    def test_10_impact_waits_for_authoritative_verification(self):
        impact_start = self.source.index("def render_impact(")
        impact = self.source[impact_start:self.source.index("def render_", impact_start + 4)]
        self.assertIn("MITIGATION IMPACT", impact)
        self.assertIn("Impact is not evaluated until an authoritative cycle reaches verification.", impact)
        self.assertNotIn("LIVE MITIGATION IMPACT", impact)

    def test_11_authoritative_metrics_distinguish_missing_from_zero(self):
        self.assertEqual(app.authoritative_metric(None, kind="confidence"), "N/A")
        self.assertEqual(app.authoritative_metric(0, kind="confidence"), "0%")
        self.assertEqual(app.authoritative_metric(None, kind="blast_radius"), "N/A")
        self.assertEqual(app.authoritative_metric(0, kind="blast_radius"), "0%")
        self.assertEqual(app.authoritative_metric(None, kind="qod_cost"), "N/A")
        self.assertEqual(app.authoritative_metric(0, kind="qod_cost"), "$0.00")
        self.assertEqual(app.authoritative_metric(None, kind="actions"), "N/A")
        self.assertEqual(app.authoritative_metric([], kind="actions"), "0")

    def test_12_authoritative_fixture_metrics_and_live_unavailable_kpis_remain_truthful(self):
        self.assertEqual(app.authoritative_metric(.86, kind="confidence"), "86%")
        self.assertEqual(app.authoritative_metric(.12, kind="blast_radius"), "12%")
        self.assertEqual(app.authoritative_metric(.75, kind="qod_cost"), "$0.75")
        self.assertEqual(app.authoritative_metric([{"kind": "qos"}, {"kind": "slice_attach"}], kind="actions"), "2")
        self.assertEqual(app.authoritative_verification_label({"verified": True}), "PASSED")
        self.assertEqual(app.authoritative_verification_label({"verified": False}), "REVIEW")
        self.assertEqual(app.authoritative_verification_label({}), "N/A")
        self.assertTrue(math.isnan(app.optional_float(None)))


if __name__ == "__main__":
    unittest.main()
