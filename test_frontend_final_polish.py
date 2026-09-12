import asyncio
import math
import tomllib
import unittest
from pathlib import Path

import app
from config import AppSettings
from memory import IncidentMemory, MemoryStore, SqliteHistoryRepository


class FinalFrontendPolishTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = Path("app.py").read_text(encoding="utf-8")

    def test_streamlit_same_origin_cors_protection_is_enabled(self):
        config = tomllib.loads(Path(".streamlit/config.toml").read_text(encoding="utf-8"))
        self.assertIs(config["server"]["enableCORS"], True)

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

    def test_durable_history_and_separate_audit_truth_are_not_conflated(self):
        durable = {
            "mode": "postgres", "status": "READY", "repository_ready": True,
            "reconstruction": "COMPLETE",
        }
        caption = app.history_storage_caption(durable)
        self.assertIn("DURABLE_REPOSITORY (PostgreSQL/Supabase)", caption)
        self.assertIn("tamper-evident, append-only audit-chain", caption)
        self.assertNotIn("process-local memory", caption)
        self.assertNotIn("configure Supabase", caption)
        self.assertNotIn("immutable", caption.lower())
        self.assertNotIn("cryptographically signed", caption.lower())
        self.assertEqual(app.audit_chain_presentation({})[0], "UNAVAILABLE")
        self.assertNotEqual(app.audit_chain_presentation({})[0], "VALID")

    def test_capability_cards_prioritize_validation_truth_over_generic_ready(self):
        configured = {"status": "SUPPORTED_AND_CONFIGURED", "reason": None}
        self.assertEqual(app.capability_presentation("qod", configured)[0], "REAL_PARTIAL")
        self.assertEqual(app.capability_presentation("slicing", configured)[0], "SANDBOX_LIMITED")
        self.assertEqual(
            app.capability_presentation("geofencing", configured)[0],
            "FAIL_CLOSED_AUTH_UNPROVEN",
        )
        trusted = app.capability_presentation("trusted_dispatch", {
            "status": "PRIVILEGED_ONLY",
            "reason": "Number Verification + SIM Swap; privileged field intervention only.",
        })
        self.assertEqual(trusted[0], "PRIVILEGED ONLY")
        self.assertIn("REAL_VALIDATED / PRIVILEGED_ONLY", trusted[1])
        self.assertIn("Number Verification + SIM Swap", trusted[1])
        self.assertIn("network verification was UNCHANGED", app.capability_presentation("qod", configured)[1])
        self.assertIn("not OPERATING", app.capability_presentation("slicing", configured)[1])
        self.assertIn("numeric fixture KPI is simulated", app.capability_presentation("congestion_insights", {"status": "READ_READY"})[1])
        self.assertIn(
            '("location", "Location Retrieval")',
            Path("app.py").read_text(encoding="utf-8"),
        )

    def test_09_deployed_autonomous_run_uses_backend_authority_with_local_fallback(self):
        controls_start = self.source.index("def render_controls()")
        controls = self.source[controls_start:self.source.index("def render_", controls_start + 4)]
        self.assertIn('"POST", "/api/nac/autonomous/run"', controls)
        self.assertIn("if settings.haris_backend_url:", controls)
        self.assertIn("result = payload[\"cycle\"]", controls)
        self.assertIn("get_system().run_cycle(", controls)
        self.assertIn('extra_headers={"Idempotency-Key": request_key}', controls)
        self.assertIn("st.session_state.fixture_demo_cycle = result", controls)
        self.assertNotIn("st.session_state.last_result = result", controls)

    def test_isolated_fixture_demo_is_presented_separately_from_durable_authority(self):
        console_start = self.source.index("def render_console()")
        console = self.source[console_start:]
        self.assertIn('fixture_demo = st.session_state.get("fixture_demo_cycle")', console)
        self.assertIn("SIMULATED / FIXTURE HARIS DEMONSTRATION", console)
        self.assertIn("Authority: PROCESS-LOCAL FIXTURE DEMO", console)
        self.assertIn("Durable operational incident: NOT CREATED", console)
        self.assertIn("Durable history write: DISABLED", console)
        self.assertIn('result = (supervisory or {}).get("cycle")', console)

    def test_field_demo_uses_bounded_idempotency_and_safe_error_copy(self):
        controls_start = self.source.index("def render_controls()")
        controls = self.source[controls_start:self.source.index("def render_", controls_start + 4)]
        self.assertIn('"Idempotency-Key": request_key', controls)
        self.assertIn('field_demo_in_progress', controls)
        self.assertIn('field_demo_completed', controls)
        self.assertIn('Field intervention could not complete.', controls)
        self.assertIn('Diagnostic stage: {exc.stage}', controls)
        self.assertNotIn('Field intervention demo failed. Review the authenticated backend status.', controls)
        self.assertIn('st.session_state.field_demo_cycle', controls)
        self.assertNotIn('st.session_state.last_result = payload.get("cycle"', controls)

    def test_fixture_demo_trace_is_explicitly_simulated_and_process_local(self):
        presented = app.fixture_demo_presentation_cycle({
            "trace": [
                "12:00 | ACTUATOR: QoD created session=fixture",
                "12:01 | ACTUATOR: slice attached device=fixture",
                "12:02 | ACTUATOR: geofence created subscription=fixture",
                "12:03 | LEARN: isolated fixture result retained in process only; outcome=verified",
            ]
        })
        rendered = " ".join(presented["trace"])
        self.assertEqual(rendered.count("SIMULATED ACTUATOR:"), 3)
        self.assertIn("process-local demo incident retained only for display", rendered)
        self.assertIn("durable history disabled", rendered)

    def test_streamlit_width_deprecation_is_removed(self):
        self.assertNotIn("use_container_width=", self.source)
        self.assertIn('width="stretch"', self.source)

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

    def test_13_operational_evidence_never_falls_back_to_fixture_rows(self):
        self.assertEqual(app.congestion_map(None), {})
        payload = {
            "congestion": [{"cell_id": "T03", "congestion_level": "Low", "congestion_pct": None, "latency_ms": None, "predicted_congestion_pct": None}],
            "pre_execution_congestion": {"T03": {"congestion_level": "Medium", "congestion_pct": None, "latency_ms": None, "predicted_congestion_pct": None}},
        }
        current, baseline = app.congestion_map(payload), app.baseline_map(payload)
        self.assertEqual(current["T03"]["congestion_level"], "Low")
        self.assertEqual(baseline["T03"]["congestion_level"], "Medium")
        self.assertTrue(math.isnan(current["T03"]["latency_ms"]))
        self.assertTrue(math.isnan(current["T03"]["predicted_congestion_pct"]))
        changed = {"congestion": [{"cell_id": "T99", "congestion_level": "High"}]}
        self.assertEqual(set(app.congestion_map(changed)), {"T99"})

    def test_14_impact_uses_categorical_authoritative_evidence(self):
        impact_start = self.source.index("def render_impact(")
        impact = self.source[impact_start:self.source.index("def render_", impact_start + 4)]
        self.assertIn("CONGESTION · {safe_text(target)}", impact)
        self.assertIn("IMPROVED", impact)
        self.assertIn("available_kpis", impact)
        self.assertNotIn("fixture(\"congestion\"", impact)

    def test_15_authoritative_cycle_payload_drives_result_metrics_without_defaults(self):
        cycle = {
            "plan": {"confidence": .89, "blast_radius": .12, "expected_cost_usd": .75, "actions": [{}, {}]},
            "execution": {"executed": True, "actions": [{"device_id": "ambulance-01", "success": True}, {"device_id": "ambulance-01", "success": True}]},
            "verification": {"verified": True, "target_cells": ["T03"], "level_improved": True},
            "devices": [{"device_id": "ambulance-01", "tier": 1}],
        }
        self.assertEqual(app.authoritative_metric(cycle["plan"]["confidence"], kind="confidence"), "89%")
        self.assertEqual(app.authoritative_metric(cycle["plan"]["blast_radius"], kind="blast_radius"), "12%")
        self.assertEqual(app.authoritative_metric(cycle["plan"]["expected_cost_usd"], kind="qod_cost"), "$0.75")
        self.assertEqual(app.authoritative_metric(cycle["plan"]["actions"], kind="actions"), "2")
        self.assertEqual(app.authoritative_verification_label(cycle["verification"]), "PASSED")
        self.assertEqual(app.protected_tier1_count(cycle), "1")
        self.assertEqual(app.protected_tier1_count({"execution": {"actions": []}}), "N/A")

    def test_16_protected_tier_one_count_requires_successful_current_execution(self):
        cycle = {
            "devices": [{"device_id": "ambulance-01", "tier": 1}, {"device_id": "scada-01", "tier": 1}],
            "execution": {"actions": [
                {"device_id": "ambulance-01", "kind": "qos", "success": True},
                {"device_id": "ambulance-01", "kind": "slice_attach", "success": True},
                {"device_id": "scada-01", "kind": "qos", "success": False},
            ]},
        }
        self.assertEqual(app.protected_tier1_count(cycle), "1")

    def test_live_topology_refresh_tracks_t03_to_t05_transition(self):
        first = {
            "T03": {"nokia_congestion": "High", "freshness": "FRESH", "source": "NOKIA_LIVE"},
            "T05": {"nokia_congestion": "Low", "freshness": "FRESH", "source": "NOKIA_LIVE"},
        }
        second = {
            "T03": {"nokia_congestion": "Low", "freshness": "FRESH", "source": "NOKIA_LIVE"},
            "T05": {"nokia_congestion": "High", "freshness": "FRESH", "source": "NOKIA_LIVE"},
        }
        first_svg, second_svg = app.topology_svg(first, False), app.topology_svg(second, False)
        self.assertIn("T03</text>", first_svg)
        self.assertIn("NOKIA: HIGH / HARIS: INCIDENT_OPEN / NOKIA LIVE", first_svg)
        self.assertIn("NOKIA: LOW / HARIS: STABLE / NOKIA LIVE", second_svg)
        self.assertIn("NOKIA: HIGH / HARIS: INCIDENT_OPEN / NOKIA LIVE", second_svg)
        self.assertNotEqual(first_svg, second_svg)

    def test_live_topology_supports_multiple_affected_configured_towers(self):
        svg = app.topology_svg({
            "T03": {"nokia_congestion": "High", "freshness": "FRESH", "source": "NOKIA_LIVE"},
            "T05": {"nokia_congestion": "High", "freshness": "FRESH", "source": "NOKIA_LIVE"},
        }, False)
        self.assertEqual(svg.count("NOKIA: HIGH / HARIS: INCIDENT_OPEN / NOKIA LIVE"), 2)

    def test_stale_or_unavailable_topology_evidence_is_never_fresh(self):
        for freshness in ("STALE", "UNAVAILABLE"):
            entity = {"nokia_congestion": "High", "freshness": freshness, "source": "NOKIA_LIVE"}
            self.assertIsNone(app.current_network_level(entity))
            svg = app.topology_svg({"T03": entity}, False)
            self.assertIn(f"NOKIA: UNAVAILABLE / HARIS: STALE / NOKIA LIVE", svg)
            self.assertNotIn("NOKIA: HIGH / HARIS: INCIDENT_OPEN", svg)
        unavailable_source = {"nokia_congestion": "High", "freshness": "FRESH", "source": "UNAVAILABLE"}
        self.assertIsNone(app.current_network_level(unavailable_source))

    def test_topology_auto_refresh_is_bounded_and_does_not_trigger_execution(self):
        section_start = self.source.index("def network_presentation_fingerprint")
        section = self.source[section_start:self.source.index("# KPI impact", section_start)]
        self.assertIn("topology_svg(data=data, active=False)", section)
        monitor_start = self.source.index("@st.fragment(run_every=3)\ndef render_observation_monitor")
        monitor = self.source[monitor_start:self.source.index("def render_trusted_dispatch", monitor_start)]
        self.assertIn('"GET", "/api/nac/network-state"', monitor)
        self.assertIn('"GET", "/api/nac/observations/latest"', monitor)
        self.assertIn('st.rerun(scope="app")', monitor)
        self.assertNotIn("topology_svg(", monitor)
        for forbidden in (
            "/api/nac/autonomous/run", "run_cycle(", "run_field_intervention_demo(",
            "request_qos(", "attach_slice(", "create_geofence(", "public_dust_feed_url",
            "gemini", "groq", "crewai",
        ):
            self.assertNotIn(forbidden, section.lower())

    def test_network_presentation_fingerprint_ignores_order_and_timestamps(self):
        first = {
            "T05": {"nokia_congestion": "Low", "freshness": "FRESH", "source": "NOKIA_LIVE", "observed_at": 1},
            "T03": {"nokia_congestion": "High", "freshness": "FRESH", "source": "NOKIA_LIVE", "next_poll_at": 2},
        }
        repeated = {
            "T03": {"nokia_congestion": "High", "freshness": "FRESH", "source": "NOKIA_LIVE", "next_poll_at": 999},
            "T05": {"nokia_congestion": "Low", "freshness": "FRESH", "source": "NOKIA_LIVE", "observed_at": 999},
        }
        self.assertEqual(app.network_presentation_fingerprint(first), app.network_presentation_fingerprint(repeated))

    def test_network_presentation_fingerprint_tracks_semantic_changes(self):
        base = {"T03": {"nokia_congestion": "High", "freshness": "FRESH", "source": "NOKIA_LIVE"}}
        for changed in (
            {"T03": {"nokia_congestion": "Low", "freshness": "FRESH", "source": "NOKIA_LIVE"}},
            {"T03": {"nokia_congestion": "High", "freshness": "STALE", "source": "NOKIA_LIVE"}},
            {"T03": {"nokia_congestion": "High", "freshness": "FRESH", "source": "UNAVAILABLE"}},
        ):
            self.assertNotEqual(app.network_presentation_fingerprint(base), app.network_presentation_fingerprint(changed))


if __name__ == "__main__":
    unittest.main()
