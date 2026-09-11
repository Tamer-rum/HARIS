"""Offline tests for Phase 8B production persistence and readiness gates."""
from __future__ import annotations

import os
from pathlib import Path
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import runtime
import run_api
from config import AppSettings
from platform_lifecycle import PlatformLifecycle, PlatformLifecycleState
from postgres_persistence import (
    PersistenceHostNotAllowed, PersistenceNetworkBlocked,
    PersistenceTransportUnavailable,
)
from runtime import RuntimeEnvironment
from supabase_transport import authorize_persistence_network


PROJECT_URL = "https://project.supabase.co"


def persistence_environment(runtime_name: str, **extra: str) -> dict[str, str]:
    values = {
        "HARIS_RUNTIME_ENV": runtime_name,
        "HARIS_PERSISTENCE_MODE": "postgres",
        "SUPABASE_URL": PROJECT_URL,
    }
    values.update(extra)
    return values


class ProductionPersistenceAuthorizationTests(unittest.TestCase):
    def test_production_authorizes_only_configured_persistence_hostname(self):
        with patch.dict(os.environ, persistence_environment("production"), clear=True), patch.object(
            runtime.sys, "argv", ["server"]
        ), patch.object(runtime, "record_provider_access") as recorded:
            authorize_persistence_network("project.supabase.co")
        recorded.assert_called_once_with("supabase_rpc")

        with patch.dict(os.environ, persistence_environment("production"), clear=True), patch.object(
            runtime.sys, "argv", ["server"]
        ), self.assertRaises(PersistenceHostNotAllowed):
            authorize_persistence_network("different.supabase.co")

    def test_production_persistence_gate_does_not_authorize_arbitrary_host_or_mode(self):
        wrong_mode = persistence_environment("production", HARIS_PERSISTENCE_MODE="memory")
        with patch.dict(os.environ, wrong_mode, clear=True), patch.object(
            runtime.sys, "argv", ["server"]
        ), self.assertRaises(PersistenceNetworkBlocked):
            authorize_persistence_network("project.supabase.co")

    def test_test_and_development_remain_persistence_network_denied(self):
        for runtime_name in ("test", "development"):
            with self.subTest(runtime=runtime_name), patch.dict(
                os.environ, persistence_environment(runtime_name), clear=True
            ), patch.object(runtime.sys, "argv", ["server"]), self.assertRaises(
                PersistenceNetworkBlocked
            ):
                authorize_persistence_network("project.supabase.co")

    def test_existing_persistence_integration_gate_remains_explicit(self):
        denied = persistence_environment("persistence_integration")
        with patch.dict(os.environ, denied, clear=True), patch.object(
            runtime.sys, "argv", ["validator"]
        ), self.assertRaises(PersistenceNetworkBlocked):
            authorize_persistence_network("project.supabase.co")

        allowed = persistence_environment(
            "persistence_integration", HARIS_ALLOW_PERSISTENCE_INTEGRATION="true"
        )
        with patch.dict(os.environ, allowed, clear=True), patch.object(
            runtime.sys, "argv", ["validator"]
        ), patch.object(runtime, "record_provider_access") as recorded:
            authorize_persistence_network("project.supabase.co")
        recorded.assert_called_once_with("supabase_rpc")

    def test_real_qod_validation_gate_is_unchanged(self):
        allowed = persistence_environment(
            "real_qod_validation", HARIS_ALLOW_REAL_QOD_VALIDATION="true"
        )
        with patch.dict(os.environ, allowed, clear=True), patch.object(
            runtime.sys, "argv", ["harness"]
        ), patch.object(runtime, "record_provider_access") as recorded:
            authorize_persistence_network("project.supabase.co")
        recorded.assert_called_once_with("supabase_rpc")


class ProductionReadinessTests(unittest.TestCase):
    def setUp(self):
        self.previous_core = run_api._durable_core
        self.previous_lifecycle = run_api._platform_lifecycle

    def tearDown(self):
        run_api._durable_core = self.previous_core
        run_api._platform_lifecycle = self.previous_lifecycle

    def test_not_ready_uses_non_success_status_while_liveness_remains_public(self):
        settings = AppSettings(
            nac_mode="fixture", haris_persistence_mode="postgres",
            supabase_url=PROJECT_URL, supabase_key="test-placeholder",
        )
        run_api._durable_core = None
        run_api._platform_lifecycle = PlatformLifecycle()
        with patch("run_api.get_settings", return_value=settings), patch(
            "nokia_clients.get_settings", return_value=settings
        ), patch(
            "run_api.build_repository_bundle",
            side_effect=PersistenceTransportUnavailable("PERSISTENCE_UNAVAILABLE"),
        ), TestClient(run_api.app) as client:
            readiness = client.get("/api/platform/health")
            liveness = client.get("/api/nac/health")
        self.assertEqual(readiness.status_code, 503)
        self.assertEqual(readiness.json()["status"], "NOT_READY")
        self.assertEqual(readiness.json()["persistence"]["reason"], "PERSISTENCE_UNAVAILABLE")
        self.assertEqual(liveness.status_code, 200)
        self.assertEqual(liveness.json()["status"], "ok")

    def test_ready_requires_reconstructed_durable_core(self):
        settings = AppSettings(nac_mode="fixture", haris_persistence_mode="memory")
        with patch("run_api.get_settings", return_value=settings):
            self.assertTrue(run_api.initialize_platform(settings=settings, runtime=RuntimeEnvironment.DEVELOPMENT))
        with patch("run_api.get_settings", return_value=settings), patch(
            "nokia_clients.get_settings", return_value=settings
        ), TestClient(run_api.app) as client:
            readiness = client.get("/api/platform/health")
        self.assertEqual(readiness.status_code, 200)
        self.assertEqual(readiness.json()["status"], "READY")

    def test_render_manifest_selects_postgres_and_authoritative_readiness(self):
        manifest = (Path(__file__).resolve().parent / "render.yaml").read_text(encoding="utf-8")
        self.assertIn("healthCheckPath: /api/platform/health", manifest)
        self.assertIn("key: HARIS_RUNTIME_ENV", manifest)
        self.assertIn("value: production", manifest)
        self.assertIn("key: HARIS_PERSISTENCE_MODE", manifest)
        self.assertIn("value: postgres", manifest)
        self.assertIn("key: SUPABASE_URL", manifest)
        self.assertIn("key: SUPABASE_KEY", manifest)
        self.assertIn("key: HARIS_OPERATIONAL_API_TOKEN", manifest)
        self.assertNotIn("supabase.co", manifest)


if __name__ == "__main__":
    unittest.main(verbosity=2)
