import unittest
from pathlib import Path
from unittest.mock import patch

import run_api
from app import unavailable_backend_status
from config import AppSettings
from platform_lifecycle import PlatformLifecycle
from runtime import RuntimeEnvironment


class ProductionDeploymentContractTests(unittest.TestCase):
    def setUp(self):
        self.previous_core = run_api._durable_core
        self.previous_lifecycle = run_api._platform_lifecycle

    def tearDown(self):
        run_api._durable_core = self.previous_core
        run_api._platform_lifecycle = self.previous_lifecycle

    def test_production_cannot_use_process_local_persistence_authority(self):
        settings = AppSettings(nac_mode="fixture", haris_persistence_mode="memory")
        run_api._platform_lifecycle = PlatformLifecycle()
        self.assertFalse(run_api.initialize_platform(
            settings=settings, runtime=RuntimeEnvironment.PRODUCTION,
        ))
        self.assertIsNone(run_api._durable_core)
        self.assertEqual(
            run_api._platform_lifecycle.public_status()["reason"],
            "PERSISTENCE_NOT_CONFIGURED",
        )

    def test_render_contract_is_one_instance_one_worker_and_durable_ready(self):
        root = Path(__file__).resolve().parent
        manifest = (root / "render.yaml").read_text(encoding="utf-8")
        source = (root / "run_api.py").read_text(encoding="utf-8")
        self.assertIn("numInstances: 1", manifest)
        self.assertIn("startCommand: python run_api.py", manifest)
        self.assertIn("healthCheckPath: /api/platform/health", manifest)
        self.assertIn("value: production", manifest)
        self.assertIn("value: postgres", manifest)
        self.assertIn("value: fixture", manifest)
        self.assertIn("workers=1", source)

    def test_backend_failure_marks_cached_authority_stale_and_not_ready(self):
        result = unavailable_backend_status(
            {"haris_state": "READY", "active_incident": {}},
            "BACKEND_UNAVAILABLE",
        )
        self.assertEqual(result["haris_state"], "NOT_READY")
        self.assertTrue(result["backend_status_stale"])
        self.assertEqual(result["backend_connection_state"], "UNAVAILABLE")
        self.assertEqual(result["backend_error"], "BACKEND_UNAVAILABLE")

    def test_fixture_deployment_remains_explicitly_simulated(self):
        settings = AppSettings(nac_mode="fixture")
        self.assertEqual(settings.operating_mode_label, "FIXTURE / FULL DEMO")
        self.assertFalse(settings.is_live)


if __name__ == "__main__":
    unittest.main()
