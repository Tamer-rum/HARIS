import asyncio
import importlib
import os
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class TestSandboxTests(unittest.TestCase):
    def hostile(self):
        return {
            "HARIS_RUNTIME_ENV": "test",
            "NAC_MODE": "live_write",
            "NOKIA_OBSERVATION_ENABLED": "true",
            "NAC_API_TOKEN": "dummy-real-looking-token",
            "NAC_QOD_PROFILE_MAP": '{"guaranteed":"HOST_ENV_PROFILE"}',
            "NAC_QOD_SERVICE_IPV4": "198.51.100.10",
            "GEMINI_API_KEY": "dummy",
            "GROQ_API_KEY": "dummy",
            "SUPABASE_URL": "https://example.invalid",
            "SUPABASE_KEY": "dummy",
        }

    def test_test_runtime_overrides_hostile_environment_before_factory(self):
        from config import AppSettings, get_settings
        from nokia_clients import FixtureNokiaClient, build_nokia_client
        with tempfile.TemporaryDirectory() as temporary_directory:
            dotenv_path = Path(temporary_directory) / ".env"
            dotenv_path.write_text(
                "APP_NAME=HOST_DOTENV_APP\n"
                "NAC_GEOFENCE_SINK=https://example.invalid/geofence\n"
                "NAC_EMERGENCY_SLICE_ID=HOST_DOTENV_SLICE\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, self.hostile(), clear=False):
                get_settings.cache_clear(); settings = get_settings()
                self.assertEqual(settings.nac_mode, "fixture")
                self.assertFalse(settings.nokia_observation_enabled)
                self.assertIsNone(settings.nac_api_token)
                self.assertEqual(settings.nac_qod_profile_map, {})
                self.assertIsNone(settings.nac_qod_service_ipv4)
                self.assertIsInstance(build_nokia_client(settings), FixtureNokiaClient)

                direct = AppSettings(_env_file=dotenv_path)
                self.assertEqual(direct.app_name, "HARIS")
                self.assertIsNone(direct.nac_geofence_sink)
                self.assertIsNone(direct.nac_emergency_slice_id)
                self.assertEqual(direct.nac_qod_profile_map, {})
                self.assertIsNone(direct.nac_qod_service_ipv4)

                explicit = AppSettings(
                    _env_file=dotenv_path,
                    nac_mode="live_write",
                    nac_qod_profile_map={"guaranteed": "EXPLICIT_PROFILE"},
                    nac_qod_service_ipv4="203.0.113.10",
                )
                self.assertEqual(explicit.nac_mode, "live_write")
                self.assertEqual(
                    explicit.nac_qod_profile_map,
                    {"guaranteed": "EXPLICIT_PROFILE"},
                )
                self.assertEqual(explicit.nac_qod_service_ipv4, "203.0.113.10")

    def test_test_runtime_blocks_real_sdk_construction_and_use(self):
        from config import AppSettings
        from nokia_clients import LiveNokiaClient
        from runtime import ExternalProviderAccessBlocked

        hostile = AppSettings(nac_mode="live_write", nac_api_token="dummy-real-looking-token")
        with patch("network_as_code.NetworkAsCodeClient", side_effect=AssertionError("real SDK must not be constructed")):
            client = LiveNokiaClient(hostile)
        self.assertIsNone(client._nac)
        with self.assertRaises(ExternalProviderAccessBlocked):
            asyncio.run(client.congestion_insights(["T03"]))

    def test_test_runtime_blocks_llm_and_remote_history_adapters(self):
        from agents import ReasoningRouter
        from config import AppSettings
        from memory import MemoryStore
        from runtime import external_access_policy

        hostile = AppSettings(
            nac_mode="live_read_only", gemini_api_key="dummy", groq_api_key="dummy",
            haris_history_persistence_enabled=True, supabase_url="https://example.invalid",
            supabase_key="dummy",
        )
        router = ReasoningRouter(hostile)
        store = MemoryStore(hostile)
        policy = external_access_policy()
        self.assertEqual(router.availability_reason, "runtime_policy_blocks_llm")
        self.assertIsNone(store._history_repository)
        self.assertFalse(policy.allow_nokia_read)
        self.assertFalse(policy.allow_nokia_write)
        self.assertFalse(policy.allow_llm)
        self.assertFalse(policy.allow_oauth)
        self.assertFalse(policy.allow_remote_database)
        self.assertFalse(policy.allow_remote_event_bus)
        self.assertFalse(policy.allow_external_http)

    def test_external_integration_requires_both_explicit_opt_in_flags(self):
        from external._guard import require_external_integration
        from runtime import RuntimeEnvironment, runtime_environment

        with patch.dict(os.environ, {
            "HARIS_RUNTIME_ENV": "external_integration",
            "HARIS_ALLOW_EXTERNAL_TESTS": "false",
            "HARIS_OFFLINE_TESTS": "false",
        }, clear=False):
            self.assertEqual(runtime_environment(), RuntimeEnvironment.TEST)
            with self.assertRaisesRegex(RuntimeError, "require"):
                require_external_integration()
        with patch.dict(os.environ, {
            "HARIS_RUNTIME_ENV": "external_integration",
            "HARIS_ALLOW_EXTERNAL_TESTS": "true",
            "HARIS_OFFLINE_TESTS": "false",
        }, clear=False):
            self.assertEqual(runtime_environment(), RuntimeEnvironment.EXTERNAL_INTEGRATION)

    def test_test_runtime_disables_llm_and_observation_start(self):
        from agents import HarisAgentSystem
        from config import get_settings
        from nokia_clients import build_nokia_client
        from observations import ObservationStore
        with patch.dict(os.environ, self.hostile(), clear=False):
            get_settings.cache_clear(); settings = get_settings()
            system = HarisAgentSystem(build_nokia_client(settings), settings=settings)
            self.assertEqual(system._crewai_init_reason, "runtime_policy_blocks_llm")
            store = ObservationStore(system.client, settings)
            self.assertFalse(asyncio.run(store.start()))
            asyncio.run(store.stop())
            self.assertIsNone(store._task)

    def test_network_kill_switch_blocks_external_and_allows_localhost(self):
        from test_network_guard import ExternalNetworkAccessBlocked, OfflineNetworkGuard
        guard = OfflineNetworkGuard(); guard.install()
        try:
            with self.assertRaises(ExternalNetworkAccessBlocked):
                socket.create_connection(("example.invalid", 443), timeout=.01)
            self.assertEqual(len(guard.attempts), 1)
        finally:
            guard.uninstall()

    def test_imports_have_no_network_side_effects(self):
        import app, nokia_clients, run_api  # noqa: F401
        nokia_clients._api_client = None
        with patch.object(nokia_clients, "build_nokia_client", side_effect=AssertionError("import must be inert")):
            importlib.reload(run_api)
        # Reload once more after the patch context so the module-level imported
        # factory reference is restored before local ASGI tests use it.
        importlib.reload(run_api)
        self.assertIsNone(nokia_clients._api_client)

    def test_subprocess_inherits_sanitized_environment(self):
        code = (
            "import os; print('|'.join((os.getenv('HARIS_RUNTIME_ENV',''), "
            "os.getenv('HARIS_ALLOW_EXTERNAL_TESTS',''), "
            "str(os.getenv('NAC_API_TOKEN') is None), "
            "str(os.getenv('HARIS_OPERATIONAL_API_TOKEN') is None), "
            "str(os.getenv('HARIS_BACKEND_API_TOKEN') is None))))"
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            env={
                "NAC_API_TOKEN": "dummy-real-looking-token",
                "HARIS_OPERATIONAL_API_TOKEN": "dummy-operational-token",
                "HARIS_BACKEND_API_TOKEN": "dummy-backend-token",
            },
            check=True, capture_output=True, text=True,
        )
        self.assertEqual(result.stdout.strip(), "test|false|True|True|True")

    def test_child_process_inherits_the_network_kill_switch(self):
        code = (
            "import socket; socket.create_connection(('example.invalid', 443), timeout=.01)"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], check=False, capture_output=True, text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("External network access blocked", result.stderr)

    def test_local_fastapi_and_websocket_remain_available(self):
        from fastapi.testclient import TestClient
        import run_api
        from config import AppSettings

        settings = AppSettings(
            nac_mode="fixture", haris_operational_api_token="test-operational-token",
            gemini_api_key=None, groq_api_key=None,
        )
        headers = {"Authorization": "Bearer test-operational-token"}
        with patch("nokia_clients.get_settings", return_value=settings), patch("run_api.get_settings", return_value=settings), TestClient(run_api.app) as api:
            self.assertEqual(api.get("/api/nac/health").status_code, 200)
            with api.websocket_connect("/ws/noc", headers=headers) as websocket:
                snapshot = websocket.receive_json()
        self.assertEqual(snapshot["type"], "snapshot")


if __name__ == "__main__":
    unittest.main()
