"""QoD SDK lifecycle contract test. All SDK mutation methods are mocked."""
import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from config import AppSettings
from nokia_clients import LiveNokiaClient


class NokiaQodContractTests(unittest.TestCase):
    def test_qod_creation_is_constructed_without_live_mutation(self):
        settings = AppSettings(
            nac_mode="live_write", nac_api_token="test-token",
            nac_qod_profile_map={"guaranteed": "OPERATOR_GOLD"},
            nac_qod_service_ipv4="203.0.113.10",
        )
        client = LiveNokiaClient(settings)
        with patch("nokia_clients.Device.create_qod_session", return_value=SimpleNamespace(id="mock-qod", status="REQUESTED")) as create:
            session = asyncio.run(client.request_qos("ambulance-01", "guaranteed", 60))
        self.assertEqual(session.session_id, "mock-qod")
        self.assertEqual(create.call_args.args[:3], ("OPERATOR_GOLD", 60, "203.0.113.10"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
