"""Mock-only tests for Number Verification state and Trusted Dispatch trust decisions."""
import asyncio
import time
import unittest
from unittest.mock import patch

from config import AppSettings
from fastapi import HTTPException
from nokia_clients import NumberVerificationStateStore, TrustedDispatchRequest, trusted_dispatch, verified_identities, number_verification_callback, number_verification_states


class DispatchSecurityTests(unittest.TestCase):
    def test_state_valid_unknown_expired_and_single_use(self):
        store = NumberVerificationStateStore()
        state = store.create("+999900000001")
        self.assertEqual(store.consume(state).phone_number, "+999900000001")
        with self.assertRaises(ValueError): store.consume(state)
        with self.assertRaises(ValueError): store.consume("unknown")
        expired = store.create("+999900000002")
        store._pending[expired].created_at = time.time() - 301
        with self.assertRaises(ValueError): store.consume(expired)

    def test_trusted_dispatch_blocks_unverified(self):
        result = asyncio.run(trusted_dispatch(TrustedDispatchRequest(phone_number="+999900000001")))
        self.assertEqual(result["decision"], "BLOCK")

    def test_trusted_dispatch_sim_swap_and_clean_allow_are_mocked(self):
        settings = AppSettings(nac_mode="fixture", nac_api_token="test-token", trusted_dispatch_sim_swap_max_age_hours=240)
        calls = []
        class Device:
            def __init__(self, recent): self.recent = recent
            def verify_sim_swap(self, max_age):
                calls.append(max_age)
                return self.recent
        class Devices:
            def __init__(self, recent): self.recent = recent
            def get(self, **_): return Device(self.recent)
        class Client:
            def __init__(self, token): self.devices = Devices(Client.recent)
        with patch("nokia_clients.get_settings", return_value=settings), patch("network_as_code.NetworkAsCodeClient", Client):
            verified_identities.record("+999900000001")
            Client.recent = True
            self.assertEqual(asyncio.run(trusted_dispatch(TrustedDispatchRequest(phone_number="+999900000001")))["decision"], "BLOCK")
            Client.recent = False
            self.assertEqual(asyncio.run(trusted_dispatch(TrustedDispatchRequest(phone_number="+999900000001")))["decision"], "ALLOW")
        self.assertEqual(calls, [240, 240])

    def test_expired_verification_and_api_error_fail_closed(self):
        settings = AppSettings(nac_mode="fixture", nac_api_token="test-token", trusted_dispatch_verification_ttl_seconds=1, trusted_dispatch_sim_swap_max_age_hours=240)
        verified_identities._verified_at["+999900000003"] = time.time() - 2
        with patch("nokia_clients.get_settings", return_value=settings):
            self.assertEqual(asyncio.run(trusted_dispatch(TrustedDispatchRequest(phone_number="+999900000003")))["decision"], "BLOCK")
        verified_identities.record("+999900000004")
        with patch("nokia_clients.get_settings", return_value=settings), patch("network_as_code.NetworkAsCodeClient", side_effect=RuntimeError("api failure")):
            self.assertEqual(asyncio.run(trusted_dispatch(TrustedDispatchRequest(phone_number="+999900000004")))["decision"], "BLOCK")

    def test_number_verification_failure_logs_no_code_or_state(self):
        settings = AppSettings(nac_mode="fixture", nac_api_token="test-token")
        state = number_verification_states.create("+999900000005")
        code = "sensitive-oauth-code"
        class LeakyError(Exception):
            status_code = 401
            def __str__(self): return f"/verify?code={code}&state={state}"
        class Devices:
            def get(self, **_): raise LeakyError()
        class Client:
            def __init__(self, token): self.devices = Devices()
        with patch("nokia_clients.get_settings", return_value=settings), patch("network_as_code.NetworkAsCodeClient", Client), self.assertLogs("haris.nokia", level="WARNING") as logs:
            with self.assertRaises(HTTPException):
                asyncio.run(number_verification_callback(code=code, state=state))
        output = "\n".join(logs.output)
        self.assertNotIn(code, output)
        self.assertNotIn(state, output)
        self.assertIn("status=401", output)


if __name__ == "__main__": unittest.main(verbosity=2)
