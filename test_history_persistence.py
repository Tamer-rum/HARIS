import asyncio
import tempfile
import unittest
from pathlib import Path

from config import AppSettings
from memory import IncidentMemory, MemoryStore, SqliteHistoryRepository, _safe_history_value


def incident(identifier: str, *, cycle_id: str | None = None, outcome: str = "mitigated", audit=None) -> IncidentMemory:
    return IncidentMemory(
        incident_id=identifier, cycle_id=cycle_id or f"cycle-{identifier}", summary=f"Incident {identifier}",
        storm_type="sandstorm", peak_congestion_level="High", peak_confidence_level=90,
        affected_cells=["T03"], affected_devices=["ambulance-01"], actions=["qos_request"],
        executed_actions=["qos_request"], outcome=outcome, mode="fixture", audit=audit or {},
    )


class FailingRepository:
    def load_records(self):
        return []

    def append_record(self, record):
        raise RuntimeError("database endpoint unavailable")


class PersistentHistoryTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.path = Path(self.tempdir.name) / "history.sqlite"
        self.settings = AppSettings(nac_mode="fixture", fixture_dir="fixtures")

    def tearDown(self):
        self.tempdir.cleanup()

    def store(self) -> MemoryStore:
        return MemoryStore(self.settings, history_repository=SqliteHistoryRepository(self.path))

    def remember(self, store, record):
        asyncio.run(store.remember_incident(record))

    def test_01_history_survives_new_store_instance(self):
        self.remember(self.store(), incident("one"))
        restored = self.store()
        self.assertEqual(restored.get_incident("cycle-one").incident_id, "one")

    def test_02_two_incidents_keep_chronological_chain_order(self):
        store = self.store()
        self.remember(store, incident("one"))
        self.remember(store, incident("two"))
        restored = self.store()
        self.assertEqual([item.incident_id for item in reversed(restored.recent_incidents())], ["one", "two"])

    def test_03_hash_chain_is_valid_after_reload(self):
        store = self.store()
        self.remember(store, incident("one"))
        self.remember(store, incident("two"))
        self.assertTrue(self.store().verify_audit_chain()["valid"])

    def test_04_existing_hashed_record_is_not_mutated_by_later_append(self):
        store = self.store()
        self.remember(store, incident("one"))
        original = self.store().get_incident("cycle-one").model_dump()
        self.remember(store, incident("two"))
        self.assertEqual(self.store().get_incident("cycle-one").model_dump(), original)

    def test_05_trusted_dispatch_history_reloads_in_normalized_view(self):
        audit = {"trusted_dispatch": {"status": "BLOCKED", "engineer_id": "eng-a"}, "dispatch_history": [{"decision": "BLOCK"}]}
        self.remember(self.store(), incident("dispatch", audit=audit))
        view = self.store().normalized_view(self.store().get_incident("cycle-dispatch"))
        self.assertEqual(view["trusted_dispatch"]["status"], "BLOCKED")
        self.assertEqual(view["dispatch_history"][0]["decision"], "BLOCK")

    def test_06_pending_checkpoint_is_not_labeled_as_terminal(self):
        self.remember(self.store(), incident("pending", outcome="identity_verification_pending"))
        restored = self.store().get_incident("cycle-pending")
        self.assertEqual(restored.outcome, "identity_verification_pending")
        self.assertIsNone(restored.completed_at)

    def test_07_terminal_outcome_is_persisted_once_per_cycle(self):
        store = self.store()
        record = incident("terminal", cycle_id="cycle-terminal", outcome="mitigated")
        self.remember(store, record)
        self.remember(store, record)
        self.assertEqual(len([item for item in self.store().recent_incidents() if item.cycle_id == "cycle-terminal"]), 1)

    def test_08_secrets_and_raw_consent_material_are_excluded(self):
        audit = {"authorization_url": "https://example/?state=raw-state&code=raw-code", "oauth_state": "raw-state", "access_token": "secret", "phone_number": "+99999991000"}
        self.remember(self.store(), incident("safe", audit=audit))
        stored = self.store().get_incident("cycle-safe").model_dump_json()
        for forbidden in ("raw-state", "raw-code", "secret", "+99999991000"):
            self.assertNotIn(forbidden, stored)
        self.assertIn("***1000", stored)

    def test_09_persistence_failure_is_explicit_and_does_not_claim_a_save(self):
        store = MemoryStore(self.settings, history_repository=FailingRepository())
        self.remember(store, incident("failure"))
        self.assertEqual(store.count(), 0)
        self.assertEqual(store.persistence_status["status"], "UNAVAILABLE")

    def test_10_persistence_is_lazy_until_history_is_requested(self):
        class CountingRepository:
            def __init__(self): self.loads = 0
            def load_records(self): self.loads += 1; return []
            def append_record(self, record): pass
        repository = CountingRepository()
        store = MemoryStore(self.settings, history_repository=repository)
        self.assertEqual(repository.loads, 0)
        store.recent_incidents()
        self.assertEqual(repository.loads, 1)

    def test_11_checkpoint_id_makes_retry_idempotent(self):
        store = self.store()
        record = incident("retry", cycle_id="cycle-retry")
        self.remember(store, record)
        self.remember(store, record)
        self.assertEqual(len(self.store().recent_incidents()), 1)
        self.assertTrue(self.store().recent_incidents()[0].checkpoint_id)

    def test_12_distinct_checkpoints_append_in_order(self):
        store = self.store()
        self.remember(store, incident("one", cycle_id="shared", audit={"stage": "one"}))
        second = incident("two", cycle_id="shared", audit={"stage": "two"})
        second.checkpoint_type, second.checkpoint_ordinal = "trusted_dispatch_transition", 1
        self.remember(store, second)
        restored = self.store()
        self.assertEqual(len(restored.recent_incidents()), 2)
        self.assertTrue(restored.verify_audit_chain()["valid"])

    def test_13_timeout_style_retry_reuses_existing_checkpoint(self):
        class TimeoutAfterInsert:
            def __init__(self, repository): self.repository, self.once = repository, True
            def load_records(self): return self.repository.load_records()
            def append_record(self, record):
                stored = self.repository.append_record(record)
                if self.once:
                    self.once = False
                    raise TimeoutError("simulated response timeout after insert")
                return stored
        wrapper = TimeoutAfterInsert(SqliteHistoryRepository(self.path))
        store = MemoryStore(self.settings, history_repository=wrapper)
        record = incident("timeout")
        self.remember(store, record)
        self.assertEqual(store.count(), 0)
        self.remember(store, record)
        self.assertEqual(len(self.store().recent_incidents()), 1)

    def test_14_sanitizer_redacts_oauth_context_and_free_text(self):
        value = _safe_history_value({"oauth": {"state": "raw-state", "code": "raw-code"}, "trace": "callback?state=raw-state&code=raw-code Bearer abc.def"})
        self.assertEqual(value["oauth"]["state"], "[REDACTED]")
        self.assertEqual(value["oauth"]["code"], "[REDACTED]")
        self.assertNotIn("raw-state", value["trace"])
        self.assertNotIn("abc.def", value["trace"])

    def test_15_sanitizer_redacts_tokens_and_masks_phone_in_trace(self):
        value = _safe_history_value({"api_key": "key", "client_secret": "secret", "trace": "dispatch +99999991000 / 99999991001 access_token=abc refresh_token=xyz"})
        self.assertEqual(value["api_key"], "[REDACTED]")
        self.assertEqual(value["client_secret"], "[REDACTED]")
        self.assertIn("***1000", value["trace"])
        self.assertIn("***1001", value["trace"])
        self.assertNotIn("access_token=abc", value["trace"])

    def test_16_masked_phone_remains_useful(self):
        value = _safe_history_value({"masked_phone_number": "***1000", "phone_number": "+99999991000"})
        self.assertEqual(value["masked_phone_number"], "***1000")
        self.assertEqual(value["phone_number"], "***1000")

    def test_17_migration_denies_public_table_access_and_uses_rpc(self):
        migration = Path("supabase/migrations/001_haris_audit_history.sql").read_text(encoding="utf-8")
        self.assertIn("enable row level security", migration)
        self.assertIn("revoke all on table public.haris_audit_records from public, anon, authenticated, service_role", migration)
        self.assertIn("grant execute on function public.append_haris_audit_record", migration)
        self.assertNotIn("grant select, insert on table public.haris_audit_records", migration)

    def test_18_migration_uses_serialized_checkpoint_append(self):
        migration = Path("supabase/migrations/001_haris_audit_history.sql").read_text(encoding="utf-8")
        self.assertIn("checkpoint_id text not null unique", migration)
        self.assertIn("pg_advisory_xact_lock", migration)
        self.assertIn("haris_audit_tail_conflict", migration)


if __name__ == "__main__":
    unittest.main()
