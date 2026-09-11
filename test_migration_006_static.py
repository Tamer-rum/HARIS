import hashlib
import re
import unittest
from pathlib import Path

FROZEN = {
    "002_haris_durable_event_incident_core.sql": "7afe3fb2b16def35d6b73a0ec5d17ee95f0462de9d64523d7ef5c231e9dead1f",
    "003_haris_conflict_status_alignment.sql": "8ef41c1d77eb4a44afc463cc4d618a56733bb83e12d3a5975ea6fb408b0e2122",
    "004_haris_outbox_claim_isolation.sql": "bd898186e26b60c8807a588760779c1a86d73c136b81089206f228427a5210d5",
    "005_haris_outbox_per_run_isolation.sql": "4f18b170281c78cf4611085633d31b24ebbdedd9a85f5e6d022e73e9a26a1faf",
}

class Migration006StaticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = Path("supabase/migrations/006_haris_recovery_concurrency.sql").read_text(encoding="utf-8").lower()

    def test_prior_migrations_are_unchanged(self):
        root = Path("supabase/migrations")
        for name, digest in FROZEN.items():
            self.assertEqual(hashlib.sha256((root / name).read_bytes()).hexdigest(), digest)

    def test_scope_is_recovery_cas_only(self):
        self.assertIn("add column if not exists version bigint not null default 0", self.sql)
        self.assertIn("haris_save_recovery(jsonb,bigint)", re.sub(r"\s+", "", self.sql))
        self.assertIn("p_expected_version", self.sql)
        self.assertIn("for update", self.sql)
        self.assertIn("haris_version_conflict", self.sql)
        for forbidden in ("haris_action_commands", "haris_resource_ownership", "haris_verifications", "drop table", "delete from", "truncate"):
            self.assertNotIn(forbidden, self.sql)

    def test_terminal_and_monotonic_guards_exist(self):
        self.assertIn("current_state in ('failed','complete')", self.sql)
        self.assertIn("haris_invalid_recovery_transition", self.sql)
        self.assertIn("current_state = target_state", self.sql)
        self.assertIn("current_state = 'partial' and target_state = 'complete'", self.sql)
        self.assertNotIn("current_state = 'partial' and target_state = 'failed'", self.sql)

    def test_update_identity_is_bound_to_recovery_and_incident(self):
        self.assertGreaterEqual(self.sql.count("incident_id = p_record->>'incident_id'"), 3)

    def test_permissions_are_backend_only(self):
        compact = re.sub(r"\s+", "", self.sql)
        self.assertIn("revokeallonfunctionpublic.haris_save_recovery(jsonb,bigint)frompublic,anon,authenticated", compact)
        self.assertIn("grantexecuteonfunctionpublic.haris_save_recovery(jsonb,bigint)toservice_role", compact)

    def test_verifier_is_read_only_and_single_result(self):
        sql = Path("supabase/verification/verify_006.sql").read_text(encoding="utf-8").lower()
        self.assertIn("begin transaction read only", sql)
        self.assertIn("rollback", sql)
        self.assertIn("'overall'", sql)
        for check in ("recovery_version_column", "no_other_overloads", "rpc_metadata", "cas_contract", "identity_binding", "transition_contract", "backend_only_execute"):
            self.assertIn(check, sql)
        self.assertEqual(sql.count("select check_name, passed, details_or_missing from all_checks"), 1)

if __name__ == "__main__": unittest.main()
