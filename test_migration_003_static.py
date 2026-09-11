import hashlib
import re
import unittest
from pathlib import Path

from external.validate_real_qod_closed_loop import _canonical_migration_bytes


MIGRATION_002_SHA256 = "7afe3fb2b16def35d6b73a0ec5d17ee95f0462de9d64523d7ef5c231e9dead1f"
TARGETS = {
    "haris_ack_outbox": ("p_outbox_id text,p_owner text", "void", "haris_outbox_claim_conflict"),
    "haris_write_network_state": ("p_record jsonb,p_expected_version bigint", "jsonb", "haris_version_conflict"),
    "haris_update_incident": ("p_record jsonb,p_expected_version bigint", "jsonb", "haris_version_conflict"),
    "haris_save_action": ("p_record jsonb,p_expected_version bigint", "jsonb", "haris_version_conflict"),
    "haris_transition_incident": ("p_incident_id text,p_from_state text,p_to_state text,p_expected_version bigint,p_actor text,p_reason_code text,p_trace_id text,p_occurred_at timestamptz", "jsonb", "haris_version_conflict"),
    "haris_acquire_resource": ("p_record jsonb", "jsonb", "haris_resource_already_owned"),
    "haris_release_resource": ("p_resource_key text,p_incident_id text,p_expected_version bigint", "jsonb", "haris_resource_conflict"),
    "haris_fail_outbox": ("p_outbox_id text,p_owner text,p_error_safe text,p_retry boolean", "void", "haris_outbox_claim_conflict"),
}


def functions(sql):
    pattern = re.compile(
        r"create\s+or\s+replace\s+function\s+public\.(haris_[a-z_]+)\s*\(([^)]*)\)\s*returns\s+([^\s]+)(.*?\$\$;)",
        re.I | re.S,
    )
    return {match.group(1).lower(): match.groups()[1:] for match in pattern.finditer(sql)}


def compact(value):
    return re.sub(r"\s+", "", value.lower())


def normalize_conflict_signal(value):
    normalized=compact(value)
    return re.sub(
        r"raisesqlstate'pt409'usingmessage='([^']+)'",
        r"raiseexception'\1'",
        normalized,
    )


class Migration003StaticValidation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = Path("supabase/migrations/003_haris_conflict_status_alignment.sql")
        cls.sql = cls.path.read_text(encoding="utf-8").lower()
        cls.previous = Path("supabase/migrations/002_haris_durable_event_incident_core.sql").read_text(encoding="utf-8").lower()
        cls.current_functions = functions(cls.sql)
        cls.previous_functions = functions(cls.previous)

    def test_migration_002_is_byte_for_byte_unchanged(self):
        payload=Path("supabase/migrations/002_haris_durable_event_incident_core.sql").read_bytes()
        self.assertEqual(hashlib.sha256(_canonical_migration_bytes(payload)).hexdigest(),MIGRATION_002_SHA256)

    def test_only_the_eight_intended_functions_are_replaced(self):
        self.assertEqual(set(self.current_functions),set(TARGETS))
        self.assertEqual(self.sql.count("create or replace function public."),len(TARGETS))

    def test_signatures_returns_and_hardening_match_migration_002(self):
        for name,(arguments,return_type,_message) in TARGETS.items():
            with self.subTest(function=name):
                self.assertIn(name,self.previous_functions)
                current_args,current_return,current_body=self.current_functions[name]
                previous_args,previous_return,_previous_body=self.previous_functions[name]
                self.assertEqual(compact(current_args),compact(arguments))
                self.assertEqual(compact(current_args),compact(previous_args))
                self.assertEqual(current_return.lower(),return_type)
                self.assertEqual(current_return.lower(),previous_return.lower())
                self.assertIn("security definer",current_body)
                self.assertRegex(current_body,r"set\s+search_path\s*=\s*public\s*,\s*pg_temp")

    def test_only_known_conflicts_changed_to_pt409(self):
        self.assertEqual(self.sql.count("raise sqlstate 'pt409'"),len(TARGETS))
        for name,(_arguments,_return_type,message) in TARGETS.items():
            body=self.current_functions[name][2]
            with self.subTest(function=name):
                self.assertEqual(body.count("raise sqlstate 'pt409'"),1)
                self.assertIn(f"message = '{message}'",body)
                self.assertNotIn(f"raise exception '{message}'",body)

    def test_each_function_body_otherwise_matches_migration_002(self):
        for name in TARGETS:
            with self.subTest(function=name):
                self.assertEqual(
                    normalize_conflict_signal(self.current_functions[name][2]),
                    compact(self.previous_functions[name][2]),
                )

    def test_grants_remain_backend_only(self):
        signatures=(
            "public.haris_ack_outbox(text,text)","public.haris_write_network_state(jsonb,bigint)",
            "public.haris_update_incident(jsonb,bigint)","public.haris_save_action(jsonb,bigint)",
            "public.haris_transition_incident(text,text,text,bigint,text,text,text,timestamptz)",
            "public.haris_acquire_resource(jsonb)","public.haris_release_resource(text,text,bigint)",
            "public.haris_fail_outbox(text,text,text,boolean)",
        )
        for signature in signatures:
            self.assertIn(signature,self.sql)
        self.assertIn("from public, anon, authenticated",self.sql)
        self.assertIn("to service_role",self.sql)

    def test_no_schema_data_or_audit_rewrite(self):
        uncommented="\n".join(line.split("--",1)[0] for line in self.sql.splitlines())
        for forbidden in (
            "create table","alter table","drop table","truncate table","delete from",
            "create index","alter sequence","setval(","haris_audit_records",
        ):
            self.assertNotIn(forbidden,uncommented)

    def test_read_only_verification_is_single_result_set(self):
        verification=Path("supabase/verification/verify_003.sql").read_text(encoding="utf-8").lower()
        for required in (
            "begin transaction read only","rollback","functions_present","no_overloads",
            "pt409_conflict_contract","security_definer","hardened_search_path",
            "function_permissions","'overall'","select check_name,passed,details_or_missing\nfrom all_checks",
        ):
            self.assertIn(required,verification)
        self.assertEqual(verification.count("from all_checks"),1)
        for forbidden in ("insert into","update public","delete from","truncate table","drop table"):
            self.assertNotIn(forbidden,verification)


if __name__ == "__main__":
    unittest.main()
