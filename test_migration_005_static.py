import hashlib
import re
import unittest
from pathlib import Path

from external.validate_real_qod_closed_loop import _canonical_migration_bytes


FROZEN_HASHES = {
    "002_haris_durable_event_incident_core.sql": "7afe3fb2b16def35d6b73a0ec5d17ee95f0462de9d64523d7ef5c231e9dead1f",
    "003_haris_conflict_status_alignment.sql": "8ef41c1d77eb4a44afc463cc4d618a56733bb83e12d3a5975ea6fb408b0e2122",
    "004_haris_outbox_claim_isolation.sql": "bd898186e26b60c8807a588760779c1a86d73c136b81089206f228427a5210d5",
    "005_haris_outbox_per_run_isolation.sql": "4f18b170281c78cf4611085633d31b24ebbdedd9a85f5e6d022e73e9a26a1faf",
}
SIGNATURE = "p_owner text,p_limit integer,p_lease_seconds integer"
OWNER_PATTERN = re.compile(r"^PERSISTENCE-TEST-([0-9a-f]{32})-WORKER-[A-Za-z0-9_-]+$")
PERSISTENCE_PREFIX = "PERSISTENCE-TEST-"
CLAIM_TOKENS = {
    "eligible_pending": "'pending'",
    "eligible_failed": "'failed'",
    "expired_claimed": "state='claimed'andclaim_expires_at<now()",
    "created_at_order": "orderbycreated_atasc",
    "bounded_limit": "limitp_limit",
    "skip_locked": "forupdateskiplocked",
    "candidate_update_join": "fromcandidatescwhereo.outbox_id=c.outbox_id",
    "state_claimed": "setstate='claimed'",
    "claim_owner": "claim_owner=p_owner",
    "claimed_at": "claimed_at=now()",
    "lease_expiry": "claim_expires_at=now()+make_interval(secs=>p_lease_seconds)",
    "attempt_increment": "attempt_count=o.attempt_count+1",
    "last_attempt_at": "last_attempt_at=now()",
    "returned_rows": "returningo.*",
}


def compact(value: str) -> str:
    return re.sub(r"\s+", "", value.lower())


def function(sql: str) -> tuple[str, str]:
    match = re.search(
        r"create\s+or\s+replace\s+function\s+public\.haris_claim_outbox\s*\(([^)]*)\)\s*returns\s+setof\s+public\.haris_event_outbox(.*?\$\$;)",
        sql,
        re.I | re.S,
    )
    if not match:
        raise AssertionError("haris_claim_outbox definition missing")
    return match.group(1), match.group(2)


def run_namespace(owner: str) -> str | None:
    match = OWNER_PATTERN.fullmatch(owner)
    if match:
        return f"{PERSISTENCE_PREFIX}{match.group(1)}"
    if owner.startswith(PERSISTENCE_PREFIX):
        raise ValueError("haris_invalid_outbox_claim_owner")
    return None


def claim_allows(owner: str, event_id: str) -> bool:
    namespace = run_namespace(owner)
    if namespace is not None:
        return event_id.startswith(f"{namespace}-")
    return not event_id.startswith(PERSISTENCE_PREFIX)


class Migration005StaticValidation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path("supabase/migrations")
        cls.sql = (root / "005_haris_outbox_per_run_isolation.sql").read_text(encoding="utf-8")
        cls.arguments, cls.body = function(cls.sql)
        _, cls.previous_body = function((root / "004_haris_outbox_claim_isolation.sql").read_text(encoding="utf-8"))

    def test_migrations_002_through_005_are_frozen(self):
        root = Path("supabase/migrations")
        for name, expected in FROZEN_HASHES.items():
            with self.subTest(migration=name):
                payload = _canonical_migration_bytes((root / name).read_bytes())
                self.assertEqual(hashlib.sha256(payload).hexdigest(), expected)

    def test_replaces_only_existing_claim_rpc_with_exact_signature(self):
        functions = re.findall(r"create\s+or\s+replace\s+function\s+public\.(haris_[a-z_]+)", self.sql, re.I)
        self.assertEqual(functions, ["haris_claim_outbox"])
        self.assertEqual(compact(self.arguments), compact(SIGNATURE))
        self.assertIn("returnssetofpublic.haris_event_outbox", compact(self.sql))

    def test_canonical_owner_grammar_and_namespace_extraction(self):
        run_id = "a" * 32
        self.assertEqual(run_namespace(f"PERSISTENCE-TEST-{run_id}-WORKER-A"), f"PERSISTENCE-TEST-{run_id}")
        self.assertEqual(run_namespace(f"PERSISTENCE-TEST-{run_id}-WORKER-worker_2"), f"PERSISTENCE-TEST-{run_id}")
        compact_body = compact(self.body)
        self.assertIn(
            "p_owner!~'^persistence-test-[0-9a-f]{32}-worker-[a-za-z0-9_-]+$'",
            compact_body,
        )
        self.assertIn("raiseexception'haris_invalid_outbox_claim_owner'", compact_body)
        self.assertIn(
            "v_run_namespace:=left(p_owner,length('persistence-test-')+32)",
            compact_body,
        )
        for invalid in (
            "PERSISTENCE-TEST-short-WORKER-A",
            f"PERSISTENCE-TEST-{'A' * 32}-WORKER-A",
            f"PERSISTENCE-TEST-{run_id}-WORKER-",
            f"PERSISTENCE-TEST-{run_id}-OTHER-A",
        ):
            with self.subTest(owner=invalid):
                with self.assertRaises(ValueError):
                    run_namespace(invalid)

    def test_cross_run_and_operational_isolation_matrix(self):
        run_a, run_b = "a" * 32, "b" * 32
        owner_a = f"PERSISTENCE-TEST-{run_a}-WORKER-A"
        owner_b = f"PERSISTENCE-TEST-{run_b}-WORKER-B"
        event_a = f"PERSISTENCE-TEST-{run_a}-EVT-1"
        event_b = f"PERSISTENCE-TEST-{run_b}-EVT-1"
        operational = "evt-operational-1"
        self.assertTrue(claim_allows(owner_a, event_a))
        self.assertFalse(claim_allows(owner_a, event_b))
        self.assertFalse(claim_allows(owner_b, event_a))
        self.assertTrue(claim_allows(owner_b, event_b))
        self.assertFalse(claim_allows(owner_a, operational))
        self.assertFalse(claim_allows("haris-core", event_a))
        self.assertFalse(claim_allows("haris-core", event_b))
        self.assertTrue(claim_allows("haris-core", operational))

    def test_historical_run_cannot_be_claimed_by_current_run(self):
        current, historical = "c" * 32, "d" * 32
        owner = f"PERSISTENCE-TEST-{current}-WORKER-A"
        self.assertFalse(claim_allows(owner, f"PERSISTENCE-TEST-{historical}-OUTBOX-OLD"))

    def test_database_predicate_precedes_skip_locked_and_uses_no_broad_integration_match(self):
        body = compact(self.body)
        same_run = "v_run_namespaceisnotnullandleft(event_id,length(v_run_namespace)+1)=v_run_namespace||'-'"
        operational = "v_run_namespaceisnullandleft(event_id,length('persistence-test-'))<>'persistence-test-'"
        self.assertIn(same_run, body)
        self.assertIn(operational, body)
        self.assertLess(body.index(same_run), body.index("forupdateskiplocked"))
        self.assertLess(body.index(operational), body.index("forupdateskiplocked"))
        self.assertNotIn("event_idlike'persistence-test-%'", body)

    def test_original_claim_semantics_are_preserved(self):
        current, previous = compact(self.body), compact(self.previous_body)
        for semantic, token in CLAIM_TOKENS.items():
            with self.subTest(semantic=semantic):
                self.assertIn(token, current)
                self.assertIn(token, previous)
        for token in ("p_ownerisnull", "length(p_owner)=0", "p_limit<1", "p_limit>100", "p_lease_seconds<1"):
            self.assertIn(token, current)
            self.assertIn(token, previous)

    def test_security_and_backend_only_grants_are_preserved(self):
        body = compact(self.sql)
        self.assertIn("languageplpgsqlsecuritydefinersetsearch_path=public,pg_temp", body)
        self.assertIn("revokeallonfunctionpublic.haris_claim_outbox(text,integer,integer)frompublic,anon,authenticated", body)
        self.assertIn("grantexecuteonfunctionpublic.haris_claim_outbox(text,integer,integer)toservice_role", body)

    def test_no_schema_data_or_historical_row_rewrite(self):
        uncommented = "\n".join(line.split("--", 1)[0] for line in self.sql.lower().splitlines())
        for forbidden in (
            "create table", "alter table", "drop table", "truncate table", "delete from",
            "insert into", "create index", "alter index", "alter sequence", "setval(",
            "haris_audit_records", "haris_incidents", "haris_action_commands",
        ):
            self.assertNotIn(forbidden, uncommented)

    def test_contract_model_tolerates_formatting_normalization(self):
        reformatted = self.body.replace(",", ",  \n").replace("=", " = ").replace("+", " + ")
        original, normalized = compact(self.body), compact(reformatted)
        for token in CLAIM_TOKENS.values():
            self.assertIn(token, original)
            self.assertIn(token, normalized)
        self.assertIn("v_run_namespace:=left(p_owner,length('persistence-test-')+32)", normalized)

    def test_verifier_is_read_only_comprehensive_and_one_result_set(self):
        verifier = Path("supabase/verification/verify_005.sql").read_text(encoding="utf-8").lower()
        for required in (
            "begin transaction read only", "rollback", "function_present", "exact_signature",
            "oidvectortypes(proargtypes)", "proargnames=array['p_owner','p_limit','p_lease_seconds']::text[]",
            "prorettype='public.haris_event_outbox'::regtype",
            "no_overloads", "security_definer", "hardened_search_path", "backend_only_permissions",
            "strict_integration_owner", "run_namespace_extraction", "same_run_predicate",
            "cross_run_truth_table", "operational_exclusion", "claim_semantics", "'overall'",
            "eligible_pending", "eligible_failed", "expired_claimed", "created_at_order",
            "bounded_limit", "skip_locked", "lease_expiry", "attempt_increment", "returned_rows",
            "select check_name,passed,details_or_missing\nfrom all_checks",
        ):
            self.assertIn(required, verifier)
        self.assertEqual(verifier.count("from all_checks"), 1)
        for forbidden in ("insert into", "update public", "delete from", "truncate table", "drop table"):
            self.assertNotIn(forbidden, verifier)


if __name__ == "__main__":
    unittest.main()
