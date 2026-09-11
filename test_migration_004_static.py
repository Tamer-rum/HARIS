import hashlib
import re
import unittest
from pathlib import Path

from external.validate_real_qod_closed_loop import _canonical_migration_bytes


FROZEN_HASHES = {
    "002_haris_durable_event_incident_core.sql":"7afe3fb2b16def35d6b73a0ec5d17ee95f0462de9d64523d7ef5c231e9dead1f",
    "003_haris_conflict_status_alignment.sql":"8ef41c1d77eb4a44afc463cc4d618a56733bb83e12d3a5975ea6fb408b0e2122",
    "004_haris_outbox_claim_isolation.sql":"bd898186e26b60c8807a588760779c1a86d73c136b81089206f228427a5210d5",
}
SIGNATURE="p_owner text,p_limit integer,p_lease_seconds integer"
SEMANTIC_TOKENS = {
    "eligible_pending":("'pending'",),
    "eligible_failed":("'failed'",),
    "expired_claimed":("state='claimed'andclaim_expires_at<now()",),
    "created_at_order":("orderbycreated_atasc",),
    "bounded_limit":("limitp_limit",),
    "skip_locked":("forupdateskiplocked",),
    "candidate_update_join":("fromcandidatescwhereo.outbox_id=c.outbox_id",),
    "state_claimed":("setstate='claimed'",),
    "claim_owner":("claim_owner=p_owner",),
    "claimed_at":("claimed_at=now()",),
    "lease_expiry":("claim_expires_at=now()+make_interval(secs=>p_lease_seconds)",),
    "attempt_increment":("attempt_count=o.attempt_count+1",),
    "last_attempt_at":("last_attempt_at=now()",),
    "returned_rows":("returningo.*",),
    "input_validation":("p_ownerisnull","length(p_owner)=0","p_limit<1","p_limit>100","p_lease_seconds<1"),
}


def function(sql):
    match=re.search(
        r"create\s+or\s+replace\s+function\s+public\.haris_claim_outbox\s*\(([^)]*)\)\s*returns\s+setof\s+public\.haris_event_outbox(.*?\$\$;)",
        sql,re.I|re.S,
    )
    if not match:
        raise AssertionError("haris_claim_outbox definition missing")
    return match.group(1),match.group(2)


def compact(value):
    return re.sub(r"\s+","",value.lower())


def partition_allows(owner,event_id):
    integration_owner=owner.startswith("PERSISTENCE-TEST-")
    integration_event=event_id.startswith("PERSISTENCE-TEST-")
    return integration_owner == integration_event


def claim_semantics(body):
    normalized=compact(body)
    return {name:all(token in normalized for token in tokens) for name,tokens in SEMANTIC_TOKENS.items()}


def namespace_semantics(body):
    normalized=compact(body)
    return all(token in normalized for token in (
        "p_ownerlike'persistence-test-%'","event_idlike'persistence-test-%'",
        "p_ownernotlike'persistence-test-%'","event_idnotlike'persistence-test-%'",
    ))


class Migration004StaticValidation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path=Path("supabase/migrations/004_haris_outbox_claim_isolation.sql")
        cls.sql=cls.path.read_text(encoding="utf-8").lower()
        cls.old_sql=Path("supabase/migrations/002_haris_durable_event_incident_core.sql").read_text(encoding="utf-8").lower()
        cls.arguments,cls.body=function(cls.sql)
        cls.old_arguments,cls.old_body=function(cls.old_sql)

    def test_migrations_002_003_and_004_are_byte_for_byte_unchanged(self):
        root=Path("supabase/migrations")
        for name,expected in FROZEN_HASHES.items():
            with self.subTest(migration=name):
                payload = _canonical_migration_bytes((root/name).read_bytes())
                self.assertEqual(hashlib.sha256(payload).hexdigest(),expected)

    def test_only_claim_rpc_is_replaced_with_exact_signature(self):
        functions=re.findall(r"create\s+or\s+replace\s+function\s+public\.(haris_[a-z_]+)",self.sql,re.I)
        self.assertEqual(functions,["haris_claim_outbox"])
        self.assertEqual(compact(self.arguments),compact(SIGNATURE))
        self.assertEqual(compact(self.arguments),compact(self.old_arguments))
        self.assertIn("returns setof public.haris_event_outbox",self.sql)

    def test_partition_predicate_is_symmetric_and_precedes_locking(self):
        compact_body=compact(self.body)
        integration="(p_ownerlike'persistence-test-%'andevent_idlike'persistence-test-%')"
        operational="(p_ownernotlike'persistence-test-%'andevent_idnotlike'persistence-test-%')"
        self.assertIn(integration,compact_body)
        self.assertIn(operational,compact_body)
        self.assertLess(compact_body.index(integration),compact_body.index("forupdateskiplocked"))
        self.assertLess(compact_body.index(operational),compact_body.index("forupdateskiplocked"))

    def test_partition_truth_table(self):
        self.assertTrue(partition_allows("PERSISTENCE-TEST-worker","PERSISTENCE-TEST-event"))
        self.assertTrue(partition_allows("haris-core","evt-operational"))
        self.assertFalse(partition_allows("PERSISTENCE-TEST-worker","evt-operational"))
        self.assertFalse(partition_allows("haris-core","PERSISTENCE-TEST-event"))

    def test_lock_order_limit_lease_and_counter_logic_are_unchanged(self):
        for required in (
            "for update skip locked","order by created_at asc","limit p_limit",
            "update public.haris_event_outbox o set state='claimed', claimed_at=now(), claim_owner=p_owner",
            "claim_expires_at=now() + make_interval(secs => p_lease_seconds)",
            "attempt_count=o.attempt_count + 1","last_attempt_at=now()",
            "from candidates c where o.outbox_id=c.outbox_id returning o.*",
            "p_limit < 1 or p_limit > 100 or p_lease_seconds < 1",
            "state in ('pending','failed')","state = 'claimed' and claim_expires_at < now()",
        ):
            self.assertIn(compact(required),compact(self.body))
            self.assertIn(compact(required),compact(self.old_body))

    def test_verifier_model_accepts_valid_function_and_formatting_changes(self):
        self.assertTrue(all(claim_semantics(self.body).values()))
        reformatted=self.body.replace(",",",   \n").replace("="," = ")
        self.assertTrue(all(claim_semantics(reformatted).values()))

    def test_removing_each_critical_semantic_fails_verifier_model(self):
        normalized=compact(self.body)
        for semantic,tokens in SEMANTIC_TOKENS.items():
            for token in tokens:
                with self.subTest(semantic=semantic,token=token):
                    mutated=normalized.replace(token,"semantic_removed",1)
                    result=claim_semantics(mutated)
                    self.assertFalse(result[semantic])
                    self.assertFalse(all(result.values()))

    def test_namespace_predicate_remains_mandatory(self):
        normalized=compact(self.body)
        self.assertTrue(namespace_semantics(normalized))
        for token in (
            "p_ownerlike'persistence-test-%'","event_idlike'persistence-test-%'",
            "p_ownernotlike'persistence-test-%'","event_idnotlike'persistence-test-%'",
        ):
            with self.subTest(token=token):
                self.assertFalse(namespace_semantics(normalized.replace(token,"partition_removed",1)))

    def test_security_and_backend_only_grants_are_preserved(self):
        self.assertIn("language plpgsql security definer",self.sql)
        self.assertRegex(self.sql,r"set\s+search_path\s*=\s*public\s*,\s*pg_temp")
        self.assertIn("revoke all on function public.haris_claim_outbox(text,integer,integer) from public, anon, authenticated",self.sql)
        self.assertIn("grant execute on function public.haris_claim_outbox(text,integer,integer) to service_role",self.sql)

    def test_no_schema_data_or_audit_rewrite(self):
        uncommented="\n".join(line.split("--",1)[0] for line in self.sql.splitlines())
        for forbidden in (
            "create table","alter table","drop table","truncate table","delete from",
            "create index","alter index","alter sequence","setval(","haris_audit_records",
        ):
            self.assertNotIn(forbidden,uncommented)

    def test_read_only_verification_is_one_result_set(self):
        verification=Path("supabase/verification/verify_004.sql").read_text(encoding="utf-8").lower()
        for required in (
            "begin transaction read only","rollback","function_present","no_overloads",
            "security_definer","hardened_search_path","backend_only_permissions",
            "namespace_partition","claim_semantics","'overall'",
            "claim_semantic_parts","eligible_pending","eligible_failed","expired_claimed",
            "candidate_update_join","lease_expiry","attempt_increment","returned_rows",
            "select check_name,passed,details_or_missing\nfrom all_checks",
        ):
            self.assertIn(required,verification)
        self.assertEqual(verification.count("from all_checks"),1)
        for forbidden in ("insert into","update public","delete from","truncate table","drop table"):
            self.assertNotIn(forbidden,verification)


if __name__ == "__main__":
    unittest.main()
