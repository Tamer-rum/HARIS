import unittest
from pathlib import Path

class Migration002StaticValidation(unittest.TestCase):
    def test_static_schema_validation(self):
        sql=Path('supabase/migrations/002_haris_durable_event_incident_core.sql').read_text(encoding='utf-8').lower()
        for table in ('domain_events','network_state','incidents','incident_transitions','action_commands','resource_ownership','verifications','recoveries','event_inbox','event_outbox','projection_snapshots','policy_cost_ledger'):
            self.assertIn('haris_'+table,sql)
        for fn in ('append_domain_event','process_inbound_event','read_domain','event_sequence','write_network_state','create_incident','update_incident','transition_incident','append_incident_transition','save_action','acquire_resource','release_resource','save_verification','save_recovery','claim_inbox','claim_outbox','ack_outbox','fail_outbox','append_outbox','pending_outbox','save_checkpoint','latest_checkpoint','append_policy_cost','policy_cost_totals','read_policy_cost'):
            self.assertIn('haris_'+fn,sql)
        for required in ('for update skip locked','security definer','set search_path','enable row level security','revoke all','to service_role','haris_incidents_one_active_correlation_idx','idempotency_key text not null unique','version bigint','haris_resource_one_owner_idx','pg_advisory_xact_lock'):
            self.assertIn(required,sql)
        self.assertIn("if p_expected_version=-1 then perform pg_advisory_xact_lock",sql)
        self.assertIn("select * into r from public.haris_action_commands where command_id",sql)
        self.assertIn("jsonb_build_object('_created',created)",sql)
        self.assertIn("provider_resource_id=p_record->>'provider_resource_id'",sql)
        self.assertIn("completed_at=(p_record->>'completed_at')::timestamptz",sql)
        self.assertIn("plan_version integer not null",sql)
        self.assertIn("(p_event->>'source_timestamp')::timestamptz",sql)
        self.assertNotIn("to_timestamp((p_event->>'source_timestamp')::double precision)",sql)
        self.assertIn("else raise exception 'haris_invalid_read_kind'; end case;\nend; $$;",sql)
        self.assertNotIn('haris_audit_records',sql)
        self.assertNotIn('revoke all on all functions',sql)
        self.assertNotIn('revoke all on all sequences',sql)
        uncommented='\n'.join(line.split('--',1)[0] for line in sql.splitlines())
        for destructive in ('drop table','truncate table'):
            self.assertNotIn(destructive,uncommented)
        verification=Path('supabase/verification/verify_002.sql').read_text(encoding='utf-8').lower()
        for required in (
            'begin transaction read only','rollback','tables_check',
            'rpc_contract_check','rpc_overload_check','rls_check',
            'table_permissions_check','rls_and_table_grants_check',
            'rpc_grants_check','sequence_grants_check',
            'critical_indexes_check','contract_columns_check',
            "'overall'",'coalesce(bool_and(passed),false)',
            'select check_name,passed,details_or_missing\nfrom all_checks',
        ):
            self.assertIn(required,verification)
        self.assertEqual(verification.count('from all_checks'),1)
        for forbidden in ('insert into','update public','delete from','truncate table','drop table'):
            self.assertNotIn(forbidden,verification)

if __name__=='__main__': unittest.main()
