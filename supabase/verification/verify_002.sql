-- HARIS migration 002 metadata verification (read-only).
-- Run only after applying 002_haris_durable_event_incident_core.sql.
-- The single SELECT below returns every check plus one aggregate OVERALL row.
begin transaction read only;
set local statement_timeout = '10s';

with
expected_tables(name) as (
  values
    ('haris_domain_events'),('haris_network_state'),('haris_incidents'),
    ('haris_incident_transitions'),('haris_resource_ownership'),
    ('haris_action_commands'),('haris_verifications'),('haris_recoveries'),
    ('haris_event_inbox'),('haris_event_outbox'),
    ('haris_projection_snapshots'),('haris_policy_cost_ledger')
),
expected_rpcs(name) as (
  values
    ('haris_ack_outbox'),('haris_acquire_resource'),
    ('haris_append_domain_event'),('haris_append_incident_transition'),
    ('haris_append_outbox'),('haris_append_policy_cost'),
    ('haris_claim_inbox'),('haris_claim_outbox'),('haris_create_incident'),
    ('haris_event_sequence'),('haris_fail_outbox'),('haris_latest_checkpoint'),
    ('haris_pending_outbox'),('haris_policy_cost_totals'),
    ('haris_process_inbound_event'),('haris_read_domain'),
    ('haris_read_policy_cost'),('haris_release_resource'),
    ('haris_save_action'),('haris_save_checkpoint'),('haris_save_recovery'),
    ('haris_save_verification'),('haris_transition_incident'),
    ('haris_update_incident'),('haris_write_network_state')
),
expected_sequences(name) as (
  values
    ('haris_domain_events_sequence_seq'),
    ('haris_incident_transitions_transition_id_seq'),
    ('haris_resource_ownership_ownership_id_seq'),
    ('haris_projection_snapshots_snapshot_id_seq')
),
expected_indexes(name) as (
  values
    ('haris_domain_events_pkey'),('haris_domain_events_event_id_key'),
    ('haris_domain_events_idempotency_key_key'),
    ('haris_domain_events_source_dedupe_idx'),
    ('haris_incidents_one_active_correlation_idx'),
    ('haris_resource_one_owner_idx'),
    ('haris_action_commands_idempotency_key_key'),
    ('haris_event_outbox_unsent_idx'),
    ('haris_projection_snapshots_latest_idx')
),
expected_columns(table_name,column_name) as (
  values
    ('haris_domain_events','sequence'),
    ('haris_domain_events','idempotency_key'),
    ('haris_action_commands','plan_version'),
    ('haris_action_commands','version'),
    ('haris_incidents','version'),
    ('haris_network_state','version'),
    ('haris_event_outbox','claim_expires_at'),
    ('haris_event_outbox','last_error_safe')
),
table_inventory as (
  select e.name, c.oid, c.relrowsecurity
  from expected_tables e
  left join pg_catalog.pg_class c
    on c.relname=e.name
   and c.relnamespace='public'::regnamespace
   and c.relkind='r'
),
rpc_inventory as (
  select e.name, p.oid, p.prosecdef, p.proconfig
  from expected_rpcs e
  left join pg_catalog.pg_proc p
    on p.proname=e.name and p.pronamespace='public'::regnamespace
),
rpc_counts as (
  select name, count(oid) as implementation_count
  from rpc_inventory
  group by name
),
sequence_inventory as (
  select e.name, c.oid
  from expected_sequences e
  left join pg_catalog.pg_class c
    on c.relname=e.name
   and c.relnamespace='public'::regnamespace
   and c.relkind='S'
),
index_inventory as (
  select e.name, i.indexrelid, i.indisvalid
  from expected_indexes e
  left join pg_catalog.pg_class ic
    on ic.relname=e.name and ic.relnamespace='public'::regnamespace
  left join pg_catalog.pg_index i on i.indexrelid=ic.oid
),
column_inventory as (
  select e.table_name, e.column_name, a.attname
  from expected_columns e
  left join pg_catalog.pg_class c
    on c.relname=e.table_name and c.relnamespace='public'::regnamespace
  left join pg_catalog.pg_attribute a
    on a.attrelid=c.oid
   and a.attname=e.column_name
   and a.attnum>0
   and not a.attisdropped
),
tables_check as (
  select
    'tables'::text as check_name,
    count(*) filter (where oid is not null)=count(*) as passed,
    coalesce(
      array_to_string(array_agg(name order by name) filter (where oid is null),', '),
      'none'
    ) as details_or_missing
  from table_inventory
),
rpc_contract_check as (
  select
    'rpc_contract'::text as check_name,
    count(*) filter (
      where oid is not null
        and prosecdef
        and coalesce(array_to_string(proconfig,','),'') like '%search_path=public, pg_temp%'
    )=count(*) as passed,
    coalesce(
      array_to_string(
        array_agg(distinct name order by name) filter (
          where oid is null
             or not prosecdef
             or coalesce(array_to_string(proconfig,','),'') not like '%search_path=public, pg_temp%'
        ),
        ', '
      ),
      'none'
    ) as details_or_missing
  from rpc_inventory
),
rpc_overload_check as (
  select
    'rpc_overload_ambiguity'::text as check_name,
    bool_and(implementation_count=1) as passed,
    coalesce(
      array_to_string(
        array_agg(name||' ('||implementation_count||')' order by name)
          filter (where implementation_count<>1),
        ', '
      ),
      'none'
    ) as details_or_missing
  from rpc_counts
),
rls_check as (
  select
    'rls_enabled'::text as check_name,
    count(*) filter (where oid is not null and relrowsecurity)=count(*) as passed,
    coalesce(
      array_to_string(array_agg(name order by name) filter (where oid is null or not relrowsecurity),', '),
      'none'
    ) as details_or_missing
  from table_inventory
),
table_permissions_check as (
  select
    'table_permissions'::text as check_name,
    count(*) filter (
      where oid is not null
        and not has_table_privilege('anon',oid,'SELECT,INSERT,UPDATE,DELETE')
        and not has_table_privilege('authenticated',oid,'SELECT,INSERT,UPDATE,DELETE')
    )=count(*) as passed,
    coalesce(
      array_to_string(
        array_agg(name order by name) filter (
          where oid is null
             or has_table_privilege('anon',oid,'SELECT,INSERT,UPDATE,DELETE')
             or has_table_privilege('authenticated',oid,'SELECT,INSERT,UPDATE,DELETE')
        ),
        ', '
      ),
      'none'
    ) as details_or_missing
  from table_inventory
),
rls_and_table_grants_check as (
  select
    'rls_and_table_grants'::text as check_name,
    r.passed and t.passed as passed,
    case
      when r.passed and t.passed then 'none'
      else 'rls='||r.details_or_missing||'; table_permissions='||t.details_or_missing
    end as details_or_missing
  from rls_check r cross join table_permissions_check t
),
rpc_grants_check as (
  select
    'rpc_grants'::text as check_name,
    count(*) filter (
      where oid is not null
        and has_function_privilege('service_role',oid,'EXECUTE')
        and not has_function_privilege('anon',oid,'EXECUTE')
        and not has_function_privilege('authenticated',oid,'EXECUTE')
    )=count(*) as passed,
    coalesce(
      array_to_string(
        array_agg(distinct name order by name) filter (
          where oid is null
             or not has_function_privilege('service_role',oid,'EXECUTE')
             or has_function_privilege('anon',oid,'EXECUTE')
             or has_function_privilege('authenticated',oid,'EXECUTE')
        ),
        ', '
      ),
      'none'
    ) as details_or_missing
  from rpc_inventory
),
sequence_grants_check as (
  select
    'sequence_grants'::text as check_name,
    count(*) filter (
      where oid is not null
        and not has_sequence_privilege('anon',oid,'USAGE,SELECT,UPDATE')
        and not has_sequence_privilege('authenticated',oid,'USAGE,SELECT,UPDATE')
    )=count(*) as passed,
    coalesce(
      array_to_string(
        array_agg(name order by name) filter (
          where oid is null
             or has_sequence_privilege('anon',oid,'USAGE,SELECT,UPDATE')
             or has_sequence_privilege('authenticated',oid,'USAGE,SELECT,UPDATE')
        ),
        ', '
      ),
      'none'
    ) as details_or_missing
  from sequence_inventory
),
critical_indexes_check as (
  select
    'critical_indexes'::text as check_name,
    count(*) filter (where indexrelid is not null and indisvalid)=count(*) as passed,
    coalesce(
      array_to_string(
        array_agg(name order by name) filter (where indexrelid is null or not indisvalid),
        ', '
      ),
      'none'
    ) as details_or_missing
  from index_inventory
),
contract_columns_check as (
  select
    'contract_columns'::text as check_name,
    count(*) filter (where attname is not null)=count(*) as passed,
    coalesce(
      array_to_string(
        array_agg(table_name||'.'||column_name order by table_name,column_name)
          filter (where attname is null),
        ', '
      ),
      'none'
    ) as details_or_missing
  from column_inventory
),
individual_checks(sort_order,check_name,passed,details_or_missing) as (
  select 10,check_name,passed,details_or_missing from tables_check
  union all select 20,check_name,passed,details_or_missing from rpc_contract_check
  union all select 30,check_name,passed,details_or_missing from rpc_overload_check
  union all select 40,check_name,passed,details_or_missing from rls_check
  union all select 50,check_name,passed,details_or_missing from table_permissions_check
  union all select 60,check_name,passed,details_or_missing from rls_and_table_grants_check
  union all select 70,check_name,passed,details_or_missing from rpc_grants_check
  union all select 80,check_name,passed,details_or_missing from sequence_grants_check
  union all select 90,check_name,passed,details_or_missing from critical_indexes_check
  union all select 100,check_name,passed,details_or_missing from contract_columns_check
),
all_checks(sort_order,check_name,passed,details_or_missing) as (
  select sort_order,check_name,passed,details_or_missing from individual_checks
  union all
  select
    999,
    'OVERALL',
    coalesce(bool_and(passed),false),
    case
      when coalesce(bool_and(passed),false) then 'all checks passed'
      else 'failed checks: '||coalesce(string_agg(check_name,', ' order by sort_order) filter (where not passed),'unknown')
    end
  from individual_checks
)
select check_name,passed,details_or_missing
from all_checks
order by sort_order;

rollback;
