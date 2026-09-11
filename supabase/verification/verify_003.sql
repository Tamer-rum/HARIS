-- HARIS migration 003 metadata verification (read-only).
-- Run only after applying 003_haris_conflict_status_alignment.sql.
begin transaction read only;
set local statement_timeout = '10s';

with
expected(signature, conflict_message) as (
  values
    ('public.haris_ack_outbox(text,text)', 'haris_outbox_claim_conflict'),
    ('public.haris_write_network_state(jsonb,bigint)', 'haris_version_conflict'),
    ('public.haris_update_incident(jsonb,bigint)', 'haris_version_conflict'),
    ('public.haris_save_action(jsonb,bigint)', 'haris_version_conflict'),
    ('public.haris_transition_incident(text,text,text,bigint,text,text,text,timestamp with time zone)', 'haris_version_conflict'),
    ('public.haris_acquire_resource(jsonb)', 'haris_resource_already_owned'),
    ('public.haris_release_resource(text,text,bigint)', 'haris_resource_conflict'),
    ('public.haris_fail_outbox(text,text,text,boolean)', 'haris_outbox_claim_conflict')
),
inventory as (
  select e.signature, e.conflict_message, to_regprocedure(e.signature) as oid
  from expected e
),
definitions as (
  select i.*, p.prosecdef, p.proconfig,
         case when i.oid is null then '' else lower(pg_get_functiondef(i.oid)) end as definition
  from inventory i left join pg_catalog.pg_proc p on p.oid=i.oid
),
individual_checks(sort_order,check_name,passed,details_or_missing) as (
  select 10, 'functions_present', bool_and(oid is not null),
         coalesce(string_agg(signature, ', ' order by signature) filter (where oid is null), 'none')
  from definitions
  union all
  select 20, 'no_overloads', bool_and((select count(*) from pg_catalog.pg_proc q where q.pronamespace='public'::regnamespace and q.proname=split_part(split_part(signature,'.',2),'(',1))=1),
         coalesce(string_agg(signature, ', ' order by signature) filter (where (select count(*) from pg_catalog.pg_proc q where q.pronamespace='public'::regnamespace and q.proname=split_part(split_part(signature,'.',2),'(',1))<>1), 'none')
  from definitions
  union all
  select 30, 'pt409_conflict_contract', bool_and(definition like '%raise sqlstate ''pt409''%' and definition like '%message = '''||conflict_message||'''%'),
         coalesce(string_agg(signature, ', ' order by signature) filter (where definition not like '%raise sqlstate ''pt409''%' or definition not like '%message = '''||conflict_message||'''%'), 'none')
  from definitions
  union all
  select 40, 'security_definer', bool_and(coalesce(prosecdef,false)),
         coalesce(string_agg(signature, ', ' order by signature) filter (where not coalesce(prosecdef,false)), 'none')
  from definitions
  union all
  select 50, 'hardened_search_path', bool_and(coalesce(array_to_string(proconfig,','),'') like '%search_path=public, pg_temp%'),
         coalesce(string_agg(signature, ', ' order by signature) filter (where coalesce(array_to_string(proconfig,','),'') not like '%search_path=public, pg_temp%'), 'none')
  from definitions
  union all
  select 60, 'function_permissions', bool_and(oid is not null and has_function_privilege('service_role',oid,'EXECUTE') and not has_function_privilege('anon',oid,'EXECUTE') and not has_function_privilege('authenticated',oid,'EXECUTE')),
         coalesce(string_agg(signature, ', ' order by signature) filter (where oid is null or not has_function_privilege('service_role',oid,'EXECUTE') or has_function_privilege('anon',oid,'EXECUTE') or has_function_privilege('authenticated',oid,'EXECUTE')), 'none')
  from definitions
),
all_checks(sort_order,check_name,passed,details_or_missing) as (
  select sort_order,check_name,passed,details_or_missing from individual_checks
  union all
  select 999, 'OVERALL', coalesce(bool_and(passed),false),
         case when coalesce(bool_and(passed),false) then 'all checks passed'
              else 'failed checks: '||coalesce(string_agg(check_name,', ' order by sort_order) filter (where not passed),'unknown') end
  from individual_checks
)
select check_name,passed,details_or_missing
from all_checks
order by sort_order;

rollback;
