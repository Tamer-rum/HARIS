-- HARIS migration 004 metadata verification (read-only).
-- Run only after applying 004_haris_outbox_claim_isolation.sql.
begin transaction read only;
set local statement_timeout = '10s';

with
target as (
  select to_regprocedure('public.haris_claim_outbox(text,integer,integer)') as oid
),
definition as (
  select t.oid, p.prosecdef, p.proconfig,
         case when t.oid is null then '' else lower(pg_get_functiondef(t.oid)) end as body
  from target t left join pg_catalog.pg_proc p on p.oid=t.oid
),
claim_semantic_parts(semantic_name,passed) as (
  select 'eligible_pending', body ~ $re$state\s+in\s*\([^)]*'pending'$re$ from definition
  union all select 'eligible_failed', body ~ $re$state\s+in\s*\([^)]*'failed'$re$ from definition
  union all select 'expired_claimed', body ~ $re$state\s*=\s*'claimed'\s+and\s+claim_expires_at\s*<\s*now\s*\(\s*\)$re$ from definition
  union all select 'created_at_order', body ~ $re$order\s+by\s+created_at\s+asc$re$ from definition
  union all select 'bounded_limit', body ~ $re$limit\s+p_limit$re$ from definition
  union all select 'skip_locked', body ~ $re$for\s+update\s+skip\s+locked$re$ from definition
  union all select 'candidate_update_join', body ~ $re$from\s+candidates\s+c\s+where\s+o\.outbox_id\s*=\s*c\.outbox_id$re$ from definition
  union all select 'state_claimed', body ~ $re$set\s+state\s*=\s*'claimed'$re$ from definition
  union all select 'claim_owner', body ~ $re$claim_owner\s*=\s*p_owner$re$ from definition
  union all select 'claimed_at', body ~ $re$claimed_at\s*=\s*now\s*\(\s*\)$re$ from definition
  union all select 'lease_expiry', body ~ $re$claim_expires_at\s*=\s*\(?\s*now\s*\(\s*\)\s*\+\s*make_interval\s*\(\s*secs\s*=>\s*p_lease_seconds\s*\)\s*\)?$re$ from definition
  union all select 'attempt_increment', body ~ $re$attempt_count\s*=\s*\(?\s*o\.attempt_count\s*\+\s*1\s*\)?$re$ from definition
  union all select 'last_attempt_at', body ~ $re$last_attempt_at\s*=\s*now\s*\(\s*\)$re$ from definition
  union all select 'returned_rows', body ~ $re$returning\s+o\.\*$re$ from definition
  union all select 'input_validation',
    body ~ $re$p_owner\s+is\s+null$re$
      and body ~ $re$length\s*\(\s*p_owner\s*\)\s*=\s*0$re$
      and body ~ $re$p_limit\s*<\s*1$re$
      and body ~ $re$p_limit\s*>\s*100$re$
      and body ~ $re$p_lease_seconds\s*<\s*1$re$
    from definition
),
individual_checks(sort_order,check_name,passed,details_or_missing) as (
  select 10, 'function_present', oid is not null,
         case when oid is not null then 'none' else 'public.haris_claim_outbox(text,integer,integer)' end
  from definition
  union all
  select 20, 'no_overloads',
         (select count(*)=1 from pg_catalog.pg_proc where pronamespace='public'::regnamespace and proname='haris_claim_outbox'),
         case when (select count(*)=1 from pg_catalog.pg_proc where pronamespace='public'::regnamespace and proname='haris_claim_outbox') then 'none' else 'haris_claim_outbox' end
  union all
  select 30, 'security_definer', coalesce(prosecdef,false),
         case when coalesce(prosecdef,false) then 'none' else 'haris_claim_outbox' end
  from definition
  union all
  select 40, 'hardened_search_path',
         coalesce(array_to_string(proconfig,','),'') like '%search_path=public, pg_temp%',
         case when coalesce(array_to_string(proconfig,','),'') like '%search_path=public, pg_temp%' then 'none' else 'haris_claim_outbox' end
  from definition
  union all
  select 50, 'backend_only_permissions',
         oid is not null and has_function_privilege('service_role',oid,'EXECUTE') and not has_function_privilege('anon',oid,'EXECUTE') and not has_function_privilege('authenticated',oid,'EXECUTE'),
         case when oid is not null and has_function_privilege('service_role',oid,'EXECUTE') and not has_function_privilege('anon',oid,'EXECUTE') and not has_function_privilege('authenticated',oid,'EXECUTE') then 'none' else 'haris_claim_outbox' end
  from definition
  union all
  select 60, 'namespace_partition',
         position('p_owner like ''persistence-test-%''' in body)>0
           and position('event_id like ''persistence-test-%''' in body)>0
           and position('p_owner not like ''persistence-test-%''' in body)>0
           and position('event_id not like ''persistence-test-%''' in body)>0,
         case when position('p_owner like ''persistence-test-%''' in body)>0
                    and position('event_id like ''persistence-test-%''' in body)>0
                    and position('p_owner not like ''persistence-test-%''' in body)>0
                    and position('event_id not like ''persistence-test-%''' in body)>0
              then 'none' else 'integration/operational event_id partition' end
  from definition
  union all
  select 70, 'claim_semantics',
         coalesce(bool_and(passed),false),
         coalesce(string_agg(semantic_name,', ' order by semantic_name) filter (where not passed),'none')
  from claim_semantic_parts
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
