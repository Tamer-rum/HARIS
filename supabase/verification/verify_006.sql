begin transaction read only;
with fn as (
  select p.oid, p.prosecdef, p.prorettype, p.proargnames,
         pg_get_functiondef(p.oid) as definition,
         coalesce(array_to_string(p.proconfig, ','), '') as configuration
    from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.proname='haris_save_recovery'
     and oidvectortypes(p.proargtypes)='jsonb, bigint'
), checks(check_name, passed, details_or_missing) as (
  select 'recovery_version_column', exists(
    select 1 from information_schema.columns
     where table_schema='public' and table_name='haris_recoveries'
       and column_name='version' and data_type='bigint' and is_nullable='NO'
       and column_default='0'
  ), 'version bigint not null default 0'
  union all select 'new_rpc_signature', count(*)=1, 'exactly one jsonb,bigint overload' from fn
  union all select 'no_other_overloads', count(*)=1, 'no ambiguous haris_save_recovery overloads'
    from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.proname='haris_save_recovery'
  union all select 'rpc_metadata', coalesce(bool_and(
    prosecdef and prorettype='jsonb'::regtype
    and proargnames=array['p_record','p_expected_version']::text[]
    and configuration like '%search_path=public, pg_temp%'
  ),false), 'security definer; jsonb return; named arguments; hardened search_path' from fn
  union all select 'cas_contract', coalesce(bool_and(
    definition like '%version = p_expected_version%'
    and definition like '%version = version + 1%'
    and definition like '%haris_version_conflict%'
  ),false), 'expected-version CAS and single increment' from fn
  union all select 'identity_binding', coalesce(bool_and(
    definition like '%incident_id = p_record->>''incident_id''%'
  ),false), 'recovery_id and incident_id bound on updates' from fn
  union all select 'transition_contract', coalesce(bool_and(
    definition like '%current_state in (''FAILED'',''COMPLETE'')%'
    and definition like '%current_state = ''PARTIAL'' and target_state = ''COMPLETE''%'
  ),false), 'terminal immutable; PARTIAL to COMPLETE only' from fn
  union all select 'backend_only_execute',
    has_function_privilege('service_role','public.haris_save_recovery(jsonb,bigint)','EXECUTE')
    and not has_function_privilege('anon','public.haris_save_recovery(jsonb,bigint)','EXECUTE')
    and not has_function_privilege('authenticated','public.haris_save_recovery(jsonb,bigint)','EXECUTE'),
    'service_role only'
), all_checks as (
  select * from checks
  union all select 'OVERALL', bool_and(passed), case when bool_and(passed) then 'all checks passed' else 'one or more checks failed' end from checks
)
select check_name, passed, details_or_missing from all_checks;
rollback;
