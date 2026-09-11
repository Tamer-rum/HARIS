-- HARIS Phase 8D: recovery CAS and monotonic transition enforcement only.
begin;

alter table public.haris_recoveries
    add column if not exists version bigint not null default 0;

drop function if exists public.haris_save_recovery(jsonb);

create or replace function public.haris_save_recovery(
    p_record jsonb,
    p_expected_version bigint
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
    r public.haris_recoveries%rowtype;
    current_state text;
    target_state text := p_record->>'state';
begin
    if target_state not in ('PENDING','RELEASING','VERIFYING_RELEASE','PARTIAL','FAILED','COMPLETE') then
        raise exception 'haris_invalid_recovery_transition';
    end if;

    if p_expected_version = -1 then
        insert into public.haris_recoveries
        select * from jsonb_populate_record(
            null::public.haris_recoveries,
            p_record || jsonb_build_object('version', 0)
        )
        on conflict do nothing
        returning * into r;
        if not found then raise exception 'haris_version_conflict'; end if;
        return to_jsonb(r);
    end if;

    select state into current_state
      from public.haris_recoveries
     where recovery_id = p_record->>'recovery_id'
       and incident_id = p_record->>'incident_id'
       and version = p_expected_version
     for update;
    if not found then raise exception 'haris_version_conflict'; end if;

    if current_state = target_state then
        select * into r from public.haris_recoveries
         where recovery_id = p_record->>'recovery_id'
           and incident_id = p_record->>'incident_id';
        return to_jsonb(r);
    end if;
    if current_state in ('FAILED','COMPLETE') then
        raise exception 'haris_invalid_recovery_transition';
    end if;
    if not (
        (current_state = 'PENDING' and target_state in ('RELEASING','VERIFYING_RELEASE','PARTIAL','FAILED','COMPLETE')) or
        (current_state = 'RELEASING' and target_state in ('VERIFYING_RELEASE','PARTIAL','FAILED','COMPLETE')) or
        (current_state = 'VERIFYING_RELEASE' and target_state in ('PARTIAL','FAILED','COMPLETE')) or
        (current_state = 'PARTIAL' and target_state = 'COMPLETE')
    ) then
        raise exception 'haris_invalid_recovery_transition';
    end if;

    update public.haris_recoveries
       set state = target_state,
           completed_at = (p_record->>'completed_at')::timestamptz,
           failure_reason = p_record->>'failure_reason',
           version = version + 1
     where recovery_id = p_record->>'recovery_id'
       and incident_id = p_record->>'incident_id'
       and version = p_expected_version
     returning * into r;
    if not found then raise exception 'haris_version_conflict'; end if;
    return to_jsonb(r);
end;
$$;

revoke all on function public.haris_save_recovery(jsonb,bigint) from public, anon, authenticated;
grant execute on function public.haris_save_recovery(jsonb,bigint) to service_role;

commit;
