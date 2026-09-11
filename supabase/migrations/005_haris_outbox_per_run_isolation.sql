-- HARIS migration 005: isolate integration outbox claims by validator run.
-- p_owner is routing metadata only; RPC authorization remains service-role only.

create or replace function public.haris_claim_outbox(p_owner text, p_limit integer, p_lease_seconds integer)
returns setof public.haris_event_outbox
language plpgsql security definer set search_path = public, pg_temp as $$
declare
  v_run_namespace text;
begin
  if p_owner is null or length(p_owner) = 0 or p_limit < 1 or p_limit > 100 or p_lease_seconds < 1 then
    raise exception 'haris_invalid_outbox_claim';
  end if;

  if left(p_owner, length('PERSISTENCE-TEST-')) = 'PERSISTENCE-TEST-' then
    if p_owner !~ '^PERSISTENCE-TEST-[0-9a-f]{32}-WORKER-[A-Za-z0-9_-]+$' then
      raise exception 'haris_invalid_outbox_claim_owner';
    end if;
    v_run_namespace := left(p_owner, length('PERSISTENCE-TEST-') + 32);
  end if;

  return query
  with candidates as (
    select outbox_id from public.haris_event_outbox
    where (state in ('PENDING','FAILED') or (state = 'CLAIMED' and claim_expires_at < now()))
      and (
        (v_run_namespace is not null
          and left(event_id, length(v_run_namespace) + 1) = v_run_namespace || '-')
        or
        (v_run_namespace is null
          and left(event_id, length('PERSISTENCE-TEST-')) <> 'PERSISTENCE-TEST-')
      )
    order by created_at asc limit p_limit for update skip locked
  )
  update public.haris_event_outbox o set state='CLAIMED', claimed_at=now(), claim_owner=p_owner,
    claim_expires_at=now() + make_interval(secs => p_lease_seconds), attempt_count=o.attempt_count + 1,
    last_attempt_at=now()
  from candidates c where o.outbox_id=c.outbox_id returning o.*;
end; $$;

revoke all on function public.haris_claim_outbox(text,integer,integer) from public, anon, authenticated;
grant execute on function public.haris_claim_outbox(text,integer,integer) to service_role;
