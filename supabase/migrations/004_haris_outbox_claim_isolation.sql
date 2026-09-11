-- HARIS forward-only outbox claim isolation.
-- Integration-test ownership is a backend routing category, not an
-- authorization boundary. Existing backend-only RPC permissions remain.

create or replace function public.haris_claim_outbox(p_owner text, p_limit integer, p_lease_seconds integer)
returns setof public.haris_event_outbox
language plpgsql security definer set search_path = public, pg_temp as $$
begin
  if p_owner is null or length(p_owner) = 0 or p_limit < 1 or p_limit > 100 or p_lease_seconds < 1 then
    raise exception 'haris_invalid_outbox_claim';
  end if;
  return query
  with candidates as (
    select outbox_id from public.haris_event_outbox
    where (state in ('PENDING','FAILED') or (state = 'CLAIMED' and claim_expires_at < now()))
      and (
        (p_owner like 'PERSISTENCE-TEST-%' and event_id like 'PERSISTENCE-TEST-%')
        or
        (p_owner not like 'PERSISTENCE-TEST-%' and event_id not like 'PERSISTENCE-TEST-%')
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
