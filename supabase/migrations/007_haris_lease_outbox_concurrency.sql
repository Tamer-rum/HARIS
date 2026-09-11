-- HARIS migration 007: resource-lease and outbox-claim concurrency safety only.
begin;

alter table public.haris_event_outbox
  add column if not exists claim_generation bigint not null default 0;

create or replace function public.haris_acquire_resource(p_record jsonb)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare r public.haris_resource_ownership%rowtype; v_now timestamptz:=now();
begin
  perform pg_advisory_xact_lock(hashtextextended(p_record->>'resource_key',0));
  select * into r from public.haris_resource_ownership
   where resource_key=p_record->>'resource_key' and ownership_state='OWNED' for update;
  if found then
    if r.lease_expires_at is null or r.lease_expires_at > v_now then
      if r.owner_incident_id=p_record->>'owner_incident_id' then return to_jsonb(r); end if;
      raise sqlstate 'PT409' using message='haris_resource_already_owned';
    end if;
    update public.haris_resource_ownership set
      resource_type=p_record->>'resource_type', owner_incident_id=p_record->>'owner_incident_id',
      acquired_at=coalesce((p_record->>'acquired_at')::timestamptz,v_now), released_at=null,
      lease_started_at=coalesce((p_record->>'lease_started_at')::timestamptz,v_now),
      lease_expires_at=(p_record->>'lease_expires_at')::timestamptz,
      renewable=coalesce((p_record->>'renewable')::boolean,false),
      adopted_by_incident=coalesce((p_record->>'adopted_by_incident')::boolean,false),
      provider_resource_id=null, version=r.version+1
     where ownership_id=r.ownership_id and version=r.version returning * into r;
    if not found then raise sqlstate 'PT409' using message='haris_resource_conflict'; end if;
    return to_jsonb(r);
  end if;
  insert into public.haris_resource_ownership(resource_key,resource_type,owner_incident_id,ownership_state,acquired_at,released_at,lease_started_at,lease_expires_at,renewable,adopted_by_incident,provider_resource_id,version)
  values(p_record->>'resource_key',p_record->>'resource_type',p_record->>'owner_incident_id','OWNED',coalesce((p_record->>'acquired_at')::timestamptz,v_now),null,coalesce((p_record->>'lease_started_at')::timestamptz,v_now),(p_record->>'lease_expires_at')::timestamptz,coalesce((p_record->>'renewable')::boolean,false),coalesce((p_record->>'adopted_by_incident')::boolean,false),p_record->>'provider_resource_id',0)
  returning * into r; return to_jsonb(r);
end; $$;

create or replace function public.haris_renew_resource(p_resource_key text,p_incident_id text,p_expected_version bigint,p_lease_seconds integer)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare r public.haris_resource_ownership%rowtype;
begin
  if p_lease_seconds < 1 or p_lease_seconds > 3600 then raise exception 'haris_invalid_lease'; end if;
  update public.haris_resource_ownership set lease_expires_at=now()+make_interval(secs=>p_lease_seconds),version=version+1
   where resource_key=p_resource_key and owner_incident_id=p_incident_id and ownership_state='OWNED'
     and version=p_expected_version and lease_expires_at>now() returning * into r;
  if not found then raise sqlstate 'PT409' using message='haris_resource_conflict'; end if;
  return to_jsonb(r);
end; $$;

-- Release already has owner + version CAS; retain the signature and harden expiry-independent ownership.
create or replace function public.haris_release_resource(p_resource_key text,p_incident_id text,p_expected_version bigint)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare r public.haris_resource_ownership%rowtype;
begin
 update public.haris_resource_ownership set ownership_state='RELEASED',released_at=now(),version=version+1
  where resource_key=p_resource_key and owner_incident_id=p_incident_id and ownership_state='OWNED' and version=p_expected_version returning * into r;
 if not found then raise sqlstate 'PT409' using message='haris_resource_conflict'; end if;
 return to_jsonb(r);
end; $$;

create or replace function public.haris_claim_outbox(p_owner text,p_limit integer,p_lease_seconds integer)
returns setof public.haris_event_outbox language plpgsql security definer set search_path=public,pg_temp as $$
declare v_run_namespace text;
begin
 if p_owner is null or length(p_owner)=0 or p_limit<1 or p_limit>100 or p_lease_seconds<1 then raise exception 'haris_invalid_outbox_claim'; end if;
 if left(p_owner,length('PERSISTENCE-TEST-'))='PERSISTENCE-TEST-' then
  if p_owner !~ '^PERSISTENCE-TEST-[0-9a-f]{32}-WORKER-[A-Za-z0-9_-]+$' then raise exception 'haris_invalid_outbox_claim_owner'; end if;
  v_run_namespace:=left(p_owner,length('PERSISTENCE-TEST-')+32);
 end if;
 return query with candidates as (
  select outbox_id from public.haris_event_outbox
   where (state in ('PENDING','FAILED') or (state='CLAIMED' and claim_expires_at<now()))
    and ((v_run_namespace is not null and left(event_id,length(v_run_namespace)+1)=v_run_namespace||'-')
      or (v_run_namespace is null and left(event_id,length('PERSISTENCE-TEST-'))<>'PERSISTENCE-TEST-'))
   order by created_at limit p_limit for update skip locked)
 update public.haris_event_outbox o set state='CLAIMED',claimed_at=now(),claim_owner=p_owner,
  claim_expires_at=now()+make_interval(secs=>p_lease_seconds),claim_generation=o.claim_generation+1,
  attempt_count=o.attempt_count+1,last_attempt_at=now()
 from candidates c where o.outbox_id=c.outbox_id returning o.*;
end; $$;

create or replace function public.haris_renew_outbox_claim(p_outbox_id text,p_owner text,p_claim_generation bigint,p_lease_seconds integer)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare r public.haris_event_outbox%rowtype;
begin
 if p_lease_seconds<1 or p_lease_seconds>3600 then raise exception 'haris_invalid_outbox_lease'; end if;
 update public.haris_event_outbox set claim_expires_at=now()+make_interval(secs=>p_lease_seconds)
  where outbox_id=p_outbox_id and state='CLAIMED' and claim_owner=p_owner
    and claim_generation=p_claim_generation and claim_expires_at>now() returning * into r;
 if not found then raise sqlstate 'PT409' using message='haris_outbox_claim_conflict'; end if;
 return to_jsonb(r);
end; $$;

drop function if exists public.haris_ack_outbox(text,text);
create or replace function public.haris_ack_outbox(p_outbox_id text,p_owner text,p_claim_generation bigint)
returns void language plpgsql security definer set search_path=public,pg_temp as $$ begin
 update public.haris_event_outbox set state='SENT',sent_at=now(),claim_expires_at=null
  where outbox_id=p_outbox_id and state='CLAIMED' and claim_owner=p_owner and claim_generation=p_claim_generation;
 if not found then raise sqlstate 'PT409' using message='haris_outbox_claim_conflict'; end if;
end; $$;

drop function if exists public.haris_fail_outbox(text,text,text,boolean);
create or replace function public.haris_fail_outbox(p_outbox_id text,p_owner text,p_error_safe text,p_retry boolean,p_claim_generation bigint)
returns void language plpgsql security definer set search_path=public,pg_temp as $$ begin
 update public.haris_event_outbox set state=case when p_retry then 'PENDING' else 'FAILED' end,
  last_error_safe=left(p_error_safe,256),claim_owner=null,claim_expires_at=null
  where outbox_id=p_outbox_id and state='CLAIMED' and claim_owner=p_owner and claim_generation=p_claim_generation;
 if not found then raise sqlstate 'PT409' using message='haris_outbox_claim_conflict'; end if;
end; $$;

revoke all on function public.haris_acquire_resource(jsonb),public.haris_renew_resource(text,text,bigint,integer),public.haris_release_resource(text,text,bigint),public.haris_claim_outbox(text,integer,integer),public.haris_renew_outbox_claim(text,text,bigint,integer),public.haris_ack_outbox(text,text,bigint),public.haris_fail_outbox(text,text,text,boolean,bigint) from public,anon,authenticated;
grant execute on function public.haris_acquire_resource(jsonb),public.haris_renew_resource(text,text,bigint,integer),public.haris_release_resource(text,text,bigint),public.haris_claim_outbox(text,integer,integer),public.haris_renew_outbox_claim(text,text,bigint,integer),public.haris_ack_outbox(text,text,bigint),public.haris_fail_outbox(text,text,text,boolean,bigint) to service_role;
commit;
