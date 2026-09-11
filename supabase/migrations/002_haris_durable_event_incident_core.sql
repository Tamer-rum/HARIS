-- HARIS durable event and incident core.  Migration only: do not execute from
-- browser code or the normal offline test suite.  All domain tables are
-- server-side service-role/RPC only; the Streamlit/browser client gets no
-- table grants and no direct Supabase domain access.

create table if not exists public.haris_domain_events (
    sequence bigint generated always as identity primary key,
    event_id text not null unique,
    schema_version integer not null default 1,
    event_type text not null,
    source text not null,
    source_mode text not null,
    source_event_id text,
    source_timestamp timestamptz not null,
    received_at timestamptz not null,
    created_at timestamptz not null,
    entity_type text not null,
    entity_id text not null,
    correlation_key text not null,
    provenance text not null,
    payload jsonb not null,
    trace_id text not null,
    idempotency_key text not null unique,
    check ((payload ?| array['access_token','refresh_token','api_key','client_secret','oauth_state','authorization_url','consent_action_token','phone_number','msisdn']) = false)
);
create unique index if not exists haris_domain_events_source_dedupe_idx
    on public.haris_domain_events(source, source_event_id) where source_event_id is not null;
create index if not exists haris_domain_events_entity_idx on public.haris_domain_events(entity_id, sequence);
create index if not exists haris_domain_events_correlation_idx on public.haris_domain_events(correlation_key, sequence);

create table if not exists public.haris_network_state (
    entity_id text primary key,
    entity_type text not null,
    mapping_source text not null,
    provenance text not null,
    raw_congestion text,
    raw_congestion_observed_at timestamptz,
    reachability_summary jsonb,
    reachability_observed_at timestamptz,
    location_summary jsonb,
    location_observed_at timestamptz,
    freshness text not null,
    haris_operational_state text not null,
    active_incident_ids jsonb not null default '[]'::jsonb,
    last_source_change_at timestamptz,
    last_operational_change_at timestamptz,
    version bigint not null default 0,
    updated_at timestamptz not null
);

create table if not exists public.haris_incidents (
    incident_id text primary key,
    schema_version integer not null default 1,
    correlation_key text not null,
    primary_entity text not null,
    affected_entities jsonb not null,
    affected_devices jsonb not null,
    trigger_event_id text not null references public.haris_domain_events(event_id),
    trigger_provenance text not null,
    trigger_source_timestamp timestamptz not null,
    opened_at timestamptz not null,
    updated_at timestamptz not null,
    severity text not null,
    priority text not null,
    state text not null,
    plan_version integer not null default 0,
    warden_decision text,
    verification_state text,
    recovery_state text,
    outcome text,
    closed_at timestamptz,
    version bigint not null default 0,
    trace_id text not null
);
create unique index if not exists haris_incidents_one_active_correlation_idx
    on public.haris_incidents(correlation_key)
    where state in ('DETECTED','EVALUATING','PLANNED','WARDEN_REVIEW','APPROVED','MITIGATING','VERIFYING','RECOVERING','ESCALATED');
create index if not exists haris_incidents_active_idx on public.haris_incidents(state, updated_at desc);

create table if not exists public.haris_incident_transitions (
    transition_id bigint generated always as identity primary key,
    incident_id text not null references public.haris_incidents(incident_id),
    from_state text,
    to_state text not null,
    occurred_at timestamptz not null,
    trigger_event_id text references public.haris_domain_events(event_id),
    actor text not null,
    reason_code text not null,
    trace_id text not null
);
create index if not exists haris_incident_transitions_incident_idx on public.haris_incident_transitions(incident_id, transition_id);

create table if not exists public.haris_resource_ownership (
    ownership_id bigint generated always as identity primary key,
    resource_key text not null,
    resource_type text not null,
    owner_incident_id text not null references public.haris_incidents(incident_id),
    ownership_state text not null,
    acquired_at timestamptz not null,
    released_at timestamptz,
    lease_started_at timestamptz,
    lease_expires_at timestamptz,
    renewable boolean not null default false,
    adopted_by_incident boolean not null default false,
    provider_resource_id text,
    version bigint not null default 0
);
create unique index if not exists haris_resource_one_owner_idx
    on public.haris_resource_ownership(resource_key) where ownership_state = 'OWNED';

create table if not exists public.haris_action_commands (
    command_id text primary key,
    incident_id text not null references public.haris_incidents(incident_id),
    command_type text not null,
    resource_key text not null,
    device_id text,
    plan_version integer not null,
    requested_at timestamptz not null,
    idempotency_key text not null unique,
    preconditions jsonb not null default '{}'::jsonb,
    parameters_safe jsonb not null default '{}'::jsonb,
    state text not null,
    provider_resource_id text,
    attempt_count integer not null default 0,
    last_attempt_at timestamptz,
    completed_at timestamptz,
    failure_reason text,
    version bigint not null default 0
);
create index if not exists haris_action_commands_pending_idx on public.haris_action_commands(state, requested_at);

create table if not exists public.haris_verifications (
    verification_id text primary key,
    incident_id text not null references public.haris_incidents(incident_id),
    evidence_event_ids jsonb not null default '[]'::jsonb,
    verification_type text not null,
    state text not null,
    started_at timestamptz not null,
    updated_at timestamptz not null,
    result jsonb,
    reason text,
    source_provenance text not null
);
create index if not exists haris_verifications_incident_idx on public.haris_verifications(incident_id, updated_at desc);

create table if not exists public.haris_recoveries (
    recovery_id text primary key,
    incident_id text not null unique references public.haris_incidents(incident_id),
    resource_keys jsonb not null default '[]'::jsonb,
    state text not null,
    started_at timestamptz not null,
    completed_at timestamptz,
    failure_reason text
);

create table if not exists public.haris_event_inbox (
    idempotency_key text primary key,
    event_id text not null,
    claimed_at timestamptz not null
);
create table if not exists public.haris_event_outbox (
    outbox_id text primary key,
    event_id text not null references public.haris_domain_events(event_id),
    event_type text not null,
    payload jsonb not null default '{}'::jsonb,
    trace_id text not null,
    state text not null default 'PENDING' check (state in ('PENDING','CLAIMED','SENT','FAILED')),
    created_at timestamptz not null,
    claimed_at timestamptz,
    sent_at timestamptz
    ,attempt_count integer not null default 0 check (attempt_count >= 0)
    ,last_attempt_at timestamptz
    ,last_error_safe text
    ,claim_owner text
    ,claim_expires_at timestamptz
);
create index if not exists haris_event_outbox_unsent_idx on public.haris_event_outbox(state, created_at) where state in ('PENDING','FAILED');

create table if not exists public.haris_projection_snapshots (
    snapshot_id bigint generated always as identity primary key,
    snapshot_version integer not null,
    last_event_sequence bigint not null,
    projection jsonb not null,
    created_at timestamptz not null
);
create index if not exists haris_projection_snapshots_latest_idx on public.haris_projection_snapshots(last_event_sequence desc);

create table if not exists public.haris_policy_cost_ledger (
    ledger_id text primary key,
    incident_id text not null references public.haris_incidents(incident_id),
    command_id text references public.haris_action_commands(command_id),
    estimated_policy_cost numeric,
    committed_at timestamptz not null,
    released_at timestamptz,
    day_bucket date not null,
    cost_basis text not null default 'HARIS_POLICY_COST_MODEL'
);

-- Defense in depth: no browser role has direct access.  A server-side
-- service-role adapter may use controlled RPCs/transactions, never browser
-- credentials.  No public/anon/authenticated policies are created.
do $$
declare table_name text;
begin
  foreach table_name in array array[
    'haris_domain_events','haris_network_state','haris_incidents',
    'haris_incident_transitions','haris_resource_ownership','haris_action_commands',
    'haris_verifications','haris_recoveries','haris_event_inbox','haris_event_outbox',
    'haris_projection_snapshots','haris_policy_cost_ledger'
  ] loop
    execute format('alter table public.%I enable row level security', table_name);
    execute format('revoke all on table public.%I from public, anon, authenticated', table_name);
  end loop;
end; $$;

-- Only migration-002 identity sequences are denied here. Migration 001 audit
-- objects and unrelated project objects are deliberately untouched.
revoke all on sequence public.haris_domain_events_sequence_seq,
    public.haris_incident_transitions_transition_id_seq,
    public.haris_resource_ownership_ownership_id_seq,
    public.haris_projection_snapshots_snapshot_id_seq
    from public, anon, authenticated;

-- The actual adapter performs this inside one transaction before it publishes
-- to the transient event bus.  It is intentionally server-only.
create or replace function public.haris_append_domain_event(p_event jsonb)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare v_existing jsonb;
begin
  perform pg_advisory_xact_lock(hashtext('haris_domain_event_append'));
  select jsonb_build_object('event_id', event_id, 'idempotency_key', idempotency_key)
    into v_existing from public.haris_domain_events
    where event_id = p_event->>'event_id' or idempotency_key = p_event->>'idempotency_key'
    limit 1;
  if found then return jsonb_build_object('inserted', false, 'event', v_existing); end if;
  insert into public.haris_domain_events(
    event_id,schema_version,event_type,source,source_mode,source_event_id,
    source_timestamp,received_at,created_at,entity_type,entity_id,correlation_key,
    provenance,payload,trace_id,idempotency_key
  ) values (
    p_event->>'event_id',(p_event->>'schema_version')::integer,p_event->>'event_type',
    p_event->>'source',p_event->>'source_mode',nullif(p_event->>'source_event_id',''),
    (p_event->>'source_timestamp')::timestamptz,(p_event->>'received_at')::timestamptz,
    (p_event->>'created_at')::timestamptz,p_event->>'entity_type',p_event->>'entity_id',
    p_event->>'correlation_key',p_event->>'provenance',p_event->'payload',p_event->>'trace_id',
    p_event->>'idempotency_key'
  );
  return jsonb_build_object('inserted', true, 'event_id', p_event->>'event_id');
end;
$$;
revoke all on function public.haris_append_domain_event(jsonb) from public, anon, authenticated;
grant execute on function public.haris_append_domain_event(jsonb) to service_role;

-- Repository RPC contract.  Each function is deliberately narrow, validates
-- an expected version where relevant, and is invoked only by backend code.
-- The Phase 6C.2 transport executes an accepted inbound event as one
-- transaction-scoped SECURITY DEFINER RPC; no sequence of HTTP calls is ever
-- represented as a transaction.
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
    where state in ('PENDING','FAILED') or (state = 'CLAIMED' and claim_expires_at < now())
    order by created_at asc limit p_limit for update skip locked
  )
  update public.haris_event_outbox o set state='CLAIMED', claimed_at=now(), claim_owner=p_owner,
    claim_expires_at=now() + make_interval(secs => p_lease_seconds), attempt_count=o.attempt_count + 1,
    last_attempt_at=now()
  from candidates c where o.outbox_id=c.outbox_id returning o.*;
end; $$;

create or replace function public.haris_ack_outbox(p_outbox_id text, p_owner text)
returns void language plpgsql security definer set search_path = public, pg_temp as $$
begin
  update public.haris_event_outbox set state='SENT', sent_at=now(), claim_expires_at=null
  where outbox_id=p_outbox_id and state='CLAIMED' and claim_owner=p_owner;
  if not found then raise exception 'haris_outbox_claim_conflict'; end if;
end; $$;

create or replace function public.haris_claim_inbox(p_idempotency_key text, p_event_id text)
returns boolean language plpgsql security definer set search_path = public, pg_temp as $$
begin
  insert into public.haris_event_inbox(idempotency_key,event_id,claimed_at)
  values(p_idempotency_key,p_event_id,now()) on conflict (idempotency_key) do nothing;
  return found;
end; $$;

revoke all on function public.haris_claim_outbox(text,integer,integer) from public, anon, authenticated;
revoke all on function public.haris_ack_outbox(text,text) from public, anon, authenticated;
revoke all on function public.haris_claim_inbox(text,text) from public, anon, authenticated;
grant execute on function public.haris_claim_outbox(text,integer,integer) to service_role;
grant execute on function public.haris_ack_outbox(text,text) to service_role;
grant execute on function public.haris_claim_inbox(text,text) to service_role;

-- Bounded read RPCs. They return JSON so the server transport has one stable
-- contract while domain services remain independent from Supabase.
create or replace function public.haris_read_domain(p_kind text, p_key text default null, p_after bigint default 0, p_limit integer default 200)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
begin
 if p_limit<1 or p_limit>1000 then raise exception 'haris_invalid_limit'; end if;
 case p_kind
 when 'event' then return coalesce((select jsonb_agg(to_jsonb(x)) from (select * from public.haris_domain_events where sequence>p_after and (p_key is null or event_id=p_key) order by sequence limit p_limit)x),'[]');
 when 'network' then return coalesce((select jsonb_agg(to_jsonb(x)) from (select * from public.haris_network_state where p_key is null or entity_id=p_key order by entity_id limit p_limit)x),'[]');
 when 'incident' then return coalesce((select jsonb_agg(to_jsonb(x)) from (select * from public.haris_incidents where p_key is null or incident_id=p_key order by updated_at desc limit p_limit)x),'[]');
 when 'transition' then return coalesce((select jsonb_agg(to_jsonb(x)) from (select * from public.haris_incident_transitions where incident_id=p_key order by transition_id limit p_limit)x),'[]');
 when 'action' then return coalesce((select jsonb_agg(to_jsonb(x)) from (select * from public.haris_action_commands where p_key is null or command_id=p_key order by requested_at limit p_limit)x),'[]');
 when 'ownership' then return coalesce((select jsonb_agg(to_jsonb(x)) from (select * from public.haris_resource_ownership where p_key is null or resource_key=p_key order by acquired_at desc limit p_limit)x),'[]');
 when 'verification' then return coalesce((select jsonb_agg(to_jsonb(x)) from (select * from public.haris_verifications where p_key is null or verification_id=p_key order by updated_at desc limit p_limit)x),'[]');
 when 'recovery' then return coalesce((select jsonb_agg(to_jsonb(x)) from (select * from public.haris_recoveries where p_key is null or recovery_id=p_key order by started_at desc limit p_limit)x),'[]');
 else raise exception 'haris_invalid_read_kind'; end case;
end; $$;

create or replace function public.haris_write_network_state(p_record jsonb,p_expected_version bigint)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$ declare r public.haris_network_state%rowtype; begin
 if p_expected_version=0 then insert into public.haris_network_state select * from jsonb_populate_record(null::public.haris_network_state,p_record||jsonb_build_object('version',1,'haris_operational_state',coalesce(p_record->>'haris_operational_state','MONITORING'))) returning * into r;
 else update public.haris_network_state set
   entity_type=p_record->>'entity_type',mapping_source=p_record->>'mapping_source',provenance=p_record->>'provenance',
   raw_congestion=p_record->>'raw_congestion',raw_congestion_observed_at=(p_record->>'raw_congestion_observed_at')::timestamptz,
   reachability_summary=p_record->'reachability_summary',reachability_observed_at=(p_record->>'reachability_observed_at')::timestamptz,
   location_summary=p_record->'location_summary',location_observed_at=(p_record->>'location_observed_at')::timestamptz,
   freshness=p_record->>'freshness',haris_operational_state=p_record->>'haris_operational_state',
   active_incident_ids=p_record->'active_incident_ids',last_source_change_at=(p_record->>'last_source_change_at')::timestamptz,
   last_operational_change_at=(p_record->>'last_operational_change_at')::timestamptz,updated_at=(p_record->>'updated_at')::timestamptz,
   version=version+1
   where entity_id=p_record->>'entity_id' and version=p_expected_version returning * into r;
   if not found then raise exception 'haris_version_conflict'; end if;
 end if; return to_jsonb(r); end; $$;

create or replace function public.haris_create_incident(p_record jsonb) returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$ declare r public.haris_incidents%rowtype; created boolean:=false; begin insert into public.haris_incidents select * from jsonb_populate_record(null::public.haris_incidents,p_record) on conflict do nothing returning * into r; if found then created:=true; else select * into r from public.haris_incidents where correlation_key=p_record->>'correlation_key' and state in ('DETECTED','EVALUATING','PLANNED','WARDEN_REVIEW','APPROVED','MITIGATING','VERIFYING','RECOVERING','ESCALATED'); end if; return to_jsonb(r)||jsonb_build_object('_created',created); end; $$;
create or replace function public.haris_update_incident(p_record jsonb,p_expected_version bigint) returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$ declare r public.haris_incidents%rowtype; begin update public.haris_incidents set affected_entities=p_record->'affected_entities',affected_devices=p_record->'affected_devices',updated_at=(p_record->>'updated_at')::timestamptz,severity=p_record->>'severity',priority=p_record->>'priority',state=p_record->>'state',plan_version=(p_record->>'plan_version')::integer,warden_decision=p_record->>'warden_decision',verification_state=p_record->>'verification_state',recovery_state=p_record->>'recovery_state',outcome=p_record->>'outcome',closed_at=(p_record->>'closed_at')::timestamptz,version=version+1 where incident_id=p_record->>'incident_id' and version=p_expected_version returning * into r; if not found then raise exception 'haris_version_conflict'; end if; return to_jsonb(r); end; $$;
create or replace function public.haris_append_incident_transition(p_record jsonb) returns jsonb language sql security definer set search_path=public,pg_temp as $$ insert into public.haris_incident_transitions(incident_id,from_state,to_state,occurred_at,trigger_event_id,actor,reason_code,trace_id) values(p_record->>'incident_id',p_record->>'from_state',p_record->>'to_state',(p_record->>'occurred_at')::timestamptz,p_record->>'trigger_event_id',p_record->>'actor',p_record->>'reason_code',p_record->>'trace_id') returning to_jsonb(haris_incident_transitions) $$;

create or replace function public.haris_save_action(p_record jsonb,p_expected_version bigint) returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$ declare r public.haris_action_commands%rowtype; created boolean:=false; begin if p_expected_version=-1 then perform pg_advisory_xact_lock(hashtextextended(p_record->>'idempotency_key',0)); select * into r from public.haris_action_commands where command_id=p_record->>'command_id' or idempotency_key=p_record->>'idempotency_key' limit 1; if not found then insert into public.haris_action_commands select * from jsonb_populate_record(null::public.haris_action_commands,p_record||jsonb_build_object('version',0)) returning * into r; created:=true; end if; return to_jsonb(r)||jsonb_build_object('_created',created); else update public.haris_action_commands set state=p_record->>'state',provider_resource_id=p_record->>'provider_resource_id',attempt_count=(p_record->>'attempt_count')::integer,last_attempt_at=(p_record->>'last_attempt_at')::timestamptz,completed_at=(p_record->>'completed_at')::timestamptz,failure_reason=p_record->>'failure_reason',version=version+1 where command_id=p_record->>'command_id' and version=p_expected_version returning * into r; if not found then raise exception 'haris_version_conflict'; end if; return to_jsonb(r); end if; end; $$;
create or replace function public.haris_acquire_resource(p_record jsonb) returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$ declare r public.haris_resource_ownership%rowtype; begin perform pg_advisory_xact_lock(hashtextextended(p_record->>'resource_key',0)); insert into public.haris_resource_ownership(resource_key,resource_type,owner_incident_id,ownership_state,acquired_at,released_at,lease_started_at,lease_expires_at,renewable,adopted_by_incident,provider_resource_id,version) values(p_record->>'resource_key',p_record->>'resource_type',p_record->>'owner_incident_id','OWNED',(p_record->>'acquired_at')::timestamptz,null,(p_record->>'lease_started_at')::timestamptz,(p_record->>'lease_expires_at')::timestamptz,coalesce((p_record->>'renewable')::boolean,false),coalesce((p_record->>'adopted_by_incident')::boolean,false),p_record->>'provider_resource_id',0) returning * into r; return to_jsonb(r); exception when unique_violation then raise exception 'haris_resource_already_owned'; end; $$;
create or replace function public.haris_release_resource(p_resource_key text,p_incident_id text,p_expected_version bigint) returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$ declare r public.haris_resource_ownership%rowtype; begin update public.haris_resource_ownership set ownership_state='RELEASED',released_at=now(),version=version+1 where resource_key=p_resource_key and owner_incident_id=p_incident_id and ownership_state='OWNED' and version=p_expected_version returning * into r; if not found then raise exception 'haris_resource_conflict'; end if; return to_jsonb(r); end; $$;

create or replace function public.haris_save_verification(p_record jsonb) returns jsonb language sql security definer set search_path=public,pg_temp as $$ insert into public.haris_verifications select * from jsonb_populate_record(null::public.haris_verifications,p_record) on conflict(verification_id) do update set state=excluded.state,updated_at=excluded.updated_at,result=excluded.result,reason=excluded.reason returning to_jsonb(haris_verifications) $$;
create or replace function public.haris_save_recovery(p_record jsonb) returns jsonb language sql security definer set search_path=public,pg_temp as $$ insert into public.haris_recoveries select * from jsonb_populate_record(null::public.haris_recoveries,p_record) on conflict(recovery_id) do update set state=excluded.state,completed_at=excluded.completed_at,failure_reason=excluded.failure_reason returning to_jsonb(haris_recoveries) $$;
create or replace function public.haris_save_checkpoint(p_record jsonb) returns jsonb language sql security definer set search_path=public,pg_temp as $$ insert into public.haris_projection_snapshots(snapshot_version,last_event_sequence,projection,created_at) values((p_record->>'snapshot_version')::integer,(p_record->>'last_event_sequence')::bigint,p_record->'projection',(p_record->>'created_at')::timestamptz) returning to_jsonb(haris_projection_snapshots) $$;
create or replace function public.haris_latest_checkpoint() returns jsonb language sql security definer set search_path=public,pg_temp as $$ select coalesce((select to_jsonb(x) from public.haris_projection_snapshots x order by last_event_sequence desc limit 1),'{}') $$;
create or replace function public.haris_append_policy_cost(p_record jsonb) returns jsonb language sql security definer set search_path=public,pg_temp as $$ insert into public.haris_policy_cost_ledger select * from jsonb_populate_record(null::public.haris_policy_cost_ledger,p_record||jsonb_build_object('cost_basis','HARIS_POLICY_COST_MODEL')) returning to_jsonb(haris_policy_cost_ledger) $$;
create or replace function public.haris_policy_cost_totals(p_incident_id text,p_day_bucket date) returns jsonb language sql security definer set search_path=public,pg_temp as $$ select jsonb_build_object('incident_total',coalesce(sum(estimated_policy_cost) filter(where incident_id=p_incident_id),0),'day_total',coalesce(sum(estimated_policy_cost) filter(where day_bucket=p_day_bucket),0)) from public.haris_policy_cost_ledger $$;
create or replace function public.haris_fail_outbox(p_outbox_id text,p_owner text,p_error_safe text,p_retry boolean) returns void language plpgsql security definer set search_path=public,pg_temp as $$ begin update public.haris_event_outbox set state=case when p_retry then 'PENDING' else 'FAILED' end,last_error_safe=left(p_error_safe,256),claim_owner=null,claim_expires_at=null where outbox_id=p_outbox_id and state='CLAIMED' and claim_owner=p_owner; if not found then raise exception 'haris_outbox_claim_conflict'; end if; end; $$;
create or replace function public.haris_event_sequence() returns jsonb language sql security definer set search_path=public,pg_temp as $$ select jsonb_build_object('sequence',coalesce(max(sequence),0)) from public.haris_domain_events $$;
create or replace function public.haris_transition_incident(p_incident_id text,p_from_state text,p_to_state text,p_expected_version bigint,p_actor text,p_reason_code text,p_trace_id text,p_occurred_at timestamptz) returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$ declare r public.haris_incidents%rowtype; begin update public.haris_incidents set state=p_to_state,updated_at=p_occurred_at,closed_at=case when p_to_state in ('RESOLVED','BLOCKED','FAILED','CANCELLED') then p_occurred_at else closed_at end,version=version+1 where incident_id=p_incident_id and state=p_from_state and version=p_expected_version returning * into r; if not found then raise exception 'haris_version_conflict'; end if; insert into public.haris_incident_transitions(incident_id,from_state,to_state,occurred_at,actor,reason_code,trace_id) values(p_incident_id,p_from_state,p_to_state,p_occurred_at,p_actor,p_reason_code,p_trace_id); return to_jsonb(r); end; $$;
create or replace function public.haris_append_outbox(p_record jsonb) returns jsonb language sql security definer set search_path=public,pg_temp as $$ insert into public.haris_event_outbox(outbox_id,event_id,event_type,payload,trace_id,state,created_at) values(p_record->>'outbox_id',p_record->>'event_id',p_record->>'event_type',coalesce(p_record->'payload','{}'),p_record->>'trace_id','PENDING',(p_record->>'created_at')::timestamptz) returning to_jsonb(haris_event_outbox) $$;
create or replace function public.haris_pending_outbox(p_limit integer) returns setof public.haris_event_outbox language sql security definer set search_path=public,pg_temp as $$ select * from public.haris_event_outbox where state in ('PENDING','FAILED','CLAIMED') order by created_at limit greatest(1,least(p_limit,500)) $$;
create or replace function public.haris_read_policy_cost(p_incident_id text,p_limit integer) returns setof public.haris_policy_cost_ledger language sql security definer set search_path=public,pg_temp as $$ select * from public.haris_policy_cost_ledger where incident_id=p_incident_id order by committed_at desc limit greatest(1,least(p_limit,500)) $$;

-- One HTTP RPC = one PostgreSQL transaction for inbound canonical evidence.
create or replace function public.haris_process_inbound_event(p_event jsonb,p_projection jsonb default null,p_incident jsonb default null,p_transition jsonb default null,p_outbox jsonb default null)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare claimed_count integer; event_result jsonb; projection_result jsonb; incident_result jsonb;
begin
 if p_event is null or nullif(p_event->>'idempotency_key','') is null or nullif(p_event->>'event_id','') is null then raise exception 'haris_invalid_event'; end if;
 perform pg_advisory_xact_lock(hashtextextended(coalesce(p_event->>'correlation_key',p_event->>'idempotency_key'),0));
 insert into public.haris_event_inbox(idempotency_key,event_id,claimed_at) values(p_event->>'idempotency_key',p_event->>'event_id',now()) on conflict do nothing;
 get diagnostics claimed_count = row_count;
 if claimed_count = 0 then return jsonb_build_object('status','duplicate','event_id',p_event->>'event_id'); end if;
 event_result:=public.haris_append_domain_event(p_event);
 if coalesce((event_result->>'inserted')::boolean,false)=false then return jsonb_build_object('status','duplicate','event_id',p_event->>'event_id'); end if;
 if p_projection is not null then projection_result:=public.haris_write_network_state(p_projection,coalesce((p_projection->>'expected_version')::bigint,0)); end if;
 if p_incident is not null then incident_result:=public.haris_create_incident(p_incident); end if;
 if p_transition is not null then perform public.haris_append_incident_transition(p_transition); end if;
 if p_outbox is not null then insert into public.haris_event_outbox(outbox_id,event_id,event_type,payload,trace_id,state,created_at) values(p_outbox->>'outbox_id',p_event->>'event_id',p_outbox->>'event_type',coalesce(p_outbox->'payload','{}'),p_event->>'trace_id','PENDING',now()); end if;
 return jsonb_build_object('status','accepted','event',event_result,'projection',projection_result,'incident',incident_result);
end; $$;

revoke all on function public.haris_read_domain(text,text,bigint,integer),public.haris_write_network_state(jsonb,bigint),public.haris_create_incident(jsonb),public.haris_update_incident(jsonb,bigint),public.haris_append_incident_transition(jsonb),public.haris_save_action(jsonb,bigint),public.haris_acquire_resource(jsonb),public.haris_release_resource(text,text,bigint),public.haris_save_verification(jsonb),public.haris_save_recovery(jsonb),public.haris_save_checkpoint(jsonb),public.haris_latest_checkpoint(),public.haris_append_policy_cost(jsonb),public.haris_policy_cost_totals(text,date),public.haris_fail_outbox(text,text,text,boolean),public.haris_process_inbound_event(jsonb,jsonb,jsonb,jsonb,jsonb),public.haris_event_sequence(),public.haris_transition_incident(text,text,text,bigint,text,text,text,timestamptz),public.haris_append_outbox(jsonb),public.haris_pending_outbox(integer),public.haris_read_policy_cost(text,integer) from public, anon, authenticated;
grant execute on function public.haris_read_domain(text,text,bigint,integer),public.haris_write_network_state(jsonb,bigint),public.haris_create_incident(jsonb),public.haris_update_incident(jsonb,bigint),public.haris_append_incident_transition(jsonb),public.haris_save_action(jsonb,bigint),public.haris_acquire_resource(jsonb),public.haris_release_resource(text,text,bigint),public.haris_save_verification(jsonb),public.haris_save_recovery(jsonb),public.haris_save_checkpoint(jsonb),public.haris_latest_checkpoint(),public.haris_append_policy_cost(jsonb),public.haris_policy_cost_totals(text,date),public.haris_fail_outbox(text,text,text,boolean),public.haris_process_inbound_event(jsonb,jsonb,jsonb,jsonb,jsonb),public.haris_event_sequence(),public.haris_transition_incident(text,text,text,bigint,text,text,text,timestamptz),public.haris_append_outbox(jsonb),public.haris_pending_outbox(integer),public.haris_read_policy_cost(text,integer) to service_role;
