-- HARIS forward-only conflict status alignment.
-- Replaces only the eight deployed RPC bodies whose known domain conflicts
-- must be surfaced by PostgREST as HTTP 409. No table or durable row is
-- created, altered, rewritten, or removed by this migration.

create or replace function public.haris_ack_outbox(p_outbox_id text, p_owner text)
returns void language plpgsql security definer set search_path = public, pg_temp as $$
begin
  update public.haris_event_outbox set state='SENT', sent_at=now(), claim_expires_at=null
  where outbox_id=p_outbox_id and state='CLAIMED' and claim_owner=p_owner;
  if not found then
    raise sqlstate 'PT409' using message = 'haris_outbox_claim_conflict';
  end if;
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
   if not found then
     raise sqlstate 'PT409' using message = 'haris_version_conflict';
   end if;
 end if; return to_jsonb(r); end; $$;

create or replace function public.haris_update_incident(p_record jsonb,p_expected_version bigint)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$ declare r public.haris_incidents%rowtype; begin
 update public.haris_incidents set affected_entities=p_record->'affected_entities',affected_devices=p_record->'affected_devices',updated_at=(p_record->>'updated_at')::timestamptz,severity=p_record->>'severity',priority=p_record->>'priority',state=p_record->>'state',plan_version=(p_record->>'plan_version')::integer,warden_decision=p_record->>'warden_decision',verification_state=p_record->>'verification_state',recovery_state=p_record->>'recovery_state',outcome=p_record->>'outcome',closed_at=(p_record->>'closed_at')::timestamptz,version=version+1 where incident_id=p_record->>'incident_id' and version=p_expected_version returning * into r;
 if not found then
   raise sqlstate 'PT409' using message = 'haris_version_conflict';
 end if;
 return to_jsonb(r); end; $$;

create or replace function public.haris_save_action(p_record jsonb,p_expected_version bigint)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$ declare r public.haris_action_commands%rowtype; created boolean:=false; begin
 if p_expected_version=-1 then perform pg_advisory_xact_lock(hashtextextended(p_record->>'idempotency_key',0)); select * into r from public.haris_action_commands where command_id=p_record->>'command_id' or idempotency_key=p_record->>'idempotency_key' limit 1; if not found then insert into public.haris_action_commands select * from jsonb_populate_record(null::public.haris_action_commands,p_record||jsonb_build_object('version',0)) returning * into r; created:=true; end if; return to_jsonb(r)||jsonb_build_object('_created',created);
 else update public.haris_action_commands set state=p_record->>'state',provider_resource_id=p_record->>'provider_resource_id',attempt_count=(p_record->>'attempt_count')::integer,last_attempt_at=(p_record->>'last_attempt_at')::timestamptz,completed_at=(p_record->>'completed_at')::timestamptz,failure_reason=p_record->>'failure_reason',version=version+1 where command_id=p_record->>'command_id' and version=p_expected_version returning * into r;
   if not found then
     raise sqlstate 'PT409' using message = 'haris_version_conflict';
   end if;
   return to_jsonb(r);
 end if; end; $$;

create or replace function public.haris_transition_incident(p_incident_id text,p_from_state text,p_to_state text,p_expected_version bigint,p_actor text,p_reason_code text,p_trace_id text,p_occurred_at timestamptz)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$ declare r public.haris_incidents%rowtype; begin
 update public.haris_incidents set state=p_to_state,updated_at=p_occurred_at,closed_at=case when p_to_state in ('RESOLVED','BLOCKED','FAILED','CANCELLED') then p_occurred_at else closed_at end,version=version+1 where incident_id=p_incident_id and state=p_from_state and version=p_expected_version returning * into r;
 if not found then
   raise sqlstate 'PT409' using message = 'haris_version_conflict';
 end if;
 insert into public.haris_incident_transitions(incident_id,from_state,to_state,occurred_at,actor,reason_code,trace_id) values(p_incident_id,p_from_state,p_to_state,p_occurred_at,p_actor,p_reason_code,p_trace_id);
 return to_jsonb(r); end; $$;

create or replace function public.haris_acquire_resource(p_record jsonb)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$ declare r public.haris_resource_ownership%rowtype; begin
 perform pg_advisory_xact_lock(hashtextextended(p_record->>'resource_key',0));
 insert into public.haris_resource_ownership(resource_key,resource_type,owner_incident_id,ownership_state,acquired_at,released_at,lease_started_at,lease_expires_at,renewable,adopted_by_incident,provider_resource_id,version) values(p_record->>'resource_key',p_record->>'resource_type',p_record->>'owner_incident_id','OWNED',(p_record->>'acquired_at')::timestamptz,null,(p_record->>'lease_started_at')::timestamptz,(p_record->>'lease_expires_at')::timestamptz,coalesce((p_record->>'renewable')::boolean,false),coalesce((p_record->>'adopted_by_incident')::boolean,false),p_record->>'provider_resource_id',0) returning * into r;
 return to_jsonb(r);
 exception when unique_violation then
   raise sqlstate 'PT409' using message = 'haris_resource_already_owned';
end; $$;

create or replace function public.haris_release_resource(p_resource_key text,p_incident_id text,p_expected_version bigint)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$ declare r public.haris_resource_ownership%rowtype; begin
 update public.haris_resource_ownership set ownership_state='RELEASED',released_at=now(),version=version+1 where resource_key=p_resource_key and owner_incident_id=p_incident_id and ownership_state='OWNED' and version=p_expected_version returning * into r;
 if not found then
   raise sqlstate 'PT409' using message = 'haris_resource_conflict';
 end if;
 return to_jsonb(r); end; $$;

create or replace function public.haris_fail_outbox(p_outbox_id text,p_owner text,p_error_safe text,p_retry boolean)
returns void language plpgsql security definer set search_path=public,pg_temp as $$ begin
 update public.haris_event_outbox set state=case when p_retry then 'PENDING' else 'FAILED' end,last_error_safe=left(p_error_safe,256),claim_owner=null,claim_expires_at=null where outbox_id=p_outbox_id and state='CLAIMED' and claim_owner=p_owner;
 if not found then
   raise sqlstate 'PT409' using message = 'haris_outbox_claim_conflict';
 end if;
end; $$;

revoke all on function public.haris_ack_outbox(text,text),public.haris_write_network_state(jsonb,bigint),public.haris_update_incident(jsonb,bigint),public.haris_save_action(jsonb,bigint),public.haris_transition_incident(text,text,text,bigint,text,text,text,timestamptz),public.haris_acquire_resource(jsonb),public.haris_release_resource(text,text,bigint),public.haris_fail_outbox(text,text,text,boolean) from public, anon, authenticated;
grant execute on function public.haris_ack_outbox(text,text),public.haris_write_network_state(jsonb,bigint),public.haris_update_incident(jsonb,bigint),public.haris_save_action(jsonb,bigint),public.haris_transition_incident(text,text,text,bigint,text,text,text,timestamptz),public.haris_acquire_resource(jsonb),public.haris_release_resource(text,text,bigint),public.haris_fail_outbox(text,text,text,boolean) to service_role;
