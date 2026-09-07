-- HARIS tamper-evident append-only audit history.
-- Apply only from the Supabase SQL editor as the database owner. The Render
-- backend uses the two RPCs below; browsers receive no table privileges.

create table if not exists public.haris_audit_records (
    sequence bigint generated always as identity primary key,
    checkpoint_id text not null unique,
    cycle_id text,
    incident_id text,
    created_at timestamptz not null,
    previous_hash text,
    record_hash text not null unique,
    record jsonb not null
);

create index if not exists haris_audit_records_cycle_id_idx
    on public.haris_audit_records (cycle_id);
create index if not exists haris_audit_records_incident_id_idx
    on public.haris_audit_records (incident_id);
create index if not exists haris_audit_records_created_at_idx
    on public.haris_audit_records (created_at, sequence);

-- No browser/public table access. RLS is defence in depth; permissions below
-- are the direct-table control. No RLS policy is created deliberately.
alter table public.haris_audit_records enable row level security;
revoke all on table public.haris_audit_records from public, anon, authenticated, service_role;
revoke all on sequence public.haris_audit_records_sequence_seq from public, anon, authenticated, service_role;

-- Read is only available through this controlled, ordered RPC.
create or replace function public.read_haris_audit_records()
returns table (
    sequence bigint,
    checkpoint_id text,
    cycle_id text,
    incident_id text,
    created_at timestamptz,
    previous_hash text,
    record_hash text,
    record jsonb
)
language sql
security definer
set search_path = public, pg_temp
as $$
    select sequence, checkpoint_id, cycle_id, incident_id, created_at,
           previous_hash, record_hash, record
    from public.haris_audit_records
    order by sequence asc;
$$;

-- Serializes the entire tail-check/insert operation. A duplicate checkpoint is
-- accepted only when every immutable value matches exactly; it is never updated.
create or replace function public.append_haris_audit_record(
    p_checkpoint_id text,
    p_cycle_id text,
    p_incident_id text,
    p_created_at timestamptz,
    p_previous_hash text,
    p_record_hash text,
    p_record jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
    v_existing public.haris_audit_records%rowtype;
    v_tail_hash text;
    v_record jsonb;
begin
    perform pg_advisory_xact_lock(hashtext('haris_audit_records_append'));

    select * into v_existing
    from public.haris_audit_records
    where checkpoint_id = p_checkpoint_id;

    if found then
        if v_existing.cycle_id is distinct from p_cycle_id
           or v_existing.incident_id is distinct from p_incident_id
           or v_existing.created_at is distinct from p_created_at
           or v_existing.previous_hash is distinct from p_previous_hash
           or v_existing.record_hash is distinct from p_record_hash
           or v_existing.record is distinct from p_record then
            raise exception 'haris_audit_checkpoint_conflict';
        end if;
        return jsonb_build_object('record', v_existing.record, 'inserted', false);
    end if;

    select record_hash into v_tail_hash
    from public.haris_audit_records
    order by sequence desc
    limit 1;

    if v_tail_hash is distinct from p_previous_hash then
        raise exception 'haris_audit_tail_conflict';
    end if;

    insert into public.haris_audit_records (
        checkpoint_id, cycle_id, incident_id, created_at,
        previous_hash, record_hash, record
    ) values (
        p_checkpoint_id, p_cycle_id, p_incident_id, p_created_at,
        p_previous_hash, p_record_hash, p_record
    ) returning record into v_record;

    return jsonb_build_object('record', v_record, 'inserted', true);
end;
$$;

-- The server-only service_role can execute controlled RPCs, but has no direct
-- SELECT/INSERT/UPDATE/DELETE table privileges. service_role bypasses RLS, so
-- do not describe RLS as protection from a leaked service-role credential.
revoke all on function public.read_haris_audit_records() from public, anon, authenticated;
revoke all on function public.append_haris_audit_record(text, text, text, timestamptz, text, text, jsonb)
    from public, anon, authenticated;
grant execute on function public.read_haris_audit_records() to service_role;
grant execute on function public.append_haris_audit_record(text, text, text, timestamptz, text, text, jsonb)
    to service_role;
