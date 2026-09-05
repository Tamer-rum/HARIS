create extension if not exists vector;

create table if not exists haris_incidents (
  incident_id text primary key,
  summary text not null,
  storm_type text not null,
  peak_congestion_level text not null,
  peak_confidence_level integer not null check (peak_confidence_level between 0 and 100),
  affected_cells jsonb not null,
  affected_devices jsonb not null,
  actions jsonb not null,
  executed_actions jsonb not null,
  outcome text not null,
  verification jsonb not null default '{}'::jsonb,
  rollback jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists haris_device_policies (
  device_id text primary key,
  mission_tier integer not null check (mission_tier between 1 and 3),
  max_qos_cost_usd double precision not null,
  emergency_slice text not null,
  allow_autonomous_action boolean not null
);

create index if not exists haris_incidents_created_at_idx on haris_incidents(created_at desc);
