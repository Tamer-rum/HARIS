# HARIS persistence activation

Phase 6C.2-A supplies the server-only Supabase HTTP/RPC transport and the
offline-reviewed migration contract. Nothing in the normal test suite applies
the migration or contacts Supabase.

## Manual activation order

A. Open the intended Supabase project using an authorized database-owner
   session.

B. Open that project's SQL Editor. Do not run the migration from HARIS,
   Streamlit, browser JavaScript, or a public client.

C. Paste `supabase/migrations/002_haris_durable_event_incident_core.sql` into
   the SQL Editor.

D. Run migration 002 exactly once.

E. Still in the SQL Editor, run
   `supabase/verification/verify_002.sql`. Every returned `passed` value must be
   `true`, with no missing or unsafe object. This script opens a read-only
   transaction, inspects metadata only, and rolls back.

F. Configure these values only on the controlled backend environment used for
   validation (and later on Render/FastAPI after Phase 6C.2-B approval):

   ```text
   HARIS_PERSISTENCE_MODE=postgres
   SUPABASE_URL=https://<exact-project-ref>.supabase.co
   SUPABASE_KEY=<server-only service-role/secret key>
   ```

   `SUPABASE_KEY` must be a server-only service-role/secret key because the
   migration denies `anon` and `authenticated` direct table/RPC access. Never
   use a publishable/anon key for this adapter. Never put any of these values in
   Streamlit/browser configuration, logs, screenshots, audit records, or
   client responses. HARIS accepts one exact configured HTTPS project hostname;
   redirects, URL credentials, query strings, fragments, custom paths, wildcard
   hosts, and non-Supabase hosts are rejected.

G. Before any write/restart validation, run the smallest read-only RPC
   preflight from a controlled backend shell:

   ```powershell
   $env:HARIS_RUNTIME_ENV='persistence_integration'
   $env:HARIS_ALLOW_PERSISTENCE_INTEGRATION='true'
   $env:HARIS_PERSISTENCE_MODE='postgres'
   .\.venv\Scripts\python.exe -m external.validate_persistence_restart --preflight
   ```

   Expected safe output is `PERSISTENCE_PREFLIGHT_OK`. The probe invokes only
   `haris_read_domain` for a guaranteed non-matching synthetic network key with
   limit 1. It does not create or update a HARIS record.

   Only after the preflight succeeds, run the double-gated two-process restart
   validator:

   ```powershell
   .\.venv\Scripts\python.exe -m external.validate_persistence_restart
   ```

   Stop unless the final status is `RESTART_PERSISTENCE_VALIDATED`. Its records
   use the reserved `PERSISTENCE-TEST-*` namespace and are excluded from NOC
   operational views. This is the first step that intentionally writes test
   records to the configured project.

## Safe failure meanings

- `PERSISTENCE_NOT_CONFIGURED`: mode, URL, key, HTTPS project host, or URL shape
  is missing/invalid.
- `PERSISTENCE_UNAVAILABLE`: timeout, DNS/TLS/connectivity problem, redirect,
  rate limit, upstream 5xx response, or a closed integration network gate.
- `PERSISTENCE_AUTH_FAILED`: Supabase returned 401/403; verify only the backend
  secret/key and project pairing.
- `PERSISTENCE_SCHEMA_NOT_READY`: the required RPC route returned 404; apply and
  verify migration 002 in the exact configured project.
- `RECONSTRUCTION_FAILED`: persistence was reachable but the bounded repository
  reconstruction contract failed.
- `PROJECTION_INTEGRITY_FAILED`: persisted domain state violated a reconstruction
  invariant. HARIS remains not ready; do not bypass or fall back to memory.

The offline result `OFFLINE_HARNESS_ORCHESTRATION=PASS` proves only subprocess
orchestration. It is not evidence that Supabase persistence has been validated.
No Nokia, OAuth, LLM, Redis, or network-action integration belongs in either
persistence validation command.
