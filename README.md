# HARIS

**Hybrid Agent for Resilient Infrastructure and Service-continuity** — a safety-bounded autonomous network-resilience engineer for MENA service continuity during sandstorms, heat, congestion, and critical-service risk.

## Judge view

HARIS senses Nokia/CAMARA network evidence, creates a bounded plan, has WARDEN enforce safety policy, verifies the outcome, reverses failed changes, and stores a tamper-evident audit trail. The public console is supervisory; Render/FastAPI is the authority for execution, Trusted Dispatch, and durable history.

```text
Browser -> Streamlit supervisory console -> Render/FastAPI
        -> authoritative HarisAgentSystem -> LangGraph -> WARDEN
        -> Nokia Network as Code clients -> MemoryStore -> Supabase
```

Set the public backend base URL as `HARIS_BACKEND_URL` in Streamlit. Configure Nokia, Supabase, OAuth, and LLM secrets only in the backend host.

## Closed loop and roles

`SENTINEL -> CARTOGRAPHER -> TRIAGE -> WARDEN -> ACTUATOR -> VERIFY -> ROLLBACK/LEARN`

The five role-scoped responsibilities are sensing, exposure/location interpretation, bounded planning, action rationale/execution, and safety/trust authority. LangGraph and deterministic policy own execution. Optional CrewAI/LLM advice is bounded and cannot invent assets or KPIs, bypass WARDEN, or execute actions.

## Seven capability groups

1. Congestion Insights
2. Device Status / Reachability
3. Location Retrieval
4. Geofencing
5. Quality on Demand (QoD)
6. Network Slicing
7. Number Verification / SIM Swap

Trusted Dispatch uses the final group only for privileged field intervention: deterministic engineer selection -> consent-bound Number Verification -> server-side receipt -> SIM Swap -> WARDEN allow/block -> fresh fallback engineer. Routine remediation never requires identity verification.

## Playbooks and guardrails

- **Storm Shield:** dust evidence, meaningful categorical congestion, and tier-1 assets trigger QoD, protected-slice proposal where available, and policy-controlled geofence scope.
- **Capacity Harvest:** High congestion plus the configured minimum of tier-3 bulk assets requests low-bandwidth QoD and records a *defer non-critical upload recommendation*. HARIS does not claim to operate an application upload queue.
- **Energy Guard:** low battery plus timestamped sustained High congestion (default 10 minutes) preserves emergency traffic and sheds eligible bulk traffic.
- **Trusted Dispatch:** privileged physical intervention only, fail-closed.

WARDEN enforces confidence, QoD spend, maximum devices per cycle, blast radius, capability configuration, and live-write policy. Verify uses fresh Nokia categorical congestion evidence; unavailable live numeric values remain `N/A`. Failed actions roll back only HARIS-owned resources.

## Modes and fallback

| Mode | Behaviour |
| --- | --- |
| `fixture` | Deterministic, explicitly simulated demo evidence and operations. |
| `live_read_only` | Real Nokia reads and WARDEN-reviewed proposals; no writes. |
| `live_write` | Explicit writes only after WARDEN validates supported operator resources. |

`NAC_MODE=live` maps safely to `live_read_only`. The scheduler is opt-in, non-overlapping, defaults to 60 seconds, and requires `ENABLE_LIVE_WRITE_LOOP=true` in addition to `live_write`. If `PUBLIC_DUST_FEED_URL` is unset, demo weather is fixture-backed; the adapter has validation, timeout, cached/fallback labels, and safe failure handling.

## Nokia validation status

**Fully Nokia Simulator / E2E verified:** QoD create/get/delete lifecycle, Number Verification, SIM Swap, and Geofencing callback.

**Partial / sandbox-limited:** Network Slicing integration is implemented. The protected sandbox slice reached `AVAILABLE`; activation was accepted/in-progress but never reached `OPERATING`. HARIS did not force device attachment or retry activation.

5G-to-LTE switching and microwave-to-fibre steering are operator-domain roadmap functions, not executed controls.

## History and security

HARIS uses **backend-only Supabase durable history with a tamper-evident append-only SHA-256 audit chain. Persistence across a Render backend restart has been verified.** It is not cryptographically immutable or digitally signed. Supabase credentials are backend-only. OAuth code/state, authorization URLs, tokens, secrets, consent tokens, and unmasked phone numbers are excluded from status, trace, audit, and history views.

## Local setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
python run_api.py
streamlit run app.py
```

Use `NAC_MODE=fixture` for the deterministic judge demo. The public frontend URL is the Streamlit deployment URL; the public backend base URL is the Render deployment URL configured through `HARIS_BACKEND_URL`.

## Demo walkthrough

1. Run Autonomous HARIS: fixture storm -> congestion -> tier-1 prioritization -> WARDEN-gated QoD/slice-degraded path -> verify -> learn/audit.
2. Run the fixture-only Field Intervention Demo: simulated physical-site evidence -> Trusted Dispatch -> Nokia consent -> SIM Swap -> WARDEN block/fallback.
3. Inspect History & Audit for sanitized replayable records and chain validity, including the verified persistence across a Render backend restart.

## Safe test command

```powershell
.venv\Scripts\python.exe -m unittest -q test_haris_core.py test_engineering_completion.py test_dispatch_security.py test_dispatch_continuation.py test_autonomous_dispatch.py test_nokia_qod.py test_nokia_congestion.py test_history_persistence.py test_frontend_final_polish.py tests.test_core
```

Tests use fixtures and mocks. They do not make real Nokia calls, OAuth flows, or mutations.

## Limitations / roadmap

Live writes fail closed without explicit operator configuration and a supported Nokia capability. HARIS does not create new slices, force stuck slice activation, perform bearer switching, steer transport paths, or control third-party upload applications. Production requires protected backend secrets and operator resource mappings; the durable Supabase history deployment is already verified.
