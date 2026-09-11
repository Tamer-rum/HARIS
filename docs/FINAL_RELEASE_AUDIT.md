# HARIS final pitch-to-code audit

This is an internal release aid. Evidence terms are restricted to **REAL**, **DERIVED**, **SIMULATED**, and **UNAVAILABLE**. “Implemented” describes code state, not proof of a live provider outcome.

| Claim | Code location | Status | Evidence class | Risk | Final demo wording |
|---|---|---|---|---|---|
| Autonomous Network Resilience Engineer | `runtime_events.py`, `durable_reasoning.py`, `durable_execution.py`, `durable_reconciliation.py` | IMPLEMENTED | DERIVED / REAL_PARTIAL QoD evidence | Controlled real QoD reached provider `AVAILABLE`, network verification remained `UNCHANGED`, and cleanup was verified | “HARIS durably senses, reasons, authorizes, acts, verifies, and reconciles without equating provider acceptance with network improvement.” |
| Five named agent roles | `agents.py` (`HarisAgentSystem` graph) | IMPLEMENTED | SIMULATED / DERIVED | Crew advisory is optional; WARDEN remains deterministic | “Sentinel, Cartographer, Triage, Actuator, and WARDEN are explicit graph roles.” |
| LangGraph supervisor | `agents.py` graph construction | IMPLEMENTED | DERIVED | Dependency warning is non-blocking | “LangGraph coordinates the bounded reasoning stages.” |
| CrewAI advisory | `agents.py` advisory path | IMPLEMENTED | DERIVED / UNAVAILABLE | Gracefully unavailable without configured approved provider | “CrewAI may advise; it cannot authorize execution.” |
| Deterministic WARDEN | `agents.py::_warden`, `durable_reasoning.py`, `durable_execution.py::_authorization_error` | IMPLEMENTED | DERIVED | None known | “WARDEN is the final deterministic safety authority.” |
| Confidence guardrail | `config.py::Guardrails`, `durable_execution.py` | IMPLEMENTED | DERIVED | None known | “Below 0.72 fails closed.” |
| Blast-radius guardrail | `durable_reasoning.py`, `durable_execution.py` | IMPLEMENTED | DERIVED | Requires a fresh authoritative observed fleet | “Modified unique devices divided by the currently observed registered fleet; above 0.70 fails closed.” |
| Two-device guardrail | `config.py::Guardrails`, `durable_execution.py` | IMPLEMENTED | DERIVED | None known | “At most two protected devices per cycle.” |
| Cost guardrail | `durable_core.py`, `durable_execution.py` | IMPLEMENTED | DERIVED | Policy cost, not Nokia billing | “Durable policy-cost reservations are bounded.” |
| Resource ownership | `durable_core.py`, `postgres_persistence.py`, migrations 002-007 | IMPLEMENTED | REAL database proof / DERIVED runtime | None known | “Database-enforced ownership prevents concurrent control of the same resource; Migration 007 adds lease-generation ownership safety.” |
| Durable event and incident core | `durable_core.py`, `runtime_events.py`, `postgres_persistence.py` | IMPLEMENTED | REAL database proof | Push-first Nokia event coverage is partial | “Supabase/PostgreSQL is authoritative; process views are disposable.” |
| Controlled actuation | `durable_execution.py` | IMPLEMENTED | DERIVED / SIMULATED / REAL_PARTIAL | Controlled real QoD executed once; network improvement was not proven | “READY→SENT is durable; SENT and provider acceptance are never called network success.” |
| Verification after action | `durable_execution.py`, `durable_reconciliation.py` | IMPLEMENTED | DERIVED / SIMULATED | Live numeric evidence may remain unavailable | “Success requires authoritative post-action verification.” |
| Rollback and recovery | `durable_execution.py`, `durable_reconciliation.py`, `agents.py::recover_normalized_incident` | IMPLEMENTED | DERIVED / SIMULATED | Provider ambiguity requires reconciliation, not resend | “Unchanged/degraded results enter controlled recovery; rollback is reverified.” |
| Crash/replay safety | `durable_core.py`, `runtime_events.py`, migrations 002-007 | IMPLEMENTED | REAL database proof | None known | “Restart, idempotency, stale-write, recovery CAS, lease generation, claim renewal, and per-run outbox isolation were validated.” |
| Continuous loop | `observations.py`, `runtime_events.py`, `durable_reconciliation.py`, `run_api.py` | IMPLEMENTED | DERIVED | Push-first provider subscriptions remain partial | “Bounded polling and durable events drive the loop; reconciliation is safe-read only.” |
| Sense-to-decide-to-act under 60 seconds | `config.py`, `scheduler.py`, capability-specific observation cadence | PARTIAL | DERIVED / UNAVAILABLE | Not measured as end-to-end provider recovery | “Default cadence is 60 seconds; no measured provider recovery SLA is claimed.” |
| Congestion Insights | `nokia_clients.py`, `observations.py`, `agents.py` | IMPLEMENTED | REAL where returned | Nokia evidence is categorical; unavailable numeric KPIs remain N/A | “HARIS consumes Nokia categorical congestion without inventing numeric KPIs.” |
| Device Status / Reachability | `nokia_clients.py`, `observations.py`, `agents.py` | IMPLEMENTED | REAL where returned | Unavailable observers are excluded | “Returned reachability is used; failure is not converted to unreachable.” |
| Location Retrieval | `nokia_clients.py`, `agents.py` | IMPLEMENTED | REAL where returned | No fabricated coordinates | “Only provider-returned positions are displayed.” |
| Geofencing | `nokia_clients.py`, `agents.py`, callback route | IMPLEMENTED / FAIL-CLOSED | Transport path evidence / SIMULATED orchestration | Authenticated provider callback provenance remains unproven | “The callback transport path was demonstrated; HARIS currently refuses geofence callbacks until provider authentication can be proven.” |
| Quality on Demand | `nokia_clients.py`, `durable_execution.py`, real-QoD harness | IMPLEMENTED | REAL_PARTIAL | Provider reached `AVAILABLE`; network verification was `UNCHANGED`; rollback cleanup was verified | “The controlled real provider lifecycle is proven, but network improvement is not.” |
| Network Slicing | `nokia_clients.py`, `durable_execution.py`, `playbooks.py` | PARTIAL | REAL read / UNAVAILABLE attachment | Existing slice did not reach OPERATING | “HARIS inspected the protected slice and correctly refused attachment while not OPERATING.” |
| Number Verification / SIM Swap | `nokia_clients.py`, `dispatch.py`, `agents.py` | IMPLEMENTED | REAL simulator proof | Consent and fresh receipt required | “Identity verification and SIM Swap evidence feed WARDEN only for privileged intervention.” |
| Trusted Dispatch | `agents.py`, `dispatch.py`, backend-owned OAuth routes | IMPLEMENTED | REAL integration proof / SIMULATED field incident | Fallback Engineer B completion is not claimed | “A recent SIM swap blocked Engineer A and opened a separate consent-bound fallback.” |
| Storm Shield | `playbooks.py` | IMPLEMENTED | SIMULATED / DERIVED | Slice step may degrade truthfully | “Correlates storm and congestion evidence, then protects tier-1 assets under WARDEN.” |
| Capacity Harvest | `playbooks.py` | IMPLEMENTED | SIMULATED / DERIVED | Upload deferral is recommendation state only | “Bulk telemetry is deprioritized by policy; no third-party upload control is claimed.” |
| Energy Guard | `playbooks.py` | IMPLEMENTED | SIMULATED / DERIVED | Battery evidence is fixture-only unless genuinely supplied | “Timestamped sustained evidence is required; malformed or stale history fails safe.” |
| Learning/memory | `memory.py`, `agents.py` | IMPLEMENTED | DERIVED | Bounded episodic adjustment, not model training | “Relevant prior outcomes may add at most +0.03 confidence and never override WARDEN.” |
| Reasoning trace | `agents.py`, `durable_reasoning.py`, `app.py` | IMPLEMENTED | DERIVED | Sanitized summaries only | “The console shows the bounded agent and safety trace.” |
| Audit chain | `memory.py`, Supabase history persistence | IMPLEMENTED | REAL restart proof | Tamper-evident, not signed or immutable | “Backend-only append-only SHA-256 chaining persisted across a Render restart.” |
| 5G/LTE fallback | policy/reasoning text only | PARTIAL | UNAVAILABLE | No supported bearer-switch API | “Recommendation only; no live bearer change is claimed.” |
| Microwave/fibre steering | roadmap only | NOT_IMPLEMENTED | UNAVAILABLE | No control API | “Roadmap only.” |

## Release interpretation

- Automated verification is **OFFLINE_SAFE** and must report zero external network/provider calls.
- The completed controlled real-QoD validation used durable authority and WARDEN approval: provider lifecycle succeeded, network verification was `UNCHANGED`, cleanup was verified, and the final classification remains `REAL_PARTIAL`.
- Migrations 002-007 are part of the current reviewed durable contract; migrations 006 and 007 were remotely verified by the operator.
