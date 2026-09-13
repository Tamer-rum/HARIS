# HARIS — Prototype Phase Submission

Copy each section into the matching field of the HackerEarth submission form.

| field | value |
|---|---|
| Project | **HARIS (حَارِس)** — Hybrid Agent for Resilient Infrastructure and Service-continuity |
| Theme | Theme 6 — Climate Resilience & Environmental Monitoring |
| Code repository | <https://github.com/Tamer-rum/HARIS> |
| Live web app | <https://8eke73hh9xuxojhb3tabkh.streamlit.app/> (FastAPI backend on Render: <https://haris-0wjm.onrender.com>) |
| Demo video (≤ 3 min) | `<YouTube unlisted / Drive link>` ← fill in |

---

## 1 · Demo description

HARIS is an autonomous AI operations engineer that keeps mission-critical
devices connected when sandstorms and extreme heat degrade mobile networks
across MENA. It runs a safety-bounded loop — **Sense → Reason → Guard → Act →
Verify → Learn** — over GSMA Open Gateway CAMARA capabilities on Nokia Network
as Code. The video shows:

1. **The storm arrives.** In the Autonomous Operations view, one press runs a
   full cycle. SENTINEL reads congestion and device status under a dust
   advisory: High congestion on cells T02, T03 and T05, eight exposed devices,
   and a 15-minute risk forecast.
2. **The agent decides.** CARTOGRAPHER locates the assets. TRIAGE turns the
   Storm Shield and Capacity Harvest playbooks into six bounded actions for the
   two tier-1 assets (an ambulance and a SCADA gateway). An AI advisor ranks
   those candidates — and it can run **on the laptop itself** (a local Ollama
   model), because a storm can take the internet with it.
3. **WARDEN guards.** The deterministic safety authority checks confidence,
   blast radius (25 %), QoD spend ($1.50) and the device cap before anything
   touches the network. The AI can only rank what policy already allowed.
4. **The agent acts and verifies.** ACTUATOR opens guaranteed Quality on Demand
   sessions, attaches the protected slice and sets geofences. VERIFY reads the
   network back: T03 falls from High to Low — on the storm map the tower turns from red to green — 8 of 8 devices reachable,
   incident MITIGATED. If verification fails, HARIS rolls back only the
   resources it owns.
5. **The trust layer refuses.** A tower power fault needs a technician on
   site. Trusted Dispatch runs Nokia Number Verification (OAuth consent) and
   SIM Swap on the engineer. A recently swapped SIM is detected, **WARDEN
   blocks the dispatch**, and HARIS opens a fresh consent path for the next
   engineer instead of trusting a hijackable number.
6. **Proof on the real platform.** One button runs a live Nokia read check
   (congestion, reachability, location), and History & Audit shows the
   SHA-256 hash-chained audit trail and its verification.

Every claim on screen carries an evidence label — **REAL, DERIVED, SIMULATED
or UNAVAILABLE** — and HARIS never fills a missing measurement with an
invented number.

---

## 2 · API usage synopsis

Seven CAMARA capability groups on Nokia Network as Code, each a typed tool the
agent **decides** to call through WARDEN; the LLMs never hold mutation
authority.

| CAMARA capability | role | what HARIS does with it | agent | evidence |
|---|---|---|---|---|
| Congestion Insights | sense | primary degradation signal; categorical levels drive policy | SENTINEL | **REAL** read-only |
| Device Status / Reachability | sense · verify | exposure list; post-action verification | SENTINEL · VERIFY | **REAL** read-only |
| Location Retrieval | scope | where each critical asset actually is | CARTOGRAPHER | **REAL** read-only |
| Geofencing | scope | storm-impact area subscriptions and callbacks | CARTOGRAPHER | transport observed; callback fails closed until provider authentication is proven |
| Quality on Demand | act | guaranteed profile for tier-1, lower bandwidth for bulk; expiry and rollback | ACTUATOR | **REAL_PARTIAL** — session reached AVAILABLE, cleanup verified |
| Network Slicing | act | protected slice for tier-1 devices | ACTUATOR | partial — slice AVAILABLE, not OPERATING, so attachment fails closed |
| Number Verification + SIM Swap | trust | privileged field dispatch only; recent swap blocks | WARDEN | **REAL** — OAuth consent flow and swap detection on the Nokia simulator |

**Tools, all from the Resource & Tooling Guide:** LangGraph supervisor (§2)
with CrewAI role-scoped advisors; Google AI Studio Gemini and Groq (§3) with a
local Ollama model as the offline tier; FastAPI and Streamlit (§6); Supabase
PostgreSQL for durable events, incidents, actions and the audit chain.

---

## 3 · Commercial value summary

**Who pays.** Operators with large desert and off-grid footprints, and the
enterprises that depend on them: energy and utilities, pipelines, mega-project
construction, logistics, public safety.

**Three revenue streams.**

1. **Enterprise SaaS** — per managed critical device per month, tiered by
   response guarantee.
2. **Operator white-label** — the MNO sells HARIS as Resilience-as-a-Service;
   revenue share to us, zero customer-acquisition cost, the operator keeps the
   customer.
3. **Aggregator marketplace** — packaged listing on Open Gateway aggregator
   channels for cross-border distribution without new integration work.

**Why it is fundable.** Most Open Gateway applications call a network API once
per user journey. HARIS calls them continuously, for an entire fleet, and its
value grows with the number of devices under management — exactly the
consumption profile operators need for Open Gateway to become commercially
material.

**Route to first customer.** One operator, one governorate, a few hundred
devices in a paid pilot; publish the incident record as the reference case;
then list on an aggregator channel.

---

## 4 · Business impact statement

| metric | modelled result |
|---|---|
| time from storm onset to mitigation | ~47 min (manual NOC) → one autonomous cycle, seconds |
| tier-1 devices with an explicit, logged decision | 100 % |
| operating coverage | 24/7, no night shift; keeps reasoning offline |
| vendor integrations required | 1 standard CAMARA surface |

- **Operators** — fewer emergency truck rolls and SLA penalties during storm
  season; a resilience product they can sell rather than absorb as cost.
- **Enterprises** — field operations continue through storm windows instead of
  pausing.
- **Society** — ambulances, environmental sensors and pilgrimage-season crowd
  systems stay reachable during exactly the events that create emergencies.

*Figures are our engineering model, not measured operator telemetry.
Validating them on a live operator network is the first pilot milestone.*

---

## 5 · Honest scope

- CAMARA exposes **device- and session-level** control, not radio-access
  control. HARIS acts on that layer; 5G/LTE bearer switching and
  microwave/fibre steering remain operator-domain roadmap work.
- The storm scenario runs on deterministic fixtures and is labelled
  **SIMULATED** on screen — Nokia's simulator cannot raise a dust storm. Nokia
  evidence is labelled **REAL** only where it was demonstrated against the
  platform (table above).
- Durable state lives in Supabase PostgreSQL (migrations 002–007). Restart
  and concurrency were validated for real: an action in flight during a crash
  comes back as `OUTCOME_UNKNOWN` and is reconciled, never resent or reported
  as success.
- The audit trail is SHA-256 hash-chained and tamper-evident; it is not
  described as signed or immutable storage.
- Quality: **690 offline tests pass** (`python run_offline_tests.py`) with
  zero external network or provider calls.

---

## 6 · Three-minute video script (English narration)

Record the local console (`START-HARIS.bat` → <http://localhost:8501>, local AI
on) and, for the Trusted Dispatch and live-read moments, the deployed app.
Press F11 for full screen; Win+Alt+R starts and stops the Xbox Game Bar
recorder. For an AI voiceover, paste the narration into ElevenLabs (§7 of the
Resource & Tooling Guide).

| time | on screen | narration |
|---|---|---|
| 0:00–0:20 | title slide, then the console Overview | "Every summer the same dust storms that darken the Gulf sky quietly take mobile networks with them. Today an engineer notices twenty minutes late. This is HARIS — the network engineer that never sleeps." |
| 0:20–0:40 | Network Intelligence: topology, capability matrix | "HARIS watches the cells and critical assets that matter — ambulances, SCADA gateways, fleets and sensors — through seven GSMA Open Gateway capabilities on Nokia Network as Code. Every value is labelled: real, derived, simulated or unavailable." |
| 0:40–1:05 | Autonomous Operations: press RUN AUTONOMOUS HARIS; the eight-stage decision engine lights up | "A dust advisory lands. SENTINEL sees High congestion on three cells and opens an incident. CARTOGRAPHER finds eight exposed devices. TRIAGE proposes six bounded actions for the two lives-and-safety assets." |
| 1:05–1:25 | trace line `AI_PLANNER_USED=true MODEL=qwen2.5:1.5b (local)` | "An AI advisor ranks those actions — and this one runs on the laptop, with no internet at all. If the storm takes the backhaul, HARIS keeps thinking. The model can only rank what policy allowed." |
| 1:25–1:50 | WARDEN approved line, ACTUATOR lines, storm map before/after, Mitigation Impact card | "WARDEN, the deterministic safety authority, checks confidence, blast radius and cost. Then HARIS acts: guaranteed Quality on Demand, a protected slice, a geofence. And it verifies — on the map, tower T03 turns from red to green, and eight of eight devices are reachable." |
| 1:50–2:25 | Trusted Dispatch (deployed app): Number Verification, SIM Swap, BLOCK, fallback engineer | "A tower needs a technician. Before anyone gets site access, WARDEN runs Nokia Number Verification and SIM Swap. This number's SIM was swapped recently — dispatch blocked. HARIS moves to the next engineer and asks for fresh consent. A hijacked number never reaches the tower." |
| 2:25–2:50 | RUN REAL NOKIA READ CHECK result; History & Audit chain verified | "This is not a mock-up. One press reads the real Nokia platform. Every decision lands in a durable, hash-chained audit trail that survives restarts — and 690 automated tests hold it all together." |
| 2:50–3:00 | end card: HARIS · repo · app URL | "HARIS. Autonomous, bounded and honest — on any Open Gateway operator. Thank you." |
