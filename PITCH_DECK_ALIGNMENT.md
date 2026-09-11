# HARIS Pitch-Deck Alignment

| Pitch promise | Current implementation | Status |
| --- | --- | --- |
| Seven Nokia/CAMARA capability groups | Seven Nokia/CAMARA capability groups: Congestion Insights, Device Status / Reachability, Location Retrieval, Geofencing, Quality on Demand, Network Slicing, and Number Verification / SIM Swap. | COMPLETE |
| Closed loop | Sentinel -> Cartographer -> Triage -> WARDEN -> Actuator -> Verify -> Rollback/Learn | COMPLETE |
| Storm Shield, Capacity Harvest, Energy Guard, Trusted Dispatch | Deterministic playbooks; WARDEN gates all network actions | COMPLETE / FIXTURE-DEMO |
| 60-second autonomy | Backend scheduler default cadence is 60 seconds and non-overlapping; this is an engineering configuration/target, not a measured provider-recovery SLA | TRUE WITH BOUNDARY |
| Durable learning and audit | Backend-only Supabase durable history with a tamper-evident append-only SHA-256 audit chain; persistence across a Render backend restart has been verified. | COMPLETE |
| QoD, Number Verification, SIM Swap | Provider lifecycle/security paths verified against Nokia Simulator; QoD network verification was `UNCHANGED`, so overall QoD evidence is `REAL_PARTIAL` | TRUE WITH BOUNDARY |
| Geofencing callback | Transport path was demonstrated, but authenticated callback provenance is not proven and the current endpoint fails closed | FAIL-CLOSED / AUTH UNPROVEN |
| Network Slicing attachment | Integration exists; sandbox slice never reached `OPERATING`, so attachment was not forced | PARTIAL / SANDBOX-LIMITED |
| 5G/LTE and microwave/fibre control | Recommendation/operator-controller integration only | ROADMAP |
