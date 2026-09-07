# HARIS Pitch-Deck Alignment

| Pitch promise | Current implementation | Status |
| --- | --- | --- |
| Seven Nokia/CAMARA capability groups | Seven Nokia/CAMARA capability groups: Congestion Insights, Device Status / Reachability, Location Retrieval, Geofencing, Quality on Demand, Network Slicing, and Number Verification / SIM Swap. | COMPLETE |
| Closed loop | Sentinel -> Cartographer -> Triage -> WARDEN -> Actuator -> Verify -> Rollback/Learn | COMPLETE |
| Storm Shield, Capacity Harvest, Energy Guard, Trusted Dispatch | Deterministic playbooks; WARDEN gates all network actions | COMPLETE / FIXTURE-DEMO |
| 60-second autonomy | Backend scheduler is opt-in, non-overlapping, default 60 seconds; live-write needs an additional explicit flag | COMPLETE |
| Durable learning and audit | Backend-only Supabase durable history with a tamper-evident append-only SHA-256 audit chain; persistence across a Render backend restart has been verified. | COMPLETE |
| QoD, Number Verification, SIM Swap, geofence callback | Verified against Nokia Simulator | COMPLETE |
| Network Slicing attachment | Integration exists; sandbox slice never reached `OPERATING`, so attachment was not forced | PARTIAL / SANDBOX-LIMITED |
| 5G/LTE and microwave/fibre control | Recommendation/operator-controller integration only | ROADMAP |
