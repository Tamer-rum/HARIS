# HARIS pitch-deck alignment

| Claim | Status | Engineering position |
| --- | --- | --- |
| Six retained network capabilities | IMPLEMENTED | Congestion, device status, location, geofencing, QoD, slicing. |
| Seven APIs / identity APIs | REMOVE/OUTDATED | Identity and SIM workflows are intentionally excluded from network-resilience scope. |
| Sandstorm degradation prediction | IMPLEMENTED | Transparent categorical 15-minute risk forecasting model; not trained ML. |
| Memory improves the next storm | IMPLEMENTED | Similar verified corridor incidents are retrieved and cause a bounded, traced TRIAGE confidence adjustment. |
| 5G/LTE fallback / bearer steering | ROADMAP | Installed NaC SDK exposes slices and QoD, not RAT/bearer selection. HARIS does not execute RAT switching. |
| LangGraph orchestration | IMPLEMENTED | Runtime graph runs Sentinel, Cartographer, Triage, Warden, Actuator, Verify, Rollback, Learn. |
| CrewAI | IMPLEMENTED (optional) | A bounded TRIAGE advisory Crew runs when configured; malformed/failing output falls back and cannot create actions or override WARDEN. |
| Gemini / Groq | IMPLEMENTED (optional) | Advisory planner call is used when a configured key is present; deterministic fallback is traced and authoritative safety remains WARDEN. |
| Continuous 60-second loop | IMPLEMENTED | Backend-owned, opt-in, non-overlapping scheduler; live writes require an extra flag. |
| Supabase / Mem0 | PARTIAL | Adapters are real but require credentials; local append-only fallback is the tested path. |
| Chroma | REMOVE/OUTDATED | Not used by this repository. |
| Public repository/demo URL | ROADMAP | Deployment configuration is included; hosting remains an external step. |
| Retry, timeout, fallback | PARTIAL | Nokia adapter has bounded retry and fixture fallback; dust feed has timeout/fallback. No circuit-breaker claim is made. |
| Geofence callback | IMPLEMENTED | Validated receipt endpoint records only accepted event type; public hosting is an external dependency. |
