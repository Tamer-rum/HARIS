# HARIS judge demo (2–3 minutes)

1. Start in **FIXTURE / FULL DEMO**. The topology is explicitly simulated.
2. Select **Inject Sandstorm & Run HARIS**. The deterministic sandstorm/heat scenario affects congested cells serving Tier-1 critical assets.
3. Follow SENTINEL, CARTOGRAPHER, TRIAGE and WARDEN: congestion is detected, critical assets are prioritized, and policy safety is checked.
4. Show fixture ACTUATOR actions, VERIFY’s improved categories/KPIs, and LEARN’s recorded mitigation.
5. Select **Force Verify Failure** — a fixture-only demo control. Show unchanged verification, reverse operations, rollback verification, and `rolled_back_safely`.
6. Switch to `NAC_MODE=live_read_only` and restart Streamlit. The header says **LIVE / READ ONLY** and the banner says network writes are disabled.
7. Run a cycle to show actual Nokia categorical congestion evidence. Missing numeric live KPIs remain `N/A`.
8. Show the WARDEN capability matrix: read telemetry is ready; geofencing needs callback/configuration; QoD and slicing need operator resources.

Close with: **“Even when an operator resource is unavailable, HARIS does not hallucinate network capability or perform an unsafe action.”**
