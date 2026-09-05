# HARIS

**Hybrid Agent for Resilient Infrastructure and Service-continuity**

HARIS is an autonomous network-operations system for protecting mobile service continuity during desert sandstorms and extreme heat. It combines Nokia Network as Code / CAMARA evidence with a bounded multi-agent control loop: **Sense → Reason → Act → Verify → Learn**.

## Operating modes

| Mode | Behavior |
| --- | --- |
| `fixture` | Full deterministic demo: simulated QoD, slicing and geofencing execute, verify, and can roll back. |
| `live_read_only` | Reads real Nokia telemetry and produces a WARDEN-reviewed proposal. Network writes are intentionally disabled. |
| `live_write` | Enables mutation only after WARDEN validates every configured Nokia/operator resource. Missing resources fail closed. |

`NAC_MODE=live` is a safe legacy alias for `live_read_only`; it never silently grants write authority.

## Live Nokia transparency

The current Nokia account supports live categorical congestion telemetry, device status, and location retrieval. HARIS preserves actual congestion categories (`None`, `Low`, `Medium`, `High`) and confidence levels; it never fabricates percentages, latency, or predictions. Unavailable live numeric values display as `N/A`.

Live mutation remains fail-closed until an operator provides a public geofencing callback sink and areas, valid QoD profile/service-IP values, and existing slice resources with HARIS mappings. This is production safety, not simulated completion.

## Network capabilities

- Congestion Insights
- Device Status
- Location Retrieval
- Geofencing
- Quality on Demand (QoD)
- Network Slicing

## Architecture

`SENTINEL → CARTOGRAPHER → TRIAGE → WARDEN → ACTUATOR → VERIFY → ROLLBACK → LEARN`

Sentinel identifies risk; Cartographer maps registered assets; Triage proposes bounded action; Warden applies confidence, blast-radius, spend and capability safety checks; Actuator executes only authorized work; Verify uses fresh categorical evidence; Rollback reverses only concrete actions; Learn audits the outcome.

## Technology stack

Nokia Network as Code / CAMARA, LangGraph, CrewAI, Gemini 2.5 Flash, Groq Llama 3.3 70B, FastAPI, Streamlit, Supabase / pgvector, and Mem0.

## Run

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
python run_cycle.py
streamlit run app.py
```

Run the API façade with `python run_api.py`. For the presentation use `NAC_MODE=fixture`; run the sandstorm scenario for mitigation, or **Force Verify Failure** for the fixture-only rollback demonstration.

## Validation

```bash
python test_haris_core.py
python test_live_decision_pipeline.py
python -m py_compile *.py
```

The live decision test performs Nokia read-only calls only and requires valid credentials plus `NAC_MODE=live_read_only` (or legacy `live`).

## Forecasting, learning, and continuous operation

HARIS includes a transparent 15-minute **categorical risk forecasting model**. It uses observed Nokia congestion categories/history and the dust advisory; it is not trained ML and does not fabricate live numeric KPIs. A prior verified incident affecting the same corridor is retrieved from the local/Supabase/Mem0-compatible memory interface and adds a bounded, traceable confidence adjustment to TRIAGE.

The backend scheduler is disabled by default. Set `ENABLE_CONTINUOUS_LOOP=true` to run at `CYCLE_SECONDS` (default 60) without overlapping cycles. Continuous `live_write` additionally requires `ENABLE_LIVE_WRITE_LOOP=true`.

LangGraph owns the safety-critical workflow. When credentials are configured, CrewAI performs a bounded TRIAGE advisory and Gemini or Groq provides a separate advisory assessment; neither can create actions or override WARDEN. Their absence or failure falls back to deterministic policy.

## Deployment

Fixture mode starts without Nokia credentials and is suitable for Streamlit Community Cloud or Render. `render.yaml` provides a FastAPI service definition; Streamlit uses `.streamlit/config.toml`.

```bash
streamlit run app.py
python run_api.py
```

The API exposes `GET /api/nac/health`, `GET /api/nac/mode`, `GET /api/nac/capabilities`, and append-only incident replay endpoints. `POST /api/nac/callbacks/nokia/geofence` validates and acknowledges Nokia geofence events without triggering any mutation. Configure secrets in the hosting provider, never in source control. Public callback hosting remains operator infrastructure.
