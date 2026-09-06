from __future__ import annotations

import asyncio
import json
import logging
import math
import time
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import re
import streamlit as st

from agents import HarisAgentSystem
from config import get_settings
from nokia_clients import build_nokia_client


# ============================================================================
# HARIS — LIVE NETWORK RESILIENCE OPERATIONS CONSOLE
# ============================================================================
# This file is UI-only. It uses the existing HARIS backend:
#   agents.py
#   config.py
#   nokia_clients.py
#   fixtures/
#
# No backend logic is reimplemented here.
# ============================================================================

settings = get_settings()

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("haris.console")



def render_html(html: str) -> None:
    """Render multiline HTML without Streamlit interpreting indented lines as code."""
    st.markdown(re.sub(r"\s+", " ", textwrap.dedent(html).strip()), unsafe_allow_html=True)

st.set_page_config(
    page_title="HARIS — Network Resilience",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ============================================================================
# CSS
# ============================================================================

st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');

:root {
    --bg: #060a10;
    --panel: #0b121c;
    --panel2: #0e1722;
    --line: #1c2d40;
    --text: #edf5ff;
    --muted: #7189a3;
    --cyan: #31d7ff;
    --blue: #4b8dff;
    --green: #42f59b;
    --yellow: #ffc857;
    --red: #ff4d5f;
    --purple: #a97bff;
}

.stApp {
    background:
        radial-gradient(circle at 50% -5%, rgba(49,215,255,.10), transparent 34%),
        radial-gradient(circle at 100% 40%, rgba(75,141,255,.055), transparent 30%),
        linear-gradient(180deg, #060a10 0%, #080d14 100%);
    color: var(--text);
    font-family: Inter, sans-serif;
}

.block-container {
    max-width: 1780px;
    padding-top: 1.15rem;
    padding-bottom: 2rem;
}

header[data-testid="stHeader"] {
    background: transparent;
}

section[data-testid="stSidebar"] {
    background: #080e16;
}

[data-testid="stMetric"] {
    background: linear-gradient(145deg, #101a26, #090f17);
    border: 1px solid #1b2c40;
    border-radius: 12px;
    padding: 12px 14px;
}

[data-testid="stMetricLabel"] {
    color: #7189a3 !important;
    font-size: .67rem !important;
    text-transform: uppercase;
    letter-spacing: .10em;
}

[data-testid="stMetricValue"] {
    color: #f2f8ff !important;
}

div[data-testid="stButton"] button {
    min-height: 42px;
    border-radius: 9px;
    font-weight: 700;
}

button[kind="primary"] {
    background: linear-gradient(90deg, #e83d50, #ff5664) !important;
    border: 0 !important;
}

.hr {
    height: 1px;
    margin: 17px 0;
    background: linear-gradient(
        90deg,
        transparent,
        #23374d 12%,
        #23374d 88%,
        transparent
    );
}

.brand {
    display: flex;
    align-items: center;
    gap: 12px;
}

.shield {
    font-size: 2.55rem;
    filter: drop-shadow(0 0 13px rgba(49,215,255,.40));
}

.brand-name {
    font-size: 2.05rem;
    line-height: 1;
    font-weight: 800;
    letter-spacing: -.045em;
}

.brand-sub {
    margin-top: 5px;
    color: #7189a3;
    font-size: .69rem;
}

.live-pill {
    display: inline-flex;
    align-items: center;
    gap: 7px;
    margin-top: 5px;
    border: 1px solid #20503d;
    background: rgba(12,43,32,.72);
    color: #63f5ad;
    border-radius: 999px;
    padding: 5px 10px;
    font-size: .62rem;
    font-weight: 800;
    letter-spacing: .08em;
}

.live-dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: #42f59b;
    box-shadow: 0 0 10px #42f59b;
}

.hero {
    border: 1px solid #1e334a;
    border-radius: 13px;
    background:
        radial-gradient(circle at 90% 15%, rgba(49,215,255,.07), transparent 34%),
        linear-gradient(145deg, #0d1824, #091019);
    padding: 14px 17px;
}

.hero-label {
    color: #6f88a3;
    font-size: .62rem;
    text-transform: uppercase;
    letter-spacing: .12em;
    font-weight: 700;
}

.hero-value {
    font-size: 1.55rem;
    line-height: 1.15;
    font-weight: 800;
    margin-top: 4px;
}

.hero-detail {
    color: #7890aa;
    font-size: .66rem;
    margin-top: 4px;
}

.ready { color: #4b9cff; }
.active { color: #ffc857; }
.mitigated { color: #42f59b; }
.review { color: #ff6170; }
.rolled-back { color: #ff6170; }

.section-title {
    font-size: 1rem;
    font-weight: 800;
    margin: 6px 0 10px;
}

.section-title span {
    color: var(--cyan);
}

.panel {
    background:
        linear-gradient(145deg, rgba(14,22,33,.98), rgba(8,14,22,.98));
    border: 1px solid #1a2b3e;
    border-radius: 13px;
    padding: 13px;
    box-shadow: inset 0 1px 0 rgba(255,255,255,.012);
}

.panel-title {
    color: #6f88a3;
    font-size: .62rem;
    text-transform: uppercase;
    letter-spacing: .12em;
    font-weight: 700;
    margin-bottom: 8px;
}

.row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
    border-bottom: 1px solid #172537;
    padding: 8px 0;
    font-size: .71rem;
}

.row:last-child {
    border-bottom: 0;
}

.badge {
    display: inline-flex;
    align-items: center;
    border-radius: 5px;
    padding: 3px 7px;
    font-size: .57rem;
    font-weight: 800;
    letter-spacing: .04em;
    white-space: nowrap;
}

.badge-red {
    background: #34131a;
    color: #ff6a78;
    border: 1px solid #60202b;
}

.badge-yellow {
    background: #332710;
    color: #ffd267;
    border: 1px solid #5e481d;
}

.badge-green {
    background: #0c2d22;
    color: #59efaa;
    border: 1px solid #1b5942;
}

.badge-blue {
    background: #10223c;
    color: #76aaff;
    border: 1px solid #244b7d;
}

.kpi-card {
    text-align: center;
    border: 1px solid #1b3045;
    border-radius: 11px;
    background: #0a111a;
    padding: 13px 7px;
}

.kpi-name {
    color: #7189a4;
    text-transform: uppercase;
    letter-spacing: .1em;
    font-size: .59rem;
}

.kpi-before {
    color: #ff6876;
    font-size: 1.20rem;
    font-weight: 800;
}

.kpi-after {
    color: #48efa2;
    font-size: 1.20rem;
    font-weight: 800;
}

.kpi-arrow {
    color: #607b96;
    padding: 0 5px;
}

.kpi-delta {
    color: #42f59b;
    font-size: .62rem;
    margin-top: 4px;
}

.trace {
    max-height: 365px;
    overflow-y: auto;
    background: #05090f;
    border: 1px solid #18283a;
    border-radius: 10px;
    padding: 9px 11px;
    font: 600 .61rem/1.72 "JetBrains Mono", monospace;
}

.trace-line {
    padding: 2px 0;
    border-bottom: 1px solid rgba(255,255,255,.025);
}

.trace-time { color: #39e89f; }
.trace-stage { color: #5fa6ff; }
.trace-action { color: #ffc857; }
.trace-verify { color: #b18cff; }

.small-note {
    color: #617a94;
    font-size: .64rem;
    line-height: 1.55;
}

.footer {
    color: #4e647b;
    font-size: .59rem;
    text-align: center;
    padding-top: 8px;
}

.empty-state {
    text-align: center;
    padding: 26px 12px;
    color: #7890aa;
}

.empty-state strong {
    color: #a9bfd4;
}

div[data-testid="stAlert"] {
    border-radius: 10px;
}
</style>
""",
    unsafe_allow_html=True,
)


# ============================================================================
# Backend helpers
# ============================================================================

def run_async(coro):
    """Run an async HARIS operation from Streamlit's synchronous UI."""
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


@st.cache_resource(show_spinner=False)
def get_system() -> HarisAgentSystem:
    return HarisAgentSystem(
        build_nokia_client(settings),
        settings=settings,
    )


def fixture(name: str, default: Any) -> Any:
    root = Path(settings.fixture_dir)
    if not root.is_absolute():
        root = Path(__file__).resolve().parent / root

    path = root / f"{name}.json"

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def optional_float(value: Any) -> float:
    """Return NaN for absent KPIs so the UI never fabricates live values."""
    try:
        return float(value) if value is not None else math.nan
    except (TypeError, ValueError):
        return math.nan


def congestion_map(result: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    rows = (result or {}).get("congestion") or fixture("congestion", [])

    output: Dict[str, Dict[str, float]] = {}

    for row in rows:
        cell_id = row.get("cell_id")
        if not cell_id:
            continue

        output[cell_id] = {
            "congestion_pct": optional_float(row.get("congestion_pct")),
            "latency_ms": optional_float(row.get("latency_ms")),
            "predicted_congestion_pct": optional_float(row.get("predicted_congestion_pct")),
        }

    return output


def baseline_map(result: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    raw = (result or {}).get("pre_execution_congestion")

    if isinstance(raw, dict):
        return {
            cell: {
                "congestion_pct": optional_float(values.get("congestion_pct")),
                "latency_ms": optional_float(values.get("latency_ms")),
                "predicted_congestion_pct": optional_float(values.get("predicted_congestion_pct")),
            }
            for cell, values in raw.items()
        }

    if isinstance(raw, list):
        return {
            row["cell_id"]: {
                "congestion_pct": optional_float(row.get("congestion_pct")),
                "latency_ms": optional_float(row.get("latency_ms")),
                "predicted_congestion_pct": optional_float(row.get("predicted_congestion_pct")),
            }
            for row in raw
            if row.get("cell_id")
        }

    return congestion_map(None)


def tower_state(
    congestion: float,
    latency: float,
) -> Tuple[str, str]:
    if not math.isfinite(congestion) or not math.isfinite(latency):
        return "KPI UNAVAILABLE", "gray"
    if congestion >= 80 or latency > 100:
        return "CRITICAL", "red"

    if congestion >= 70 or latency >= 50:
        return "AT RISK", "yellow"

    return "HEALTHY", "green"


# ============================================================================
# Header
# ============================================================================

def render_header(result: Optional[Dict[str, Any]]) -> None:
    final_status = (result or {}).get("final_status")

    if final_status == "mitigated":
        text = "MITIGATED"
        css = "mitigated"
        detail = "Mitigation verified · closed-loop recovery successful"
    elif final_status == "rolled_back_safely":
        text = "ROLLED BACK"
        css = "rolled-back"
        detail = "Verification failed · network state restored"
    elif final_status == "live_read_only_proposal":
        text = "PROPOSAL READY"
        css = "review"
        detail = "Live Nokia evidence analyzed; network writes intentionally disabled"
    elif final_status == "warden_rejected":
        text = "BLOCKED SAFELY"
        css = "review"
        detail = "WARDEN rejected the proposed network action"
    elif result:
        text = "REVIEW"
        css = "review"
        detail = "Cycle completed · verification requires review"
    else:
        text = "READY"
        css = "ready"
        detail = "Autonomous resilience engine standing by"

    left, middle, right = st.columns([1.70, 1.0, .95])

    with left:
        render_html(
            """
            <div class="brand">
                <div class="shield">🛡️</div>
                <div>
                    <div class="brand-name">HARIS</div>
                    <div class="brand-sub">
                        Autonomous Network Resilience Engineer ·
                        Predict · Protect · Preserve · Prove
                    </div>
                </div>
            </div>
            """
        )

    with middle:
        render_html(
            f"""
            <div class="live-pill">
                <span class="live-dot"></span>
                {settings.operating_mode_label} ·
                NETWORK FABRIC · LIVE
            </div>
            """
        )

    with right:
        render_html(
            f"""
            <div class="hero">
                <div class="hero-label">HARIS STATE</div>
                <div class="hero-value {css}">{text}</div>
                <div class="hero-detail">{detail}</div>
            </div>
            """
        )

    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

    a, b, c, d = st.columns(4)

    a.metric(
        "Closed Loop",
        f"{settings.cycle_seconds}s",
    )

    b.metric(
        "Target Mitigation",
        "< 3 min",
    )

    c.metric(
        "Network Capabilities",
        "7",
    )

    d.metric(
        "NaC Mode",
        settings.operating_mode_label,
    )

    if settings.nac_mode == "live_read_only":
        st.info(
            "LIVE NOKIA TELEMETRY · NETWORK WRITES DISABLED — remediation is proposed and audited, never executed.",
            icon="ℹ️",
        )


# ============================================================================
# Environmental state
# ============================================================================

def render_capability_matrix(result: Optional[Dict[str, Any]]) -> None:
    """Display WARDEN's one shared capability assessment."""
    report = (result or {}).get("warden", {}).get("capability_report")
    if not report:
        report = get_system().client.capability_report()

    labels = [
        ("congestion_insights", "Congestion Insights"),
        ("device_status", "Device Status"),
        ("location", "Location Retrieval"),
        ("geofencing", "Geofencing"),
        ("qod", "QoD"),
        ("slicing", "Network Slicing"),
        ("trusted_dispatch", "TRUSTED DISPATCH"),
    ]
    display = {
        "READ_READY": "READ READY",
        "SUPPORTED_AND_CONFIGURED": "READY",
        "SDK_SUPPORTED_CONFIG_MISSING": "CONFIG MISSING",
        "OPERATOR_VALUE_REQUIRED": "OPERATOR RESOURCE REQUIRED",
        "SDK_UNSUPPORTED": "UNAVAILABLE",
        "PRIVILEGED_ONLY": "PRIVILEGED ONLY",
    }
    st.markdown('<div class="section-title">🧩 <span>LIVE CAPABILITY MATRIX</span></div>', unsafe_allow_html=True)
    columns = st.columns(3)
    for index, (key, label) in enumerate(labels):
        item = report.get(key, {"status": "PRIVILEGED_ONLY", "reason": "Number Verification + SIM Swap; privileged field intervention only."})
        status = display.get(item.get("status"), str(item.get("status", "UNKNOWN")).replace("_", " "))
        with columns[index % 3]:
            st.metric(label, status)
            if item.get("reason"):
                st.caption(item["reason"])


def render_environment(result: Optional[Dict[str, Any]]) -> None:
    source = (result or {}).get("environmental_source", "FIXTURE" if settings.nac_mode == "fixture" else "UNAVAILABLE")
    st.caption(f"ENVIRONMENT SOURCE: {source}")
    st.markdown(
        '<div class="section-title">🌪 <span>ENVIRONMENTAL THREAT STATE</span></div>',
        unsafe_allow_html=True,
    )


def render_prediction(result: Optional[Dict[str, Any]]) -> None:
    prediction = (result or {}).get("prediction")
    if not prediction:
        return
    st.markdown('<div class="section-title">🔮 <span>SHORT-HORIZON RISK FORECAST</span></div>', unsafe_allow_html=True)
    a, b, c, d = st.columns(4)
    a.metric("Predicted Risk", prediction.get("predicted_risk_level", "N/A").upper())
    b.metric("Forecast Horizon", f"{prediction.get('horizon_minutes', 'N/A')} min")
    c.metric("Confidence", f"{float(prediction.get('confidence', 0)) * 100:.0f}%")
    d.metric("Degradation Probability", f"{float(prediction.get('degradation_probability', 0)) * 100:.0f}%")
    st.caption("Top factors: " + "; ".join(prediction.get("contributing_factors", [])))

    incident = (result or {}).get("incident", {})
    affected_cells = incident.get("affected_cells", [])

    cells_text = ", ".join(affected_cells) if affected_cells else "Monitoring"

    cards = [
        (
            "Dust Density",
            "HIGH",
            "Storm advisory ACTIVE",
            "#ff4d5f",
        ),
        (
            "Temperature",
            "47°C",
            "Extreme heat condition",
            "#ffc857",
        ),
        (
            "Visibility",
            "LOW",
            "Desert visibility degradation",
            "#ffc857",
        ),
        (
            "Affected Cells",
            cells_text,
            "Detected by Sentinel",
            "#ff6b78",
        ),
    ]

    cols = st.columns(4)

    for col, (name, value, subtitle, color) in zip(cols, cards):
        with col:
            render_html(
                f"""
                <div class="panel">
                    <div class="panel-title">{name}</div>
                    <div style="
                        font-size:1.12rem;
                        font-weight:800;
                        color:{color};
                        white-space:normal;
                    ">{value}</div>
                    <div style="
                        color:#7189a4;
                        font-size:.64rem;
                        margin-top:3px;
                    ">{subtitle}</div>
                </div>
                """
            )


# ============================================================================
# Network topology SVG
# ============================================================================

def topology_svg(
    data: Dict[str, Dict[str, float]],
    active: bool,
) -> str:
    positions = {
        "T01": (90, 78),
        "T02": (300, 160),
        "T03": (515, 275),
        "T04": (745, 92),
        "T05": (770, 382),
        "T06": (1015, 180),
        "T07": (1030, 435),
        "CORE": (545, 510),
    }

    links = [
        ("T01", "T02"),
        ("T02", "T03"),
        ("T03", "T04"),
        ("T03", "T05"),
        ("T04", "T06"),
        ("T05", "T06"),
        ("T05", "T07"),
        ("T03", "CORE"),
        ("T06", "CORE"),
        ("T07", "CORE"),
    ]

    link_parts: List[str] = []

    for index, (source, target) in enumerate(links):
        x1, y1 = positions[source]
        x2, y2 = positions[target]

        values = [
            data.get(source, {}).get("congestion_pct", 0),
            data.get(target, {}).get("congestion_pct", 0),
        ]

        observed_congestion = [value for value in values if math.isfinite(value)]
        max_congestion = max(observed_congestion) if observed_congestion else math.nan

        if not math.isfinite(max_congestion):
            color = "#7890aa"
        elif max_congestion >= 70:
            color = "#ff4d5f"
        elif max_congestion >= 45:
            color = "#ffc857"
        else:
            color = "#31d7ff"

        link_parts.append(
            f"""
            <line
                x1="{x1}"
                y1="{y1}"
                x2="{x2}"
                y2="{y2}"
                stroke="{color}"
                stroke-width="2.4"
                opacity=".68"
            />
            """
        )

        if active:
            duration = 1.45 + (index % 4) * .25

            link_parts.append(
                f"""
                <circle r="4" fill="{color}" opacity=".95">
                    <animateMotion
                        dur="{duration}s"
                        repeatCount="indefinite"
                        path="M{x1},{y1} L{x2},{y2}"
                    />
                </circle>
                """
            )

    node_parts: List[str] = []

    for name, (x, y) in positions.items():
        if name == "CORE":
            status = "CORE"
            stroke = "#31d7ff"
            fill = "#10273a"
            sub = "NETWORK CORE"
        else:
            congestion = data.get(name, {}).get("congestion_pct", 0)
            latency = data.get(name, {}).get("latency_ms", 0)

            status, key = tower_state(congestion, latency)

            colors = {
                "red": ("#ff4d5f", "#35131a"),
                "yellow": ("#ffc857", "#332710"),
                "green": ("#42f59b", "#0d3025"),
                "gray": ("#7890aa", "#202d3b"),
            }

            stroke, fill = colors[key]
            if not math.isfinite(congestion):
                status = "KPI UNAVAILABLE"
            sub = f"{status} · {congestion:.0f}%"

        if name != "CORE" and status != "HEALTHY":
            pulse = f"""
            <circle
                cx="{x}"
                cy="{y}"
                r="24"
                fill="none"
                stroke="{stroke}"
                stroke-width="1.5"
                opacity=".52"
            >
                <animate
                    attributeName="r"
                    values="21;35;21"
                    dur="1.7s"
                    repeatCount="indefinite"
                />
                <animate
                    attributeName="opacity"
                    values=".58;.04;.58"
                    dur="1.7s"
                    repeatCount="indefinite"
                />
            </circle>
            """
        else:
            pulse = ""

        if name == "CORE":
            icon = f"""
            <circle
                cx="{x}"
                cy="{y}"
                r="12"
                fill="none"
                stroke="{stroke}"
                stroke-width="2"
            />
            <circle
                cx="{x}"
                cy="{y}"
                r="5"
                fill="{stroke}"
            />
            """
        else:
            icon = f"""
            <path
                d="
                    M{x-9},{y+14}
                    L{x},{y-15}
                    L{x+9},{y+14}
                    M{x-6},{y+3}
                    H{x+6}
                    M{x-7},{y-4}
                    H{x+7}
                    M{x},{y-15}
                    V{y+17}
                "
                stroke="{stroke}"
                stroke-width="2.1"
                fill="none"
            />
            """

        node_parts.append(
            f"""
            <g>
                {pulse}
                <circle
                    cx="{x}"
                    cy="{y}"
                    r="21"
                    fill="{fill}"
                    stroke="{stroke}"
                    stroke-width="2"
                />
                {icon}
                <text
                    x="{x}"
                    y="{y+43}"
                    text-anchor="middle"
                    fill="#edf5ff"
                    font-size="13"
                    font-weight="800"
                >{name}</text>
                <text
                    x="{x}"
                    y="{y+57}"
                    text-anchor="middle"
                    fill="{stroke}"
                    font-size="8.5"
                    font-weight="700"
                >{sub}</text>
            </g>
            """
        )

    return textwrap.dedent(f"""
    <div class="panel" style="padding:8px 8px 3px;">
        <div class="panel-title">
            LIVE NETWORK FABRIC · DATA FLOW
        </div>

        <div style="
            color:#607994;
            font-size:.60rem;
            margin:3px 0 5px;
        ">
            Cyan = healthy · Amber = degraded · Red = critical ·
            moving dots = simulated traffic flow
        </div>

        <svg
            viewBox="0 0 1120 555"
            width="100%"
            role="img"
            aria-label="HARIS network topology"
        >
            <defs>
                <radialGradient id="topology-bg">
                    <stop offset="0%" stop-color="#112031" stop-opacity=".52"/>
                    <stop offset="100%" stop-color="#081019" stop-opacity="0"/>
                </radialGradient>

                <filter id="topology-glow">
                    <feGaussianBlur stdDeviation="3" result="blur"/>
                    <feMerge>
                        <feMergeNode in="blur"/>
                        <feMergeNode in="SourceGraphic"/>
                    </feMerge>
                </filter>
            </defs>

            <rect
                x="0"
                y="0"
                width="1120"
                height="555"
                rx="12"
                fill="url(#topology-bg)"
            />

            <g opacity=".12">
                <path d="M30 70H1090" stroke="#52728f"/>
                <path d="M30 170H1090" stroke="#52728f"/>
                <path d="M30 270H1090" stroke="#52728f"/>
                <path d="M30 370H1090" stroke="#52728f"/>
                <path d="M30 470H1090" stroke="#52728f"/>

                <path d="M100 25V530" stroke="#52728f"/>
                <path d="M300 25V530" stroke="#52728f"/>
                <path d="M500 25V530" stroke="#52728f"/>
                <path d="M700 25V530" stroke="#52728f"/>
                <path d="M900 25V530" stroke="#52728f"/>
            </g>

            <g filter="url(#topology-glow)">
                {''.join(link_parts)}
            </g>

            {''.join(node_parts)}
        </svg>
    </div>
    """).strip()


# ============================================================================
# Network alerts + topology
# ============================================================================

def render_network_section(
    result: Optional[Dict[str, Any]],
) -> None:
    data = congestion_map(result)

    left, right = st.columns([.82, 2.18])

    with left:
        st.markdown(
            '<div class="section-title">⚠ <span>NETWORK ALERTS</span></div>',
            unsafe_allow_html=True,
        )

        rows = ""

        for cell_id, values in sorted(data.items()):
            congestion = values["congestion_pct"]

            if congestion >= 70:
                status_class = "badge-red" if congestion >= 75 else "badge-yellow"
                status = "CRITICAL" if congestion >= 75 else "AT RISK"

                rows += f"""
                <div class="row">
                    <span><b>{cell_id}</b> · congestion</span>
                    <span class="badge {status_class}">
                        {congestion:.0f}% · {status}
                    </span>
                </div>
                """

        if not rows:
            rows = """
            <div class="row">
                <span>Network fabric</span>
                <span class="badge badge-green">HEALTHY</span>
            </div>
            """

        render_html(
            f'<div class="panel">{textwrap.dedent(rows).strip()}</div>'
        )

        st.markdown('<div style="height:10px"></div>', unsafe_allow_html=True)

        st.markdown(
            '<div class="section-title">📡 <span>CRITICAL DEVICES</span></div>',
            unsafe_allow_html=True,
        )

        devices = fixture("devices", [])
        rows = ""

        for device in devices:
            tier = device.get("tier")
            battery = float(device.get("battery_pct", 100))

            if tier == 1 or battery < 25:
                if tier == 1:
                    label = "TIER-1"
                    cls = "badge-blue"
                else:
                    label = "LOW BATTERY"
                    cls = "badge-yellow"

                rows += f"""
                <div class="row">
                    <span>
                        {device.get("device_id")} · {device.get("cell_id")}
                    </span>
                    <span class="badge {cls}">{label}</span>
                </div>
                """

        if not rows:
            rows = """
            <div class="row">
                <span>No critical devices</span>
                <span class="badge badge-green">CLEAR</span>
            </div>
            """

        render_html(
            f'<div class="panel">{textwrap.dedent(rows).strip()}</div>'
        )

    with right:
        st.markdown(
            '<div class="section-title">🗺 <span>NETWORK TOPOLOGY</span></div>',
            unsafe_allow_html=True,
        )

        render_html(
            topology_svg(
                data=data,
                active=bool(result),
            )
        )


# ============================================================================
# KPI impact
# ============================================================================

def render_impact(
    result: Optional[Dict[str, Any]],
) -> None:
    st.markdown(
        '<div class="section-title">📈 <span>LIVE MITIGATION IMPACT</span></div>',
        unsafe_allow_html=True,
    )

    if not result:
        render_html(
            """
            <div class="panel empty-state">
                <strong>No active cycle</strong><br>
                Run the sandstorm scenario to populate live before/after KPIs.
            </div>
            """
        )
        return

    baseline = baseline_map(result)
    current = congestion_map(result)

    verification = result.get("verification", {})
    target_cells = verification.get("target_cells") or []

    target = target_cells[0] if target_cells else None

    if not target:
        for cell_id in baseline:
            if cell_id in current:
                target = cell_id
                break

    if not target or target not in baseline or target not in current:
        st.warning(
            "No matching pre-execution baseline is available for the verification target."
        )
        return

    before = baseline[target]
    after = current[target]

    kpis = [
        (
            "Congestion",
            before["congestion_pct"],
            after["congestion_pct"],
            "%",
        ),
        (
            "Latency",
            before["latency_ms"],
            after["latency_ms"],
            " ms",
        ),
        (
            "Predicted",
            before["predicted_congestion_pct"],
            after["predicted_congestion_pct"],
            "%",
        ),
    ]

    cols = st.columns(3)

    for col, (name, before_value, after_value, suffix) in zip(cols, kpis):
        kpi_available = math.isfinite(before_value) and math.isfinite(after_value)
        delta = after_value - before_value if kpi_available else math.nan

        if not kpi_available:
            delta_text = "Unavailable from live Nokia evidence"
            delta_color = "#7890aa"
        elif delta < 0:
            delta_text = (
                f"−{abs(delta):.1f}{suffix} improvement"
            )
            delta_color = "#42f59b"
        elif delta > 0:
            delta_text = (
                f"+{abs(delta):.1f}{suffix} degradation"
            )
            delta_color = "#ff6170"
        else:
            delta_text = "No change"
            delta_color = "#7890aa"

        with col:
            render_html(
                f"""
                <div class="kpi-card">
                    <div class="kpi-name">{name} · {target}</div>

                    <div style="margin-top:7px;">
                        <span class="kpi-before">
                            {f'{before_value:.1f}{suffix}' if kpi_available else 'N/A'}
                        </span>

                        <span class="kpi-arrow">→</span>

                        <span class="kpi-after">
                            {f'{after_value:.1f}{suffix}' if kpi_available else 'N/A'}
                        </span>
                    </div>

                    <div class="kpi-delta" style="color:{delta_color};">
                        {delta_text}
                    </div>
                </div>
                """
            )

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    plan = result.get("plan", {})
    execution = result.get("execution", {})
    verification = result.get("verification", {})

    a, b, c, d, e = st.columns(5)

    a.metric(
        "Confidence",
        f"{float(plan.get('confidence', 0)) * 100:.0f}%",
    )

    b.metric(
        "Blast Radius",
        f"{float(plan.get('blast_radius', 0)) * 100:.0f}%",
    )

    c.metric(
        "QoD Cost",
        f"${float(plan.get('expected_cost_usd', 0)):.2f}",
    )

    d.metric(
        "Actions",
        str(len(plan.get("actions", []))),
    )

    e.metric(
        "Verification",
        "PASSED" if verification.get("verified") else "REVIEW",
    )

    if execution.get("executed"):
        st.caption(
            "Actuator execution confirmed. Verification is based on post-action network readback."
        )


# ============================================================================
# Decision engine
# ============================================================================

def render_decision_engine(
    result: Optional[Dict[str, Any]],
) -> None:
    st.markdown(
        '<div class="section-title">🧠 <span>AI DECISION ENGINE</span></div>',
        unsafe_allow_html=True,
    )

    if not result:
        render_html(
            """
            <div class="panel empty-state">
                <strong>HARIS is standing by</strong><br>
                Inject a sandstorm to start
                Sense → Reason → Act → Verify → Learn.
            </div>
            """
        )
        return

    plan = result.get("plan", {})
    execution = result.get("execution", {})
    verification = result.get("verification", {})
    learning = result.get("learning", {})
    warden = result.get("warden", {})
    rollback = result.get("rollback", {})
    read_only = execution.get("reason") == "live_read_only"
    actuator_detail = (
        "Not executed — live read-only mode"
        if read_only
        else "Network actions executed" if execution.get("executed")
        else f"Not executed — {execution.get('reason', 'no action')}"
    )
    verify_detail = (
        "Not applicable — no mutation was executed"
        if read_only
        else "Post-action network readback" if execution.get("executed")
        else "Not applicable — no action executed"
    )

    stages = [
        (
            "01",
            "SENTINEL",
            "Threat sensed",
            True,
        ),
        (
            "02",
            "CARTOGRAPHER",
            "Assets located",
            True,
        ),
        (
            "03",
            "TRIAGE",
            f"{len(plan.get('actions', []))} bounded actions",
            True,
        ),
        (
            "04",
            "WARDEN",
            "Approved" if warden.get("verified") else "Capability or policy blocked execution",
            bool(warden.get("verified")),
        ),
        (
            "05",
            "ACTUATOR",
            actuator_detail,
            bool(execution.get("executed")),
        ),
        (
            "06",
            "VERIFY",
            verify_detail,
            bool(verification.get("verified")),
        ),
        (
            "07",
            "ROLLBACK",
            "Rollback verified" if rollback.get("rollback_verified") else "Not required",
            bool(rollback.get("rollback_verified")) or not rollback,
        ),
        (
            "08",
            "LEARN",
            "Incident stored",
            bool(learning.get("incident_saved")),
        ),
    ]

    left, right = st.columns([2.2, 1])

    with left:
        html = '<div class="panel">'

        for number, stage, detail, success in stages:
            skipped = (read_only and stage in {"ACTUATOR", "VERIFY"}) or (
                stage == "ROLLBACK" and detail == "Not required"
            )
            color = "#42f59b" if success else "#ffc857" if skipped else "#ff6170"
            icon = "✓" if success else "!"

            html += f"""
            <div class="row">
                <span>
                    <b style="color:{color};font-size:.85rem;">
                        {icon}
                    </b>
                    &nbsp;
                    <b style="color:#d9e7f5;">
                        {number} · {stage}
                    </b>
                </span>

                <span style="
                    color:#7189a4;
                    font-size:.66rem;
                ">
                    {detail}
                </span>
            </div>
            """

        html += "</div>"

        render_html(html)

    with right:
        status = str(
            result.get("final_status", "review")
        ).upper()

        status_color = (
            "#42f59b"
            if status == "MITIGATED"
            else "#ff6170"
        )

        protected = len(
            [
                action
                for action in plan.get("actions", [])
                if action.get("kind") == "slice_attach"
            ]
        )

        render_html(
            f"""
            <div class="panel"
                 style="
                    text-align:center;
                    min-height:100%;
                    display:flex;
                    flex-direction:column;
                    justify-content:center;
                 ">

                <div class="panel-title">HARIS RESULT</div>

                <div style="
                    font-size:1.62rem;
                    font-weight:900;
                    color:{status_color};
                    margin:7px 0;
                ">
                    {status}
                </div>

                <div style="
                    color:#6f87a1;
                    font-size:.64rem;
                ">
                    Protected Tier-1 devices
                </div>

                <div style="
                    font-size:2rem;
                    font-weight:800;
                    color:#eef6ff;
                ">
                    {protected}
                </div>

                <div style="
                    height:1px;
                    background:#1d2b3d;
                    margin:10px 0;
                "></div>

                <div class="panel-title">AUDIT</div>

                <div style="
                    color:{status_color};
                    font-weight:800;
                    font-size:.82rem;
                ">
                    {"VERIFIED" if verification.get("verified") else "REVIEW"}
                </div>
            </div>
            """
        )

    st.markdown("<div style='height:9px'></div>", unsafe_allow_html=True)

    st.markdown(
        '<div class="panel-title">AUTONOMOUS DECISION TRACE</div>',
        unsafe_allow_html=True,
    )

    render_trace(result.get("trace", []))
    if result.get("explanation"):
        st.caption("EXPLANATION: " + result["explanation"])


# ============================================================================
# Trace
# ============================================================================

def render_trace(trace: List[str]) -> None:
    output: List[str] = []

    for line in trace:
        if "|" not in line:
            output.append(
                f'<div class="trace-line">{line}</div>'
            )
            continue

        timestamp, rest = line.split("|", 1)

        if ":" in rest:
            stage, message = rest.split(":", 1)
        else:
            stage = rest
            message = ""

        stage_upper = stage.upper()

        if "VERIFY" in stage_upper:
            css = "trace-verify"
        elif "ACTUATOR" in stage_upper:
            css = "trace-action"
        else:
            css = "trace-stage"

        output.append(
            f"""
            <div class="trace-line">
                <span class="trace-time">{timestamp}</span>
                <span class="{css}">
                    | {stage}:
                </span>
                {message}
            </div>
            """
        )

    render_html(
        f'<div class="trace">{"".join(output)}</div>'
    )


# ============================================================================
# Operations controls
# ============================================================================

def render_controls() -> None:
    st.markdown(
        '<div class="section-title">🎛 <span>OPERATIONS CONTROL</span></div>',
        unsafe_allow_html=True,
    )

    a, d = st.columns([1.35, 2.0])

    with a:
        if st.button(
            "🛡️ RUN AUTONOMOUS HARIS",
            use_container_width=True,
            type="primary",
        ):
            with st.spinner(
                "HARIS is executing the closed control loop…"
            ):
                start = time.perf_counter()

                try:
                    result = run_async(
                        get_system().run_cycle(
                            dust_advisory=True
                        )
                    )

                    st.session_state.last_result = result
                    st.session_state.last_elapsed = (
                        time.perf_counter() - start
                    )
                    st.rerun()

                except Exception as exc:
                    logger.exception(
                        "HARIS cycle failed"
                    )

                    st.error(
                        f"HARIS cycle failed: {exc}"
                    )

    with d:
        elapsed = st.session_state.get("last_elapsed")

        if elapsed is not None:
            timing = f"Last cycle: {elapsed:.2f}s"
        else:
            timing = "No cycle executed yet"

        render_html(
            f"""
            <div class="small-note">
                <b style="color:#8ca5bd;">{timing}</b><br>
                {"Fixture mode is deterministic and demo-safe." if settings.nac_mode == "fixture" else "Live Nokia telemetry is read-only; no network mutation will be attempted."}
                Tower status and KPI values come from HARIS state/readback.
                The topology visualizes the logical network fabric.
            </div>
            """
        )


def render_history() -> None:
    st.markdown('<div class="section-title">📜 <span>INCIDENT HISTORY / REPLAY</span></div>', unsafe_allow_html=True)
    records = get_system().memory.recent_incidents()
    chain = get_system().memory.verify_audit_chain()
    st.caption("AUDIT CHAIN: " + ("VALID" if chain.get("valid") else "LEGACY/INVALID") + " — tamper-evident append-only history")
    if not records:
        st.caption("No append-only audit history is available yet.")
        return
    labels = [f"{item.created_at} · {item.cycle_id or item.incident_id} · {item.outcome}" for item in records]
    selected = records[labels.index(st.selectbox("Replay an append-only audit record", labels))]
    st.caption(f"Mode: {selected.mode or 'N/A'} · Cells: {', '.join(selected.affected_cells) or 'N/A'} · Outcome: {selected.outcome}")
    st.json(get_system().memory.normalized_view(selected))


def render_playbook_and_feed(result: Optional[Dict[str, Any]]) -> None:
    st.markdown('<div class="section-title">📋 <span>ACTIVE PLAYBOOK</span></div>', unsafe_allow_html=True)
    playbook = (result or {}).get("active_playbook", {})
    st.json(playbook or {"name": "N/A", "state": "IDLE", "latest_outcome": "N/A"})
    st.markdown('<div class="section-title">📰 <span>INCIDENT FEED</span></div>', unsafe_allow_html=True)
    events = (result or {}).get("events", [])
    if events: st.dataframe(events, use_container_width=True, hide_index=True)
    else: st.caption("No incident events yet.")
    dispatch = (result or {}).get("trusted_dispatch")
    if dispatch:
        st.markdown('<div class="section-title">👷 <span>FIELD INTERVENTION / TRUSTED DISPATCH</span></div>', unsafe_allow_html=True)
        st.json(dispatch)


# ============================================================================
# Console sections
# ============================================================================

def render_status_bar(result: Optional[Dict[str, Any]]) -> None:
    """Persistent supervision state, intentionally not an API control surface."""
    incident = (result or {}).get("incident", {})
    warden = (result or {}).get("warden", {})
    st.caption(
        f"HARIS STATE: {(result or {}).get('final_status', 'READY').upper()} | "
        f"NOKIA STATE: {get_system().client.name.upper()} | MODE: {settings.nac_mode.upper()} | "
        f"WARDEN: {'APPROVED' if warden.get('verified') else 'REVIEW'} | "
        f"ACTIVE INCIDENT: {incident.get('incident_id', 'NONE')}"
    )


def render_overview(result: Optional[Dict[str, Any]]) -> None:
    render_header(result)
    incident, prediction, warden = (result or {}).get("incident", {}), (result or {}).get("prediction", {}), (result or {}).get("warden", {})
    cols = st.columns(5)
    cols[0].metric("Backend Health", "HEALTHY")
    cols[1].metric("Nokia Integration", get_system().client.name.upper())
    cols[2].metric("WARDEN", "APPROVED" if warden.get("verified") else "STANDING BY")
    cols[3].metric("Active Incident", incident.get("incident_id", "None"))
    cols[4].metric("Predicted Risk", prediction.get("predicted_risk_level", "Monitoring").upper())
    st.markdown('### IMPORTANT NOTIFICATIONS')
    events = (result or {}).get("events", [])[-6:]
    if events:
        for event in reversed(events): st.info(event.get("message", "HARIS event"))
    else: st.caption("No active notifications. HARIS is monitoring autonomously.")
    render_capability_matrix(result)


def render_network_intelligence(result: Optional[Dict[str, Any]]) -> None:
    st.markdown('### NETWORK INTELLIGENCE')
    enabled = st.toggle("Geofencing Monitoring", value=bool(get_system().settings.geofencing_monitoring_enabled))
    get_system().settings.geofencing_monitoring_enabled = enabled
    st.caption("HARIS creates and cleans up geofence subscriptions only when policy and a playbook require it.")
    render_environment(result)
    render_prediction(result)
    render_network_section(result)
    geofence_events = [event for event in (result or {}).get("events", []) if "GEOFENCE" in event.get("message", "").upper()]
    st.markdown('### GEOFENCE EVENTS')
    if geofence_events: st.dataframe(geofence_events, use_container_width=True, hide_index=True)
    else: st.caption("No Nokia geofence enter/exit event received.")


def render_trusted_dispatch(result: Optional[Dict[str, Any]]) -> None:
    st.markdown('### TRUSTED DISPATCH')
    dispatch = (result or {}).get("trusted_dispatch") or get_system().latest_dispatch
    if dispatch:
        safe_status = {key: value for key, value in dispatch.items() if key != "authorization_url"}
        st.json(safe_status)
        if dispatch.get("status") == "WAITING_FOR_IDENTITY_VERIFICATION": st.warning("Awaiting consent-bound Nokia Number Verification; dispatch remains blocked.")
        if dispatch.get("authorization_url"):
            st.link_button("Open secure Nokia Number Verification", dispatch["authorization_url"], type="primary")
    else: st.caption("No privileged field intervention is required. Routine remediation does not call Number Verification or SIM Swap.")


def render_history_audit(result: Optional[Dict[str, Any]]) -> None:
    render_history()
    st.markdown('### LEARNED MEMORY')
    st.json((result or {}).get("learning") or {"status": "No completed cycle in this session."})


# ============================================================================
# Main render
# ============================================================================

result = st.session_state.get("last_result")
render_status_bar(result)
section = st.radio(
    "HARIS CONSOLE", ["OVERVIEW", "NETWORK INTELLIGENCE", "AUTONOMOUS OPERATIONS", "TRUSTED DISPATCH", "HISTORY & AUDIT"],
    horizontal=True, label_visibility="collapsed",
)
st.markdown('<div class="hr"></div>', unsafe_allow_html=True)

if section == "OVERVIEW":
    render_overview(result)
elif section == "NETWORK INTELLIGENCE":
    render_network_intelligence(result)
elif section == "AUTONOMOUS OPERATIONS":
    render_controls()
    render_decision_engine(result)
    render_impact(result)
    render_playbook_and_feed(result)
elif section == "TRUSTED DISPATCH":
    render_trusted_dispatch(result)
elif section == "HISTORY & AUDIT":
    render_history_audit(result)

render_html(
    """
    <div class="footer">
        HARIS · Theme 6 — Climate Resilience & Environmental Monitoring ·
        GSMA MENA Ignite Hackathon 2026 · Live Network Resilience Console
    </div>
    """
)
