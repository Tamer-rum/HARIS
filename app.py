from __future__ import annotations

import asyncio
import html
import json
import logging
import math
import time
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import re
import streamlit as st
from streamlit.runtime.scriptrunner import get_script_run_ctx

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


def safe_mapping(value: Any) -> Dict[str, Any]:
    """Treat nullable backend JSON objects as empty presentation objects."""
    return value if isinstance(value, dict) else {}


def authoritative_metric(value: Any, *, kind: str) -> str:
    """Format a present authoritative value without turning absence into zero."""
    if kind == "actions":
        return str(len(value)) if isinstance(value, list) else "N/A"
    if value is None:
        return "N/A"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "N/A"
    if not math.isfinite(number):
        return "N/A"
    if kind in {"confidence", "blast_radius"}:
        return f"{number * 100:.0f}%"
    if kind == "qod_cost":
        return f"${number:.2f}"
    return "N/A"


def authoritative_verification_label(verification: Dict[str, Any]) -> str:
    """Keep absent verification evidence distinct from an authoritative fail."""
    if "verified" not in verification or verification.get("verified") is None:
        return "N/A"
    return "PASSED" if verification["verified"] is True else "REVIEW"


def protected_tier1_count(result: Dict[str, Any]) -> str:
    """Count only current-cycle executed actions targeting observed tier-1 devices."""
    execution = safe_mapping(result.get("execution"))
    actions = execution.get("actions")
    devices = result.get("devices")
    if not isinstance(actions, list) or not isinstance(devices, list):
        return "N/A"
    tier1 = {safe_mapping(item).get("device_id") for item in devices if safe_mapping(item).get("tier") == 1}
    targeted = {
        safe_mapping(action).get("device_id")
        for action in actions
        if safe_mapping(action).get("success") is True
    }
    return str(len((tier1 & targeted) - {None}))


def safe_upper(value: Any, fallback: str) -> str:
    """Format optional API strings without displaying a literal ``None``."""
    return str(value or fallback).upper()


def safe_text(value: Any, fallback: str = "N/A") -> str:
    """Escape presentation values without changing their source or meaning."""
    return html.escape(str(value or fallback))


def semantic_tone(value: Any) -> str:
    """Map authoritative status text to a presentation-only semantic colour."""
    state = safe_upper(value, "NEUTRAL")
    if any(token in state for token in ("BLOCK", "CRITICAL", "FAILED", "DENIED", "ROLL", "UNAVAILABLE")):
        return "danger"
    if any(token in state for token in ("WAIT", "PENDING", "REVIEW", "RISK", "WARNING", "AT RISK")):
        return "warning"
    if any(token in state for token in ("READY", "HEALTHY", "VERIFIED", "APPROVED", "MITIGATED", "CONNECTED", "OPERATING")):
        return "success"
    return "neutral"


def render_svg_icon(name: str, css_class: str = "console-icon") -> str:
    """Return a small, non-data inline SVG from the shared console icon set."""
    paths = {
        "server": '<rect x="4" y="4" width="16" height="5" rx="1"/><rect x="4" y="11" width="16" height="5" rx="1"/><rect x="4" y="18" width="16" height="2" rx="1"/><path d="M7 6.5h.01M7 13.5h.01M17 6.5h1M17 13.5h1"/>',
        "nokia": '<path d="M3 17V7l7 10V7l7 10V7l4 10"/><path d="M3 20h18"/>',
        "shield": '<path d="M12 3 20 6v5c0 5-3.3 8.3-8 10-4.7-1.7-8-5-8-10V6l8-3Z"/><path d="M9 12l2 2 4-4"/>',
        "radar": '<circle cx="12" cy="12" r="8"/><circle cx="12" cy="12" r="4"/><circle cx="12" cy="12" r="1"/><path d="M12 2v2M22 12h-2M12 22v-2M2 12h2"/>',
        "risk": '<path d="M4 19V14M9 19V10M14 19V6M19 19V3"/><path d="M3 21h18"/>',
        "cycle": '<path d="M19 8a8 8 0 0 0-13.5-2L3 9"/><path d="M3 4v5h5"/><path d="M5 16a8 8 0 0 0 13.5 2L21 15"/><path d="M21 20v-5h-5"/>',
        "congestion": '<rect x="4" y="8" width="16" height="11" rx="1"/><path d="M7 8V5M12 8V3M17 8V6M7 13h10M7 16h6"/>',
        "device": '<rect x="7" y="3" width="10" height="18" rx="2"/><path d="M10 6h4M11.5 18h1"/>',
        "location": '<path d="M12 21s7-5.2 7-11a7 7 0 1 0-14 0c0 5.8 7 11 7 11Z"/><circle cx="12" cy="10" r="2"/>',
        "geofence": '<path d="M4 5l5-2 6 3 5-2v15l-5 2-6-3-5 2V5Z"/><path d="M9 3v15M15 6v15"/><circle cx="15" cy="10" r="2"/>',
        "qod": '<path d="M4 7h16M4 12h16M4 17h16"/><circle cx="9" cy="7" r="2"/><circle cx="15" cy="12" r="2"/><circle cx="7" cy="17" r="2"/>',
        "slicing": '<path d="m12 3 8 4-8 4-8-4 8-4Z"/><path d="m4 12 8 4 8-4M4 17l8 4 8-4"/>',
        "dispatch": '<circle cx="12" cy="7" r="3"/><circle cx="5" cy="10" r="2"/><circle cx="19" cy="10" r="2"/><path d="M7 20v-2a5 5 0 0 1 10 0v2M2 20v-2a4 4 0 0 1 3-3.9M22 20v-2a4 4 0 0 0-3-3.9"/>',
    }
    path = paths.get(name, paths["radar"])
    return f'<svg class="{css_class}" viewBox="0 0 24 24" aria-hidden="true">{path}</svg>'


def render_haris_wordmark() -> str:
    """Project-owned geometric vector wordmark; no external logo asset."""
    return """<svg class="haris-wordmark" viewBox="0 0 510 88" role="img" aria-label="HARIS">
      <g fill="none" stroke="currentColor" stroke-width="7" stroke-linecap="square" stroke-linejoin="miter">
        <path d="M10 12v64M10 44h66M76 12v64"/>
        <path d="m102 76 31-64 31 64M114 51h38"/>
        <path d="M190 76V12h38c17 0 25 8 25 20s-8 20-25 20h-38M224 52l34 24"/>
        <path d="M290 12v64"/>
        <path d="M370 16c-10-6-25-7-37-2-20 9-14 29 6 33l17 3c22 4 24 25 5 31-15 5-31 1-42-7"/>
      </g>
    </svg>"""


def render_nokia_wordmark() -> str:
    """Legible typographic Nokia treatment, confined to the integration card."""
    return ('<svg class="nokia-wordmark" viewBox="0 0 150 42" role="img" aria-label="NOKIA">'
            '<text x="1" y="31" fill="currentColor" font-family="Arial, sans-serif" font-size="30" font-weight="600" letter-spacing="1">NOKIA</text></svg>')


def operational_card(label: str, value: Any, detail: Any = None, tone: Optional[str] = None, icon: str = "radar") -> str:
    """Reusable custom status panel for authoritative console state."""
    card_tone = tone or semantic_tone(value)
    symbol = {"success": "&#10003;", "warning": "&#9651;", "danger": "&#215;", "neutral": "&#9679;"}.get(card_tone, "&#9679;")
    detail_html = f'<div class="ops-card-detail">{safe_text(detail, "")}</div>' if detail else ""
    card_visual = render_nokia_wordmark() if icon == "nokia" else render_svg_icon(icon, "ops-card-icon")
    return (
        f'<div class="ops-card {card_tone}">'
        f'<div class="ops-card-content"><div class="ops-card-top"><div class="ops-card-label"><span class="ops-card-symbol">{symbol}</span>{safe_text(label)}</div>{card_visual}</div>'
        f'<div class="ops-card-value">{safe_text(value)}</div>{detail_html}</div><div class="ops-card-motif" aria-hidden="true"></div></div>'
    )


def capability_card(label: str, value: Any, detail: Any, icon: str) -> str:
    """Shared capability card; status and description remain authoritative."""
    tone = semantic_tone(value)
    return (
        f'<div class="capability-card {tone}">{render_svg_icon(icon, "capability-icon")}<div>'
        f'<div class="capability-label">{safe_text(label)}</div><div class="capability-value">{safe_text(value)}</div>'
        f'<div class="capability-detail">{safe_text(detail, "No capability detail available.")}</div></div></div>'
    )


def render_section_header(title: str, subtitle: Optional[str] = None) -> None:
    subtitle_html = f'<div class="section-subtitle">{safe_text(subtitle)}</div>' if subtitle else ""
    render_html(f'<div class="console-section-header"><div class="console-section-title">{safe_text(title)}</div><div class="console-section-rule"></div>{subtitle_html}</div>')


def authoritative_haris_state(result: Optional[Dict[str, Any]], supervisory: Optional[Dict[str, Any]] = None) -> str:
    """Prefer Render's active workflow state over a nullable completed-cycle value."""
    supervisory = safe_mapping(supervisory)
    cycle = safe_mapping(result)
    dispatch = safe_mapping(cycle.get("trusted_dispatch"))
    return safe_upper(
        supervisory.get("haris_state")
        or dispatch.get("status")
        or cycle.get("final_status"),
        "READY",
    )


def presentation_mode_label() -> str:
    """Compact truthful label that does not clip on the judge-facing header."""
    return {
        "fixture": "FIXTURE / DEMO",
        "live_read_only": "LIVE / READ-ONLY",
        "live_write": "LIVE / WRITE ENABLED",
    }.get(settings.nac_mode, safe_upper(settings.nac_mode, "UNKNOWN"))


def overview_notifications(result: Optional[Dict[str, Any]], supervisory: Optional[Dict[str, Any]] = None) -> List[Tuple[str, str, str]]:
    """Concise, evidence-derived overview notices; detailed traces stay elsewhere."""
    cycle, supervisory = safe_mapping(result), safe_mapping(supervisory)
    dispatch = safe_mapping(cycle.get("trusted_dispatch"))
    attempts = supervisory.get("dispatch_history") or cycle.get("dispatch_history") or []
    notices: List[Tuple[str, str, str]] = []
    blocked = next((safe_mapping(item) for item in reversed(attempts)
                    if safe_mapping(item).get("sim_swap_status") == "RECENT_SWAP"
                    and safe_mapping(item).get("warden_decision") == "BLOCK"), None)
    if blocked:
        notices.append(("danger", "SECURITY ALERT", "Recent SIM swap detected — field engineer blocked by WARDEN."))
    if dispatch.get("fallback_from"):
        notices.append(("warning", "AUTOMATIC FALLBACK", "Fallback engineer selected automatically after the previous engineer was blocked."))
    if dispatch.get("status") == "WAITING_FOR_IDENTITY_VERIFICATION":
        notices.append(("warning", "ACTION REQUIRED", "Awaiting secure Nokia Number Verification consent for the selected engineer."))
    return notices

def _streamlit_entry_active() -> bool:
    """UI side effects are legal only under ``streamlit run``."""
    return get_script_run_ctx(suppress_warning=True) is not None


if _streamlit_entry_active():
    st.set_page_config(
    page_title="HARIS — Network Resilience",
    page_icon="H",
    layout="wide",
        initial_sidebar_state="collapsed",
    )


# ============================================================================
# CSS
# ============================================================================

def _render_global_css(content: str, *, unsafe_allow_html: bool = False) -> None:
    if _streamlit_entry_active():
        st.markdown(content, unsafe_allow_html=unsafe_allow_html)


_render_global_css(
    """
<style>
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
    perspective: 1400px;
    transform-style: preserve-3d;
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
    width: 38px;
    height: 38px;
    display: grid;
    place-items: center;
    border: 1px solid rgba(49,215,255,.65);
    border-radius: 10px;
    color: var(--cyan);
    font: 800 1.1rem/1 "JetBrains Mono", monospace;
    box-shadow: 0 0 16px rgba(49,215,255,.20), inset 0 0 12px rgba(49,215,255,.05);
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

/* Presentation-only command-console system. Audit/JSON/table content remains
   selectable and copy-friendly; these rules never alter application state. */
.stApp::before { content:""; position:fixed; inset:0; z-index:-1; pointer-events:none; opacity:.22; background-image:linear-gradient(rgba(49,215,255,.055) 1px,transparent 1px),linear-gradient(90deg,rgba(49,215,255,.04) 1px,transparent 1px); background-size:72px 72px; mask-image:linear-gradient(to bottom,black,transparent 72%); }
.command-header { position:relative; overflow:hidden; display:grid; grid-template-columns:minmax(290px,1.6fr) minmax(500px,2.4fr); gap:26px; margin:0 0 17px; padding:23px 24px 20px; border:1px solid rgba(49,215,255,.32); border-radius:16px; background:radial-gradient(circle at 88% 8%,rgba(49,215,255,.15),transparent 29%),linear-gradient(135deg,rgba(12,29,45,.98),rgba(6,12,20,.98)); box-shadow:inset 0 1px 0 rgba(255,255,255,.04),0 17px 40px rgba(0,0,0,.28),0 0 34px rgba(49,215,255,.08); }
.command-header::after { content:""; position:absolute; left:24px; right:24px; bottom:0; height:1px; background:linear-gradient(90deg,transparent,rgba(49,215,255,.75),transparent); }
.command-kicker { color:#68dcf9; font-size:.61rem; font-weight:800; letter-spacing:.18em; }.command-brand { display:flex; align-items:center; gap:13px; margin-top:8px; }.command-mark { width:44px; height:44px; display:grid; place-items:center; border:1px solid #45dcff; border-radius:11px; color:#dff8ff; font:800 1.25rem/1 "JetBrains Mono",monospace; box-shadow:inset 0 0 16px rgba(49,215,255,.12),0 0 19px rgba(49,215,255,.23); }.command-name { color:#f1faff; font-size:2.15rem; font-weight:800; letter-spacing:-.055em; line-height:.9; }.command-subtitle { margin-top:7px; color:#9ab0c5; font-size:.72rem; line-height:1.45; }.command-states { align-self:end; display:grid; grid-template-columns:repeat(5,minmax(82px,1fr)); border-left:1px solid rgba(49,215,255,.14); }.command-state { min-height:60px; padding:4px 11px 4px 15px; border-right:1px solid rgba(49,215,255,.14); }.command-state:last-child { border-right:0; }.command-state-label { color:#7290a8; font-size:.55rem; font-weight:800; letter-spacing:.12em; white-space:nowrap; }.command-state-value { margin-top:9px; color:#e9f7ff; font-size:.75rem; font-weight:800; overflow-wrap:anywhere; }.command-state.success .command-state-value { color:#5af1af; }.command-state.warning .command-state-value { color:#ffd06a; }.command-state.danger .command-state-value { color:#ff6978; }.command-state.neutral .command-state-value { color:#6fdaff; }
.overview-banner { display:flex; align-items:flex-end; justify-content:space-between; gap:20px; padding:5px 2px 16px; }.overview-banner-title { color:#edf8ff; font-size:1.25rem; font-weight:800; letter-spacing:.02em; }.overview-banner-detail { color:#7591a8; font-size:.71rem; max-width:640px; text-align:right; line-height:1.45; }
.ops-card { position:relative; isolation:isolate; min-height:110px; overflow:hidden; padding:14px 15px; border:1px solid #244055; border-radius:13px; background:linear-gradient(145deg,rgba(16,31,45,.98),rgba(7,14,23,.98)); box-shadow:inset 0 1px 0 rgba(255,255,255,.03); transform-style:preserve-3d; will-change:transform,box-shadow; transition:transform 210ms cubic-bezier(.18,.72,.2,1),border-color 210ms ease,box-shadow 210ms ease; }
.ops-card::before { content:""; position:absolute; inset:auto 12px -19px; height:34px; z-index:-1; opacity:0; filter:blur(15px); background:rgba(49,215,255,.52); transition:opacity 210ms ease; }.ops-card:hover { transform:translateZ(15px) scale(1.018); border-color:rgba(78,224,255,.85); box-shadow:0 26px 38px rgba(0,0,0,.48),0 14px 36px rgba(49,215,255,.23),inset 0 1px 0 rgba(255,255,255,.08); }.ops-card:hover::before { opacity:.72; }.ops-card.success { border-color:rgba(66,245,155,.38); }.ops-card.success::before { background:rgba(66,245,155,.44); }.ops-card.warning { border-color:rgba(255,200,87,.42); }.ops-card.warning::before { background:rgba(255,200,87,.44); }.ops-card.danger { border-color:rgba(255,77,95,.48); }.ops-card.danger::before { background:rgba(255,77,95,.46); }.ops-card-label { color:#88a2b8; font-size:.59rem; font-weight:800; letter-spacing:.12em; text-transform:uppercase; }.ops-card-symbol { margin-right:7px; color:#5de3ff; font-size:.75rem; }.ops-card.success .ops-card-symbol,.ops-card.success .ops-card-value { color:#5af1af; }.ops-card.warning .ops-card-symbol,.ops-card.warning .ops-card-value { color:#ffd06a; }.ops-card.danger .ops-card-symbol,.ops-card.danger .ops-card-value { color:#ff6e7c; }.ops-card-value { margin-top:13px; color:#edf8ff; font-size:1.06rem; font-weight:800; letter-spacing:.01em; overflow-wrap:anywhere; }.ops-card-detail { margin-top:7px; color:#7691a9; font-size:.64rem; line-height:1.35; }
[data-testid="stMetric"], .panel, .kpi-card, .hero, .status-card { transform-style:preserve-3d; will-change:transform,box-shadow; transition:transform 210ms cubic-bezier(.18,.72,.2,1),border-color 210ms ease,box-shadow 210ms ease; }
[data-testid="stMetric"]:hover, .panel:hover, .kpi-card:hover, .hero:hover, .status-card:hover { transform:translateZ(12px) scale(1.012); border-color:rgba(49,215,255,.58); box-shadow:0 22px 35px rgba(0,0,0,.45),0 13px 31px rgba(49,215,255,.18),0 0 27px rgba(49,215,255,.15); }
div[data-testid="stButton"] button {
    transition: transform 180ms ease, border-color 180ms ease, box-shadow 180ms ease, filter 180ms ease;
    border: 1px solid rgba(49,215,255,.24);
}
div[data-testid="stButton"] button:hover {
    transform: translateZ(4px) scale(1.004);
    border-color: rgba(49,215,255,.70);
    box-shadow: 0 10px 20px rgba(0,0,0,.32), 0 0 17px rgba(49,215,255,.18);
    filter: brightness(1.07);
}
div[role="radiogroup"] { gap:1.45rem; border-bottom:1px solid rgba(49,215,255,.18); padding:0 .15rem .36rem; }
div[role="radiogroup"] input[type="radio"], [data-testid="stRadio"] label > div:first-child { position:absolute !important; opacity:0 !important; width:1px !important; height:1px !important; overflow:hidden !important; pointer-events:none !important; }
div[role="radiogroup"] label {
    background: transparent !important; border: 0 !important; border-radius: 0 !important;
    color:#7890aa !important; padding:.42rem 0 !important; font-size:.68rem !important;
    font-weight:800 !important; letter-spacing:.09em; transition:color 180ms ease,text-shadow 180ms ease,box-shadow 180ms ease;
}
div[role="radiogroup"] label:hover { color: #eaf8ff !important; text-shadow: 0 0 10px rgba(49,215,255,.34); }
div[role="radiogroup"] label:has(input:checked) { color:#eafcff !important; box-shadow:inset 0 -2px 0 #31d7ff; text-shadow:0 0 13px rgba(49,215,255,.52); }
.brand, .hero, .section-title, .panel-title, .ops-card, .command-header, [data-testid="stMetric"] { user-select:none; cursor:default; }
.notice { position:relative; overflow:hidden; border:1px solid rgba(49,215,255,.28); border-left:3px solid #31d7ff; background:linear-gradient(105deg,rgba(16,38,56,.9),rgba(8,16,26,.84)); border-radius:10px; padding:13px 15px; margin:9px 0; color:#c8d9e9; font-size:.75rem; line-height:1.5; box-shadow:inset 0 1px 0 rgba(255,255,255,.025); }.notice b { color:#eaf8ff; font-size:.66rem; letter-spacing:.12em; }.notice.warning { border-color:rgba(255,200,87,.34); border-left-color:#ffc857; }.notice.danger { border-color:rgba(255,77,95,.38); border-left-color:#ff4d5f; }.notice.success { border-color:rgba(66,245,155,.34); border-left-color:#42f59b; }
.workflow-panel { padding:4px 15px; }.workflow-row { display:grid; grid-template-columns:46px minmax(120px,.65fr) minmax(220px,1.8fr); align-items:center; gap:12px; min-height:50px; border-bottom:1px solid rgba(84,121,150,.18); border-left:2px solid #4c6f89; padding:7px 10px; }.workflow-row:last-child { border-bottom:0; }.workflow-row.success { border-left-color:#42f59b; }.workflow-row.warning { border-left-color:#ffc857; }.workflow-row.danger { border-left-color:#ff4d5f; }.workflow-number { font:700 .63rem "JetBrains Mono",monospace; color:#7895ac; }.workflow-stage { color:#e6f5ff; font-size:.69rem; font-weight:800; letter-spacing:.08em; }.workflow-detail { color:#8ea7bc; font-size:.68rem; text-align:right; line-height:1.35; }.workflow-state { font-size:.73rem; margin-right:6px; }
.dispatch-board { display:grid; grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); gap:10px; margin:0 0 13px; }.dispatch-attempt { border:1px solid #284257; border-top:2px solid #5fdaff; border-radius:10px; padding:12px; background:linear-gradient(145deg,rgba(13,29,43,.94),rgba(7,14,23,.98)); }.dispatch-attempt.warning { border-top-color:#ffc857; }.dispatch-attempt.danger { border-top-color:#ff4d5f; }.dispatch-attempt.success { border-top-color:#42f59b; }.dispatch-attempt-label { color:#8ba6bd; font-size:.57rem; font-weight:800; letter-spacing:.12em; }.dispatch-attempt-name { color:#eff9ff; margin-top:8px; font-size:.88rem; font-weight:800; }.dispatch-attempt-state { margin-top:8px; font-size:.66rem; font-weight:800; }.audit-chain-card { display:flex; align-items:center; gap:12px; margin:0 0 10px; padding:13px 15px; border:1px solid rgba(66,245,155,.42); border-radius:11px; background:linear-gradient(100deg,rgba(12,47,34,.7),rgba(7,15,23,.9)); }.audit-chain-card.invalid { border-color:rgba(255,77,95,.42); background:linear-gradient(100deg,rgba(55,17,26,.65),rgba(7,15,23,.9)); }.audit-chain-symbol { color:#5af1af; font-size:1.15rem; }.audit-chain-card.invalid .audit-chain-symbol { color:#ff6e7c; }.audit-chain-title { color:#a7bfce; font-size:.59rem; font-weight:800; letter-spacing:.13em; }.audit-chain-value { margin-top:3px; color:#eafaff; font-size:.9rem; font-weight:800; }
.section-mark { color:var(--cyan); font-size:.72rem; margin-right:.38rem; }.section-mark.warning { color:var(--yellow); }
@media (max-width:1100px) { .command-header { grid-template-columns:1fr; gap:18px; }.command-states { border-left:0; }.overview-banner { align-items:flex-start; flex-direction:column; }.overview-banner-detail { text-align:left; }.workflow-row { grid-template-columns:38px 120px 1fr; } }
@media (max-width:900px) { div[role="radiogroup"] { gap:.55rem; flex-wrap:wrap; } div[role="radiogroup"] label { font-size:.59rem !important; } .brand-sub { display:none; } .command-header { padding:18px; }.command-states { grid-template-columns:repeat(2,1fr); }.command-state { border-bottom:1px solid rgba(49,215,255,.12); }.workflow-row { grid-template-columns:32px 1fr; }.workflow-detail { grid-column:2; text-align:left; }.ops-card:hover { transform:translateZ(9px) scale(1.008); } }

div[data-testid="stAlert"] {
    border-radius: 10px;
}

/* Reference command-center reconstruction: a full-width, non-data visual shell. */
:root { --ink:#020810; --navy:#04111d; --panel-ink:rgba(5,18,31,.84); --cyan:#32d9ff; --cyan-soft:#60e7ff; --green:#43e6ad; --amber:#ffc84a; --red:#ff4d5a; --muted:#91a7bb; --primary:#f4f8fc; }
.stApp { min-height:100vh; background:radial-gradient(ellipse at 86% 2%,rgba(22,95,174,.24),transparent 28%),radial-gradient(ellipse at 12% -8%,rgba(25,81,145,.17),transparent 30%),linear-gradient(140deg,#020810 0%,#04111d 46%,#020810 100%) !important; }
.block-container { max-width:none !important; width:100% !important; padding:1.35rem 1.35rem 1.2rem !important; }
.stApp::before { opacity:.13; background-size:94px 94px; }
.haris-decor { position:fixed; inset:0; z-index:-1; overflow:hidden; pointer-events:none; }
.haris-globe { position:absolute; right:-10vw; top:-15vh; width:min(64vw,920px); height:min(64vw,920px); opacity:.64; filter:drop-shadow(0 0 34px rgba(46,145,255,.24)); }
.haris-topology-lines { position:absolute; left:-4vw; top:0; width:40vw; height:34vh; opacity:.4; }.haris-floor-grid { position:absolute; left:-4vw; right:-4vw; bottom:-8vh; height:31vh; opacity:.27; }
.command-header { display:grid; grid-template-columns:minmax(260px,1.15fr) minmax(290px,1.25fr) minmax(520px,2.1fr); align-items:center; gap:26px; margin:0; padding:18px 6px 23px; border:0; border-radius:0; background:transparent; box-shadow:none; overflow:visible; }
.command-header::after { left:0; right:0; bottom:0; background:linear-gradient(90deg,rgba(50,217,255,.15),rgba(96,231,255,.88) 18%,rgba(50,217,255,.18) 63%,transparent); }
.command-kicker { color:#99c7ee; font-size:.78rem; letter-spacing:.17em; line-height:1.75; }.command-brand { margin:0; gap:0; border-right:1px solid rgba(130,207,255,.78); padding-right:25px; min-height:84px; align-items:center; }.command-mark { display:none; }.command-name { color:#f5fbff; font-size:3.15rem; letter-spacing:.24em; line-height:1; text-shadow:0 0 14px rgba(142,211,255,.68),0 0 28px rgba(64,149,255,.32); }.command-subtitle { max-width:290px; color:#c3dcf4; font-size:.54rem; letter-spacing:.16em; line-height:1.65; text-transform:uppercase; }.command-message { color:#a7c9ef; font-size:.89rem; font-weight:700; letter-spacing:.18em; line-height:1.75; text-transform:uppercase; }.command-states { align-self:center; grid-template-columns:repeat(5,minmax(82px,1fr)); border-left:0; }.command-state { min-height:51px; padding:3px 10px 3px 15px; border-right:0; border-left:1px solid rgba(131,199,242,.37); }.command-state-label { color:#9db5cb; font-size:.49rem; letter-spacing:.08em; }.command-state-value { display:flex; align-items:center; gap:6px; margin-top:8px; color:#d9efff; font-size:.61rem; white-space:nowrap; }.command-state-value::before { content:""; width:8px; height:8px; flex:0 0 8px; border-radius:50%; background:#65dff8; box-shadow:0 0 11px rgba(96,231,255,.72); }.command-state.success .command-state-value { color:#7cf2bd; }.command-state.success .command-state-value::before { background:#43e6ad; box-shadow:0 0 11px rgba(67,230,173,.74); }.command-state.warning .command-state-value { color:#ffd66c; }.command-state.warning .command-state-value::before { background:#ffc84a; box-shadow:0 0 11px rgba(255,200,74,.74); }.command-state.danger .command-state-value { color:#ff7882; }.command-state.danger .command-state-value::before { background:#ff4d5a; box-shadow:0 0 11px rgba(255,77,90,.7); }
div[role="radiogroup"] { display:grid !important; grid-template-columns:repeat(5,minmax(0,1fr)); width:100%; gap:0; border-bottom:1px solid rgba(64,179,239,.38); padding:0 0 .02rem; margin:0; }
div[role="radiogroup"] label { display:flex !important; justify-content:center; align-items:center; min-height:49px; padding:.55rem .35rem !important; color:#9bb7cf !important; font-size:.67rem !important; letter-spacing:.12em; text-align:center; position:relative; }
div[role="radiogroup"] label::after { content:""; position:absolute; left:50%; bottom:-1px; width:0; height:2px; background:#60e7ff; box-shadow:0 0 11px rgba(96,231,255,.9),0 0 20px rgba(50,217,255,.52); transition:width 190ms ease,left 190ms ease; }.command-header + div[role="radiogroup"] { margin-top:0; }
div[role="radiogroup"] label:hover::after { width:52%; left:24%; } div[role="radiogroup"] label:has(input:checked)::after { width:58%; left:21%; } div[role="radiogroup"] label:has(input:checked) { color:#f2fbff !important; text-shadow:0 0 13px rgba(96,231,255,.72); }
.overview-banner { padding:27px 5px 20px; align-items:flex-start; }.overview-banner-title,.console-section-title { color:#7ee7ff; font-size:1.15rem; letter-spacing:.20em; text-transform:uppercase; text-shadow:0 0 12px rgba(50,217,255,.24); }.overview-banner-title::after { content:""; display:block; width:35px; height:2px; margin-top:13px; background:#70e7ff; box-shadow:0 0 10px rgba(96,231,255,.7); }.overview-banner-detail { max-width:none; text-align:left; color:#9bb5cb; font-size:.55rem; letter-spacing:.14em; text-transform:uppercase; }.ops-card { min-height:168px; padding:17px; border-radius:12px; background:linear-gradient(135deg,rgba(7,23,39,.88),rgba(4,14,25,.84)); border-color:rgba(96,184,244,.45); box-shadow:inset 0 1px 0 rgba(223,251,255,.05),0 9px 22px rgba(0,0,0,.24); }.ops-card-top { display:flex; justify-content:space-between; gap:8px; align-items:flex-start; }.ops-card-label { color:#a2bed7; font-size:.57rem; letter-spacing:.12em; }.ops-card-value { margin-top:27px; color:#f2f8fc; font-size:1.2rem; }.ops-card-detail { margin-top:9px; color:#94b8cc; font-size:.67rem; }.ops-card-icon { width:48px; height:48px; color:#71dfff; opacity:.76; fill:none; stroke:currentColor; stroke-width:1.45; stroke-linecap:round; stroke-linejoin:round; }.ops-card-motif { position:absolute; left:17px; bottom:16px; width:58%; height:11px; opacity:.46; background:linear-gradient(135deg,transparent 0 9%,currentColor 10% 12%,transparent 13% 25%,currentColor 26% 28%,transparent 29% 42%,currentColor 43% 45%,transparent 46% 61%,currentColor 62% 64%,transparent 65%); mask-image:linear-gradient(90deg,black,transparent); }.ops-card.success { border-color:rgba(67,230,173,.76); color:#66ecb4; }.ops-card.warning { border-color:rgba(255,200,74,.76); color:#ffd263; }.ops-card.danger { border-color:rgba(255,77,90,.78); color:#ff7781; }.ops-card.neutral { border-color:rgba(96,198,255,.68); color:#7edfff; }.ops-card:hover { transform:translateZ(18px) scale(1.008); box-shadow:0 29px 46px rgba(0,0,0,.52),0 15px 34px color-mix(in srgb,currentColor 25%,transparent),inset 0 1px 0 rgba(255,255,255,.1); }
.console-section-header { margin:28px 5px 16px; }.console-section-rule { width:34px; height:2px; margin-top:13px; background:#62e5ff; box-shadow:0 0 10px rgba(96,231,255,.7); }.section-subtitle { margin:9px 0 0 56px; color:#9eb9cf; font-size:.51rem; font-weight:800; letter-spacing:.16em; text-transform:uppercase; }.section-title { color:#7ee7ff; letter-spacing:.15em; margin:26px 5px 13px; }.section-title span { color:inherit; }
.notice { min-height:86px; margin:0; padding:17px 19px; border-left:0; border-color:rgba(96,198,255,.4); background:linear-gradient(120deg,rgba(6,25,43,.92),rgba(5,14,24,.78)); }.notice::before { content:""; display:inline-block; width:9px; height:9px; margin-right:11px; border-radius:50%; background:#60e7ff; box-shadow:0 0 10px rgba(96,231,255,.65); }.notice b { color:#b9d8ef; }.notice.warning::before { background:#ffc84a; }.notice.danger::before { background:#ff4d5a; }.notice.success::before { background:#43e6ad; }
.cycle-panel { min-height:86px; display:flex; align-items:center; gap:15px; padding:16px 18px; border:1px solid rgba(96,198,255,.4); border-radius:10px; background:linear-gradient(120deg,rgba(6,25,43,.92),rgba(5,14,24,.78)); }.cycle-icon { width:46px; height:46px; flex:0 0 46px; color:#7ee7ff; fill:none; stroke:currentColor; stroke-width:1.3; stroke-linecap:round; stroke-linejoin:round; }.cycle-panel-label { color:#a2bed7; font-size:.57rem; font-weight:800; letter-spacing:.12em; }.cycle-panel-value { margin-top:8px; color:#edf8ff; font-size:.73rem; }.cycle-panel-value b { color:#ffd263; }
.capability-card { min-height:107px; display:flex; align-items:center; gap:18px; padding:15px 17px; border:1px solid rgba(67,230,173,.52); border-radius:11px; background:linear-gradient(135deg,rgba(7,23,39,.85),rgba(4,14,25,.78)); transform-style:preserve-3d; transition:transform 210ms cubic-bezier(.18,.72,.2,1),box-shadow 210ms ease,border-color 210ms ease; }.capability-card:hover { transform:translateZ(15px) scale(1.006); box-shadow:0 22px 37px rgba(0,0,0,.48),0 11px 30px rgba(67,230,173,.14); }.capability-card.warning { border-color:rgba(255,200,74,.65); }.capability-card.danger { border-color:rgba(255,77,90,.7); }.capability-card.neutral { border-color:rgba(96,198,255,.56); }.capability-icon { width:42px; height:42px; flex:0 0 42px; color:#82dfff; fill:none; stroke:currentColor; stroke-width:1.35; stroke-linecap:round; stroke-linejoin:round; }.capability-label { color:#aac4db; font-size:.56rem; font-weight:800; letter-spacing:.12em; }.capability-value { margin-top:10px; color:#70efb6; font-size:.9rem; font-weight:800; }.capability-card.warning .capability-value { color:#ffd263; }.capability-card.danger .capability-value { color:#ff7681; }.capability-detail { margin-top:7px; color:#89a9c0; font-size:.64rem; }
.haris-wordmark { display:block; width:min(100%, 430px); height:auto; color:#effaff; filter:drop-shadow(0 0 5px rgba(169,226,255,.82)) drop-shadow(0 0 14px rgba(63,147,255,.33)); }.nokia-wordmark { width:96px; height:31px; color:#80d8ff; opacity:.93; filter:drop-shadow(0 0 7px rgba(70,179,255,.45)); }
/* Streamlit 1.62.0 Radio.DIWLksx4.js renders:
   label[data-testid=stRadioOption] > div > div > div:first-child as the
   visible circular indicator. The preceding input lives in a hidden span. */
[data-testid="stRadio"] { display:block !important; width:min(96vw, 1920px) !important; max-width:none !important; margin:0 auto !important; }
[data-testid="stRadio"] div[role="radiogroup"] { width:100% !important; max-width:none !important; margin:0 auto !important; }
[data-testid="stRadioOption"] > div > div { gap:0 !important; }
[data-testid="stRadioOption"] > div > div > div:first-child { display:none !important; }
[data-testid="stRadioOption"] { gap:0 !important; justify-content:center !important; transition:color 190ms ease,text-shadow 190ms ease; }
[data-testid="stRadioOption"] > div > div > :last-child,
[data-testid="stRadioOption"] > div > div > :last-child * { color:#86bce8 !important; font-size:14px !important; letter-spacing:.09em !important; transition:color 190ms ease,text-shadow 190ms ease; }
[data-testid="stRadioOption"]:hover > div > div > :last-child,
[data-testid="stRadioOption"]:hover > div > div > :last-child * { color:#b8e9ff !important; text-shadow:0 0 8px rgba(50,217,255,.55),0 0 18px rgba(50,217,255,.20); }
[data-testid="stRadioOption"]:has(input:checked) > div > div > :last-child,
[data-testid="stRadioOption"]:has(input:checked) > div > div > :last-child * { color:#d7f5ff !important; text-shadow:0 0 8px rgba(50,217,255,.70),0 0 19px rgba(50,217,255,.30); }
[data-testid="stRadio"] label::after { height:2px; background:#60e7ff; box-shadow:0 0 5px rgba(96,231,255,.95),0 0 13px rgba(50,217,255,.72); }
[data-testid="stRadio"] label:has(input:checked)::before { content:""; position:absolute; z-index:-1; left:18%; right:18%; bottom:0; height:27px; pointer-events:none; background:linear-gradient(to top,rgba(50,217,255,.20),rgba(50,217,255,.055) 48%,transparent 100%); filter:blur(7px); }
.capability-card { margin:0 0 12px; }

/* Keep all readable content on a stable 2D layer. Only pseudo-elements carry
   the depth illusion so browser compositing cannot rasterize card typography. */
.ops-card, .capability-card { position:relative; isolation:isolate; transform:none !important; transform-style:flat !important; will-change:box-shadow,border-color !important; filter:none !important; }
.ops-card:hover, .capability-card:hover { transform:none !important; box-shadow:0 22px 38px rgba(0,0,0,.46),0 13px 29px rgba(50,217,255,.16),inset 0 1px 0 rgba(255,255,255,.08); }
.ops-card::before, .capability-card::before { content:""; position:absolute; z-index:-1; pointer-events:none; left:9px; right:9px; bottom:-12px; height:30px; border-radius:inherit; opacity:.22; transform:translateY(2px) scale(.96); transition:transform 200ms ease,opacity 200ms ease,filter 200ms ease; filter:blur(13px); background:currentColor; }
.ops-card:hover::before, .capability-card:hover::before { opacity:.55; transform:translateY(8px) scale(1.015); filter:blur(17px); }
.ops-card > *, .capability-card > * { position:relative; z-index:1; transform:none !important; filter:none !important; opacity:1; }
.ops-card-label, .capability-label { font-size:11px; letter-spacing:.08em; }.ops-card-detail, .capability-detail { font-size:12px; letter-spacing:0; }.ops-card-value { font-size:20px; }.capability-value { font-size:15px; }
.workflow-panel,.panel,.kpi-card,.trace { border-color:rgba(96,198,255,.36); background:linear-gradient(135deg,rgba(7,23,39,.86),rgba(4,14,25,.78)); }.workflow-row { min-height:57px; }.dispatch-attempt { background:linear-gradient(135deg,rgba(7,23,39,.9),rgba(4,14,25,.8)); }
/* Stable, crisp content layer for device/list/status panels. Pseudo-elements
   carry their rear glow, never the text-bearing shell. */
.panel,.kpi-card,.row,.badge,.small-note,.capability-card,.ops-card { transform:none !important; transform-style:flat !important; filter:none !important; opacity:1 !important; will-change:box-shadow,border-color !important; }
.panel:hover,.kpi-card:hover,.row:hover,.badge:hover { transform:none !important; filter:none !important; border-color:rgba(96,231,255,.56); box-shadow:0 14px 28px rgba(0,0,0,.32),0 8px 22px rgba(50,217,255,.12); }
.panel > *,.kpi-card > *,.row > *,.badge > *,.small-note > * { transform:none !important; filter:none !important; opacity:1 !important; }

/* The policy toggle is a glass control, not an alarm. Streamlit renders it as
   stCheckbox with an accessible checkbox role. */
[data-testid="stCheckbox"] [role="checkbox"] { background:rgba(50,62,74,.45) !important; border:1px solid rgba(120,145,165,.35) !important; box-shadow:inset 0 1px 0 rgba(255,255,255,.06) !important; }
[data-testid="stCheckbox"] [role="checkbox"] > * { background:#8192a0 !important; box-shadow:0 1px 3px rgba(0,0,0,.38) !important; }
[data-testid="stCheckbox"] [role="checkbox"][aria-checked="true"] { background:rgba(12,85,125,.45) !important; border-color:#32d9ff !important; box-shadow:inset 0 1px 0 rgba(218,249,255,.14),0 0 10px rgba(50,217,255,.25) !important; }
[data-testid="stCheckbox"] [role="checkbox"][aria-checked="true"] > * { background:#b9f3ff !important; box-shadow:0 0 8px rgba(96,231,255,.55) !important; }

/* Primary autonomous command: restrained navy/cyan, with a rear-only depth
   illusion so the label remains vector-crisp. */
div[data-testid="stButton"] button[kind="primary"] { position:relative; isolation:isolate; overflow:visible; transform:none !important; filter:none !important; color:#d7f5ff !important; border:1px solid rgba(50,217,255,.55) !important; background:linear-gradient(180deg,rgba(11,38,58,.82),rgba(5,20,32,.88)) !important; box-shadow:inset 0 1px 0 rgba(203,246,255,.08),0 0 12px rgba(50,217,255,.10) !important; text-shadow:none !important; }
div[data-testid="stButton"] button[kind="primary"]::before { content:""; position:absolute; z-index:-1; pointer-events:none; left:10px; right:10px; bottom:-10px; height:24px; border-radius:inherit; background:rgba(50,217,255,.48); opacity:.18; filter:blur(13px); transition:opacity 190ms ease,filter 190ms ease; }
div[data-testid="stButton"] button[kind="primary"]:hover { transform:none !important; filter:none !important; border-color:#60e7ff !important; background:linear-gradient(180deg,rgba(13,49,73,.88),rgba(5,22,35,.94)) !important; box-shadow:inset 0 1px 0 rgba(220,250,255,.14),0 0 18px rgba(50,217,255,.24),0 10px 28px rgba(0,0,0,.30) !important; }
div[data-testid="stButton"] button[kind="primary"]:hover::before { opacity:.52; filter:blur(16px); }
div[data-testid="stButton"] button[kind="primary"]:active { border-color:#a4f0ff !important; box-shadow:inset 0 0 14px rgba(50,217,255,.16),0 0 14px rgba(50,217,255,.24) !important; }

/* Cyan is the healthy audit state; red remains reserved for tampering and
   amber for legacy records that cannot be chain-verified. */
.audit-chain-card { border-color:#20c7f5; background:linear-gradient(100deg,rgba(5,36,52,.72),rgba(7,15,23,.90)); box-shadow:0 0 14px rgba(32,199,245,.18); }
.audit-chain-symbol { color:#60e7ff; }
.audit-chain-card.invalid { border-color:rgba(255,77,95,.62); background:linear-gradient(100deg,rgba(55,17,26,.65),rgba(7,15,23,.90)); box-shadow:none; }.audit-chain-card.invalid .audit-chain-symbol { color:#ff6e7c; }
.audit-chain-card.legacy { border-color:rgba(255,200,74,.58); background:linear-gradient(100deg,rgba(61,45,17,.58),rgba(7,15,23,.90)); box-shadow:none; }.audit-chain-card.legacy .audit-chain-symbol { color:#ffd263; }

/* Five-card operational row: all cards reserve identical content regions. */
.ops-card { height:180px; min-height:180px; box-sizing:border-box; display:flex; padding:17px !important; overflow:hidden; }
.ops-card-content { position:relative; z-index:2; display:flex; flex:1 1 auto; min-width:0; flex-direction:column; }
.ops-card-top { min-height:48px; align-items:flex-start; }
.ops-card-label { min-height:18px; line-height:18px; }
.ops-card-value { min-height:29px; margin-top:13px !important; line-height:29px; }
.ops-card-detail { position:relative; z-index:2; min-height:34px; max-height:34px; margin-top:auto !important; overflow:hidden; line-height:17px; }
.ops-card-motif { z-index:0; pointer-events:none; bottom:9px !important; opacity:.22 !important; }
.ops-card::before { z-index:0; pointer-events:none; }

/* Capability cards retain only a crisp semantic border halo; no under-card
   haze, white inner shadow, transform, or text-bearing blur. */
.capability-card { box-shadow:none !important; background:linear-gradient(135deg,rgba(7,23,39,.96),rgba(4,14,25,.94)) !important; }
.capability-card::before { content:none !important; display:none !important; }
.capability-card:hover { transform:none !important; filter:none !important; border-color:color-mix(in srgb,currentColor 76%,#60e7ff) !important; box-shadow:0 0 10px color-mix(in srgb,currentColor 18%,transparent) !important; }
.capability-card > * { position:relative; z-index:2; transform:none !important; filter:none !important; }

/* Streamlit toggle variants all inherit the same no-red command-center skin. */
[data-testid="stCheckbox"] [role="checkbox"],
[data-testid="stCheckbox"] [role="switch"],
[data-testid="stCheckbox"] button[aria-checked] { background:rgba(65,75,85,.45) !important; border:1px solid rgba(140,160,175,.30) !important; box-shadow:inset 0 1px 0 rgba(255,255,255,.05) !important; }
[data-testid="stCheckbox"] [role="checkbox"] > *,
[data-testid="stCheckbox"] [role="switch"] > *,
[data-testid="stCheckbox"] button[aria-checked] > * { background:#a8b6c2 !important; box-shadow:0 1px 3px rgba(0,0,0,.35) !important; }
[data-testid="stCheckbox"] [role="checkbox"][aria-checked="true"],
[data-testid="stCheckbox"] [role="switch"][aria-checked="true"],
[data-testid="stCheckbox"] button[aria-checked="true"] { background:rgba(10,75,110,.48) !important; border-color:rgba(50,217,255,.65) !important; box-shadow:0 0 10px rgba(50,217,255,.20),inset 0 1px 0 rgba(218,249,255,.12) !important; }
[data-testid="stCheckbox"] [role="checkbox"][aria-checked="true"] > *,
[data-testid="stCheckbox"] [role="switch"][aria-checked="true"] > *,
[data-testid="stCheckbox"] button[aria-checked="true"] > * { background:#b8f1ff !important; }
.footer { display:flex; justify-content:space-between; margin:38px -1.35rem -1.2rem; padding:24px 1.35rem; border-top:1px solid rgba(96,198,255,.35); color:#91b0cc; font-size:.54rem; letter-spacing:.18em; text-transform:uppercase; }
@media (max-width:1250px) { .command-header { grid-template-columns:1fr 1fr; }.command-states { grid-column:1/-1; }.command-message { padding-left:18px; }.ops-card { min-height:150px; }.command-name { font-size:2.65rem; } }
@media (max-width:850px) { .block-container { padding:.7rem !important; }.command-header { grid-template-columns:1fr; gap:13px; padding:14px 3px 18px; }.command-brand { border-right:0; padding-right:0; }.command-message { padding-left:0; }.command-states { grid-template-columns:repeat(2,1fr); }.command-state { border-bottom:1px solid rgba(131,199,242,.19); }.command-name { font-size:2.35rem; letter-spacing:.17em; } div[role="radiogroup"] { grid-template-columns:repeat(2,1fr); } div[role="radiogroup"] label { min-height:42px; font-size:.57rem !important; } .ops-card { min-height:134px; }.footer { margin-left:-.7rem; margin-right:-.7rem; padding-left:.7rem; padding-right:.7rem; gap:16px; flex-wrap:wrap; }.haris-globe { width:110vw; height:110vw; right:-43vw; top:-5vh; opacity:.36; } }
</style>
""",
    unsafe_allow_html=True,
)


# ============================================================================
# Backend helpers
# ============================================================================

class BackendAuthenticationConfigurationError(RuntimeError):
    """Safe operator-facing error for a missing Streamlit backend credential."""


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


async def backend_request(method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Use Render as authority when the console is deployed separately."""
    if not settings.haris_backend_url:
        return None
    if not settings.haris_backend_api_token:
        raise BackendAuthenticationConfigurationError(
            "HARIS backend authentication is not configured."
        )
    import httpx
    base = settings.haris_backend_url.rstrip("/")
    headers = {
        "Authorization": f"Bearer {settings.haris_backend_api_token.get_secret_value()}"
    }
    async with httpx.AsyncClient(timeout=10.0) as http:
        response = await http.request(method, f"{base}{path}", json=payload, headers=headers)
        response.raise_for_status()
        return response.json()


def unavailable_backend_status(cached: Optional[Dict[str, Any]], reason: str) -> Dict[str, Any]:
    """Mark cached backend authority stale without substituting local truth."""
    result = dict(cached or {})
    result.update({
        "backend_connection_state": "UNAVAILABLE",
        "backend_status_stale": True,
        "haris_state": "NOT_READY",
        "backend_error": reason,
    })
    return result


def authoritative_supervisory_status() -> Optional[Dict[str, Any]]:
    """Read Render's safe workflow view when Render owns the active incident."""
    if not settings.haris_backend_url:
        return None
    try:
        payload = run_async(backend_request("GET", "/api/nac/autonomous/status"))
        if payload:
            st.session_state.backend_supervisory_status = payload
            return payload
    except BackendAuthenticationConfigurationError as exc:
        st.session_state.backend_configuration_error = str(exc)
        return unavailable_backend_status(
            st.session_state.get("backend_supervisory_status"),
            "BACKEND_AUTH_CONFIGURATION_MISSING",
        )
    except Exception:
        # Never substitute Streamlit-local security state if Render is down.
        return unavailable_backend_status(
            st.session_state.get("backend_supervisory_status"),
            "BACKEND_UNAVAILABLE",
        )
    return st.session_state.get("backend_supervisory_status")


def sync_backend_consent_binding(dispatch: Dict[str, Any]) -> None:
    """Discard consent material when Render advances to another engineer."""
    pending_id, engineer_id = dispatch.get("pending_id"), dispatch.get("engineer_id")
    for prefix in ("backend_consent_action", "backend_authorization_url"):
        bound_pending = st.session_state.get(f"{prefix}_pending_id")
        bound_engineer = st.session_state.get(f"{prefix}_engineer_id")
        if bound_pending and (bound_pending != pending_id or bound_engineer != engineer_id):
            st.session_state.pop(prefix if prefix == "backend_authorization_url" else "backend_consent_action_token", None)
            st.session_state.pop(f"{prefix}_pending_id", None)
            st.session_state.pop(f"{prefix}_engineer_id", None)


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
    # Never hydrate an operational view from fixture files. A fixture value is
    # visible only when it belongs to the current authoritative fixture cycle.
    rows = safe_mapping(result).get("congestion") or []

    output: Dict[str, Dict[str, float]] = {}

    for row in rows:
        cell_id = row.get("cell_id")
        if not cell_id:
            continue

        output[cell_id] = {
            "congestion_level": row.get("congestion_level"),
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
                "congestion_level": values.get("congestion_level"),
                "congestion_pct": optional_float(values.get("congestion_pct")),
                "latency_ms": optional_float(values.get("latency_ms")),
                "predicted_congestion_pct": optional_float(values.get("predicted_congestion_pct")),
            }
            for cell, values in raw.items()
        }

    if isinstance(raw, list):
        return {
            row["cell_id"]: {
                "congestion_level": row.get("congestion_level"),
                "congestion_pct": optional_float(row.get("congestion_pct")),
                "latency_ms": optional_float(row.get("latency_ms")),
                "predicted_congestion_pct": optional_float(row.get("predicted_congestion_pct")),
            }
            for row in raw
            if row.get("cell_id")
        }

    return {}


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

def render_header(result: Optional[Dict[str, Any]], supervisory: Optional[Dict[str, Any]] = None) -> None:
    final_status = authoritative_haris_state(result, supervisory).lower()

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
    elif final_status == "waiting_for_identity_verification":
        text = "AWAITING CONSENT"
        css = "review"
        detail = "Privileged field intervention remains fail-closed pending Nokia consent"
    elif result:
        text = "REVIEW"
        css = "review"
        detail = "Cycle completed · verification requires review"
    else:
        text = "READY"
        css = "ready"
        detail = "Autonomous resilience engine standing by"

    render_html(
        f"""
        <div class="overview-banner">
            <div>
                <div class="command-kicker">CURRENT STATE · SECURE OPERATIONS · NETWORK RESILIENCE</div>
                <div class="overview-banner-title">Operational overview</div>
            </div>
            <div class="overview-banner-detail">{safe_text(detail)} Current cycle state: <b class="{css}">{safe_text(text)}</b></div>
        </div>
        """
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
    report = safe_mapping(safe_mapping(result).get("warden")).get("capability_report")
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
        "SUPPORTED_AND_CONFIGURED": "CONFIGURED",
        "SDK_SUPPORTED_CONFIG_MISSING": "CONFIG MISSING",
        "OPERATOR_VALUE_REQUIRED": "OPERATOR RESOURCE REQUIRED",
        "SDK_UNSUPPORTED": "UNAVAILABLE",
        "PRIVILEGED_ONLY": "PRIVILEGED ONLY",
    }
    render_section_header("NETWORK CAPABILITY MATRIX", "Network capabilities · Nokia NaC · service readiness")
    columns = st.columns(3)
    icons = {
        "congestion_insights": "congestion", "device_status": "device", "location": "location",
        "geofencing": "geofence", "qod": "qod", "slicing": "slicing", "trusted_dispatch": "dispatch",
    }
    for index, (key, label) in enumerate(labels):
        item = report.get(key, {"status": "PRIVILEGED_ONLY", "reason": "Number Verification + SIM Swap; privileged field intervention only."})
        status, detail = capability_presentation(key, item, display)
        with columns[index % 3]:
            render_html(capability_card(label, status, detail, icons[key]))


def capability_presentation(
    key: str, item: Dict[str, Any], display: Optional[Dict[str, str]] = None,
) -> tuple[str, str]:
    """Keep configuration readiness distinct from validation truth."""
    display = display or {
        "READ_READY": "READ READY",
        "SUPPORTED_AND_CONFIGURED": "CONFIGURED",
        "SDK_SUPPORTED_CONFIG_MISSING": "CONFIG MISSING",
        "OPERATOR_VALUE_REQUIRED": "OPERATOR RESOURCE REQUIRED",
        "SDK_UNSUPPORTED": "UNAVAILABLE",
        "PRIVILEGED_ONLY": "PRIVILEGED ONLY",
    }
    configured = display.get(
        item.get("status"), str(item.get("status", "UNKNOWN")).replace("_", " "),
    )
    fallback_truth = {
        "congestion_insights": "READ_VALIDATED / POLLING_APPROPRIATE",
        "device_status": "READ_VALIDATED / POLLING_APPROPRIATE",
        "location": "READ_VALIDATED",
        "geofencing": "FAIL_CLOSED_AUTH_UNPROVEN",
        "qod": "REAL_PARTIAL",
        "slicing": "SANDBOX_LIMITED",
        "trusted_dispatch": "REAL_VALIDATED / PRIVILEGED_ONLY",
    }
    truth = item.get("truth_status") or fallback_truth.get(key)
    provenance = item.get("provenance") or "UNAVAILABLE"
    fallback_reason = {
        "congestion_insights": "Live categorical capability; numeric fixture KPI is simulated.",
        "device_status": "Live reachability capability; battery, tier, roaming, and cell fields are HARIS/fixture metadata.",
        "location": "Location Retrieval is implemented.",
        "geofencing": "Authenticated callback provenance is not proven; events remain fail-closed.",
        "qod": "Real provider lifecycle was accepted; network verification was UNCHANGED and cleanup was verified.",
        "slicing": "AVAILABLE was observed, not OPERATING; no live Tier-1 attachment is proven.",
        "trusted_dispatch": "Number Verification + SIM Swap are used only for privileged field intervention.",
    }
    reason = item.get("reason") or fallback_reason.get(key) or "Capability configuration is available."
    if key in {"geofencing", "qod", "slicing"} and truth:
        return str(truth), f"Configuration: {configured} · {provenance} · {reason}"
    detail = f"{truth} · {provenance} · {reason}" if truth else reason
    return configured, detail


def render_environment(result: Optional[Dict[str, Any]]) -> None:
    source = safe_mapping(result).get("environmental_source") or "UNAVAILABLE"
    st.caption(f"ENVIRONMENT SOURCE: {source}")
    st.markdown(
        '<div class="section-title"><span class="section-mark warning">△</span><span>ENVIRONMENTAL THREAT STATE</span></div>',
        unsafe_allow_html=True,
    )


def render_prediction(result: Optional[Dict[str, Any]]) -> None:
    prediction = safe_mapping(result).get("prediction")
    if not prediction:
        return
    prediction = safe_mapping(prediction)
    st.markdown('<div class="section-title"><span class="section-mark">●</span><span>SHORT-HORIZON RISK FORECAST</span></div>', unsafe_allow_html=True)
    a, b, c, d = st.columns(4)
    with a: render_html(operational_card("Predicted Risk", safe_upper(prediction.get("predicted_risk_level"), "N/A")))
    with b: render_html(operational_card("Forecast Horizon", f"{prediction.get('horizon_minutes', 'N/A')} min", "Forecast window"))
    with c: render_html(operational_card("Confidence", authoritative_metric(prediction.get("confidence"), kind="confidence"), "HARIS MODEL CONFIDENCE"))
    with d: render_html(operational_card("Degradation Probability", authoritative_metric(prediction.get("degradation_probability"), kind="confidence"), "HARIS-DERIVED forecast probability"))
    st.caption("HARIS DERIVED · " + str(prediction.get("model_type") or "categorical model") + " · inputs=" + str(prediction.get("input_window") or "N/A") + " · " + "; ".join(prediction.get("contributing_factors", [])))

    incident = (result or {}).get("incident", {})
    affected_cells = incident.get("affected_cells", [])

    cells_text = ", ".join(affected_cells) if affected_cells else "N/A"

    cards = [
        (
            "Dust Advisory",
            "ACTIVE" if safe_mapping(result).get("dust_advisory") is True else "CLEAR" if safe_mapping(result).get("dust_advisory") is False else "UNAVAILABLE",
            f"{safe_upper(safe_mapping(result).get('environmental_source'), 'UNAVAILABLE')} evidence source",
            "#ff4d5f",
        ),
        (
            "Temperature",
            "UNAVAILABLE",
            "No authoritative temperature feed configured",
            "#ffc857",
        ),
        (
            "Visibility",
            "UNAVAILABLE",
            "No authoritative visibility feed configured",
            "#ffc857",
        ),
        (
            "Affected Cells",
            cells_text,
            "Current authoritative incident evidence",
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
    data: Dict[str, Dict[str, Any]],
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

        levels = [data.get(source, {}).get("nokia_congestion") or data.get(source, {}).get("congestion_level"), data.get(target, {}).get("nokia_congestion") or data.get(target, {}).get("congestion_level")]
        level = max((item for item in levels if item in {"None", "Low", "Medium", "High"}), key=lambda item: {"None": 0, "Low": 1, "Medium": 2, "High": 3}[item], default=None)
        color = {"High": "#ff4d5f", "Medium": "#ffc857", "Low": "#31d7ff", "None": "#42f59b"}.get(level, "#7890aa")

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

    node_parts: List[str] = []

    for name, (x, y) in positions.items():
        if name == "CORE":
            status = "CORE"
            stroke = "#31d7ff"
            fill = "#10273a"
            sub = "NETWORK CORE"
        else:
            entity = data.get(name, {})
            level = entity.get("nokia_congestion") or entity.get("congestion_level")
            status = entity.get("haris_state") or ("INCIDENT_OPEN" if level == "High" else "WATCHING" if level == "Medium" else "STABLE" if level in {"Low", "None"} else "STALE")
            key = {"High": "red", "Medium": "yellow", "Low": "green", "None": "green"}.get(level, "gray")

            colors = {
                "red": ("#ff4d5f", "#35131a"),
                "yellow": ("#ffc857", "#332710"),
                "green": ("#42f59b", "#0d3025"),
                "gray": ("#7890aa", "#202d3b"),
            }

            stroke, fill = colors[key]
            source_label = entity.get("source", "UNAVAILABLE").replace("_", " ")
            sub = f"NOKIA: {str(level or 'UNAVAILABLE').upper()} / HARIS: {str(status).upper()} / {source_label}"

        if name != "CORE" and status not in {"STABLE", "CORE"}:
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
            NETWORK FABRIC · CURRENT EVIDENCE
        </div>

        <div style="
            color:#607994;
            font-size:.60rem;
            margin:3px 0 5px;
        ">
            Logical HARIS cell mapping · Nokia categorical source state + HARIS operational state
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

def authoritative_network_entities(result: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Prefer backend registry; local fallback remains explicitly cycle-bound."""
    if settings.haris_backend_url:
        try:
            payload = run_async(backend_request("GET", "/api/nac/network-state")) or {}
            entities = safe_mapping(payload).get("entities")
            if isinstance(entities, dict):
                return entities
        except Exception:
            return {}
    rows = congestion_map(result)
    source = "FIXTURE_SIMULATED" if settings.nac_mode == "fixture" else "UNAVAILABLE"
    return {cell: {"entity_id": cell, "nokia_congestion": values.get("congestion_level"), "haris_state": "INCIDENT_OPEN" if values.get("congestion_level") == "High" else "WATCHING" if values.get("congestion_level") == "Medium" else "STABLE", "source": source, "source_type": "HARIS_CONFIGURED_LOGICAL_CELL"} for cell, values in rows.items()}

def render_network_section(
    result: Optional[Dict[str, Any]],
) -> None:
    data = authoritative_network_entities(result)

    left, right = st.columns([.82, 2.18])

    with left:
        st.markdown(
            '<div class="section-title"><span class="section-mark warning">△</span><span>NETWORK ALERTS</span></div>',
            unsafe_allow_html=True,
        )

        rows = ""

        for cell_id, values in sorted(data.items()):
            level = values.get("nokia_congestion") or values.get("congestion_level")

            if level in {"High", "Medium"}:
                status_class = "badge-red" if level == "High" else "badge-yellow"
                status = values.get("haris_state") or ("INCIDENT_OPEN" if level == "High" else "WATCHING")
                evidence = f"NOKIA {str(level).upper()}"

                rows += f"""
                <div class="row">
                    <span><b>{cell_id}</b> · congestion</span>
                    <span class="badge {status_class}">
                    {evidence} · HARIS {status}
                    </span>
                </div>
                """

        if not rows:
            rows = """
            <div class="row">
                <span>Network alerts</span>
                <span class="badge badge-gray">WAITING FOR EVIDENCE</span>
            </div>
            """

        render_html(
            f'<div class="panel">{textwrap.dedent(rows).strip()}</div>'
        )

        st.markdown('<div style="height:10px"></div>', unsafe_allow_html=True)

        st.markdown(
            '<div class="section-title"><span class="section-mark">●</span><span>CRITICAL DEVICES</span></div>',
            unsafe_allow_html=True,
        )

        devices = safe_mapping(result).get("devices") or []
        rows = ""

        for device in devices:
            tier = device.get("tier")
            battery = optional_float(device.get("battery_pct"))

            # Battery is HARIS fixture/policy metadata, not a Nokia live
            # Device Status measurement.  Do not present it as live telemetry.
            if tier == 1 or (settings.nac_mode == "fixture" and battery < 25):
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
                <span>Critical-device evidence</span>
                <span class="badge badge-gray">UNAVAILABLE</span>
            </div>
            """

        render_html(
            f'<div class="panel">{textwrap.dedent(rows).strip()}</div>'
        )

    with right:
        st.markdown(
            '<div class="section-title"><span class="section-mark">●</span><span>NETWORK TOPOLOGY</span></div>',
            unsafe_allow_html=True,
        )

        if data:
            render_html(topology_svg(data=data, active=False))
        else:
            render_html('<div class="panel empty-state"><strong>NETWORK TOPOLOGY UNAVAILABLE</strong><br>Waiting for current authoritative congestion evidence.</div>')


# ============================================================================
# KPI impact
# ============================================================================

def render_impact(
    result: Optional[Dict[str, Any]],
) -> None:
    st.markdown(
        '<div class="section-title"><span class="section-mark">●</span><span>MITIGATION IMPACT</span></div>',
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

    final_status = str(result.get("final_status") or "").lower()
    if final_status not in {"mitigated", "rolled_back_safely", "rollback_failed", "verification_failed", "degraded"}:
        st.info("Impact is not evaluated until an authoritative cycle reaches verification.")
        return

    baseline = baseline_map(result)
    current = congestion_map(result)

    verification = safe_mapping(result.get("verification"))
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

    before_level = before.get("congestion_level")
    after_level = after.get("congestion_level")
    if before_level and after_level:
        improved = verification.get("level_improved")
        outcome = "IMPROVED" if improved is True else "DEGRADED" if verification.get("level_degraded") is True else "UNCHANGED"
        render_html(
            f'<div class="panel"><div class="panel-title">CONGESTION · {safe_text(target)}</div>'
            f'<div style="font-size:1.15rem;font-weight:800;color:#eaf8ff">{safe_text(before_level)} → {safe_text(after_level)}</div>'
            f'<div style="margin-top:5px;color:#70efb6;font-size:.7rem;font-weight:800">{outcome}</div></div>'
        )

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

    available_kpis = [item for item in kpis if math.isfinite(item[1]) and math.isfinite(item[2])]
    if available_kpis:
        cols = st.columns(len(available_kpis))

    for col, (name, before_value, after_value, suffix) in zip(cols if available_kpis else [], available_kpis):
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

    plan = safe_mapping(result.get("plan"))
    execution = safe_mapping(result.get("execution"))
    verification = safe_mapping(result.get("verification"))

    a, b, c, d, e = st.columns(5)

    a.metric("Confidence", authoritative_metric(plan.get("confidence"), kind="confidence"))
    b.metric("Blast Radius", authoritative_metric(plan.get("blast_radius"), kind="blast_radius"))
    c.metric("QoD Cost", authoritative_metric(plan.get("expected_cost_usd"), kind="qod_cost"))
    d.metric("Actions", authoritative_metric(plan.get("actions"), kind="actions"))
    e.metric("Verification", authoritative_verification_label(verification))

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
        '<div class="section-title"><span class="section-mark">●</span><span>AI DECISION ENGINE</span></div>',
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

    plan = safe_mapping(result.get("plan"))
    execution = safe_mapping(result.get("execution"))
    verification = safe_mapping(result.get("verification"))
    learning = safe_mapping(result.get("learning"))
    warden = safe_mapping(result.get("warden"))
    rollback = safe_mapping(result.get("rollback"))
    read_only = execution.get("reason") == "live_read_only"
    identity_pending = (
        verification.get("status") == "identity_verification_pending"
        or result.get("final_status") == "waiting_for_identity_verification"
    )
    actuator_detail = (
        "Paused — awaiting authorization" if identity_pending
        else "Not executed — live read-only mode"
        if read_only
        else "Network actions executed" if execution.get("executed")
        else f"Not executed — {execution.get('reason', 'no action')}"
    )
    verify_detail = (
        "Deferred — no network mutation" if identity_pending
        else "Not applicable — no mutation was executed"
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
            f"{len(plan.get('actions') or [])} bounded actions",
            True,
        ),
        (
            "04",
            "WARDEN",
            "Identity/trust authorization pending" if identity_pending
            else "Approved" if warden.get("verified") else "Capability or policy blocked execution",
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
            "Workflow checkpoint stored" if identity_pending else "Incident stored",
            bool(learning.get("incident_saved")) or identity_pending,
        ),
    ]

    left, right = st.columns([2.2, 1])

    with left:
        html = '<div class="panel workflow-panel">'

        for number, stage, detail, success in stages:
            skipped = (identity_pending and stage in {"WARDEN", "ACTUATOR", "VERIFY"}) or (read_only and stage in {"ACTUATOR", "VERIFY"}) or (
                stage == "ROLLBACK" and detail == "Not required"
            )
            tone = "success" if success else "warning" if skipped else "danger"
            icon = "&#10003;" if success else "&#9651;" if skipped else "&#215;"

            html += f"""
            <div class="workflow-row {tone}">
                <span class="workflow-number">{number}</span>
                <span class="workflow-stage"><span class="workflow-state">{icon}</span>{safe_text(stage)}</span>
                <span class="workflow-detail">{safe_text(detail)}</span>
            </div>
            """

        html += "</div>"

        render_html(html)

    with right:
        status = safe_upper(result.get("final_status"), "REVIEW")

        status_color = {
            "success": "#42f59b", "warning": "#ffc857", "danger": "#ff6170", "neutral": "#63d9ff",
        }[semantic_tone(status)]

        protected = protected_tier1_count(result)

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

                <div class="panel-title">OUTCOME</div>

                <div style="
                    color:{status_color};
                    font-weight:800;
                    font-size:.82rem;
                ">
                    {"VERIFIED" if verification.get("verified") is True else "REVIEW" if "verified" in verification else "N/A"}
                </div>
            </div>
            """
        )

    st.markdown("<div style='height:9px'></div>", unsafe_allow_html=True)

    st.markdown(
        '<div class="panel-title">AUTONOMOUS DECISION TRACE</div>',
        unsafe_allow_html=True,
    )

    render_trace(result.get("trace") or [])
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
    render_section_header("AUTONOMOUS OPERATIONS", "Sense · predict · reason · plan · WARDEN · act · verify · learn")

    a, b, d = st.columns([1.35, 1.35, 2.0])

    with a:
        if st.button(
            "RUN AUTONOMOUS HARIS",
            use_container_width=True,
            type="primary",
        ):
            with st.spinner(
                "HARIS is executing the closed control loop…"
            ):
                start = time.perf_counter()

                try:
                    # Deployed console: Render owns autonomous execution and
                    # durable audit history.  Standalone console: retain the
                    # local fixture-only fallback for development/demo use.
                    if settings.haris_backend_url:
                        payload = run_async(
                            backend_request("POST", "/api/nac/autonomous/run")
                        )
                        if not payload or not isinstance(payload.get("cycle"), dict):
                            raise RuntimeError("Authoritative HARIS backend did not return a cycle.")
                        result = payload["cycle"]
                    else:
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
                    logger.warning("HARIS cycle failed; details suppressed")
                    st.error("HARIS cycle failed. Review the authenticated backend status.")

    with b:
        if settings.nac_mode == "fixture" and st.button(
            "RUN FIELD INTERVENTION DEMO",
            use_container_width=True,
            help="SIMULATED FIXTURE evidence: a tower power condition requires privileged physical intervention.",
        ):
            with st.spinner("HARIS is running the simulated physical-intervention workflow…"):
                start = time.perf_counter()
                try:
                    if settings.haris_backend_url:
                        payload = run_async(backend_request("POST", "/api/nac/autonomous/field-intervention-demo"))
                        if not payload:
                            raise RuntimeError("Authoritative HARIS backend did not return a demo status.")
                        st.session_state.last_result = payload.get("cycle", {})
                        st.session_state.backend_consent_action_token = payload.get("consent_action_token")
                        st.session_state.backend_workflow_session_token = payload.get("workflow_session_token")
                        dispatch = st.session_state.last_result.get("trusted_dispatch", {})
                        st.session_state.backend_consent_action_pending_id = dispatch.get("pending_id")
                        st.session_state.backend_consent_action_engineer_id = dispatch.get("engineer_id")
                        st.session_state.pop("backend_authorization_url", None)
                    else:
                        # Local standalone fixture fallback only. A deployed
                        # console must configure HARIS_BACKEND_URL so Render
                        # owns pending dispatch/OAuth state.
                        st.session_state.last_result = run_async(get_system().run_field_intervention_demo())
                    st.session_state.last_elapsed = time.perf_counter() - start
                    st.rerun()
                except Exception:
                    logger.warning("HARIS field intervention demo failed; details suppressed")
                    st.error("Field intervention demo failed. Review the authenticated backend status.")

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


def history_storage_status(memory: Any) -> Dict[str, Any]:
    """Normalize the explicit memory-store storage contract for the UI.

    Legacy/test stores predate persistence metadata; they are truthfully treated
    as available process-local memory instead of crashing History & Audit.
    """
    fallback = {"backend": "memory", "durable": False, "available": True, "status": "PROCESS_LOCAL"}
    status = getattr(memory, "persistence_status", fallback)
    status = status() if callable(status) else status
    return status if isinstance(status, dict) else fallback


def durable_history_available(status: Any) -> bool:
    """Recognize only an authoritative, ready durable repository."""
    status = safe_mapping(status)
    if status.get("durable") is True and status.get("available", True):
        return True
    return (
        str(status.get("mode") or "").lower() == "postgres"
        and status.get("repository_ready") is True
        and str(status.get("status") or "").upper() == "READY"
        and str(status.get("reconstruction") or "").upper() == "COMPLETE"
    )


def history_storage_caption(status: Any) -> str:
    """Describe operational history separately from audit-chain availability."""
    if durable_history_available(status):
        return (
            "Operational history storage: DURABLE_REPOSITORY (PostgreSQL/Supabase). "
            "This is separate from the tamper-evident, append-only audit-chain status shown above."
        )
    if not safe_mapping(status).get("available", True):
        return "Operational history storage: unavailable."
    return "Operational history storage: process-local memory; restart persistence is unavailable."


def audit_chain_presentation(chain: Any) -> tuple[str, str, str]:
    """Present separate audit-chain truth without inferring availability."""
    chain = safe_mapping(chain)
    available = bool(chain)
    valid = bool(chain.get("valid"))
    legacy = not valid and str(chain.get("reason") or "").startswith("legacy_")
    state = "VALID" if valid else ("LEGACY" if legacy else ("INVALID" if available else "UNAVAILABLE"))
    css = "" if valid else (" legacy" if legacy else (" invalid" if available else ""))
    symbol = "&#10003;" if valid else ("&#9888;" if legacy else "&#215;")
    return state, css, symbol


def render_history(supervisory: Optional[Dict[str, Any]] = None) -> None:
    render_section_header("HISTORY & AUDIT", "Incident replay · trusted dispatch history · tamper-evident evidence")
    backend_audit = (supervisory or {}).get("audit") if settings.haris_backend_url else None
    if backend_audit is not None:
        records = backend_audit.get("records", [])
        chain = backend_audit.get("chain", {})
        persistence = (
            backend_audit.get("persistence")
            or chain.get("persistence")
            or safe_mapping(supervisory).get("persistence")
            or {}
        )
    else:
        memory = get_system().memory
        records = memory.recent_incidents()
        chain = memory.verify_audit_chain()
        persistence = history_storage_status(memory)
    audit_state, audit_class, audit_symbol = audit_chain_presentation(chain)
    render_html(
        f'<div class="audit-chain-card{audit_class}">'
        f'<div class="audit-chain-symbol">{audit_symbol}</div>'
        f'<div><div class="audit-chain-title">TAMPER-EVIDENT AUDIT CHAIN</div>'
        f'<div class="audit-chain-value">AUDIT CHAIN: {audit_state}</div></div></div>'
    )
    if not persistence.get("available", True):
        st.warning("Durable audit persistence is unavailable; this cycle remains safety-controlled but was not confirmed as durably saved.")
    st.caption(history_storage_caption(persistence))
    durable_history = (supervisory or {}).get("incident_history") or []
    timeline = (supervisory or {}).get("timeline") or []
    if timeline:
        st.markdown(
            '<div class="section-title"><span class="section-mark">●</span><span>DURABLE OPERATOR TIMELINE</span></div>',
            unsafe_allow_html=True,
        )
        st.dataframe(timeline, use_container_width=True, hide_index=True)
        st.caption("Timeline authority: DERIVED FROM DURABLE incident, action, verification, and recovery records.")
    if not records and durable_history:
        records = durable_history
    if not records:
        st.caption("No durable incident or append-only audit history is available yet.")
        return
    def value(item: Any, name: str, default: Any = "N/A") -> Any:
        return item.get(name, default) if isinstance(item, dict) else getattr(item, name, default)
    if durable_history and records is durable_history:
        labels = [
            f"{safe_mapping(item.get('incident')).get('opened_at', 'N/A')} · "
            f"{safe_mapping(item.get('incident')).get('incident_id', 'N/A')} · "
            f"{item.get('final_truth', 'UNAVAILABLE')}"
            for item in records
        ]
    else:
        labels = [f"{value(item, 'created_at')} · {value(item, 'cycle_id') or value(item, 'incident_id')} · {value(item, 'outcome')}" for item in records]
    selected = records[labels.index(st.selectbox("Replay an append-only audit record", labels))]
    if durable_history and records is durable_history:
        selected_incident = safe_mapping(selected.get("incident"))
        st.caption(
            f"Source: DURABLE REPOSITORY · State: {safe_text(selected_incident.get('state'), 'N/A')} · "
            f"Outcome: {safe_text(selected.get('final_truth'), 'UNAVAILABLE')}"
        )
    else:
        st.caption(f"Mode: {value(selected, 'mode')} · Cells: {', '.join(value(selected, 'affected_cells', [])) or 'N/A'} · Outcome: {value(selected, 'outcome')}")
    st.json(selected if isinstance(selected, dict) else get_system().memory.normalized_view(selected))


def render_playbook_and_feed(result: Optional[Dict[str, Any]]) -> None:
    st.markdown('<div class="section-title"><span class="section-mark">●</span><span>ACTIVE PLAYBOOK</span></div>', unsafe_allow_html=True)
    cycle = safe_mapping(result)
    playbook = safe_mapping(cycle.get("active_playbook"))
    st.json(playbook or {"name": "N/A", "state": "IDLE", "latest_outcome": "N/A"})
    st.markdown('<div class="section-title"><span class="section-mark">●</span><span>INCIDENT FEED</span></div>', unsafe_allow_html=True)
    events = cycle.get("events") or []
    if events: st.dataframe(events, use_container_width=True, hide_index=True)
    else: st.caption("No incident events yet.")
    dispatch = safe_mapping(cycle.get("trusted_dispatch"))
    if dispatch:
        st.markdown('<div class="section-title"><span class="section-mark">●</span><span>FIELD INTERVENTION / TRUSTED DISPATCH</span></div>', unsafe_allow_html=True)
        st.json(dispatch)


# ============================================================================
# Console sections
# ============================================================================

def render_status_bar(result: Optional[Dict[str, Any]], supervisory: Optional[Dict[str, Any]] = None) -> None:
    """Persistent supervision state, intentionally not an API control surface."""
    cycle = safe_mapping(result)
    supervisory = safe_mapping(supervisory)
    incident = safe_mapping(supervisory.get("active_incident") or cycle.get("incident"))
    warden = safe_mapping(cycle.get("warden"))
    dispatch = safe_mapping(cycle.get("trusted_dispatch"))
    haris_state = authoritative_haris_state(cycle, supervisory)
    warden_state = (
        "PENDING" if dispatch.get("status") == "WAITING_FOR_IDENTITY_VERIFICATION"
        else "BLOCKED" if dispatch.get("decision") == "BLOCK"
        else "APPROVED" if warden.get("verified") else "REVIEW"
    )
    states = [
        ("SYSTEM STATUS", haris_state),
        ("NOKIA NaC", safe_upper(get_system().client.name, "UNAVAILABLE")),
        ("MODE", presentation_mode_label()),
        ("WARDEN", warden_state),
        ("ACTIVE INCIDENT", incident.get("incident_id") or "NONE"),
    ]
    state_html = "".join(
        f'<div class="command-state {semantic_tone(value)}"><div class="command-state-label">{safe_text(label)}</div><div class="command-state-value">{safe_text(value)}</div></div>'
        for label, value in states
    )
    render_html(
        f"""
        <div class="command-header">
            <div class="command-brand"><div>{render_haris_wordmark()}<div class="command-subtitle">Hybrid Agent for Resilient Infrastructure<br>and Service-continuity</div></div></div>
            <div class="command-message">AI-powered network resilience<br>for a more connected tomorrow</div>
            <div class="command-states">{state_html}</div>
        </div>
        """
    )


def render_overview(result: Optional[Dict[str, Any]], supervisory: Optional[Dict[str, Any]] = None) -> None:
    render_header(result, supervisory)
    cycle = safe_mapping(result)
    incident = safe_mapping(safe_mapping(supervisory).get("active_incident") or cycle.get("incident"))
    prediction, warden = safe_mapping(cycle.get("prediction")), safe_mapping(cycle.get("warden"))
    cols = st.columns(5)
    backend_value = "CONNECTED" if settings.haris_backend_url and supervisory else "UNAVAILABLE" if settings.haris_backend_url else "LOCAL"
    dispatch = safe_mapping(cycle.get("trusted_dispatch"))
    warden_value = "PENDING" if dispatch.get("status") == "WAITING_FOR_IDENTITY_VERIFICATION" else "APPROVED" if warden.get("verified") else "REVIEW"
    cards = [
        ("Backend Health", backend_value, "Authoritative backend supervision" if settings.haris_backend_url else "Local fixture console", "server"),
        ("Nokia Integration", safe_upper(get_system().client.name, "UNAVAILABLE"), presentation_mode_label(), "nokia"),
        ("WARDEN", warden_value, dispatch.get("reason") or "Safety authority state", "shield"),
        ("Active Incident", incident.get("incident_id") or "NONE", ", ".join(incident.get("affected_cells") or []) or "No active incident", "radar"),
        ("Predicted Risk", safe_upper(prediction.get("predicted_risk_level"), "MONITORING"), "Authoritative forecast state" if prediction else "No active forecast", "risk"),
    ]
    for column, (label, value, detail, icon) in zip(cols, cards):
        with column:
            render_html(operational_card(label, value, detail, icon=icon))
    render_section_header("OPERATIONAL NOTIFICATIONS")
    notices = overview_notifications(cycle, supervisory)
    left, right = st.columns([1, 1])
    with left:
        notification_html = "".join(
            f'<div class="notice {tone}"><b>{safe_text(title)}</b><br>{safe_text(message)}</div>'
            for tone, title, message in notices
        ) or '<div class="notice"><b>OPERATIONAL NOTIFICATIONS</b><br>No authoritative security or incident notification is active.</div>'
        render_html(notification_html)
    with right:
        current_state = authoritative_haris_state(cycle, supervisory)
        cycle_detail = safe_mapping(cycle).get("explanation") or (
            "Privileged authorization is pending." if current_state == "WAITING_FOR_IDENTITY_VERIFICATION"
            else "No completed cycle is available." if not cycle else "Current HARIS cycle state is authoritative."
        )
        render_html(
            f'<div class="cycle-panel">{render_svg_icon("cycle", "cycle-icon")}<div><div class="cycle-panel-label">CURRENT CYCLE STATE</div>'
            f'<div class="cycle-panel-value">{safe_text(cycle_detail)}<br>Current cycle state: <b>{safe_text(current_state)}</b></div></div></div>'
        )
    render_capability_matrix(result)


def render_network_intelligence(result: Optional[Dict[str, Any]]) -> None:
    render_section_header("NETWORK INTELLIGENCE", "Topology · environment · geofence policy · critical assets")
    if settings.haris_backend_url:
        render_observation_monitor()
    enabled = st.toggle("Geofencing Monitoring", value=get_system().geofencing_monitoring_enabled)
    get_system().set_geofencing_monitoring(enabled)
    st.caption("HARIS creates and cleans up geofence subscriptions only when policy and a playbook require it.")
    render_environment(result)
    render_prediction(result)
    render_network_section(result)
    geofence_events = [event for event in (safe_mapping(result).get("events") or []) if "GEOFENCE" in safe_upper(safe_mapping(event).get("message"), "")]
    st.markdown('### GEOFENCE EVENTS')
    if geofence_events: st.dataframe(geofence_events, use_container_width=True, hide_index=True)
    else: st.caption("No Nokia geofence enter/exit event received.")


@st.fragment(run_every=3)
def render_observation_monitor() -> None:
    """Refresh only backend-owned, read-only Nokia evidence for this section."""
    try:
        payload = run_async(backend_request("GET", "/api/nac/observations/latest")) or {}
        status = safe_mapping(payload.get("status"))
        observation = safe_mapping(payload.get("observation"))
        state = safe_upper(status.get("connection_status"), "DISCONNECTED")
        heading = "FIXTURE / SIMULATED MONITORING" if status.get("mode") == "fixture" else "NOKIA LIVE MONITORING"
        last = status.get("last_success_at")
        last_text = time.strftime("%H:%M:%S", time.localtime(last)) if isinstance(last, (int, float)) else "N/A"
        next_at = status.get("next_poll_at")
        next_text = time.strftime("%H:%M:%S", time.localtime(next_at)) if isinstance(next_at, (int, float)) else "N/A"
        capability_text = " · ".join(
            "{}: {} (last {}, next {})".format(
                name.title(), safe_upper(safe_mapping(item).get("status"), "DISCONNECTED"),
                time.strftime("%H:%M:%S", time.localtime(safe_mapping(item).get("last_success_at"))) if isinstance(safe_mapping(item).get("last_success_at"), (int, float)) else "N/A",
                time.strftime("%H:%M:%S", time.localtime(safe_mapping(item).get("next_due_at"))) if isinstance(safe_mapping(item).get("next_due_at"), (int, float)) else "N/A",
            ) for name, item in safe_mapping(status.get("capabilities")).items()
        ) or "No capability evidence yet."
        render_html(
            f'<div class="cycle-panel"><div><div class="cycle-panel-label">{safe_text(heading)}</div>'
            f'<div class="cycle-panel-value"><b>{safe_text(state)}</b> · Polling interval: '
            f'{safe_text(status.get("interval_seconds"))}s · Last successful read: {safe_text(last_text)} · '
            f'Next scheduled read: {safe_text(next_text)}<br>{safe_text(capability_text)}<br>Source: {safe_text(observation.get("source") or status.get("source"), "N/A")}</div></div></div>'
        )
    except Exception:
        st.caption("Nokia live monitoring: DISCONNECTED — supervisory read unavailable.")


def render_trusted_dispatch(result: Optional[Dict[str, Any]], supervisory: Optional[Dict[str, Any]] = None) -> None:
    render_section_header("TRUSTED DISPATCH", "Privileged field intervention only · WARDEN-owned trust authority")
    if settings.haris_backend_url:
        try:
            payload = run_async(backend_request("GET", "/api/nac/autonomous/status"))
            if payload:
                result = payload.get("cycle") or result
                supervisory = payload
                st.session_state.backend_supervisory_status = payload
        except Exception:
            st.warning("Authoritative HARIS backend is unavailable; Trusted Dispatch remains fail-closed.")
    dispatch = safe_mapping(safe_mapping(result).get("trusted_dispatch")) or get_system().current_dispatch_status
    if settings.haris_backend_url:
        sync_backend_consent_binding(dispatch)
    if dispatch:
        safe_status = {key: value for key, value in dispatch.items() if key != "authorization_url"}
        history = (supervisory or {}).get("dispatch_history") or (result or {}).get("dispatch_history", [])
        attempt_cards: List[str] = []
        for attempt in [safe_mapping(item) for item in history[-2:]]:
            attempt_state = attempt.get("final_dispatch_status") or attempt.get("warden_decision") or attempt.get("verification_status") or "REVIEW"
            attempt_cards.append(
                f'<div class="dispatch-attempt {semantic_tone(attempt_state)}">'
                f'<div class="dispatch-attempt-label">ENGINEER ATTEMPT</div>'
                f'<div class="dispatch-attempt-name">{safe_text(attempt.get("engineer_name") or attempt.get("engineer_id"), "AUTHORIZED ENGINEER")}</div>'
                f'<div class="dispatch-attempt-state" style="color:var(--{ "red" if semantic_tone(attempt_state) == "danger" else "yellow" if semantic_tone(attempt_state) == "warning" else "green" });">{safe_text(attempt_state)}</div></div>'
            )
        current_state = dispatch.get("status") or dispatch.get("decision") or "REVIEW"
        attempt_cards.append(
            f'<div class="dispatch-attempt {semantic_tone(current_state)}">'
            f'<div class="dispatch-attempt-label">CURRENT AUTHORIZATION</div>'
            f'<div class="dispatch-attempt-name">{safe_text(dispatch.get("engineer_name") or dispatch.get("engineer_id"), "NO ENGINEER SELECTED")}</div>'
            f'<div class="dispatch-attempt-state" style="color:var(--{ "red" if semantic_tone(current_state) == "danger" else "yellow" if semantic_tone(current_state) == "warning" else "green" });">{safe_text(current_state)}</div></div>'
        )
        render_html('<div class="dispatch-board">' + "".join(attempt_cards) + '</div>')
        st.json(safe_status)
        if history:
            st.caption("BACKEND DISPATCH ATTEMPTS")
            st.dataframe(history, use_container_width=True, hide_index=True)
        if dispatch.get("status") == "WAITING_FOR_IDENTITY_VERIFICATION": st.warning("Awaiting consent-bound Nokia Number Verification; dispatch remains blocked.")
        authorization_url = st.session_state.get("backend_authorization_url") if settings.haris_backend_url else get_system().dispatch_authorization_url
        if settings.haris_backend_url and not authorization_url and not st.session_state.get("backend_consent_action_token") and st.session_state.get("backend_workflow_session_token"):
            try:
                token_payload = run_async(backend_request("POST", "/api/nac/autonomous/consent-action-token", {"workflow_session_token": st.session_state.backend_workflow_session_token}))
                if token_payload:
                    st.session_state.backend_consent_action_token = token_payload.get("consent_action_token")
                    st.session_state.backend_consent_action_pending_id = dispatch.get("pending_id")
                    st.session_state.backend_consent_action_engineer_id = dispatch.get("engineer_id")
            except Exception:
                st.warning("No active secure consent action is available; dispatch remains fail-closed.")
        if settings.haris_backend_url and not authorization_url and st.session_state.get("backend_consent_action_token"):
            try:
                handoff = run_async(backend_request("POST", "/api/nac/autonomous/consent-action", {"action_token": st.session_state.backend_consent_action_token}))
                if handoff:
                    authorization_url = handoff.get("authorization_url")
                    st.session_state.backend_authorization_url = authorization_url
                    st.session_state.backend_authorization_url_pending_id = dispatch.get("pending_id")
                    st.session_state.backend_authorization_url_engineer_id = dispatch.get("engineer_id")
                    st.session_state.pop("backend_consent_action_token", None)
            except Exception:
                st.warning("Secure consent action is unavailable or expired; dispatch remains fail-closed.")
        if authorization_url:
            st.link_button("Open secure Nokia Number Verification", authorization_url, type="primary")
    else: st.caption("No privileged field intervention is required. Routine remediation does not call Number Verification or SIM Swap.")


def render_history_audit(result: Optional[Dict[str, Any]], supervisory: Optional[Dict[str, Any]] = None) -> None:
    render_history(supervisory)
    render_section_header("LEARNED MEMORY")
    st.json((result or {}).get("learning") or {"status": "No completed cycle in this session."})


# ============================================================================
# Main render
# ============================================================================

def render_decorative_background() -> None:
    """Static command-center artwork; it never represents HARIS telemetry."""
    render_html(
        """
        <div class="haris-decor" aria-hidden="true">
          <svg class="haris-topology-lines" viewBox="0 0 520 300"><g fill="none" stroke="#4daeff" stroke-width="1"><path opacity=".45" d="M-20 44 105 16l82 73 91-42 90 68 165-50"/><path opacity=".3" d="M-5 155 91 98l98 74 91-42 91 70 166-23"/><path opacity=".25" d="M30 238 111 164l94 58 96-46 102 70 142-19"/></g><g fill="#8ce8ff"><circle cx="105" cy="16" r="3"/><circle cx="187" cy="89" r="2.5"/><circle cx="278" cy="47" r="3"/><circle cx="368" cy="115" r="2.5"/><circle cx="91" cy="98" r="2"/><circle cx="280" cy="130" r="3"/><circle cx="403" cy="246" r="2"/></g></svg>
          <svg class="haris-globe" viewBox="0 0 800 800"><defs><radialGradient id="haris-globe-fill"><stop stop-color="#0b315a" stop-opacity=".23"/><stop offset=".72" stop-color="#061b35" stop-opacity=".45"/><stop offset="1" stop-color="#020810" stop-opacity="0"/></radialGradient><clipPath id="haris-globe-clip"><circle cx="400" cy="400" r="320"/></clipPath></defs><circle cx="400" cy="400" r="320" fill="url(#haris-globe-fill)" stroke="#4daeff" stroke-opacity=".42" stroke-width="2"/><g clip-path="url(#haris-globe-clip)" fill="none" stroke="#58bdff" stroke-opacity=".25" stroke-width="1"><ellipse cx="400" cy="400" rx="320" ry="96"/><ellipse cx="400" cy="400" rx="320" ry="190"/><ellipse cx="400" cy="400" rx="160" ry="320"/><ellipse cx="400" cy="400" rx="258" ry="320"/><path d="M90 295c155 38 310 23 610 20M84 430c160-29 390 52 632-8M128 554c203-43 392 4 544-42"/><path d="M180 154c78 118 74 326 20 517M402 80c-25 210 33 377 8 645M624 145c-103 165-74 387 1 528"/></g><g fill="#83ddff" opacity=".72"><circle cx="265" cy="227" r="3"/><circle cx="389" cy="185" r="3"/><circle cx="542" cy="268" r="2.6"/><circle cx="337" cy="350" r="3"/><circle cx="483" cy="413" r="3"/><circle cx="238" cy="485" r="2.6"/><circle cx="577" cy="535" r="3"/></g><g stroke="#7edfff" stroke-opacity=".32" fill="none"><path d="M265 227 389 185l153 83-59 145-146-63-99 135 239 50 101 0"/><path d="M238 485 337 350M483 413l94 122"/></g><circle cx="630" cy="121" r="7" fill="#c5f6ff" opacity=".9"/><circle cx="630" cy="121" r="28" fill="none" stroke="#8deaff" stroke-opacity=".44"/></svg>
          <svg class="haris-floor-grid" viewBox="0 0 1400 300" preserveAspectRatio="none"><g fill="none" stroke="#3d8ed0" stroke-width="1"><path opacity=".45" d="M0 300 500 80 1000 80 1400 300"/><path opacity=".28" d="M0 300 550 118 850 118 1400 300"/><path opacity=".18" d="M0 300 590 150 810 150 1400 300"/><path opacity=".32" d="M250 300 500 80M450 300 590 80M650 300 680 80M850 300 770 80M1050 300 860 80M1250 300 950 80"/></g></svg>
        </div>
        """
    )


def render_console() -> None:
    """Render the Streamlit entry point without executing it on test import."""
    render_decorative_background()
    supervisory = authoritative_supervisory_status()
    result = (supervisory or {}).get("cycle") or st.session_state.get("last_result")
    render_status_bar(result, supervisory)
    if st.session_state.get("backend_configuration_error"):
        st.error(st.session_state.backend_configuration_error)
    section = st.radio(
        "HARIS CONSOLE", ["OVERVIEW", "NETWORK INTELLIGENCE", "AUTONOMOUS OPERATIONS", "TRUSTED DISPATCH", "HISTORY & AUDIT"],
        horizontal=True, label_visibility="collapsed",
    )

    if section == "OVERVIEW":
        render_overview(result, supervisory)
    elif section == "NETWORK INTELLIGENCE":
        render_network_intelligence(result)
    elif section == "AUTONOMOUS OPERATIONS":
        render_controls()
        render_decision_engine(result)
        render_impact(result)
        render_playbook_and_feed(result)
    elif section == "TRUSTED DISPATCH":
        render_trusted_dispatch(result, supervisory)
    elif section == "HISTORY & AUDIT":
        render_history_audit(result, supervisory)

    render_html(
        """
        <div class="footer">
            <span>GSMA MENA IGNITE HACKATHON 2026</span>
            <span>RESILIENT &nbsp; | &nbsp; SECURE &nbsp; | &nbsp; SUSTAINABLE &nbsp; | &nbsp; CONNECTED</span>
        </div>
        """
    )


if __name__ == "__main__":
    render_console()
