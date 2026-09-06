"""Bounded, server-owned field-intervention selection and audit support.

This module deliberately does not perform Number Verification or SIM Swap.
Those sensitive checks remain in ``nokia_clients.evaluate_trusted_dispatch_phone``.
It selects an authorised engineer deterministically and records only masked,
safe operational metadata while consent-bound verification is pending.
"""
from __future__ import annotations

import json
import secrets
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


def mask_phone_number(phone_number: str) -> str:
    """Avoid exposing a full phone number in UI, logs, or audit replay."""
    if len(phone_number) <= 4:
        return "***"
    return f"***{phone_number[-4:]}"


class AuthorizedEngineer(BaseModel):
    engineer_id: str
    name: str
    phone_number: str
    role: str = "field_engineer"
    skills: List[str] = Field(default_factory=list)
    region: str = ""
    site_coverage: List[str] = Field(default_factory=list)
    availability: bool = True
    priority: int = Field(default=100, ge=0)
    enabled: bool = True


class DispatchAttempt(BaseModel):
    incident_id: str
    engineer_id: str
    masked_phone_number: str
    site: str
    intervention_type: str
    verification_status: str
    sim_swap_status: Optional[str] = None
    warden_decision: Optional[str] = None
    reason: str
    timestamp: float = Field(default_factory=time.time)
    final_dispatch_status: str


class PendingDispatch(BaseModel):
    """Server-only correlation for one consent-bound dispatch continuation."""
    pending_id: str
    incident_id: str
    engineer_id: str
    phone_number: str
    site: str
    intervention_type: str
    created_at: float
    expires_at: float
    status: str = "PENDING_VERIFICATION"
    oauth_state: Optional[str] = None
    consumed_at: Optional[float] = None


class PendingDispatchStore:
    """Atomic process-local coordinator; production requires shared storage."""
    def __init__(self) -> None:
        self._items: Dict[str, PendingDispatch] = {}
        self._lock = threading.Lock()

    def create(self, *, incident_id: str, engineer_id: str, phone_number: str, site: str, intervention_type: str, ttl_seconds: int) -> PendingDispatch:
        now = time.time()
        item = PendingDispatch(
            pending_id=secrets.token_urlsafe(24), incident_id=incident_id,
            engineer_id=engineer_id, phone_number=phone_number, site=site,
            intervention_type=intervention_type, created_at=now,
            expires_at=now + ttl_seconds,
        )
        with self._lock: self._items[item.pending_id] = item
        return item

    def bind_oauth_state(self, pending_id: str, oauth_state: str) -> None:
        with self._lock:
            item = self._items.get(pending_id)
            if item is None or item.status != "PENDING_VERIFICATION": raise ValueError("unknown_pending_dispatch")
            item.oauth_state = oauth_state

    def consume_for_resume(self, *, pending_id: str, engineer_id: str, phone_number: str, oauth_state: str) -> PendingDispatch:
        with self._lock:
            item = self._items.get(pending_id)
            if item is None: raise ValueError("unknown_pending_dispatch")
            if time.time() > item.expires_at:
                item.status = "EXPIRED"; raise ValueError("expired_pending_dispatch")
            if item.status != "PENDING_VERIFICATION" or item.consumed_at is not None:
                raise ValueError("replayed_pending_dispatch")
            if (item.engineer_id != engineer_id or item.phone_number != phone_number or item.oauth_state != oauth_state):
                raise ValueError("pending_dispatch_binding_mismatch")
            item.status, item.consumed_at = "RESUMING", time.time()
            return item.model_copy()

    def complete(self, pending_id: str, status: str) -> None:
        with self._lock:
            if pending_id in self._items: self._items[pending_id].status = status

    def get(self, pending_id: str) -> Optional[PendingDispatch]:
        with self._lock:
            item = self._items.get(pending_id)
            return item.model_copy() if item else None


class AuthorizedEngineerRegistry:
    def __init__(self, registry_path: str):
        self.registry_path = Path(registry_path)

    def engineers(self) -> List[AuthorizedEngineer]:
        try:
            raw = json.loads(self.registry_path.read_text(encoding="utf-8"))
            rows = raw.get("engineers", raw) if isinstance(raw, dict) else raw
            return [AuthorizedEngineer.model_validate(row) for row in rows]
        except (OSError, ValueError, TypeError):
            # An unavailable registry must fail closed, not produce a guessed
            # recipient for a sensitive physical intervention.
            return []

    def eligible(self, *, site: str, required_skills: List[str], role: str = "field_engineer") -> List[AuthorizedEngineer]:
        required = set(required_skills)
        candidates = [
            engineer for engineer in self.engineers()
            if engineer.enabled and engineer.availability and engineer.role == role
            and required.issubset(set(engineer.skills))
            and (not engineer.site_coverage or site in engineer.site_coverage or engineer.region == site)
        ]
        # Stable deterministic ordering: most specific coverage, then policy
        # priority, then identifier.  No model inference selects humans.
        return sorted(candidates, key=lambda e: (0 if site in e.site_coverage else 1, e.priority, e.engineer_id))


class TrustedDispatchHistory:
    """Process-local prototype history; production needs shared durable storage."""
    def __init__(self) -> None:
        self._attempts: List[DispatchAttempt] = []

    def record(self, attempt: DispatchAttempt) -> DispatchAttempt:
        self._attempts.append(attempt)
        return attempt

    def for_incident(self, incident_id: str) -> List[DispatchAttempt]:
        return [item for item in self._attempts if item.incident_id == incident_id]


trusted_dispatch_history = TrustedDispatchHistory()
pending_dispatches = PendingDispatchStore()
