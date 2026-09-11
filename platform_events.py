"""Versioned canonical event contracts for the HARIS platform core.

Provider payloads are normalized at the gateway.  The rest of HARIS operates
only on these safe, provenance-bearing envelopes.
"""
from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Provenance(str, Enum):
    NOKIA_LIVE = "NOKIA_LIVE"
    HARIS_DERIVED = "HARIS_DERIVED"
    FIXTURE_SIMULATED = "FIXTURE_SIMULATED"
    UNAVAILABLE = "UNAVAILABLE"


class EventType(str, Enum):
    NETWORK_CONGESTION_CHANGED = "NETWORK_CONGESTION_CHANGED"
    DEVICE_REACHABILITY_CHANGED = "DEVICE_REACHABILITY_CHANGED"
    DEVICE_LOCATION_UPDATED = "DEVICE_LOCATION_UPDATED"
    GEOFENCE_ENTERED = "GEOFENCE_ENTERED"
    GEOFENCE_EXITED = "GEOFENCE_EXITED"
    QOD_STATUS_CHANGED = "QOD_STATUS_CHANGED"
    SLICE_STATUS_CHANGED = "SLICE_STATUS_CHANGED"
    INCIDENT_OPENED = "INCIDENT_OPENED"
    INCIDENT_STATE_CHANGED = "INCIDENT_STATE_CHANGED"


class HarisEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: str = Field(default_factory=lambda: f"evt-{uuid.uuid4().hex}")
    event_type: EventType
    schema_version: int = 1
    source: str
    source_mode: str
    source_event_id: Optional[str] = None
    source_timestamp: float
    received_at: float = Field(default_factory=time.time)
    # Persisted independently from source and receipt timestamps so replay can
    # distinguish provider chronology from HARIS' durable record creation.
    created_at: float = Field(default_factory=time.time)
    entity_type: str
    entity_id: str
    correlation_key: str
    provenance: Provenance
    payload: Dict[str, Any]
    trace_id: str = Field(default_factory=lambda: uuid.uuid4().hex)

    @field_validator(
        "event_id", "source", "source_mode", "source_event_id", "entity_type",
        "entity_id", "correlation_key", "trace_id",
    )
    @classmethod
    def identity_fields_must_be_safe(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            _reject_sensitive_payload(value)
        return value

    @field_validator("payload")
    @classmethod
    def payload_must_be_safe(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        _reject_sensitive_payload(value)
        return value

    @property
    def idempotency_key(self) -> str:
        if self.source_event_id:
            return f"{self.source}:{self.source_event_id}"
        safe = {"event_type": self.event_type.value, "entity_type": self.entity_type, "entity_id": self.entity_id, "source_timestamp": self.source_timestamp, "payload": self.payload}
        return f"{self.source}:sha256:{hashlib.sha256(json.dumps(safe, sort_keys=True, separators=(',', ':')).encode()).hexdigest()}"


def _reject_sensitive_payload(value: Any) -> None:
    """Reject secret-bearing payloads at every nesting level.

    Durable domain events are operational evidence, not an OAuth or identity
    transport.  Rejecting key names rather than attempting redaction prevents
    an unsafe value from ever reaching the event store, outbox, snapshot, or
    audit linkage.
    """
    forbidden = {
        "token", "access_token", "refresh_token", "api_key", "authorization",
        "authorization_url", "client_secret", "oauth_state",
        "consent_action_token", "oauth_code", "phone_number", "msisdn",
    }
    if isinstance(value, dict):
        for key, nested in value.items():
            if str(key).lower() in forbidden:
                raise ValueError("canonical event payload contains forbidden sensitive fields")
            _reject_sensitive_payload(nested)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            _reject_sensitive_payload(nested)
    elif isinstance(value, str):
        if re.search(r"(?:[?&](?:code|state|token|access_token|client_secret)=|\bbearer\s+)", value, re.I):
            raise ValueError("canonical event payload contains forbidden sensitive values")
        if re.search(r"\+\d{8,15}\b", value):
            raise ValueError("canonical event payload contains an unmasked phone number")


class NokiaCongestionWebhook(BaseModel):
    """Strict minimal webhook shape; Nokia raw payloads stay at the boundary."""
    model_config = ConfigDict(extra="forbid")
    event_id: Optional[str] = None
    event_timestamp: float
    cell_id: str = Field(min_length=1, max_length=128)
    congestion_level: str
    confidence_level: Optional[int] = Field(default=None, ge=0, le=100)

    @field_validator("congestion_level")
    @classmethod
    def categorical_level(cls, value: str) -> str:
        if value not in {"None", "Low", "Medium", "High"}:
            raise ValueError("unsupported Nokia congestion category")
        return value

    def canonical(self, *, mode: str) -> HarisEvent:
        return HarisEvent(event_type=EventType.NETWORK_CONGESTION_CHANGED, source="nokia", source_mode=mode, source_event_id=self.event_id, source_timestamp=self.event_timestamp, entity_type="HARIS_CONFIGURED_LOGICAL_CELL", entity_id=self.cell_id, correlation_key=f"cell:{self.cell_id}", provenance=Provenance.NOKIA_LIVE if mode != "fixture" else Provenance.FIXTURE_SIMULATED, payload={"congestion_level": self.congestion_level, "confidence_level": self.confidence_level})
