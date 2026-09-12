"""Authoritative, provenance-aware logical network state.

This registry deliberately models HARIS logical cell/site mappings, not a
physical Nokia tower inventory.  It preserves Nokia categorical source values
and keeps HARIS operational interpretation in separate fields.
"""
from __future__ import annotations

import copy
import time
from typing import Any, Dict, Iterable


PROVENANCE = {"NOKIA_LIVE", "HARIS_DERIVED", "FIXTURE_SIMULATED", "UNAVAILABLE"}
_RANK = {"None": 0, "Low": 1, "Medium": 2, "High": 3}


class NetworkStateRegistry:
    """Disposable compatibility view of durable logical-cell evidence.

    The durable repository is authoritative in the backend runtime. This view
    is rebuilt from committed projections and never creates numeric
    congestion, topology coordinates, or substitute fixture evidence in a
    live mode.
    """

    def __init__(self, *, mode: str, stale_after_seconds: int = 30):
        self.mode = mode
        self.stale_after_seconds = stale_after_seconds
        self._entities: Dict[str, Dict[str, Any]] = {}

    @property
    def source_provenance(self) -> str:
        return "FIXTURE_SIMULATED" if self.mode == "fixture" else "NOKIA_LIVE"

    def _entity(self, entity_id: str) -> Dict[str, Any]:
        return self._entities.setdefault(entity_id, {
            "entity_id": entity_id,
            "source": self.source_provenance,
            "source_type": "HARIS_CONFIGURED_LOGICAL_CELL",
            "nokia_congestion": None,
            "congestion_observed_at": None,
            "reachability": None,
            "reachability_observed_at": None,
            "location_available": None,
            "location_observed_at": None,
            "freshness": "UNAVAILABLE",
            "haris_state": "STALE",
            "active_incident_id": None,
            "last_state_change_at": None,
        })

    def restore_projection(self, projection: Dict[str, Dict[str, Any]]) -> None:
        """Restore the durable projection without inventing source evidence.

        The durable core uses explicit domain names while this long-lived API
        keeps compatibility aliases for the current Streamlit/NOC consumers.
        """
        for entity_id, persisted in projection.items():
            if not isinstance(persisted, dict):
                continue
            entity = self._entity(str(entity_id))
            entity.update({
                "source": persisted.get("provenance") or entity["source"],
                "source_type": persisted.get("entity_type") or entity["source_type"],
                "nokia_congestion": persisted.get("raw_congestion"),
                "congestion_observed_at": persisted.get("raw_congestion_observed_at"),
                "reachability": persisted.get("reachability_summary"),
                "reachability_observed_at": persisted.get("reachability_observed_at"),
                "location_available": bool(persisted.get("location_summary")) if persisted.get("location_summary") is not None else None,
                "location_observed_at": persisted.get("location_observed_at"),
                "freshness": persisted.get("freshness") or entity["freshness"],
                "haris_state": persisted.get("haris_operational_state") or entity["haris_state"],
                "active_incident_id": (persisted.get("active_incident_ids") or [None])[0],
                "last_state_change_at": persisted.get("last_operational_change_at"),
                "version": persisted.get("version", 0),
                "mapping_source": persisted.get("mapping_source"),
                "provenance": persisted.get("provenance"),
                "updated_at": persisted.get("updated_at"),
            })

    def ingest(self, observation: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Merge source evidence without transforming its raw category."""
        observed_at = float(observation.get("observed_at") or time.time())
        source = "FIXTURE_SIMULATED" if observation.get("mode") == "fixture" else "NOKIA_LIVE"
        for row in observation.get("congestion") or []:
            cell = row.get("cell_id")
            level = row.get("congestion_level")
            if not cell or level not in _RANK:
                continue
            entity = self._entity(str(cell))
            entity.update({"source": source, "nokia_congestion": level, "congestion_observed_at": observed_at})
        by_cell: Dict[str, list[bool]] = {}
        for row in observation.get("devices") or []:
            if row.get("cell_id") is not None and isinstance(row.get("reachable"), bool):
                by_cell.setdefault(str(row["cell_id"]), []).append(row["reachable"])
        for cell, reachability in by_cell.items():
            entity = self._entity(cell)
            entity.update({"source": source, "reachability": {"reachable": sum(reachability), "unreachable": len(reachability) - sum(reachability)}, "reachability_observed_at": observed_at})
        device_cells = {
            str(row.get("device_id")): str(row.get("cell_id"))
            for row in observation.get("devices") or []
            if row.get("device_id") and row.get("cell_id")
        }
        location_cells = {
            device_cells[str(row.get("device_id"))]
            for row in observation.get("locations") or []
            if str(row.get("device_id")) in device_cells
        }
        for cell in location_cells:
            entity = self._entity(cell)
            entity["location_available"] = True
            entity["location_observed_at"] = observed_at
        self._refresh(observed_at)
        return self.snapshot()["entities"]

    def _refresh(self, now: float) -> None:
        for entity in self._entities.values():
            timestamp = entity.get("congestion_observed_at")
            fresh = timestamp is not None and now - float(timestamp) <= self.stale_after_seconds
            entity["freshness"] = "FRESH" if fresh else "STALE" if timestamp is not None else "UNAVAILABLE"
            if entity.get("active_incident_id"):
                continue
            next_state = "STALE" if not fresh else "INCIDENT_OPEN" if entity.get("nokia_congestion") == "High" else "WATCHING" if entity.get("nokia_congestion") == "Medium" else "STABLE"
            if entity.get("haris_state") != next_state:
                entity["haris_state"] = next_state
                entity["last_state_change_at"] = now

    def set_incident_state(self, entity_id: str, incident_id: str | None, state: str) -> None:
        entity = self._entity(entity_id)
        entity["active_incident_id"] = incident_id
        entity["haris_state"] = state
        entity["last_state_change_at"] = time.time()

    def apply_event(self, event: Any) -> bool:
        """Project a canonical event if it is not older than source state."""
        if getattr(event, "event_type", None).value != "NETWORK_CONGESTION_CHANGED":
            return False
        entity = self._entity(event.entity_id)
        previous = entity.get("congestion_observed_at")
        if previous is not None and float(event.source_timestamp) < float(previous):
            return False
        entity.update({"source": event.provenance.value, "nokia_congestion": event.payload["congestion_level"], "congestion_observed_at": event.source_timestamp})
        self._refresh(time.time())
        return True

    def snapshot(self) -> Dict[str, Any]:
        self._refresh(time.time())
        return {
            "source": self.source_provenance,
            "topology_type": "HARIS_CONFIGURED_LOGICAL_CELL_MAPPING",
            "entities": copy.deepcopy(self._entities),
            "generated_at": time.time(),
        }
