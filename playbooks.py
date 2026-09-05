from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List

from config import AppSettings, QualityLevel
from memory import MemoryStore
from nokia_clients import BaseNokiaClient, CongestionReading, DeviceStatus, Location

logger = logging.getLogger("haris.playbooks")


@dataclass(frozen=True)
class Action:
    kind: str
    device_id: str
    parameters: Dict[str, Any]
    reason: str


class PlaybookEngine:
    def __init__(self, settings: AppSettings, client: BaseNokiaClient, memory: MemoryStore):
        self.settings = settings
        self.client = client
        self.memory = memory

    def classify(self, congestion: CongestionReading) -> QualityLevel:
        level_to_quality = {
            "None": QualityLevel.EXCELLENT,
            "Low": QualityLevel.GOOD,
            "Medium": QualityLevel.POOR,
            "High": QualityLevel.CRITICAL,
        }

        try:
            return level_to_quality[congestion.congestion_level]
        except KeyError as exc:
            raise RuntimeError(
                f"Unsupported Nokia congestion level: "
                f"{congestion.congestion_level!r}"
            ) from exc

        
    def storm_shield(
        self,
        dust_advisory: bool,
        congestion: List[CongestionReading],
        devices: List[DeviceStatus],
    ) -> List[Action]:

        tier1_cells = {
            d.cell_id
            for d in devices
            if d.tier == 1
        }

        hot = [
            c
            for c in congestion
            if c.cell_id in tier1_cells
            and c.congestion_level in {"Medium", "High"}
        ]

        if not dust_advisory or not hot:
            return []

        hot_cells = {x.cell_id for x in hot}

        actions: List[Action] = []

        for d in devices:
            if d.tier == 1 and d.cell_id in hot_cells:
                actions.append(
                    Action(
                        "qos",
                        d.device_id,
                        {
                            "profile": "guaranteed",
                            "duration_seconds": self.settings.guardrails.rollback_seconds,
                        },
                        (
                            "Dust advisory combined with Nokia congestion "
                            f"level {next(x.congestion_level for x in hot if x.cell_id == d.cell_id)} "
                            "around a Tier-1 asset"
                        ),
                    )
                )

                actions.append(
                    Action(
                        "slice_attach",
                        d.device_id,
                        {"slice_id": "haris-emergency"},
                        "Protect Tier-1 session on emergency slice",
                    )
                )

                actions.append(
                    Action(
                        "geofence",
                        d.device_id,
                        {"polygon_id": "storm-impact"},
                        "Pre-position event-driven exposure tracking",
                    )
                )

        return actions

    def capacity_harvest(self, congestion: List[CongestionReading], devices: List[DeviceStatus],
    ) -> List[Action]:

        hot_cells = {
            c.cell_id
            for c in congestion
            if c.congestion_level == "High"
        }

        return [
            Action(
                "qos",
                d.device_id,
                {
                    "profile": "low-bandwidth",
                    "duration_seconds": 300,
                },
                "Nokia reports High congestion; reduce Tier-3 bandwidth demand",
            )
            for d in devices
            if d.tier == 3 and d.cell_id in hot_cells
    ]

    def energy_guard(
        self,
        congestion: List[CongestionReading],
        devices: List[DeviceStatus],
    ) -> List[Action]:

        hot_cells = {
            c.cell_id
            for c in congestion
            if c.congestion_level == "High"
        }

        return [
            Action(
                "qos",
                d.device_id,
                {
                    "profile": "emergency-only",
                    "duration_seconds": 300,
                    "projected_runtime_hours": round(
                        max(0.25, d.battery_pct / 12.0),
                        2,
                    ),
                },
                (
                    "Battery reserve is low while Nokia reports High "
                    "congestion; retain emergency traffic and shed "
                    "bulk telemetry"
                ),
            )
            for d in devices
            if (
                d.tier == 3
                and d.cell_id in hot_cells
                and d.battery_pct < 25
            )
        ]

    

    def evaluate(
        self,
        dust_advisory: bool,
        congestion: List[CongestionReading],
        devices: List[DeviceStatus],
    ) -> Dict[str, Any]:

        # ---------------------------------------------------------
        # Collect candidate actions from every playbook.
        # Playbooks generate proposals; they do not directly
        # determine which proposal wins when policies conflict.
        # ---------------------------------------------------------

        candidates = {
            "energy_guard": self.energy_guard(congestion, devices),
            "storm_shield": self.storm_shield(
                dust_advisory,
                congestion,
                devices,
            ),
            "capacity_harvest": self.capacity_harvest(
                congestion,
                devices,
            ),
        }

        # ---------------------------------------------------------
        # Policy precedence:
        #
        # Energy Guard > Storm Shield > Capacity Harvest
        #
        # Higher-priority network policies win when two policies
        # attempt to control the same device/resource.
        #
        # Higher-priority policies win when two policies attempt
        # to control the same device/resource.
        # ---------------------------------------------------------

        priority = {
            "energy_guard": 300,
            "storm_shield": 200,
            "capacity_harvest": 100,
        }

        selected: Dict[tuple, tuple] = {}

        for playbook_name, playbook_actions in candidates.items():
            for action in playbook_actions:

                # Network actions are exclusive per device.
                # A device must not receive two contradictory QoS
                # profiles in the same cycle.
                if action.kind == "qos":
                    resource_key = (
                        action.device_id,
                        "qos",
                    )

                elif action.kind in {
                    "slice_attach",
                    "slice_detach",
                }:
                    resource_key = (
                        action.device_id,
                        "slice",
                    )

                elif action.kind == "geofence":
                    resource_key = (
                        action.device_id,
                        "geofence",
                    )

                else:
                    # Unknown action types use their own resource key.
                    resource_key = (
                        action.device_id,
                        action.kind,
                    )

                candidate = (
                    priority[playbook_name],
                    playbook_name,
                    action,
                )

                current = selected.get(resource_key)

                if current is None or candidate[0] > current[0]:
                    selected[resource_key] = candidate

        # ---------------------------------------------------------
        # Preserve deterministic ordering.
        # ---------------------------------------------------------

        actions = [
            item[2]
            for item in sorted(
                selected.values(),
                key=lambda item: (
                    -item[0],
                    item[1],
                    item[2].device_id,
                    item[2].kind,
                ),
            )
        ]

        # ---------------------------------------------------------
        # Report which playbooks actually contributed an action.
        # ---------------------------------------------------------

        active_playbooks = sorted(
            {
                item[1]
                for item in selected.values()
            }
        )

        return {
            "actions": actions,
            "playbooks": active_playbooks,
            "evaluated_at": time.time(),
        }
