from __future__ import annotations
import asyncio
import inspect
import json
import logging
import random
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar

from fastapi import APIRouter, FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from config import AppSettings, get_settings
from memory import MemoryStore
from network_as_code.models import Device

logger = logging.getLogger("haris.nokia")
T = TypeVar("T")


class RetryPolicy(BaseModel):
    attempts: int = 3
    base_delay_seconds: float = 0.25
    max_delay_seconds: float = 2.0


async def retry_async(fn: Callable[[], Any], policy: RetryPolicy) -> Any:
    last_error: Optional[Exception] = None
    for attempt in range(policy.attempts):
        try:
            result = fn()
            if inspect.isawaitable(result):
                return await result
            return result
        except Exception as exc:
            last_error = exc
            if attempt == policy.attempts - 1:
                break
            delay = min(policy.max_delay_seconds, policy.base_delay_seconds * (2 ** attempt))
            delay += random.random() * 0.05
            await asyncio.sleep(delay)
    raise RuntimeError(f"Network API operation failed after {policy.attempts} attempts: {last_error}") from last_error


class CongestionReading(BaseModel):
    """
    Factual congestion observation returned by Nokia/CAMARA.

    These fields intentionally mirror the information provided
    by Nokia's Congestion API.
    """

    cell_id: str
    congestion_level: str
    confidence_level: int = Field(ge=0, le=100)
    interval_start: str
    interval_stop: str
    # HARIS keeps measured fixture KPIs alongside CAMARA's categorical
    # congestion evidence.  Live Nokia responses may omit these values.
    congestion_pct: Optional[float] = Field(default=None, ge=0, le=100)
    latency_ms: Optional[float] = Field(default=None, ge=0)
    predicted_congestion_pct: Optional[float] = Field(default=None, ge=0, le=100)


class DeviceStatus(BaseModel):
    device_id: str
    reachable: bool
    roaming: bool = False
    battery_pct: float = Field(ge=0, le=100)
    tier: int = Field(ge=1, le=3)
    cell_id: str


class Location(BaseModel):
    device_id: str
    latitude: float
    longitude: float
    accuracy_m: float = 50.0


class GeofenceSubscription(BaseModel):
    subscription_id: str
    device_id: str
    polygon_id: str
    active: bool


class QosSession(BaseModel):
    session_id: str
    device_id: str
    profile: str
    estimated_cost_usd: float
    active: bool


class SliceAttachment(BaseModel):
    device_id: str
    slice_id: str
    attached: bool


class ApiResponse(BaseModel, Generic[T]):
    ok: bool = True
    data: T
    source: str
    latency_ms: float


class BaseNokiaClient(ABC):
    name: str

    def __init__(self, settings: AppSettings):
        self.settings = settings
        self.retry_policy = RetryPolicy()

    def action_safety_error(self, action_kind: str, parameters: Dict[str, Any]) -> Optional[str]:
        """Return a fail-closed reason when this adapter cannot execute an action."""
        return None

    def capability_report(self) -> Dict[str, Dict[str, Any]]:
        """Read-only assessment of whether live-action inputs are constructible."""
        return {
            "congestion_insights": {"status": "READ_READY", "reason": None},
            "device_status": {"status": "READ_READY", "reason": None},
            "location": {"status": "READ_READY", "reason": None},
            "geofencing": {"status": "SUPPORTED_AND_CONFIGURED", "reason": None},
            "qod": {"status": "SUPPORTED_AND_CONFIGURED", "reason": None},
            "slicing": {"status": "SUPPORTED_AND_CONFIGURED", "reason": None},
        }

    @abstractmethod
    async def congestion_insights(self, cell_ids: Optional[List[str]] = None) -> List[CongestionReading]: ...

    @abstractmethod
    async def device_status(self, device_ids: List[str]) -> List[DeviceStatus]: ...

    @abstractmethod
    async def location_retrieval(self, device_ids: List[str]) -> List[Location]: ...

    @abstractmethod
    async def create_geofence(self, device_id: str, polygon_id: str) -> GeofenceSubscription: ...

    @abstractmethod
    async def delete_geofence(self, subscription_id: str) -> bool: ...

    @abstractmethod
    async def request_qos(self, device_id: str, profile: str, duration_seconds: int) -> QosSession: ...

    @abstractmethod
    async def release_qos(self, session_id: str) -> bool: ...

    @abstractmethod
    async def attach_slice(self, device_id: str, slice_id: str) -> SliceAttachment: ...

    @abstractmethod
    async def detach_slice(self, device_id: str, slice_id: str) -> SliceAttachment: ...

    @abstractmethod
    async def rollback_network_state(self,baseline: Dict[str, Dict[str, float]],) -> Dict[str, Any]:
        ...

class FixtureNokiaClient(BaseNokiaClient):
    name = "fixture"

    def __init__(self, settings: AppSettings):
        super().__init__(settings)
        logger.warning(
            "DEBUG: rollback_test_mode=%r",
            self.settings.rollback_test_mode,
        )
        self.root = Path(settings.fixture_dir)
        if not self.root.is_absolute():
            self.root = Path(__file__).resolve().parent / self.root
        self.state: Dict[str, Any] = {
            "qos": {},
            "geofences": {},
            "slices": {},
            "audit": [],
            "network": {},
        }
        self._initialise_network_state()

    def _initialise_network_state(self) -> None:
        rows = self._load("congestion", [])

        self.state["network"] = {
            row["cell_id"]: {
                "cell_id": row["cell_id"],
                "congestion_pct": float(row["congestion_pct"]),
                "latency_ms": float(row["latency_ms"]),
                "predicted_congestion_pct": float(
                    row["predicted_congestion_pct"]
                ),
                "congestion_level": str(row["congestion_level"]),
                "confidence_level": int(row["confidence_level"]),
                "observed_at": float(row["observed_at"]),
            }
            for row in rows
        }

    def _load(self, name: str, default: Any) -> Any:
        path = self.root / f"{name}.json"
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            logger.warning("Fixture missing: %s; using safe in-process default", path)
            return default

    async def congestion_insights(self,cell_ids: Optional[List[str]] = None,) -> List[CongestionReading]:
        rows = list(self.state["network"].values())
        if cell_ids:rows = [row for row in rows if row["cell_id"] in cell_ids]

        now = datetime.now(timezone.utc)
        readings: List[CongestionReading] = []
        for row in rows:
            observed = float(row.get("observed_at", time.time()))
            observed_at = datetime.fromtimestamp(
                observed if observed > 1_000_000_000 else now.timestamp(), timezone.utc
            )
            readings.append(CongestionReading(
                **row,
                interval_start=observed_at.isoformat(),
                interval_stop=(observed_at + timedelta(minutes=5)).isoformat(),
            ))
        return readings

    async def device_status(self, device_ids: List[str]) -> List[DeviceStatus]:
        rows = self._load("devices", [])
        selected = [x for x in rows if x["device_id"] in device_ids]
        return [DeviceStatus(**x) for x in selected]

    async def location_retrieval(self, device_ids: List[str]) -> List[Location]:
        rows = self._load("locations", [])
        return [Location(**x) for x in rows if x["device_id"] in device_ids]

    async def create_geofence(self, device_id: str, polygon_id: str) -> GeofenceSubscription:
        sub = GeofenceSubscription(
            subscription_id=f"geo-{device_id}-{int(time.time()*1000)}",
            device_id=device_id, polygon_id=polygon_id, active=True,
        )
        self.state["geofences"][sub.subscription_id] = sub.model_dump()
        return sub

    async def delete_geofence(self, subscription_id: str) -> bool:
        item = self.state["geofences"].get(subscription_id)
        if item:
            item["active"] = False
        return bool(item)

    async def request_qos(self, device_id: str, profile: str, duration_seconds: int) -> QosSession:
        cost = 0.75 if profile == "guaranteed" else 0.20
        session = QosSession(
            session_id=f"qos-{device_id}-{int(time.time()*1000)}",
            device_id=device_id, profile=profile, estimated_cost_usd=cost,
            active=True,
        )
        self.state["qos"][session.session_id] = session.model_dump()

        self._apply_qos_effect(
            device_id=device_id,
            profile=profile,
        )

        self.state["audit"].append({
            "timestamp": time.time(),
            "operation": "qos",
            "device_id": device_id,
            "profile": profile,
            "session_id": session.session_id,
        })

        return session

    def _apply_qos_effect(self, device_id: str, profile: str) -> None:
        devices = self._load("devices", [])
        device = next(
            (d for d in devices if d["device_id"] == device_id),
            None,
        )

        if not device:
            return
        cell_id = device["cell_id"]
        network = self.state["network"].get(cell_id)
        if not network:
            return
        effects = {
            "guaranteed": {
                "congestion_delta": -18.0,
                "latency_delta": -20.0,
            },
            "low-bandwidth": {
                "congestion_delta": -8.0,
                "latency_delta": -8.0,
            },
            "emergency-only": {
                "congestion_delta": -12.0,
                "latency_delta": -12.0,
            },
        }

        effect = effects.get(profile)

        if not effect:
            return

        # ---------------------------------------------------------
        # FAILURE INJECTION — TEST ONLY
        #
        # When enabled, QoD is accepted and audited normally,
        # but its KPI effect is suppressed. This intentionally
        # HARIS can exercise the rollback path.
        # ---------------------------------------------------------
        if self.settings.rollback_test_mode:
            logger.warning(
                "ROLLBACK TEST MODE: suppressing QoD KPI effect "
                "for device=%s profile=%s",
                device_id,
                profile,
            )
            return
        
        network["congestion_pct"] = max(
            0.0,
            network["congestion_pct"]
            + effect["congestion_delta"],
        )
        network["latency_ms"] = max(
            0.0,
            network["latency_ms"]
            + effect["latency_delta"],
        )
        network["predicted_congestion_pct"] = max(
            0.0,
            network["predicted_congestion_pct"]
            + effect["congestion_delta"],
        )
        level_rank = {"None": 0, "Low": 1, "Medium": 2, "High": 3}
        rank_level = {rank: level for level, rank in level_rank.items()}
        current_level = network["congestion_level"]
        if current_level not in level_rank:
            raise RuntimeError(
                f"Fixture contains unsupported congestion level: {current_level!r}"
            )
        network["congestion_level"] = rank_level[
            max(0, level_rank[current_level] - 1)
        ]
        network["observed_at"] = time.time()    

        logger.info(
            "FIXTURE KPI EFFECT: device=%s cell=%s profile=%s "
            "congestion=%.1f latency=%.1f predicted=%.1f",
            device_id,
            cell_id,
            profile,
            network["congestion_pct"],
            network["latency_ms"],
            network["predicted_congestion_pct"],
        )

    async def rollback_network_state(
        self,
        baseline: Dict[str, Dict[str, float]],
    ) -> Dict[str, Any]:
        """
        Restore the network KPI state to the pre-execution baseline.

        This is used by HARIS when post-action  fails.
        The fixture implementation restores the in-memory network state
        and records the rollback in the audit trail.
        """

        restored = []

        for cell_id, values in baseline.items():
            network = self.state["network"].get(cell_id)

            if network is None:
                logger.warning(
                    "Rollback skipped: cell %s does not exist in network state",
                    cell_id,
                )
                continue

            network["congestion_pct"] = float(
                values["congestion_pct"]
            )

            network["latency_ms"] = float(
                values["latency_ms"]
            )

            network["predicted_congestion_pct"] = float(
                values["predicted_congestion_pct"]
            )

            network["observed_at"] = time.time()

            restored.append(
                {
                    "cell_id": cell_id,
                    "congestion_pct": network["congestion_pct"],
                    "latency_ms": network["latency_ms"],
                    "predicted_congestion_pct": network[
                        "predicted_congestion_pct"
                    ],
                }
            )

        self.state["audit"].append(
            {
                "timestamp": time.time(),
                "operation": "rollback",
                "restored_cells": [x["cell_id"] for x in restored],
            }
        )

        logger.warning(
            "ROLLBACK: restored %d network cells",
            len(restored),
        )

        return {
            "rolled_back": bool(restored),
            "restored_cells": restored,
        }

    async def release_qos(self, session_id: str) -> bool:
        item = self.state["qos"].get(session_id)
        if item:
            item["active"] = False
        return bool(item)

    async def attach_slice(self, device_id: str, slice_id: str) -> SliceAttachment:
        item = SliceAttachment(device_id=device_id, slice_id=slice_id, attached=True)
        self.state["slices"][f"{device_id}:{slice_id}"] = item.model_dump()
        return item

    async def detach_slice(self, device_id: str, slice_id: str) -> SliceAttachment:
        item = SliceAttachment(device_id=device_id, slice_id=slice_id, attached=False)
        self.state["slices"][f"{device_id}:{slice_id}"] = item.model_dump()
        return item

    
    
    
class LiveNokiaClient(BaseNokiaClient):
    """Live adapter for the installed Nokia Network as Code Python SDK.

    The SDK is generated and its public namespaces can evolve. This adapter
    resolves the installed SDK namespace/method at runtime while keeping a
    strict typed HARIS interface. If a requested capability is absent, it
    fails closed rather than silently simulating a live action.
    """
    name = "live"

    def __init__(self, settings: AppSettings):
        super().__init__(settings)
        if not settings.nac_api_token:
            raise RuntimeError("NAC_API_TOKEN is required for NAC_MODE=live")
        try:
            import network_as_code as nac
        except ImportError as exc:
            raise RuntimeError("Install the Nokia SDK with: pip install network-as-code") from exc
        self._nac = nac
        self.client = nac.NetworkAsCodeClient(
            token=settings.nac_api_token.get_secret_value(),
        )
        
        self.device_phone_map = {
            "ambulance-01": "+999900000001",
            "scada-01": "+999900000002",
            "sensor-01": "+999900000003",
            "pipeline-01": "+999900000004",
            "fleet-01": "+999900000005",
            "fleet-02": "+999900000006",
            "telemetry-01": "+999900000007",
            "dispatch-01": "+999900000008",
        }

    def _resolve_slice_id(self, policy_alias: str) -> Optional[str]:
        if policy_alias == "haris-emergency" and self.settings.nac_emergency_slice_id:
            return self.settings.nac_emergency_slice_id
        return self.settings.nac_slice_id_map.get(policy_alias)

    def _geofence_area(self, polygon_id: str) -> Any:
        from network_as_code.models.geofencing import Center, Circle
        area_config = self.settings.nac_geofence_areas[polygon_id]
        return Circle(
            center=Center(latitude=area_config.latitude, longitude=area_config.longitude),
            radius=area_config.radius_m,
        )

    def action_safety_error(self, action_kind: str, parameters: Dict[str, Any]) -> Optional[str]:
        """Reject generic HARIS actions that cannot be built for this SDK safely."""
        if action_kind == "geofence":
            polygon_id = parameters.get("polygon_id")
            if not hasattr(self.client.geofencing, "subscribe"):
                return "Installed Nokia SDK does not expose geofencing.subscribe."
            if not self.settings.nac_geofence_sink:
                return "NAC_GEOFENCE_SINK is required for live geofencing."
            if not polygon_id or polygon_id not in self.settings.nac_geofence_areas:
                return "NAC_GEOFENCE_AREAS must define the requested logical polygon identifier."
            if not self.settings.nac_geofence_event_types:
                return "NAC_GEOFENCE_EVENT_TYPES must contain at least one Nokia event type."
            from network_as_code.models.geofencing import EventType
            valid_event_types = {event.value for event in EventType}
            invalid_event_types = set(self.settings.nac_geofence_event_types) - valid_event_types
            if invalid_event_types:
                return f"NAC_GEOFENCE_EVENT_TYPES contains unsupported SDK values: {sorted(invalid_event_types)}"
            return None
        if action_kind == "qos":
            profile = parameters.get("profile")
            if not self.settings.nac_qod_service_ipv4:
                return "NAC_QOD_SERVICE_IPV4 is required for live QoD session creation."
            if not profile or profile not in self.settings.nac_qod_profile_map:
                return "NAC_QOD_PROFILE_MAP must map the requested HARIS policy profile to an operator QoD profile."
            return None
        if action_kind in {"slice_attach", "slice_detach"}:
            if not hasattr(self.client, "slices"):
                return "Installed Nokia SDK does not expose the slices namespace."
            if not parameters.get("slice_id") or not self._resolve_slice_id(parameters["slice_id"]):
                return "NAC_EMERGENCY_SLICE_ID or NAC_SLICE_ID_MAP must map the requested HARIS slice alias."
            return None
        return f"Unsupported live HARIS action: {action_kind}"

    def capability_report(self) -> Dict[str, Dict[str, Any]]:
        def entry(action_kind: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
            error = self.action_safety_error(action_kind, parameters)
            if error is None:
                status = "SUPPORTED_AND_CONFIGURED"
            elif "does not expose" in error:
                status = "SDK_UNSUPPORTED"
            elif action_kind in {"qos", "slice_attach"} and (
                "PROFILE_MAP" in error or "SLICE_ID" in error
            ):
                status = "OPERATOR_VALUE_REQUIRED"
            else:
                status = "SDK_SUPPORTED_CONFIG_MISSING"
            return {"status": status, "reason": error}

        geofencing = entry("geofence", {"polygon_id": "storm-impact"})
        qod = entry("qos", {"profile": "guaranteed"})
        slicing = entry("slice_attach", {"slice_id": "haris-emergency"})
        if "guaranteed" not in self.settings.nac_qod_profile_map:
            qod["status"] = "OPERATOR_VALUE_REQUIRED"
            qod["reason"] = (
                "NAC_QOD_PROFILE_MAP must map HARIS alias 'guaranteed' to an operator QoD profile; "
                "NAC_QOD_SERVICE_IPV4 is also required before a session can be constructed."
            )
        return {
            "congestion_insights": {"status": "READ_READY", "reason": None},
            "device_status": {"status": "READ_READY", "reason": None},
            "location": {"status": "READ_READY", "reason": None},
            "geofencing": geofencing,
            "qod": qod,
            "slicing": slicing,
        }
        
    def _resolve_namespace(self, *names: str) -> Any:
        for name in names:
            obj = getattr(self.client, name, None)
            if obj is not None:
                return obj
        raise RuntimeError(f"Installed Nokia SDK does not expose any of namespaces: {names}")

    async def _call(self, candidates: List[tuple[str, str]], *args: Any, **kwargs: Any) -> Any:
        last: Optional[Exception] = None
        for namespace_name, method_name in candidates:
            try:
                namespace = self._resolve_namespace(namespace_name)
                method = getattr(namespace, method_name, None)
                if method is None:
                    continue
                return await retry_async(lambda: method(*args, **kwargs), self.retry_policy)
            except Exception as exc:
                last = exc
        raise RuntimeError(f"Nokia SDK capability unavailable for candidates={candidates}: {last}") from last

    async def congestion_insights(
        self,
        cell_ids: Optional[List[str]] = None,
    ) -> List[CongestionReading]:
        """
        Retrieve congestion information from Nokia Network as Code.

        Nokia returns categorical congestion levels and confidence evidence.
        HARIS preserves those values without manufacturing numeric KPIs.
        """

        import json

        devices_path = Path(self.settings.fixture_dir) / "devices.json"

        if not devices_path.is_absolute():
            devices_path = Path(__file__).resolve().parent / devices_path

        try:
            haris_devices = json.loads(
                devices_path.read_text(encoding="utf-8")
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                f"HARIS device metadata file not found: {devices_path}"
            ) from exc

        # HARIS devices tell us which cell each asset belongs to.
        target_cells = set(cell_ids) if cell_ids else None

        devices_by_cell = {}

        for row in haris_devices:
            cell_id = row.get("cell_id")

            if not cell_id:
                continue

            if target_cells and cell_id not in target_cells:
                continue

            devices_by_cell.setdefault(cell_id, []).append(row)

        results = []

        for cell_id, devices in devices_by_cell.items():
            # Use one representative Nokia device belonging to this cell.
            # The simulator currently exposes device-based congestion.
            device_id = devices[0]["device_id"]

            phone_number = self.device_phone_map.get(device_id)

            if not phone_number:
                logger.warning(
                    "No Nokia mapping for HARIS device=%s cell=%s; "
                    "skipping congestion lookup",
                    device_id,
                    cell_id,
                )
                continue

            device = Device(
                api=self.client._api,
                phone_number=phone_number,
            )

            data = await asyncio.to_thread(
                self.client.insights.api.congestion.fetch_congestion,
                device,
            )

            if not data:
                logger.warning(
                    "Nokia returned no congestion records for device=%s cell=%s",
                    device_id,
                    cell_id,
                )
                continue

            # Nokia normally returns multiple time intervals.
            # Use the newest interval.
            latest = max(
                data,
                key=lambda item: item.get("timeIntervalStart", ""),
            )

            level = str(
                latest.get("congestionLevel", "")
            ).strip()



            valid_levels = {"None", "Low", "Medium", "High"}

            if level not in valid_levels:
                raise RuntimeError(
                    f"Unknown Nokia congestion level: {level!r}"
                )

            confidence = latest.get("confidenceLevel")

            if confidence is None:
                raise RuntimeError(
                    f"Nokia congestion response is missing confidenceLevel "
                    f"for cell={cell_id}"
                )
            
            results.append(
                CongestionReading(
                    cell_id=cell_id,
                    congestion_level=level,
                    confidence_level=int(confidence),
                    interval_start=latest["timeIntervalStart"],
                    interval_stop=latest["timeIntervalStop"],
                )
            )

            logger.info(
                "NOKIA CONGESTION: cell=%s level=%s confidence=%s "
                "interval=%s -> %s",
                cell_id,
                level,
                confidence,
                latest["timeIntervalStart"],
                latest["timeIntervalStop"],
            )

        return results
    
    async def device_status(self, device_ids: List[str]) -> List[DeviceStatus]:
        """
        Get device reachability from Nokia and merge it with HARIS metadata.

        Nokia provides the live reachability state.
        HARIS keeps local metadata such as roaming, battery, tier and cell_id.
        """
        import json

        devices_path = Path(self.settings.fixture_dir) / "devices.json"

        if not devices_path.is_absolute():
            devices_path = Path(__file__).resolve().parent / devices_path

        try:
            haris_devices = json.loads(
                devices_path.read_text(encoding="utf-8")
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                f"HARIS device metadata file not found: {devices_path}"
            ) from exc

        metadata = {
            row["device_id"]: row
            for row in haris_devices
            if row.get("device_id") in device_ids
        }

        results = []

        for device_id in device_ids:
            phone_number = self.device_phone_map.get(device_id)

            if not phone_number:
                raise RuntimeError(
                    f"No Nokia phone mapping configured for HARIS device: {device_id}"
                )

            haris_device = metadata.get(device_id)

            if not haris_device:
                raise RuntimeError(
                    f"HARIS device metadata not found for device: {device_id}"
                )

            nokia_device = {
                "phoneNumber": phone_number
            }

            data = await asyncio.to_thread(
                self.client.device_status.api.reachability_status.get_reachability,
                nokia_device,
            )

            results.append(
                DeviceStatus(
                    device_id=device_id,
                    reachable=bool(data.get("reachable", False)),
                    roaming=bool(haris_device.get("roaming", False)),
                    battery_pct=float(haris_device["battery_pct"]),
                    tier=int(haris_device["tier"]),
                    cell_id=str(haris_device["cell_id"]),
                )
            )

        return results
    
    async def location_retrieval(self, device_ids: List[str]) -> List[Location]:
        results = []

        # The SDK's APIClient already contains the correctly configured
        # location_retrieve API instance.
        location_api = self.client._api.location_retrieve

        for device_id in device_ids:
            phone_number = self.device_phone_map.get(device_id)

            if not phone_number:
                raise RuntimeError(
                    f"No Nokia phone mapping configured for HARIS device: {device_id}"
                )

            device = Device(
                api=self.client._api,
                phone_number=phone_number,
            )

            data = await asyncio.to_thread(
                location_api.get_location,
                device,
            )

            area = data.get("area", {})
            center = area.get("center", {})

            if not center:
                raise RuntimeError(
                    f"Nokia returned no location center for HARIS device: {device_id}"
                )

            results.append(
                Location(
                    device_id=device_id,
                    latitude=float(center["latitude"]),
                    longitude=float(center["longitude"]),
                    accuracy_m=float(area.get("radius", 50.0)),
                )
            )

        return results
    async def create_geofence(self, device_id: str, polygon_id: str) -> GeofenceSubscription:
        error = self.action_safety_error("geofence", {"polygon_id": polygon_id})
        if error:
            raise RuntimeError(error)
        phone_number = self.device_phone_map.get(device_id)
        if not phone_number:
            raise RuntimeError(f"No Nokia phone mapping configured for HARIS device: {device_id}")
        device = Device(api=self.client._api, phone_number=phone_number)
        subscription = await asyncio.to_thread(
            self.client.geofencing.subscribe,
            device,
            self.settings.nac_geofence_sink,
            self.settings.nac_geofence_event_types,
            self._geofence_area(polygon_id),
            None,
            datetime.now(timezone.utc) + timedelta(seconds=self.settings.nac_geofence_expiry_seconds),
        )
        return GeofenceSubscription(
            subscription_id=subscription.event_subscription_id,
            device_id=device_id,
            polygon_id=polygon_id,
            active=True,
        )

    async def delete_geofence(self, subscription_id: str) -> bool:
        subscription = await asyncio.to_thread(self.client.geofencing.get, subscription_id)
        await asyncio.to_thread(subscription.delete)
        return True

    async def request_qos(self, device_id: str, profile: str, duration_seconds: int) -> QosSession:
        error = self.action_safety_error("qos", {"profile": profile})
        if error:
            raise RuntimeError(error)
        phone_number = self.device_phone_map.get(device_id)
        if not phone_number:
            raise RuntimeError(f"No Nokia phone mapping configured for HARIS device: {device_id}")
        device = Device(api=self.client._api, phone_number=phone_number)
        session = await asyncio.to_thread(
            device.create_qod_session,
            self.settings.nac_qod_profile_map[profile],
            duration_seconds,
            self.settings.nac_qod_service_ipv4,
            None,
            None,
            None,
            self.settings.nac_qod_sink,
        )
        return QosSession(
            session_id=session.id,
            device_id=device_id,
            profile=profile,
            estimated_cost_usd=0.0,
            active=session.status.lower() not in {"terminated", "released", "deleted"},
        )

    async def release_qos(self, session_id: str) -> bool:
        session = await asyncio.to_thread(self.client.sessions.get, session_id)
        await asyncio.to_thread(session.delete)
        return True

    async def attach_slice(self, device_id: str, slice_id: str) -> SliceAttachment:
        error = self.action_safety_error("slice_attach", {"slice_id": slice_id})
        if error:
            raise RuntimeError(error)
        phone_number = self.device_phone_map.get(device_id)
        if not phone_number:
            raise RuntimeError(f"No Nokia phone mapping configured for HARIS device: {device_id}")
        live_slice_id = self._resolve_slice_id(slice_id)
        assert live_slice_id is not None
        slice_resource = await asyncio.to_thread(self.client.slices.get, live_slice_id)
        if slice_resource is None:
            raise RuntimeError(f"Nokia slice not found: {live_slice_id}")
        device = Device(api=self.client._api, phone_number=phone_number)
        await asyncio.to_thread(slice_resource.attach, device)
        return SliceAttachment(device_id=device_id, slice_id=live_slice_id, attached=True)

    async def detach_slice(self, device_id: str, slice_id: str) -> SliceAttachment:
        error = self.action_safety_error("slice_detach", {"slice_id": slice_id})
        if error:
            raise RuntimeError(error)
        phone_number = self.device_phone_map.get(device_id)
        if not phone_number:
            raise RuntimeError(f"No Nokia phone mapping configured for HARIS device: {device_id}")
        live_slice_id = self._resolve_slice_id(slice_id)
        assert live_slice_id is not None
        slice_resource = await asyncio.to_thread(self.client.slices.get, live_slice_id)
        if slice_resource is None:
            raise RuntimeError(f"Nokia slice not found: {live_slice_id}")
        device = Device(api=self.client._api, phone_number=phone_number)
        await asyncio.to_thread(slice_resource.detach, device)
        return SliceAttachment(device_id=device_id, slice_id=live_slice_id, attached=False)

   
    
    async def rollback_network_state(
        self,
        baseline: Dict[str, Dict[str, float]],
    ) -> Dict[str, Any]:
        """
        Live rollback is intentionally fail-closed.

        HARIS fixture mode can restore its in-memory network state.
        A live Nokia rollback cannot be performed generically from a
        pre-execution KPI snapshot because the Nokia Network as Code
        APIs exposed to this adapter do not provide a generic
        "restore network KPI state" operation.
        """
        logger.error(
            "LIVE ROLLBACK REQUESTED: generic network-state rollback "
            "is not supported by the configured Nokia adapter"
        )

        raise RuntimeError(
            "Live Nokia rollback is not supported by the current "
            "Network as Code adapter. No simulated rollback was performed."
        )

    @staticmethod
    def _normalize(value: Any) -> Dict[str, Any]:
        if isinstance(value, BaseModel):
            return value.model_dump()
        if hasattr(value, "model_dump"):
            return value.model_dump()
        if hasattr(value, "__dict__"):
            return dict(value.__dict__)
        if isinstance(value, dict):
            return value
        raise TypeError(f"Unsupported SDK result type: {type(value)!r}")


def build_nokia_client(settings: Optional[AppSettings] = None) -> BaseNokiaClient:
    settings = settings or get_settings()
    if settings.is_live:
        return LiveNokiaClient(settings)
    return FixtureNokiaClient(settings)


client = build_nokia_client()
router = APIRouter(prefix="/api/nac", tags=["Nokia Network as Code"])


class PendingNumberVerification(BaseModel):
    phone_number: str = Field(min_length=3, max_length=32)
    created_at: float
    used: bool = False


class NumberVerificationStateStore:
    """Process-local, single-use OAuth state store; never logs secret values."""
    ttl_seconds = 300
    def __init__(self):
        self._pending: Dict[str, PendingNumberVerification] = {}
        self._lock = threading.Lock()
    def create(self, phone_number: str) -> str:
        state = secrets.token_urlsafe(32)
        with self._lock:
            self._pending[state] = PendingNumberVerification(phone_number=phone_number, created_at=time.time())
        return state
    def consume(self, state: str) -> PendingNumberVerification:
        with self._lock:
            item = self._pending.pop(state, None)
            if item is None or item.used:
                raise ValueError("unknown_or_replayed_state")
            if time.time() - item.created_at > self.ttl_seconds:
                raise ValueError("expired_state")
            item.used = True
            return item


number_verification_states = NumberVerificationStateStore()


class VerifiedIdentityStore:
    """Server-held verification receipts bound to a phone number and TTL."""
    def __init__(self):
        self._verified_at: Dict[str, float] = {}
        self._lock = threading.Lock()
    def record(self, phone_number: str) -> None:
        with self._lock: self._verified_at[phone_number] = time.time()
    def is_fresh(self, phone_number: str, ttl_seconds: int) -> bool:
        with self._lock:
            timestamp = self._verified_at.get(phone_number)
            if timestamp is None or time.time() - timestamp > ttl_seconds:
                return False
            return True


verified_identities = VerifiedIdentityStore()


class NumberVerificationStart(BaseModel):
    phone_number: str = Field(min_length=3, max_length=32)


class TrustedDispatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    phone_number: str = Field(min_length=3, max_length=32)


@router.post("/auth/number-verification/start")
async def number_verification_start(request: NumberVerificationStart) -> Dict[str, str]:
    settings = get_settings()
    if not settings.nac_api_token or not settings.nac_number_verification_redirect_uri:
        raise HTTPException(status_code=503, detail="Number Verification OAuth configuration is unavailable.")
    state = number_verification_states.create(request.phone_number)
    try:
        import network_as_code as nac
        oauth_client = nac.NetworkAsCodeClient(token=settings.nac_api_token.get_secret_value())
        url = await asyncio.to_thread(
            oauth_client.authorization.create_authorization_link,
            settings.nac_number_verification_redirect_uri,
            settings.nac_number_verification_scope,
            request.phone_number,
            state,
        )
    except Exception as exc:
        # State has not been used; remove it rather than retaining an unusable flow.
        with number_verification_states._lock:
            number_verification_states._pending.pop(state, None)
        logger.warning("Number Verification authorization-link generation failed")
        raise HTTPException(status_code=502, detail="Number Verification authorization is unavailable.") from exc
    # Return is necessary to redirect a user agent; it is never logged.
    return {"authorization_url": url, "expires_in_seconds": str(number_verification_states.ttl_seconds)}


@router.get("/health")
async def health() -> Dict[str, str]:
    """Unauthenticated deployment health check; does not call Nokia."""
    return {"status": "ok", "service": "HARIS", "mode": client.settings.nac_mode}


@router.get("/mode")
async def mode() -> Dict[str, Any]:
    return {
        "mode": client.settings.nac_mode,
        "label": client.settings.operating_mode_label,
        "writes_enabled": client.settings.allows_network_writes,
    }


@router.get("/capabilities")
async def capabilities() -> Dict[str, Any]:
    return {"mode": client.settings.nac_mode, "capabilities": client.capability_report()}


@router.get("/incidents")
async def incidents() -> List[Dict[str, Any]]:
    return [item.model_dump() for item in MemoryStore(client.settings).recent_incidents()]


@router.get("/incidents/{cycle_id}")
async def incident_replay(cycle_id: str) -> Dict[str, Any]:
    item = MemoryStore(client.settings).get_incident(cycle_id)
    if not item:
        raise HTTPException(status_code=404, detail="Incident record not found.")
    return item.model_dump()


async def _wrap(fn: Callable[[], Any]) -> Any:
    start = time.perf_counter()
    try:
        data = await fn()
        latency = (time.perf_counter() - start) * 1000
        return data, latency
    except Exception as exc:
        logger.exception("CAMARA operation failed")
        raise HTTPException(status_code=502, detail=str(exc)) from exc


def _require_write_mode() -> None:
    """Keep the API facade aligned with HARIS's central mode policy."""
    if not client.settings.allows_network_writes:
        raise HTTPException(
            status_code=403,
            detail="Network writes are disabled in LIVE_READ_ONLY mode.",
        )


class GeofenceCallback(BaseModel):
    event_type: str = Field(alias="type", min_length=1, max_length=200)
    event_id: Optional[str] = Field(default=None, max_length=200)


@router.post("/callbacks/nokia/geofence")
async def geofence_callback(event: GeofenceCallback) -> Dict[str, str]:
    """Receive only known geofence events; never triggers network mutation."""
    allowed = {
        "org.camaraproject.geofencing-subscriptions.v0.area-entered",
        "org.camaraproject.geofencing-subscriptions.v0.area-left",
    }
    if event.event_type not in allowed:
        raise HTTPException(status_code=422, detail="Unsupported geofence event type.")
    logger.info("Received validated Nokia geofence event type=%s", event.event_type)
    return {"status": "accepted"}


@router.get("/congestion", response_model=ApiResponse[List[CongestionReading]])
async def congestion(cell_ids: Optional[List[str]] = None):
    data, latency = await _wrap(lambda: client.congestion_insights(cell_ids))
    return ApiResponse(data=data, source=client.name, latency_ms=latency)

@router.get("/auth/number-verification/callback")
async def number_verification_callback(code: str, state: str) -> Dict[str, str]:
    """Complete Nokia Number Verification OAuth callback."""
    if not code.strip() or not state.strip():
        raise HTTPException(
            status_code=400,
            detail="Missing OAuth authorization code or state.",
        )

    try:
        pending = number_verification_states.consume(state)
        settings = get_settings()

        if not settings.nac_api_token:
            raise HTTPException(
                status_code=503,
                detail="Nokia API token is not configured.",
            )

        import network_as_code as nac

        oauth_client = nac.NetworkAsCodeClient(
            token=settings.nac_api_token.get_secret_value(),
        )

        device = oauth_client.devices.get(
            phone_number=pending.phone_number
        )

        verified = device.verify_number(
            code=code,
            state=state,
        )

    except HTTPException:
        raise

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid, expired, or already-used OAuth state.")

    except Exception as exc:
        logger.exception("Nokia Number Verification failed.")
        raise HTTPException(
            status_code=502,
            detail="Number Verification request failed.",
        ) from exc

    logger.info("Completed Nokia Number Verification; verified=%s", bool(verified))

    if verified:
        verified_identities.record(pending.phone_number)

    return {
        "status": "verified" if verified else "not_verified",
        "phone_number_verified": str(bool(verified)).lower(),
    }


@router.post("/trusted-dispatch/evaluate")
async def trusted_dispatch(request: TrustedDispatchRequest) -> Dict[str, Any]:
    """Sensitive dispatch trust gate; independent from fixture network client."""
    settings = get_settings()
    if not verified_identities.is_fresh(request.phone_number, settings.trusted_dispatch_verification_ttl_seconds):
        return {"decision": "BLOCK", "number_verified": False, "recent_sim_swap": None, "reason": "Fresh server-side Number Verification is required."}
    if not settings.nac_api_token:
        return {"decision": "BLOCK", "number_verified": True, "recent_sim_swap": None, "reason": "SIM Swap verification unavailable."}
    try:
        import network_as_code as nac
        identity_client = nac.NetworkAsCodeClient(token=settings.nac_api_token.get_secret_value())
        device = identity_client.devices.get(phone_number=request.phone_number)
        recent = bool(device.verify_sim_swap(settings.trusted_dispatch_sim_swap_window_seconds))
    except Exception:
        logger.warning("Trusted Dispatch SIM Swap verification failed closed")
        return {"decision": "BLOCK", "number_verified": True, "recent_sim_swap": None, "reason": "SIM Swap verification unavailable."}
    if recent:
        return {"decision": "BLOCK", "number_verified": True, "recent_sim_swap": True, "reason": "Recent SIM swap detected."}
    return {"decision": "ALLOW", "number_verified": True, "recent_sim_swap": False, "reason": "Number Verification passed and no recent SIM swap was detected."}


@router.post("/device-status", response_model=ApiResponse[List[DeviceStatus]])
async def device_status(device_ids: List[str]):
    data, latency = await _wrap(lambda: client.device_status(device_ids))
    return ApiResponse(data=data, source=client.name, latency_ms=latency)


@router.post("/location", response_model=ApiResponse[List[Location]])
async def location(device_ids: List[str]):
    data, latency = await _wrap(lambda: client.location_retrieval(device_ids))
    return ApiResponse(data=data, source=client.name, latency_ms=latency)


class GeofenceRequest(BaseModel):
    device_id: str
    polygon_id: str


@router.post("/geofence", response_model=ApiResponse[GeofenceSubscription])
async def geofence(req: GeofenceRequest):
    _require_write_mode()
    data, latency = await _wrap(lambda: client.create_geofence(req.device_id, req.polygon_id))
    return ApiResponse(data=data, source=client.name, latency_ms=latency)


@router.delete("/geofence/{subscription_id}", response_model=ApiResponse[bool])
async def geofence_delete(subscription_id: str):
    _require_write_mode()
    data, latency = await _wrap(lambda: client.delete_geofence(subscription_id))
    return ApiResponse(data=data, source=client.name, latency_ms=latency)


class QosRequest(BaseModel):
    device_id: str
    profile: str
    duration_seconds: int = Field(default=300, ge=30, le=3600)


@router.post("/qos", response_model=ApiResponse[QosSession])
async def qos(req: QosRequest):
    _require_write_mode()
    data, latency = await _wrap(lambda: client.request_qos(req.device_id, req.profile, req.duration_seconds))
    return ApiResponse(data=data, source=client.name, latency_ms=latency)


@router.delete("/qos/{session_id}", response_model=ApiResponse[bool])
async def qos_delete(session_id: str):
    _require_write_mode()
    data, latency = await _wrap(lambda: client.release_qos(session_id))
    return ApiResponse(data=data, source=client.name, latency_ms=latency)


class SliceRequest(BaseModel):
    device_id: str
    slice_id: str


@router.post("/slice/attach", response_model=ApiResponse[SliceAttachment])
async def slice_attach(req: SliceRequest):
    _require_write_mode()
    data, latency = await _wrap(lambda: client.attach_slice(req.device_id, req.slice_id))
    return ApiResponse(data=data, source=client.name, latency_ms=latency)


@router.post("/slice/detach", response_model=ApiResponse[SliceAttachment])
async def slice_detach(req: SliceRequest):
    _require_write_mode()
    data, latency = await _wrap(lambda: client.detach_slice(req.device_id, req.slice_id))
    return ApiResponse(data=data, source=client.name, latency_ms=latency)


def create_fastapi_app() -> FastAPI:
    api = FastAPI(title="HARIS Network Control API", version="1.0.0")
    api.include_router(router)
    return api


app = create_fastapi_app()
