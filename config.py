from __future__ import annotations

from enum import Enum
from functools import lru_cache
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class QualityLevel(str, Enum):
    EXCELLENT = "excellent"
    GOOD = "good"
    POOR = "poor"
    CRITICAL = "critical"


class EnvironmentalSource(str, Enum):
    LIVE = "LIVE"
    CACHED = "CACHED"
    FIXTURE = "FIXTURE"
    UNAVAILABLE = "UNAVAILABLE"

class CongestionPolicy(BaseModel):
    """
    Deterministic HARIS policy for Nokia/CAMARA congestion levels.

    The values here are NOT converted into percentages.
    Nokia's congestionLevel remains the source-of-truth.
    """

    level: str
    quality: QualityLevel
    decision: str


class NetworkQualityThresholds(BaseModel):
    """Numeric HARIS policy for fixture data and the KPI operations view."""

    excellent_congestion_lt: float = 30.0
    excellent_latency_lt_ms: float = 10.0
    good_congestion_min: float = 30.0
    good_congestion_max: float = 70.0
    good_latency_min_ms: float = 20.0
    good_latency_max_ms: float = 50.0
    poor_congestion_min: float = 70.0
    poor_congestion_max: float = 80.0
    poor_latency_min_ms: float = 50.0
    poor_latency_max_ms: float = 100.0
    critical_congestion_gt: float = 85.0
    critical_latency_gt_ms: float = 100.0

class QualityMatrix(BaseModel):
    """
    Maps Nokia/CAMARA congestion levels to HARIS policy outcomes.

    IMPORTANT:
    - No synthetic congestion percentages.
    - No synthetic latency values.
    - No synthetic prediction values.
    """

    none: CongestionPolicy = CongestionPolicy(
        level="None",
        quality=QualityLevel.EXCELLENT,
        decision="primary_path",
    )

    low: CongestionPolicy = CongestionPolicy(
        level="Low",
        quality=QualityLevel.GOOD,
        decision="normal_operation",
    )

    medium: CongestionPolicy = CongestionPolicy(
        level="Medium",
        quality=QualityLevel.POOR,
        decision="load_balance_away_from_edge",
    )

    high: CongestionPolicy = CongestionPolicy(
        level="High",
        quality=QualityLevel.CRITICAL,
        decision="protect_critical_assets",
    )
    numeric: NetworkQualityThresholds = Field(default_factory=NetworkQualityThresholds)

    def classify_metrics(self, congestion_pct: float, latency_ms: float) -> QualityLevel:
        """Conservatively classify numeric operator KPIs using HARIS thresholds."""
        t = self.numeric
        if congestion_pct > t.critical_congestion_gt or latency_ms > t.critical_latency_gt_ms:
            return QualityLevel.CRITICAL
        if congestion_pct >= t.poor_congestion_min or latency_ms >= t.poor_latency_min_ms:
            return QualityLevel.POOR
        if congestion_pct >= t.good_congestion_min or latency_ms >= t.good_latency_min_ms:
            return QualityLevel.GOOD
        return QualityLevel.EXCELLENT

    def classify(self, congestion_level: str) -> QualityLevel:
        """
        Classify a Nokia congestion level deterministically.

        The value is taken directly from Nokia/CAMARA.
        """

        normalized = congestion_level.strip().lower()

        mapping = {
            "none": self.none.quality,
            "low": self.low.quality,
            "medium": self.medium.quality,
            "high": self.high.quality,
        }

        if normalized not in mapping:
            raise ValueError(
                f"Unsupported Nokia congestion level: {congestion_level!r}"
            )

        return mapping[normalized]

    def policy(self, congestion_level: str) -> CongestionPolicy:
        """
        Return the deterministic HARIS policy for a Nokia congestion level.
        """

        normalized = congestion_level.strip().lower()

        mapping = {
            "none": self.none,
            "low": self.low,
            "medium": self.medium,
            "high": self.high,
        }

        if normalized not in mapping:
            raise ValueError(
                f"Unsupported Nokia congestion level: {congestion_level!r}"
            )

        return mapping[normalized]

    




class Guardrails(BaseModel):
    qos_spend_ceiling_usd: float = Field(default=5.0, gt=0)
    max_devices_reconfigured_per_cycle: int = Field(default=2, ge=1, le=1000)
    rollback_seconds: int = Field(default=300, ge=30, le=3600)
    human_approval_blast_radius: float = Field(default=0.70, ge=0.0, le=1.0)
    minimum_confidence: float = Field(default=0.72, ge=0.0, le=1.0)
    cycle_seconds: int = Field(default=60, ge=10, le=3600)


class DevicePolicy(BaseModel):
    device_id: str
    mission_tier: int = Field(ge=1, le=3)
    max_qos_cost_usd: float = Field(default=1.0, ge=0)
    emergency_slice: str = "haris-emergency"
    allow_autonomous_action: bool = True


class GeofenceArea(BaseModel):
    """A concrete circular area required by Nokia NaC geofencing."""

    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    radius_m: float = Field(gt=0)


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "HARIS"
    environment: str = "demo"
    log_level: str = "INFO"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    streamlit_port: int = 8501
    cycle_seconds: int = 60
    enable_continuous_loop: bool = False
    enable_live_write_loop: bool = False

    nac_mode: str = "fixture"
    rollback_test_mode: bool = False
    nac_api_token: Optional[SecretStr] = None
    nac_base_url: str = "https://networkascode.nokia.io"
    fixture_dir: str = "fixtures"
    nac_geofence_sink: Optional[str] = None
    nac_geofence_areas: Dict[str, GeofenceArea] = Field(default_factory=dict)
    nac_geofence_event_types: List[str] = Field(default_factory=lambda: [
        "org.camaraproject.geofencing-subscriptions.v0.area-entered",
        "org.camaraproject.geofencing-subscriptions.v0.area-left",
    ])
    nac_geofence_expiry_seconds: int = Field(default=300, ge=30, le=86400)
    nac_qod_profile_map: Dict[str, str] = Field(default_factory=dict)
    nac_qod_service_ipv4: Optional[str] = None
    nac_qod_sink: Optional[str] = None
    nac_emergency_slice_id: Optional[str] = None
    nac_slice_id_map: Dict[str, str] = Field(default_factory=dict)
    nac_number_verification_redirect_uri: Optional[str] = None
    nac_number_verification_scope: str = "dpv:FraudPreventionAndAuthentication#number-verification:verify-read"
    trusted_dispatch_verification_ttl_seconds: int = Field(default=300, ge=1, le=3600)
    trusted_dispatch_sim_swap_window_seconds: int = Field(default=86400, ge=1)

    gemini_api_key: Optional[SecretStr] = None
    groq_api_key: Optional[SecretStr] = None
    gemini_model: str = "gemini-2.5-flash"
    groq_model: str = "llama-3.3-70b-versatile"

    supabase_url: Optional[str] = None
    supabase_key: Optional[SecretStr] = None
    mem0_api_key: Optional[SecretStr] = None

    public_dust_feed_url: Optional[str] = None
    operator_tenant: str = "haris-demo"
    registered_devices: List[str] = Field(default_factory=lambda: [
        "ambulance-01", "scada-01", "pipeline-01", "dispatch-01",
        "sensor-01", "fleet-01", "fleet-02", "telemetry-01",
    ])

    quality_matrix: QualityMatrix = Field(default_factory=QualityMatrix)
    guardrails: Guardrails = Field(default_factory=Guardrails)

    @field_validator("nac_mode")
    @classmethod
    def validate_nac_mode(cls, value: str) -> str:
        value = value.lower().strip()
        # ``live`` was the original setting.  Preserve it as a safe migration
        # path: an existing live configuration must never gain write authority
        # merely because HARIS learned about a write-enabled mode.
        if value == "live":
            return "live_read_only"
        if value not in {"fixture", "live_read_only", "live_write"}:
            raise ValueError(
                "NAC_MODE must be 'fixture', 'live_read_only', or 'live_write'"
            )
        return value

    @property
    def is_live(self) -> bool:
        return self.nac_mode in {"live_read_only", "live_write"}

    @property
    def allows_network_writes(self) -> bool:
        return self.nac_mode in {"fixture", "live_write"}

    @property
    def operating_mode_label(self) -> str:
        return {
            "fixture": "FIXTURE / FULL DEMO",
            "live_read_only": "LIVE / READ ONLY",
            "live_write": "LIVE / WRITE ENABLED",
        }[self.nac_mode]

    @property
    def has_live_llm(self) -> bool:
        return bool(self.gemini_api_key or self.groq_api_key)

    @property
    def has_supabase(self) -> bool:
        return bool(self.supabase_url and self.supabase_key)

    @property
    def has_mem0(self) -> bool:
        return bool(self.mem0_api_key)


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    return AppSettings()
