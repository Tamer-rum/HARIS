from __future__ import annotations

from enum import Enum
from functools import lru_cache
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from runtime import external_access_policy


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

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        """Keep TEST settings deterministic while preserving explicit inputs."""
        if external_access_policy().is_test:
            return (init_settings,)
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )

    app_name: str = "HARIS"
    environment: str = "demo"
    log_level: str = "INFO"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    streamlit_port: int = 8501
    # Public Render API base URL used by a separately deployed Streamlit
    # supervisor. It is configuration, not a credential.
    haris_backend_url: Optional[str] = None
    # Separate server/operator and Streamlit-client credentials for the
    # authenticated operational API boundary. Values are never serialized.
    haris_operational_api_token: Optional[SecretStr] = None
    haris_backend_api_token: Optional[SecretStr] = None
    cycle_seconds: int = 60
    enable_continuous_loop: bool = False
    enable_live_write_loop: bool = False
    # Separate read-only Nokia evidence cadence. It never invokes LangGraph or
    # any Nokia mutation and is disabled until deliberately enabled.
    nokia_observation_enabled: bool = False
    nokia_observation_interval_seconds: int = Field(default=3, ge=1, le=300)
    # Conservative product defaults, not Nokia-proven rate-limit guidance.
    nokia_congestion_interval_seconds: int = Field(default=10, ge=1, le=3600)
    nokia_reachability_interval_seconds: int = Field(default=15, ge=1, le=3600)
    nokia_location_interval_seconds: int = Field(default=60, ge=1, le=3600)
    nokia_observation_timeout_seconds: int = Field(default=2, ge=1, le=30)
    nokia_observation_history_limit: int = Field(default=120, ge=1, le=10_000)
    nokia_observation_max_backoff_seconds: int = Field(default=30, ge=3, le=900)
    # Event-driven incident correlation over read-only observation evidence.
    # These are HARIS safety limits, not Nokia platform limits.
    max_active_incidents: int = Field(default=2, ge=1, le=20)
    max_parallel_executions: int = Field(default=1, ge=1, le=10)
    # Durable provider-read continuation policy. These values govern safe
    # reconciliation reads only; they never authorize mutation retries.
    durable_reconciliation_max_attempts: int = Field(default=4, ge=1, le=20)
    durable_reconciliation_base_backoff_seconds: int = Field(default=5, ge=1, le=3600)
    durable_reconciliation_max_backoff_seconds: int = Field(default=30, ge=1, le=86400)
    durable_reconciliation_deadline_seconds: int = Field(default=120, ge=5, le=86400)
    durable_reconciliation_scan_seconds: int = Field(default=5, ge=1, le=300)
    incident_recurrence_cooldown_seconds: int = Field(default=120, ge=0, le=86400)
    incident_recovery_low_observations: int = Field(default=2, ge=1, le=20)
    geofencing_monitoring_enabled: bool = True
    # Energy Guard requires sustained evidence rather than one isolated High
    # observation. Production history uses HARIS observation timestamps.
    energy_guard_sustained_congestion_seconds: int = Field(default=600, ge=60, le=86400)
    energy_guard_max_observation_gap_seconds: int = Field(default=120, ge=10, le=3600)
    energy_guard_battery_threshold_pct: float = Field(default=25.0, ge=0, le=100)
    capacity_harvest_min_bulk_devices: int = Field(default=2, ge=1, le=1000)

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
    # Nokia Fast OAuth scope validated against the simulator; SDK resolves client credentials.
    nac_number_verification_scope: str = "dpv:FraudPreventionAndDetection number-verification:verify"
    trusted_dispatch_verification_ttl_seconds: int = Field(default=300, ge=1, le=3600)
    # Nokia SDK Device.verify_sim_swap(max_age) expects hours, not seconds.
    trusted_dispatch_sim_swap_max_age_hours: int = Field(default=240, ge=1)
    # Registry data is operational policy, never client supplied.  The fixture
    # registry deliberately contains demo-only contacts; production must point
    # this at an access-controlled source of authorised engineer records.
    authorized_engineer_registry_path: str = "fixtures/authorized_engineers.json"
    trusted_dispatch_max_attempts: int = Field(default=3, ge=1, le=10)
    # Event ingestion is server-side only.  No event endpoint accepts a
    # provider notification unless this shared secret is configured.
    nokia_event_webhook_secret: Optional[SecretStr] = None

    gemini_api_key: Optional[SecretStr] = None
    groq_api_key: Optional[SecretStr] = None
    gemini_model: str = "gemini-2.5-flash"
    groq_model: str = "llama-3.3-70b-versatile"
    # Advisory inference is bounded so model-provider latency never holds the
    # deterministic LangGraph control loop open indefinitely.
    ai_provider_timeout_seconds: int = Field(default=8, ge=1, le=30)
    crewai_timeout_seconds: int = Field(default=25, ge=1, le=55)

    # Durable audit persistence is backend-only. Configure these exclusively on
    # Render/FastAPI; Streamlit consumes the sanitized backend history API and
    # must never receive SUPABASE_KEY.
    haris_history_persistence_enabled: bool = False
    # Domain persistence is selected explicitly; credentials alone never alter
    # runtime authority. Postgres mode uses the server-only 6C.2 transport and
    # fails closed whenever its explicit runtime/network gates are not open.
    haris_persistence_mode: str = "memory"
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
    def has_durable_history(self) -> bool:
        return bool(self.haris_history_persistence_enabled and self.has_supabase)

    @property
    def has_mem0(self) -> bool:
        return bool(self.mem0_api_key)


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    policy = external_access_policy()
    if policy.is_test:
        # Explicit constructor values beat .env.  A hostile local production
        # environment can therefore never turn normal tests into live clients.
        return AppSettings(
            nac_mode="fixture", nokia_observation_enabled=False,
            enable_continuous_loop=False, enable_live_write_loop=False,
            nac_api_token=None, gemini_api_key=None, groq_api_key=None,
            supabase_url=None, supabase_key=None, mem0_api_key=None,
            haris_history_persistence_enabled=False, public_dust_feed_url=None,
            haris_backend_url=None, haris_operational_api_token=None,
            haris_backend_api_token=None, nokia_event_webhook_secret=None,
        )
    return AppSettings()
