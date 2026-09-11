"""Manually gated one-device real QoD validation harness.

Importing this module is inert. ``--preflight`` and ``--dry-run`` are local
only. Provider/persistence composition exists exclusively behind ``--execute``
plus the environment gates and a short-lived local confirmation challenge.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import inspect
import json
import os
import re
import secrets
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from durable_core import RepositoryUnavailable, VersionConflict


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


RUN_PREFIX = "REAL-QOD-TEST-"
CHALLENGE_TTL_SECONDS = 300
SAFE_STAGES = (
    "LOCAL_GATES", "PERSISTENCE_READY", "DURABLE_CONTEXT", "WARDEN",
    "ACTION_SCOPE", "EXECUTION", "RECONCILIATION", "NETWORK_VERIFICATION",
    "CLEANUP", "CLEANUP_VERIFICATION", "EVIDENCE_REPORT",
)
FROZEN_MIGRATION_HASHES = {
    "002_haris_durable_event_incident_core.sql": "7afe3fb2b16def35d6b73a0ec5d17ee95f0462de9d64523d7ef5c231e9dead1f",
    "003_haris_conflict_status_alignment.sql": "8ef41c1d77eb4a44afc463cc4d618a56733bb83e12d3a5975ea6fb408b0e2122",
    "004_haris_outbox_claim_isolation.sql": "bd898186e26b60c8807a588760779c1a86d73c136b81089206f228427a5210d5",
    "005_haris_outbox_per_run_isolation.sql": "4f18b170281c78cf4611085633d31b24ebbdeDD9a85f5e6d022e73e9a26a1faf".lower(),
}


def _canonical_migration_bytes(raw: bytes) -> bytes:
    """Normalize only platform line endings for checkout-portable hashing."""
    return raw.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
SAFE_OUTPUT_KEYS = frozenset({
    "status", "stage", "run_id", "capability", "target_alias", "target_count",
    "scope", "persistence_required", "warden_required", "live_write_required",
    "cleanup_required", "verification_required", "challenge", "challenge_expires_at",
    "incident_id", "action_id", "warden_result", "action_state", "provider_state",
    "verification_state", "cleanup_state", "safe_reason", "attempt_number",
    "durability_success", "safety_success", "provider_success",
    "network_verification_success", "cleanup_success", "final_classification",
    "provenance", "summary", "started_at", "completed_at", "stages", "checks",
    "setup_evidence", "provider_evidence", "verification_evidence",
    "passed", "details", "mutation_budget", "safe_read_budget",
    "CREATE", "DELETE", "exception_class", "candidate_count",
    "authoritative_count", "unavailable_count", "target_present",
})
FORBIDDEN_TEXT = re.compile(
    r"(?:[?&](?:code|state|token|access_token|client_secret)=|\bbearer\s+|\+\d{8,15}\b)", re.I,
)
SAFE_PERSISTENCE_REASONS = frozenset({
    "PERSISTENCE_NOT_CONFIGURED",
    "PERSISTENCE_UNAVAILABLE",
    "PERSISTENCE_AUTH_FAILED",
    "PERSISTENCE_SCHEMA_NOT_READY",
    "PERSISTENCE_NETWORK_BLOCKED",
    "PERSISTENCE_HOST_NOT_ALLOWED",
    "PERSISTENCE_RPC_CONTRACT_FAILED",
    "PERSISTENCE_RESPONSE_INVALID",
})
SAFE_EXCEPTION_CLASSES = frozenset({
    "SDK_CLIENT_ERROR", "RESPONSE_SHAPE_ERROR", "MAPPING_ERROR",
    "CONTRACT_ERROR", "PERSISTENCE_RPC_ERROR", "TARGET_EVIDENCE_MISSING",
    "OBSERVATION_EMPTY",
})
SAFE_DIAGNOSTIC_METADATA = frozenset({
    "candidate_count", "authoritative_count", "unavailable_count",
    "target_present",
})


class ValidationAbort(RuntimeError):
    def __init__(
        self, stage: str, safe_reason: str, *,
        classification: str = "ABORTED_SAFE",
        exception_class: Optional[str] = None,
    ):
        super().__init__(safe_reason)
        self.stage = stage if stage in SAFE_STAGES else "LOCAL_GATES"
        self.safe_reason = _safe_symbol(safe_reason)
        self.exception_class = (
            exception_class if exception_class in SAFE_EXCEPTION_CLASSES else None
        )
        self.classification = classification if classification in {
            "ABORTED_SAFE", "RECONCILIATION_REQUIRED", "REAL_PARTIAL",
        } else "ABORTED_SAFE"


def _safe_exception_class(exc: Exception, *, provider_boundary: bool = False) -> str:
    """Classify failures without serializing exception text or provider data."""
    if isinstance(exc, (RepositoryUnavailable, VersionConflict)):
        return "PERSISTENCE_RPC_ERROR"
    if exc.__class__.__name__ in {"ValidationError", "SchemaError"}:
        return "RESPONSE_SHAPE_ERROR"
    if isinstance(exc, (KeyError, TypeError, ValueError, OSError)):
        return "MAPPING_ERROR"
    if provider_boundary:
        return "SDK_CLIENT_ERROR"
    return "CONTRACT_ERROR"


def _safe_symbol(value: Any, default: str = "VALIDATION_ABORTED") -> str:
    text = str(value or default).upper()
    return text if re.fullmatch(r"[A-Z0-9_]{1,96}", text) else default


def _truth(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() == "true"


def _target_alias(target: str) -> str:
    return "QOD-TARGET-" + hashlib.sha256(target.encode()).hexdigest()[:10].upper()


def _one_identifier(value: Optional[str]) -> bool:
    return bool(value and value.strip() == value and not re.search(r"[,;\r\n*]", value))


def _sanitize(value: Any, *, key: Optional[str] = None) -> Any:
    if key is not None and key not in SAFE_OUTPUT_KEYS:
        raise ValueError("unsafe evidence field")
    if isinstance(value, dict):
        return {name: _sanitize(item, key=name) for name, item in value.items()}
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    if isinstance(value, str):
        if FORBIDDEN_TEXT.search(value):
            raise ValueError("unsafe evidence value")
        return value[:512]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    raise ValueError("unsafe evidence type")


@dataclass(frozen=True)
class ValidationConfiguration:
    runtime_env: str
    dedicated_gate: bool
    live_write_gate: bool
    nac_mode: str
    persistence_mode: str
    target_device: str
    target_cell: str
    continuous_loop: bool
    token_present: bool
    persistence_config_present: bool
    qod_profile_present: bool
    qod_service_ip_present: bool
    max_reads: int = 4

    @classmethod
    def from_environment(cls, environment: Dict[str, str]) -> "ValidationConfiguration":
        try:
            max_reads = int(environment.get("DURABLE_RECONCILIATION_MAX_ATTEMPTS", "4"))
        except ValueError:
            max_reads = 0
        try:
            profile_map = json.loads(environment.get("NAC_QOD_PROFILE_MAP", "{}"))
        except (TypeError, ValueError):
            profile_map = {}
        return cls(
            runtime_env=environment.get("HARIS_RUNTIME_ENV", "").strip().lower(),
            dedicated_gate=_truth(environment.get("HARIS_ALLOW_REAL_QOD_VALIDATION")),
            live_write_gate=_truth(environment.get("ENABLE_LIVE_WRITE_LOOP")),
            nac_mode=environment.get("NAC_MODE", "").strip().lower(),
            persistence_mode=environment.get("HARIS_PERSISTENCE_MODE", "").strip().lower(),
            target_device=environment.get("HARIS_REAL_QOD_TEST_DEVICE", ""),
            target_cell=environment.get("HARIS_REAL_QOD_TEST_CELL", ""),
            continuous_loop=_truth(environment.get("ENABLE_CONTINUOUS_LOOP")),
            token_present=bool(environment.get("NAC_API_TOKEN")),
            persistence_config_present=bool(environment.get("SUPABASE_URL") and environment.get("SUPABASE_KEY")),
            qod_profile_present=bool(
                isinstance(profile_map, dict) and profile_map.get("guaranteed")
            ),
            qod_service_ip_present=bool(environment.get("NAC_QOD_SERVICE_IPV4")),
            max_reads=max_reads,
        )

    @property
    def alias(self) -> str:
        return _target_alias(self.target_device) if self.target_device else "UNAVAILABLE"


def _migration_checks(root: Path = PROJECT_ROOT) -> list[Dict[str, Any]]:
    checks = []
    for name, expected in FROZEN_MIGRATION_HASHES.items():
        path = root / "supabase" / "migrations" / name
        actual = (
            hashlib.sha256(_canonical_migration_bytes(path.read_bytes())).hexdigest()
            if path.is_file() else "missing"
        )
        checks.append({"passed": actual == expected, "details": f"MIGRATION_{name[:3]}_FROZEN"})
    return checks


def _validation_target_metadata_ready(
    config: ValidationConfiguration, root: Path = PROJECT_ROOT,
) -> bool:
    """Validate the one target against HARIS's configured tier/cell metadata."""
    try:
        rows = json.loads((root / "fixtures" / "devices.json").read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return False
    matches = [row for row in rows if row.get("device_id") == config.target_device]
    return bool(
        len(matches) == 1
        and matches[0].get("tier") == 1
        and matches[0].get("cell_id") == config.target_cell
    )


def _sanitization_active() -> bool:
    try:
        _sanitize({"token": "must-not-pass"})
    except ValueError:
        return True
    return False


def preflight(config: ValidationConfiguration, *, root: Path = PROJECT_ROOT) -> Dict[str, Any]:
    """Local/static only: never constructs a transport or Nokia client."""
    from durable_execution import DurableActionExecutionService, ExistingNokiaActuatorAdapter
    from durable_reconciliation import DurableActionReconciliationService
    from nokia_clients import LiveNokiaClient

    checks = [
        {"passed": config.runtime_env == "real_qod_validation", "details": "RUNTIME_ENV"},
        {"passed": config.dedicated_gate, "details": "DEDICATED_GATE"},
        {"passed": config.live_write_gate, "details": "LIVE_WRITE_GATE"},
        {"passed": config.nac_mode == "live_write", "details": "LIVE_WRITE_MODE"},
        {"passed": config.persistence_mode == "postgres", "details": "POSTGRES_REQUIRED"},
        {"passed": config.persistence_config_present, "details": "PERSISTENCE_CONFIGURATION_PRESENT"},
        {"passed": _one_identifier(config.target_device), "details": "ONE_TEST_DEVICE"},
        {"passed": _one_identifier(config.target_cell), "details": "ONE_TEST_CELL"},
        {"passed": _validation_target_metadata_ready(config, root),
         "details": "ONE_TEST_DEVICE_METADATA"},
        {"passed": not config.continuous_loop, "details": "GENERIC_AUTONOMY_DISABLED"},
        {"passed": config.token_present, "details": "NOKIA_CONFIGURATION_PRESENT"},
        {"passed": config.qod_profile_present and config.qod_service_ip_present, "details": "QOD_CONFIGURATION_PRESENT"},
        {"passed": 1 <= config.max_reads <= 20, "details": "SAFE_READ_BUDGET"},
        {"passed": all(inspect.getattr_static(ExistingNokiaActuatorAdapter, name, None) is not None
                       for name in ("execute", "reconcile", "verify", "rollback", "verify_rollback")),
         "details": "QOD_ADAPTER_CONTRACT"},
        {"passed": all(inspect.getattr_static(LiveNokiaClient, name, None) is not None
                       for name in ("request_qos", "release_qos", "congestion_insights", "device_status")),
         "details": "NOKIA_QOD_CREATE_READ_DELETE_CONTRACT"},
        {"passed": inspect.getattr_static(DurableActionExecutionService, "execute_ready_action", None) is not None,
         "details": "PHASE_7C_PRESENT"},
        {"passed": inspect.getattr_static(DurableActionReconciliationService, "reconcile_action", None) is not None,
         "details": "PHASE_7D_PRESENT"},
        {"passed": _sanitization_active(), "details": "OUTPUT_SANITIZATION_ACTIVE"},
        *_migration_checks(root),
    ]
    passed = all(item["passed"] for item in checks)
    return _sanitize({
        "status": "PREFLIGHT_PASS" if passed else "ABORTED_SAFE",
        "stage": "LOCAL_GATES", "capability": "QOD", "target_count": 1,
        "target_alias": config.alias, "checks": checks,
        "safe_reason": "PREFLIGHT_OK" if passed else "PREFLIGHT_FAILED",
    })


class ChallengeStore:
    def __init__(self, directory: Path, *, clock: Callable[[], float] = time.time):
        self.directory = directory
        self.clock = clock

    def _path(self, run_id: str) -> Path:
        if not re.fullmatch(r"REAL-QOD-TEST-[0-9a-f]{32}", run_id):
            raise ValidationAbort("LOCAL_GATES", "INVALID_RUN_ID")
        return self.directory / f".{run_id}.challenge.json"

    def issue(self, run_id: str, target_alias: str) -> tuple[str, float]:
        self.directory.mkdir(parents=True, exist_ok=True)
        token = secrets.token_urlsafe(24)
        expiry = self.clock() + CHALLENGE_TTL_SECONDS
        record = {
            "run_id": run_id, "capability": "QOD", "target_alias": target_alias,
            "scope": "ONE_ACTION", "expires_at": expiry,
            "challenge_hash": hashlib.sha256(token.encode()).hexdigest(), "consumed": False,
        }
        path = self._path(run_id)
        with path.open("x", encoding="utf-8") as handle:
            json.dump(record, handle, sort_keys=True, separators=(",", ":"))
        return token, expiry

    def consume(self, run_id: str, target_alias: str, token: str) -> None:
        path = self._path(run_id)
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            raise ValidationAbort("LOCAL_GATES", "CONFIRMATION_CHALLENGE_INVALID") from None
        valid = (
            record.get("run_id") == run_id and record.get("capability") == "QOD"
            and record.get("target_alias") == target_alias and record.get("scope") == "ONE_ACTION"
            and record.get("consumed") is False and float(record.get("expires_at") or 0) >= self.clock()
            and secrets.compare_digest(
                str(record.get("challenge_hash") or ""), hashlib.sha256(token.encode()).hexdigest(),
            )
        )
        if not valid:
            raise ValidationAbort("LOCAL_GATES", "CONFIRMATION_CHALLENGE_INVALID")
        # Consume before persistence/provider construction. A crash cannot
        # accidentally replay the confirmation.
        record["consumed"] = True
        temporary = path.with_suffix(".consumed.tmp")
        temporary.write_text(json.dumps(record, sort_keys=True, separators=(",", ":")), encoding="utf-8")
        temporary.replace(path)


def dry_run(config: ValidationConfiguration, store: ChallengeStore) -> Dict[str, Any]:
    checked = preflight(config)
    if checked["status"] != "PREFLIGHT_PASS":
        return checked
    run_id = RUN_PREFIX + uuid.uuid4().hex
    challenge, expiry = store.issue(run_id, config.alias)
    return _sanitize({
        "status": "DRY_RUN_READY", "stage": "LOCAL_GATES", "run_id": run_id,
        "capability": "QOD", "target_alias": config.alias, "target_count": 1,
        "scope": "ONE_DEVICE_ONE_ACTION", "persistence_required": True,
        "warden_required": True, "live_write_required": True,
        "cleanup_required": True, "verification_required": True,
        "challenge": challenge, "challenge_expires_at": expiry,
        "mutation_budget": {"CREATE": 1, "DELETE": 1},
        "safe_read_budget": config.max_reads, "safe_reason": "EXPLICIT_EXECUTION_REQUIRED",
    })


@dataclass
class ProviderBudget:
    max_reconciliation_attempts: int
    creates: int = 0
    deletes: int = 0
    qod_reads: int = 0
    network_reads: int = 0

    @property
    def qod_read_limit(self) -> int:
        # One initial lifecycle read, up to two SDK reads per Phase 7D
        # attempt (reconcile + verify), and one cleanup readback.
        return 2 * self.max_reconciliation_attempts + 2

    @property
    def network_read_limit(self) -> int:
        # Baseline congestion + reachability and at most one newer
        # categorical read per bounded verification attempt.
        return self.max_reconciliation_attempts + 2

    def consume(self, operation: str) -> None:
        if operation == "CREATE":
            if self.creates >= 1: raise ValidationAbort("EXECUTION", "CREATE_BUDGET_EXCEEDED")
            self.creates += 1
        elif operation == "DELETE":
            if self.deletes >= 1: raise ValidationAbort("CLEANUP", "DELETE_BUDGET_EXCEEDED", classification="REAL_PARTIAL")
            self.deletes += 1
        elif operation in {"SAFE_READ", "QOD_READ"}:
            if self.qod_reads >= self.qod_read_limit: raise ValidationAbort("RECONCILIATION", "SAFE_READ_BUDGET_EXCEEDED", classification="REAL_PARTIAL")
            self.qod_reads += 1
        elif operation == "NETWORK_READ":
            if self.network_reads >= self.network_read_limit: raise ValidationAbort("NETWORK_VERIFICATION", "SAFE_READ_BUDGET_EXCEEDED", classification="REAL_PARTIAL")
            self.network_reads += 1
        else:
            raise ValidationAbort("LOCAL_GATES", "UNSUPPORTED_PROVIDER_OPERATION")


class _SessionsProxy:
    def __init__(self, wrapped: Any, budget: ProviderBudget): self.wrapped, self.budget = wrapped, budget
    def get(self, resource_id: str) -> Any:
        self.budget.consume("QOD_READ")
        return self.wrapped.get(resource_id)


class _SdkProxy:
    def __init__(self, wrapped: Any, budget: ProviderBudget):
        self.wrapped = wrapped
        self.sessions = _SessionsProxy(wrapped.sessions, budget)
    def __getattr__(self, name: str) -> Any: return getattr(self.wrapped, name)


class BudgetedNokiaClient:
    """Counts only allowed logical provider operations; exposes no secrets."""
    def __init__(self, wrapped: Any, budget: ProviderBudget):
        self.wrapped, self.budget = wrapped, budget
        self.client = _SdkProxy(wrapped.client, budget)
    def __getattr__(self, name: str) -> Any: return getattr(self.wrapped, name)
    async def request_qos(self, *args: Any, **kwargs: Any) -> Any:
        self.budget.consume("CREATE"); return await self.wrapped.request_qos(*args, **kwargs)
    async def release_qos(self, *args: Any, **kwargs: Any) -> Any:
        self.budget.consume("DELETE"); return await self.wrapped.release_qos(*args, **kwargs)
    async def congestion_insights(self, *args: Any, **kwargs: Any) -> Any:
        self.budget.consume("NETWORK_READ"); return await self.wrapped.congestion_insights(*args, **kwargs)
    async def device_status(self, *args: Any, **kwargs: Any) -> Any:
        self.budget.consume("NETWORK_READ"); return await self.wrapped.device_status(*args, **kwargs)


@dataclass
class ValidationEvidence:
    run_id: str
    target_alias: str
    started_at: float
    stages: list[Dict[str, Any]] = field(default_factory=list)
    incident_id: Optional[str] = None
    action_id: Optional[str] = None
    warden_result: str = "UNKNOWN"
    action_state: str = "UNKNOWN"
    provider_state: str = "UNKNOWN"
    verification_state: str = "UNAVAILABLE"
    cleanup_state: str = "NOT_REQUIRED"
    durability_success: str = "UNKNOWN"
    safety_success: str = "UNKNOWN"
    provider_success: str = "UNKNOWN"
    network_verification_success: str = "UNAVAILABLE"
    cleanup_success: str = "NOT_REQUIRED"
    final_classification: str = "ABORTED_SAFE"
    setup_evidence: str = "HARIS_DERIVED_VALIDATION_SETUP"
    provider_evidence: str = "UNAVAILABLE"
    verification_evidence: str = "UNAVAILABLE"

    def stage(
        self, name: str, status: str, reason: str, *,
        exception_class: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        normalized = {
            "PASS": "PASS", "ABORT": "ABORT", "FAIL": "ABORT",
            "UNKNOWN": "UNKNOWN", "UNAVAILABLE": "UNKNOWN",
            "NOT_SAFE": "UNKNOWN", "NOT_REQUIRED": "UNKNOWN",
        }.get(str(status).upper(), "UNKNOWN")
        row: Dict[str, Any] = {
            "stage": name,
            "status": normalized,
            "safe_reason": _safe_symbol(reason),
        }
        if exception_class in SAFE_EXCEPTION_CLASSES:
            row["exception_class"] = exception_class
        for key, value in (metadata or {}).items():
            if key in SAFE_DIAGNOSTIC_METADATA and (
                isinstance(value, bool) or isinstance(value, int)
            ):
                row[key] = value
        self.stages.append(row)

    def public(self, completed_at: float) -> Dict[str, Any]:
        return _sanitize({
            "status": "VALIDATION_FINISHED", "run_id": self.run_id,
            "capability": "QOD", "target_alias": self.target_alias,
            "scope": "ONE_DEVICE_ONE_ACTION", "started_at": self.started_at,
            "completed_at": completed_at, "stages": self.stages,
            "incident_id": self.incident_id, "action_id": self.action_id,
            "warden_result": self.warden_result, "action_state": self.action_state,
            "provider_state": self.provider_state,
            "verification_state": self.verification_state,
            "cleanup_state": self.cleanup_state,
            "durability_success": self.durability_success,
            "safety_success": self.safety_success,
            "provider_success": self.provider_success,
            "network_verification_success": self.network_verification_success,
            "cleanup_success": self.cleanup_success,
            "final_classification": self.final_classification,
            "provenance": {
                "setup_evidence": self.setup_evidence,
                "provider_evidence": self.provider_evidence,
                "verification_evidence": self.verification_evidence,
            },
            "summary": {
                "scope": "1 device / 1 QoD action",
                "durability_success": self.durability_success,
                "safety_success": self.safety_success,
                "provider_success": self.provider_success,
                "network_verification_success": self.network_verification_success,
                "cleanup_success": self.cleanup_success,
                "final_classification": self.final_classification,
            },
        })


class EvidenceWriter:
    def __init__(self, directory: Path): self.directory = directory
    def write(self, evidence: Dict[str, Any]) -> Path:
        safe = _sanitize(evidence)
        run_id = safe["run_id"]
        self.directory.mkdir(parents=True, exist_ok=True)
        destination = self.directory / f"real_qod_{run_id}.json"
        temporary = destination.with_suffix(".tmp")
        temporary.write_text(json.dumps(safe, sort_keys=True, indent=2), encoding="utf-8")
        temporary.replace(destination)
        return destination


class RealQodValidationController:
    """Bounded orchestration over an injected durable backend."""
    def __init__(
        self, *, config: ValidationConfiguration, challenge_store: ChallengeStore,
        backend_factory: Callable[[ValidationConfiguration, ProviderBudget], Any],
        evidence_writer: EvidenceWriter, clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], Any] = asyncio.sleep,
        abort_requested: Callable[[], bool] = lambda: False,
    ):
        self.config, self.challenge_store = config, challenge_store
        self.backend_factory, self.evidence_writer = backend_factory, evidence_writer
        self.clock, self.sleeper, self.abort_requested = clock, sleeper, abort_requested

    async def execute(self, run_id: str, challenge: str) -> Dict[str, Any]:
        evidence = ValidationEvidence(run_id, self.config.alias, self.clock())
        try:
            checked = preflight(self.config)
            if checked["status"] != "PREFLIGHT_PASS":
                raise ValidationAbort("LOCAL_GATES", "PREFLIGHT_FAILED")
            self.challenge_store.consume(run_id, self.config.alias, challenge)
            evidence.stage("LOCAL_GATES", "PASS", "ALL_GATES_AUTHORIZED")
            budget = ProviderBudget(self.config.max_reads)
            try:
                backend = self.backend_factory(self.config, budget)
            except RepositoryUnavailable as exc:
                reason = getattr(exc, "safe_reason", None) or str(exc)
                if reason not in SAFE_PERSISTENCE_REASONS:
                    reason = "PERSISTENCE_UNAVAILABLE"
                raise ValidationAbort("PERSISTENCE_READY", reason) from None
            evidence.stage("PERSISTENCE_READY", "PASS", "POSTGRES_RECONSTRUCTED")
            diagnostic_setter = getattr(backend, "set_diagnostic_sink", None)
            if callable(diagnostic_setter):
                diagnostic_setter(
                    lambda code, metadata=None: evidence.stage(
                        "DURABLE_CONTEXT", "PASS", code,
                        metadata=metadata,
                    )
                )
            if self.abort_requested(): raise ValidationAbort("DURABLE_CONTEXT", "OPERATOR_ABORT_BEFORE_CREATE")
            try:
                evidence.incident_id = await backend.create_durable_context(
                    run_id, self.config.target_device, self.config.target_cell,
                )
            except RepositoryUnavailable as exc:
                reason = getattr(exc, "safe_reason", None) or str(exc)
                if reason not in SAFE_PERSISTENCE_REASONS:
                    reason = "PERSISTENCE_UNAVAILABLE"
                raise ValidationAbort("DURABLE_CONTEXT", reason) from None
            evidence.durability_success = "PASS"
            evidence.stage("DURABLE_CONTEXT", "PASS", "DURABLE_VALIDATION_CONTEXT_CREATED")
            decision = await backend.authorize(evidence.incident_id)
            evidence.warden_result = _safe_symbol(decision.get("warden_decision"), "UNKNOWN")
            if evidence.warden_result != "ALLOW":
                raise ValidationAbort("WARDEN", f"WARDEN_{evidence.warden_result}")
            evidence.stage("WARDEN", "PASS", "WARDEN_ALLOW")
            actions = backend.current_plan_actions(evidence.incident_id, int(decision["plan_version"]))
            if len(actions) != 1: raise ValidationAbort("ACTION_SCOPE", "EXECUTABLE_ACTION_COUNT_INVALID")
            action = actions[0]
            if action.command_type != "QOD_PLAN": raise ValidationAbort("ACTION_SCOPE", "CAPABILITY_NOT_QOD")
            if action.device_id != self.config.target_device: raise ValidationAbort("ACTION_SCOPE", "TARGET_MISMATCH")
            if action.state.value != "READY": raise ValidationAbort("ACTION_SCOPE", "ACTION_NOT_READY")
            evidence.action_id = action.command_id
            evidence.safety_success = "PASS"
            evidence.stage("ACTION_SCOPE", "PASS", "ONE_READY_QOD_ACTION")
            if self.abort_requested(): raise ValidationAbort("EXECUTION", "OPERATOR_ABORT_BEFORE_CREATE")
            execution = await backend.execute(action.command_id)
            evidence.action_state = _safe_symbol(execution.action_state)
            current = backend.action(action.command_id)
            if getattr(execution, "provider_invoked", False):
                evidence.provider_evidence = _safe_symbol(
                    execution.provider_execution_provenance or "NOKIA_LIVE"
                )
            if execution.status == "PROVIDER_FAILED":
                evidence.provider_success = "FAIL"
                raise ValidationAbort("EXECUTION", "PROVIDER_EXPLICIT_FAILURE", classification="REAL_PARTIAL")
            if current and current.provider_resource_id:
                evidence.provider_state = _safe_symbol(
                    getattr(execution, "provider_state", None) or "KNOWN"
                )
                evidence.provider_success = "PASS" if execution.status not in {"PROVIDER_FAILED", "OUTCOME_UNKNOWN"} else "UNKNOWN"
            elif execution.status == "OUTCOME_UNKNOWN":
                evidence.cleanup_state = "NOT_SAFE"
                evidence.cleanup_success = "NOT_SAFE"
                raise ValidationAbort("RECONCILIATION", "PROVIDER_OUTCOME_UNKNOWN", classification="RECONCILIATION_REQUIRED")
            evidence.stage("EXECUTION", "PASS", "PHASE_7C_BOUNDARY_COMPLETE")
            for _ in range(self.config.max_reads):
                current = backend.action(action.command_id)
                if current.state.value not in {"OUTCOME_UNKNOWN", "RECONCILIATION_REQUIRED", "ACKNOWLEDGED"}:
                    break
                result = await backend.reconcile(action.command_id)
                evidence.action_state = _safe_symbol(result.action_state)
                evidence.provider_state = _safe_symbol(result.provider_state or evidence.provider_state)
                evidence.verification_state = _safe_symbol(result.verification_state or "UNAVAILABLE")
                if result.provider_state in {"ACTIVE", "AVAILABLE"}:
                    evidence.provider_success = "PASS"
                if result.status in {"VERIFIED_IMPROVED", "RESOLVED", "RECOVERY_REQUIRED", "ESCALATED"}:
                    break
                delay = max(0.0, float(result.next_eligible_at or self.clock()) - self.clock())
                if delay: await self.sleeper(delay)
            evidence.stage("RECONCILIATION", "PASS", "BOUNDED_RECONCILIATION_COMPLETE")
            verification = backend.latest_network_verification(action.command_id)
            if verification == "IMPROVED":
                evidence.network_verification_success = "PASS"
            elif verification in {"UNCHANGED", "DEGRADED", "FAILED"}:
                evidence.network_verification_success = "FAIL"
            else:
                evidence.network_verification_success = "UNAVAILABLE"
            evidence.verification_state = verification or "UNAVAILABLE"
            if verification:
                provenance_reader = getattr(backend, "latest_network_verification_provenance", None)
                evidence.verification_evidence = _safe_symbol(
                    provenance_reader(action.command_id) if callable(provenance_reader) else "UNAVAILABLE"
                )
            evidence.stage("NETWORK_VERIFICATION", evidence.network_verification_success, "AUTHORITATIVE_NETWORK_EVIDENCE_CLASSIFIED")
            current = backend.action(action.command_id)
            if self.abort_requested() and not current.provider_resource_id:
                raise ValidationAbort("CLEANUP", "CLEANUP_NOT_SAFE", classification="RECONCILIATION_REQUIRED")
            if current.provider_resource_id:
                backend.prepare_cleanup(action.command_id, run_id)
                cleanup = await backend.execute(action.command_id)
                evidence.cleanup_state = _safe_symbol(cleanup.rollback_state or cleanup.status)
                evidence.cleanup_success = "PASS" if cleanup.status == "ROLLED_BACK" else (
                    "UNKNOWN" if cleanup.status == "OUTCOME_UNKNOWN" else "FAIL"
                )
                evidence.stage("CLEANUP", evidence.cleanup_success, "PHASE_7C_CONTROLLED_CLEANUP")
                evidence.stage("CLEANUP_VERIFICATION", evidence.cleanup_success, "CLEANUP_READBACK_CLASSIFIED")
            else:
                evidence.cleanup_state = "NOT_SAFE"
                evidence.cleanup_success = "NOT_SAFE"
            evidence.final_classification = self._classify(evidence)
        except ValidationAbort as exc:
            evidence.stage(
                exc.stage, "ABORT", exc.safe_reason,
                exception_class=exc.exception_class,
            )
            evidence.final_classification = exc.classification
            if evidence.action_id:
                current = locals().get("backend").action(evidence.action_id) if "backend" in locals() else None
                if current and current.provider_resource_id and evidence.cleanup_success in {"NOT_REQUIRED", "UNKNOWN"}:
                    try:
                        backend.prepare_cleanup(evidence.action_id, run_id)
                        cleanup = await backend.execute(evidence.action_id)
                        evidence.cleanup_state = _safe_symbol(cleanup.rollback_state or cleanup.status)
                        evidence.cleanup_success = "PASS" if cleanup.status == "ROLLED_BACK" else "FAIL"
                    except Exception:
                        evidence.cleanup_state, evidence.cleanup_success = "UNKNOWN", "FAIL"
        except Exception as exc:
            # Never allow provider/transport exception text into console or
            # evidence. Preserve ambiguity after a possible mutation boundary.
            evidence.stage(
                "EVIDENCE_REPORT", "ABORT", "VALIDATION_INTERNAL_ERROR",
                exception_class=_safe_exception_class(exc),
            )
            evidence.final_classification = "ABORTED_SAFE"
            if evidence.action_id and "backend" in locals():
                current = backend.action(evidence.action_id)
                state = current.state.value if current is not None else "UNKNOWN"
                if current and current.provider_resource_id:
                    evidence.final_classification = "REAL_PARTIAL"
                    try:
                        backend.prepare_cleanup(evidence.action_id, run_id)
                        cleanup = await backend.execute(evidence.action_id)
                        evidence.cleanup_state = _safe_symbol(cleanup.rollback_state or cleanup.status)
                        evidence.cleanup_success = "PASS" if cleanup.status == "ROLLED_BACK" else "FAIL"
                    except Exception:
                        evidence.cleanup_state, evidence.cleanup_success = "UNKNOWN", "FAIL"
                elif state in {"SENT", "OUTCOME_UNKNOWN", "RECONCILIATION_REQUIRED", "ACKNOWLEDGED"}:
                    evidence.cleanup_state = "NOT_SAFE"
                    evidence.cleanup_success = "NOT_SAFE"
                    evidence.final_classification = "RECONCILIATION_REQUIRED"
        completed = self.clock()
        output = evidence.public(completed)
        evidence.stage("EVIDENCE_REPORT", "PASS", "SANITIZED_ARTIFACT_WRITTEN")
        output = evidence.public(completed)
        self.evidence_writer.write(output)
        return output

    @staticmethod
    def _classify(evidence: ValidationEvidence) -> str:
        if evidence.safety_success != "PASS" or evidence.durability_success != "PASS": return "ABORTED_SAFE"
        if evidence.provider_success == "UNKNOWN": return "RECONCILIATION_REQUIRED"
        if (evidence.provider_success == "PASS" and evidence.network_verification_success == "PASS"
                and evidence.cleanup_success == "PASS"): return "REAL_COMPLETE"
        if evidence.provider_success == "PASS" and evidence.cleanup_success == "PASS": return "REAL_PARTIAL"
        return "REAL_PARTIAL"


class DurableRealQodBackend:
    def __init__(self, *, bundle: Any, system: Any, decision: Any, executor: Any, reconciler: Any, client: Any):
        self.bundle, self.system, self.decision = bundle, system, decision
        self.executor, self.reconciler, self.client = executor, reconciler, client
        self._diagnostic_sink: Optional[Callable[[str, Optional[Dict[str, Any]]], None]] = None

    def set_diagnostic_sink(
        self,
        sink: Callable[[str, Optional[Dict[str, Any]]], None],
    ) -> None:
        self._diagnostic_sink = sink

    def _diagnostic(self, code: str, **metadata: Any) -> None:
        if self._diagnostic_sink is not None:
            safe_metadata = {
                key: value for key, value in metadata.items()
                if key in SAFE_DIAGNOSTIC_METADATA
                and (isinstance(value, bool) or isinstance(value, int))
            }
            self._diagnostic_sink(_safe_symbol(code), safe_metadata or None)

    def _observation_cohort(self, target_device_id: str, cell_id: str) -> list[str]:
        """Return the configured Nokia-mapped fleet eligible for observation.

        Registry membership and an existing Nokia mapping establish which
        devices may be queried.  Only devices for which ``device_status``
        subsequently returns authoritative evidence enter the denominator.
        ``cell_id`` still binds the one validation target and the incident;
        it does not turn unrelated observers into affected or mutable devices.
        """
        settings = getattr(self.system, "settings", None)
        if settings is None:
            raise ValidationAbort("DURABLE_CONTEXT", "DEVICE_REGISTRY_UNAVAILABLE")
        registry_path = Path(settings.fixture_dir) / "devices.json"
        if not registry_path.is_absolute():
            registry_path = PROJECT_ROOT / registry_path
        try:
            rows = json.loads(registry_path.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError) as exc:
            raise ValidationAbort(
                "DURABLE_CONTEXT", "DEVICE_REGISTRY_UNAVAILABLE",
                exception_class=_safe_exception_class(exc),
            ) from None
        if not isinstance(rows, list):
            raise ValidationAbort(
                "DURABLE_CONTEXT", "DEVICE_REGISTRY_UNAVAILABLE",
                exception_class="RESPONSE_SHAPE_ERROR",
            )
        self._diagnostic(
            "LIVE_OBSERVATION_FLEET_ENUMERATED",
            candidate_count=len(rows),
        )
        registered = set(settings.registered_devices)
        mappings = getattr(self.client, "device_phone_map", {})
        if not isinstance(mappings, dict):
            raise ValidationAbort(
                "DURABLE_CONTEXT", "DEVICE_MAPPING_UNAVAILABLE",
                exception_class="MAPPING_ERROR",
            )
        # A key with an empty/non-string value is not a usable provider mapping.
        mapped = {
            device_id for device_id, provider_id in mappings.items()
            if isinstance(device_id, str)
            and isinstance(provider_id, str)
            and bool(provider_id.strip())
        }
        cohort = [
            str(row["device_id"])
            for row in rows
            if isinstance(row, dict)
            and row.get("device_id") in registered
            and row.get("device_id") in mapped
        ]
        self._diagnostic(
            "LIVE_OBSERVATION_MAPPED_FLEET_FILTERED",
            candidate_count=len(set(cohort)),
            target_present=target_device_id in cohort,
        )
        target_metadata = next(
            (
                row for row in rows
                if isinstance(row, dict)
                and row.get("device_id") == target_device_id
            ),
            None,
        )
        if (
            target_device_id not in cohort
            or target_metadata is None
            or target_metadata.get("cell_id") != cell_id
        ):
            raise ValidationAbort(
                "DURABLE_CONTEXT", "LIVE_DEVICE_CONTEXT_UNAVAILABLE",
                exception_class="TARGET_EVIDENCE_MISSING",
            )
        return [target_device_id] + sorted(
            device_id for device_id in set(cohort) if device_id != target_device_id
        )

    async def create_durable_context(self, run_id: str, device_id: str, cell_id: str) -> str:
        from platform_events import EventType, HarisEvent, Provenance
        self._diagnostic("LIVE_OBSERVATION_CONGESTION_START")
        try:
            reading_rows = await self.client.congestion_insights([cell_id])
        except Exception as exc:
            raise ValidationAbort(
                "DURABLE_CONTEXT", "LIVE_BASELINE_UNAVAILABLE",
                exception_class=_safe_exception_class(exc, provider_boundary=True),
            ) from None
        if not isinstance(reading_rows, list) or any(
            not all(hasattr(row, field_name) for field_name in (
                "cell_id", "congestion_level", "confidence_level",
                "interval_start", "interval_stop",
            ))
            for row in reading_rows
        ):
            raise ValidationAbort(
                "DURABLE_CONTEXT", "LIVE_BASELINE_RESPONSE_INVALID",
                exception_class="RESPONSE_SHAPE_ERROR",
            )
        self._diagnostic("LIVE_OBSERVATION_CONGESTION_OK")
        reading = next((row for row in reading_rows if row.cell_id == cell_id), None)
        if reading is None:
            raise ValidationAbort(
                "DURABLE_CONTEXT", "LIVE_BASELINE_UNAVAILABLE",
                exception_class="OBSERVATION_EMPTY",
            )
        requested_cohort = self._observation_cohort(device_id, cell_id)
        self._diagnostic(
            "LIVE_OBSERVATION_DEVICE_STATUS_START",
            candidate_count=len(requested_cohort),
        )
        try:
            device_rows = await self.client.device_status(requested_cohort)
        except Exception as exc:
            raise ValidationAbort(
                "DURABLE_CONTEXT", "LIVE_DEVICE_COHORT_UNAVAILABLE",
                exception_class=_safe_exception_class(exc, provider_boundary=True),
            ) from None
        if not isinstance(device_rows, list):
            raise ValidationAbort(
                "DURABLE_CONTEXT", "LIVE_DEVICE_COHORT_UNAVAILABLE",
                exception_class="RESPONSE_SHAPE_ERROR",
            )
        if any(
            not all(hasattr(row, field_name) for field_name in (
                "device_id", "reachable", "roaming", "battery_pct",
                "tier", "cell_id",
            ))
            for row in device_rows
        ):
            raise ValidationAbort(
                "DURABLE_CONTEXT", "LIVE_DEVICE_RESPONSE_INVALID",
                exception_class="RESPONSE_SHAPE_ERROR",
            )
        if not device_rows:
            self._diagnostic(
                "LIVE_OBSERVATION_DEVICE_STATUS_EMPTY",
                candidate_count=len(requested_cohort),
                authoritative_count=0,
                unavailable_count=len(requested_cohort),
                target_present=False,
            )
            raise ValidationAbort(
                "DURABLE_CONTEXT", "LIVE_DEVICE_CONTEXT_UNAVAILABLE",
                exception_class="OBSERVATION_EMPTY",
            )
        observed_by_id = {
            row.device_id: row for row in device_rows
            if row.device_id in requested_cohort
        }
        # Keep target-first deterministic ordering.  A configured cohort
        # member enters the denominator only when the live adapter returned
        # current authoritative device evidence for that member.
        observed_devices = [
            observed_by_id[cohort_device]
            for cohort_device in requested_cohort
            if cohort_device in observed_by_id
        ]
        unavailable_devices = [
            cohort_device for cohort_device in requested_cohort
            if cohort_device not in observed_by_id
        ]
        self._diagnostic(
            (
                "LIVE_OBSERVATION_DEVICE_STATUS_PARTIAL"
                if unavailable_devices
                else "LIVE_OBSERVATION_DEVICE_STATUS_COMPLETE"
            ),
            candidate_count=len(requested_cohort),
            authoritative_count=len(observed_devices),
            unavailable_count=len(unavailable_devices),
            target_present=device_id in observed_by_id,
        )
        target_device = next(
            (row for row in observed_devices if row.device_id == device_id), None,
        )
        if target_device is None or target_device.cell_id != cell_id:
            self._diagnostic(
                "LIVE_OBSERVATION_TARGET_MISSING",
                target_present=False,
            )
            raise ValidationAbort(
                "DURABLE_CONTEXT", "LIVE_DEVICE_CONTEXT_UNAVAILABLE",
                exception_class="TARGET_EVIDENCE_MISSING",
            )
        self._diagnostic("LIVE_OBSERVATION_TARGET_PRESENT", target_present=True)
        now = time.time(); incident_id = f"{run_id}-INCIDENT"
        try:
            current_projection = self.bundle.network_state.get(cell_id)
        except Exception as exc:
            raise ValidationAbort(
                "DURABLE_CONTEXT", "DURABLE_PROJECTION_READ_FAILED",
                exception_class=_safe_exception_class(exc),
            ) from None
        if current_projection is not None and not isinstance(current_projection, dict):
            raise ValidationAbort(
                "DURABLE_CONTEXT", "DURABLE_PROJECTION_RESPONSE_INVALID",
                exception_class="RESPONSE_SHAPE_ERROR",
            )
        try:
            expected_projection_version = int(
                (current_projection or {}).get("version", 0)
            )
        except (TypeError, ValueError) as exc:
            raise ValidationAbort(
                "DURABLE_CONTEXT", "DURABLE_PROJECTION_VERSION_INVALID",
                exception_class=_safe_exception_class(exc),
            ) from None
        if expected_projection_version < 0:
            raise ValidationAbort(
                "DURABLE_CONTEXT", "DURABLE_PROJECTION_VERSION_INVALID",
                exception_class="CONTRACT_ERROR",
            )
        prior_active_incidents = (current_projection or {}).get(
            "active_incident_ids", []
        )
        if not isinstance(prior_active_incidents, list) or any(
            not isinstance(item, str) for item in prior_active_incidents
        ):
            raise ValidationAbort(
                "DURABLE_CONTEXT", "DURABLE_PROJECTION_RESPONSE_INVALID",
                exception_class="RESPONSE_SHAPE_ERROR",
            )
        active_incident_ids = list(dict.fromkeys([
            *prior_active_incidents, incident_id,
        ]))
        self._diagnostic("DURABLE_EXISTING_PROJECTION_READ")
        try:
            event = HarisEvent(
                event_id=f"{run_id}-EVENT", event_type=EventType.NETWORK_CONGESTION_CHANGED,
                source="nokia", source_mode="real_qod_validation", source_event_id=f"{run_id}-BASELINE",
                source_timestamp=now, received_at=now, created_at=now,
                entity_type="HARIS_CONFIGURED_LOGICAL_CELL", entity_id=cell_id,
                correlation_key=f"real-qod-validation:{run_id}", provenance=Provenance.NOKIA_LIVE,
                payload={"congestion_level": reading.congestion_level,
                         "confidence_level": reading.confidence_level,
                         "interval_start": reading.interval_start, "interval_stop": reading.interval_stop,
                         "validation_scope": "ONE_DEVICE_ONE_QOD_ACTION",
                         "validation_target_device_id": device_id,
                         "validation_setup": {
                             "storm_advisory": True,
                             "provenance": "HARIS_DERIVED_VALIDATION_SETUP",
                         }},
                trace_id=f"{run_id}-TRACE",
            )
        except Exception as exc:
            raise ValidationAbort(
                "DURABLE_CONTEXT", "DURABLE_EVENT_INVALID",
                exception_class=_safe_exception_class(exc),
            ) from None
        self._diagnostic("DURABLE_EVENT_CREATED")
        projection = {
            "entity_id": cell_id,
            "entity_type": "HARIS_CONFIGURED_LOGICAL_CELL",
            "mapping_source": "HARIS_CONFIGURED_LOGICAL_CELL_MAPPING",
            "provenance": "NOKIA_LIVE",
            "raw_congestion": reading.congestion_level,
            "raw_congestion_evidence": event.payload,
            "raw_congestion_observed_at": now,
            "reachability_summary": {
                "devices": [{
                    "device_id": observed.device_id, "reachable": observed.reachable,
                    "roaming": observed.roaming, "battery_pct": observed.battery_pct,
                    "tier": observed.tier, "cell_id": observed.cell_id,
                    "reachability_provenance": "NOKIA_LIVE",
                    "metadata_provenance": "HARIS_CONFIGURED",
                } for observed in observed_devices],
                "unavailable_device_ids": unavailable_devices,
            },
            "reachability_observed_at": now,
            "location_summary": None,
            "location_observed_at": None,
            "freshness": "FRESH",
            "haris_operational_state": (
                "INCIDENT_OPEN" if reading.congestion_level == "High" else
                "WATCHING" if reading.congestion_level == "Medium" else "STABLE"
            ),
            "active_incident_ids": active_incident_ids,
            "last_source_change_at": now,
            "last_operational_change_at": now,
            "updated_at": now,
            "version": expected_projection_version,
            "expected_version": expected_projection_version,
        }
        self._diagnostic("DURABLE_SNAPSHOT_BUILT")
        incident = {
            "incident_id": incident_id,
            "schema_version": 1,
            "correlation_key": event.correlation_key,
            "primary_entity": cell_id,
            "affected_entities": [cell_id],
            "affected_devices": [
                row.device_id for row in observed_devices
                if row.cell_id == cell_id
            ],
            "trigger_event_id": event.event_id, "trigger_provenance": "NOKIA_LIVE",
            "trigger_source_timestamp": now, "opened_at": now, "updated_at": now,
            "severity": "critical", "priority": "P1", "state": "DETECTED",
            "plan_version": 1, "warden_decision": None, "verification_state": "PENDING",
            "recovery_state": "PENDING", "outcome": "VALIDATION_PENDING",
            "closed_at": None, "version": 0, "trace_id": event.trace_id,
        }
        self._diagnostic("DURABLE_PROJECTION_PREPARED")
        self._diagnostic("DURABLE_INCIDENT_PREPARED")
        self._diagnostic("DURABLE_INGEST_START")
        try:
            result = self.bundle.inbound.process(
                event,
                projection=projection,
                incident=incident,
                transition=None,
                # This isolated controller invokes durable reasoning explicitly;
                # it must not leave duplicate runtime-consumer work behind.
                outbox=None,
            )
        except Exception as exc:
            raise ValidationAbort(
                "DURABLE_CONTEXT", "DURABLE_INGEST_FAILED",
                exception_class=_safe_exception_class(exc),
            ) from None
        if not isinstance(result, dict):
            raise ValidationAbort(
                "DURABLE_CONTEXT", "DURABLE_INGEST_CONTRACT_INVALID",
                exception_class="CONTRACT_ERROR",
            )
        if result.get("status") == "duplicate":
            existing = self.bundle.incidents.get(incident_id)
            if existing is None:
                raise ValidationAbort(
                    "DURABLE_CONTEXT", "DURABLE_CONTEXT_DUPLICATE_INCOMPLETE",
                )
            return incident_id
        if result.get("status") != "accepted":
            raise ValidationAbort(
                "DURABLE_CONTEXT", "DURABLE_INGEST_CONTRACT_INVALID",
                exception_class="CONTRACT_ERROR",
            )
        self._diagnostic("DURABLE_INGEST_OK")
        self._diagnostic("DURABLE_PROJECTION_SAVED")
        self._diagnostic("DURABLE_INCIDENT_CREATED")
        self._diagnostic("DURABLE_CONTEXT_RETURNED")
        return incident_id

    async def authorize(self, incident_id: str) -> Dict[str, Any]:
        result = await self.decision.handle_durable_incident_ready({
            "event_type": "DURABLE_INCIDENT_READY", "payload": {"incident_id": incident_id},
        })
        return result.model_dump()

    def current_plan_actions(self, incident_id: str, plan_version: int) -> list[Any]:
        return [row for row in self.bundle.actions.for_incident(incident_id)
                if row.plan_version == plan_version and row.state.value == "READY"]
    def action(self, action_id: str) -> Any: return self.bundle.actions.get(action_id)
    async def execute(self, action_id: str) -> Any: return await self.executor.execute_ready_action(action_id)
    async def reconcile(self, action_id: str) -> Any: return await self.reconciler.reconcile_action(action_id)
    def prepare_cleanup(self, action_id: str, run_id: str) -> Any:
        return self.executor.prepare_validation_cleanup(action_id, run_id)
    def latest_network_verification(self, action_id: str) -> Optional[str]:
        action = self.action(action_id)
        rows = [row for row in self.bundle.verification.for_incident(action.incident_id)
                if row.get("verification_type") == "NETWORK_ACTION"
                and (row.get("result") or {}).get("command_id") == action_id]
        rows.sort(key=lambda row: float(row.get("updated_at") or 0))
        return rows[-1].get("state") if rows else None

    def latest_network_verification_provenance(self, action_id: str) -> Optional[str]:
        action = self.action(action_id)
        rows = [row for row in self.bundle.verification.for_incident(action.incident_id)
                if row.get("verification_type") == "NETWORK_ACTION"
                and (row.get("result") or {}).get("command_id") == action_id]
        rows.sort(key=lambda row: float(row.get("updated_at") or 0))
        return rows[-1].get("source_provenance") if rows else None


class ValidationReasoningAgent:
    """Add only explicitly labelled setup context to the existing graph.

    Congestion and reachability remain Nokia-live.  The derived storm signal
    exists solely to select Storm Shield's existing QoD candidate for this
    isolated validation and is never represented as Nokia evidence.
    """
    def __init__(self, wrapped: Any):
        self.wrapped = wrapped

    def __getattr__(self, name: str) -> Any:
        return getattr(self.wrapped, name)

    async def run_durable_reasoning(self, context: Dict[str, Any]) -> Any:
        incident_id = str(context.get("incident_id") or "")
        if not incident_id.startswith(RUN_PREFIX) or context.get("provenance") != "NOKIA_LIVE":
            raise ValidationAbort("WARDEN", "VALIDATION_CONTEXT_NOT_ISOLATED")
        bounded = dict(context)
        bounded["dust_advisory"] = True
        bounded["environmental_source"] = "HARIS_DERIVED_VALIDATION_SETUP"
        agent_incident = dict(bounded.get("agent_incident") or {})
        agent_incident["storm_advisory"] = True
        bounded["agent_incident"] = agent_incident
        return await self.wrapped.run_durable_reasoning(bounded)


def build_live_backend(config: ValidationConfiguration, budget: ProviderBudget) -> DurableRealQodBackend:
    """Future --execute-only composition root. It may contact persistence."""
    from agents import HarisAgentSystem
    from config import AppSettings
    from durable_execution import DurableActionExecutionService, ExistingNokiaActuatorAdapter
    from durable_reasoning import DurableIncidentDecisionService
    from durable_reconciliation import DurableActionReconciliationService
    from nokia_clients import build_nokia_client
    from platform_lifecycle import reconstruct_platform_state
    from postgres_persistence import build_repository_bundle
    from runtime import RuntimeEnvironment
    from runtime_events import RuntimeIngestionMetrics

    settings = AppSettings().model_copy(update={
        "enable_continuous_loop": False,
        "nokia_observation_enabled": False,
        "geofencing_monitoring_enabled": False,
        "gemini_api_key": None,
        "groq_api_key": None,
    })
    if settings.nac_mode != "live_write" or not settings.enable_live_write_loop:
        raise ValidationAbort("LOCAL_GATES", "LIVE_WRITE_NOT_AUTHORIZED")
    bundle = build_repository_bundle(settings, runtime=RuntimeEnvironment.REAL_QOD_VALIDATION)
    reconstruct_platform_state(bundle)
    raw_client = build_nokia_client(settings)
    if config.target_device not in getattr(raw_client, "device_phone_map", {}):
        raise ValidationAbort("LOCAL_GATES", "TEST_DEVICE_NOT_CONFIGURED")
    client = BudgetedNokiaClient(raw_client, budget)
    system = HarisAgentSystem(client, settings=settings)
    validation_agent = ValidationReasoningAgent(system)
    metrics = RuntimeIngestionMetrics(); ready = lambda: True
    decision = DurableIncidentDecisionService(
        bundle=bundle, agent_system=validation_agent, is_ready=ready, metrics=metrics,
    )
    adapter = ExistingNokiaActuatorAdapter(client, settings)
    executor = DurableActionExecutionService(bundle=bundle, adapter=adapter, settings=settings, is_ready=ready, metrics=metrics)
    reconciler = DurableActionReconciliationService(
        bundle=bundle, adapter=adapter, settings=settings,
        execution_service=executor, is_ready=ready, metrics=metrics,
    )
    return DurableRealQodBackend(
        bundle=bundle, system=system, decision=decision, executor=executor,
        reconciler=reconciler, client=client,
    )


def _emit(payload: Dict[str, Any]) -> None:
    print(json.dumps(_sanitize(payload), sort_keys=True, separators=(",", ":")))


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="HARIS controlled real QoD validation")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--run-id")
    parser.add_argument("--challenge")
    parser.add_argument("--artifact-dir", default=str(PROJECT_ROOT / "artifacts" / "validation"))
    parser.add_argument("--abort-file", default=str(PROJECT_ROOT / "artifacts" / "validation" / "STOP_REAL_QOD"))
    args = parser.parse_args(argv)
    config = ValidationConfiguration.from_environment(dict(os.environ))
    directory = Path(args.artifact_dir).resolve()
    store = ChallengeStore(directory)
    try:
        if args.preflight:
            result = preflight(config); _emit(result); return 0 if result["status"] == "PREFLIGHT_PASS" else 2
        if args.dry_run:
            result = dry_run(config, store); _emit(result); return 0 if result["status"] == "DRY_RUN_READY" else 2
        if not args.run_id or not args.challenge:
            raise ValidationAbort("LOCAL_GATES", "EXPLICIT_CONFIRMATION_REQUIRED")
        controller = RealQodValidationController(
            config=config, challenge_store=store, backend_factory=build_live_backend,
            evidence_writer=EvidenceWriter(directory),
            abort_requested=lambda: Path(args.abort_file).is_file(),
        )
        _emit(asyncio.run(controller.execute(args.run_id, args.challenge)))
        return 0
    except ValidationAbort as exc:
        _emit({"status": "ABORTED_SAFE", "stage": exc.stage, "safe_reason": exc.safe_reason,
               "final_classification": exc.classification})
        return 2
    except Exception:
        _emit({"status": "ABORTED_SAFE", "stage": "LOCAL_GATES",
               "safe_reason": "VALIDATION_INTERNAL_ERROR", "final_classification": "ABORTED_SAFE"})
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
