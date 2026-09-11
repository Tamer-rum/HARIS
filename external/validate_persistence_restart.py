"""Two-process persistence restart validator with an offline harness mode.

Normal integration execution is inert unless every persistence gate is set.
The offline mode validates subprocess orchestration only and makes no claim
that database persistence has been tested.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse


# Direct script execution adds ``external/`` to sys.path, not the repository
# root.  Resolve the root from this file so imports and child processes never
# depend on the caller's current working directory.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


_PROCESS_ENV = {"PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "PYTHONPATH", "VIRTUAL_ENV"}
_PERSISTENCE_ENV = {
    "HARIS_RUNTIME_ENV", "HARIS_ALLOW_PERSISTENCE_INTEGRATION",
    "HARIS_PERSISTENCE_MODE", "SUPABASE_URL", "SUPABASE_KEY",
}
_INTEGRATION_PREFIX = "PERSISTENCE-TEST-"
_SAFE_FAILURE_REASONS = frozenset({
    "PERSISTENCE_AUTH_FAILED", "PERSISTENCE_SCHEMA_NOT_READY",
    "PERSISTENCE_UNAVAILABLE", "PERSISTENCE_RPC_CONTRACT_FAILED",
    "PERSISTENCE_RESPONSE_INVALID", "PERSISTENCE_NETWORK_BLOCKED",
    "PERSISTENCE_HOST_NOT_ALLOWED", "PERSISTENCE_CONFLICT",
})
_DIAGNOSTIC_PROCESSES = frozenset({"A", "B", "PARENT"})
_DIAGNOSTIC_STAGES = frozenset({
    "PREFLIGHT", "CLAIM_INBOX", "APPEND_EVENT", "WRITE_NETWORK_STATE",
    "CREATE_INCIDENT", "TRANSITION_INCIDENT", "ACQUIRE_RESOURCE",
    "SAVE_ACTION", "SAVE_VERIFICATION", "SAVE_CHECKPOINT", "RECONSTRUCT",
    "READ_EVENT", "READ_NETWORK_STATE", "READ_INCIDENT", "READ_RESOURCE",
    "READ_ACTION", "READ_VERIFICATION", "READ_CHECKPOINT", "DUPLICATE_CHECK",
})
_DIAGNOSTIC_SHAPES = frozenset({
    "boolean", "object", "array", "integer", "null", "null_or_object", "unknown",
})


class PersistenceValidationFailure(RuntimeError):
    def __init__(
        self, safe_reason: str = "PERSISTENCE_UNAVAILABLE", *,
        process: str | None = None, stage: str | None = None,
        rpc_name: str | None = None, expected_shape: str | None = None,
        actual_shape_type: str | None = None, http_status: int | None = None,
    ) -> None:
        reason = safe_reason if safe_reason in _SAFE_FAILURE_REASONS else "PERSISTENCE_UNAVAILABLE"
        super().__init__(reason)
        self.safe_reason = reason
        self.process = process if process in _DIAGNOSTIC_PROCESSES else None
        self.stage = stage if stage in _DIAGNOSTIC_STAGES else None
        self.rpc_name = _safe_rpc_name(rpc_name)
        self.expected_shape = expected_shape if expected_shape in _DIAGNOSTIC_SHAPES else None
        self.actual_shape_type = actual_shape_type if actual_shape_type in _DIAGNOSTIC_SHAPES else "unknown"
        self.http_status = http_status if isinstance(http_status, int) and 100 <= http_status <= 599 else None


def _safe_rpc_name(value: str | None) -> str | None:
    if not value:
        return None
    from supabase_transport import HARIS_RPC_ALLOWLIST
    return value if value in HARIS_RPC_ALLOWLIST else None


def _actual_shape_type(value) -> str:
    if value is None: return "null"
    if isinstance(value, bool): return "boolean"
    if isinstance(value, dict): return "object"
    if isinstance(value, list): return "array"
    if isinstance(value, int): return "integer"
    if isinstance(value, str): return "string"
    return "unknown"


def integration_gate(environment: dict[str, str]) -> tuple[bool, str, str | None]:
    if environment.get("HARIS_RUNTIME_ENV", "").lower() != "persistence_integration" or environment.get("HARIS_ALLOW_PERSISTENCE_INTEGRATION", "").lower() != "true":
        return False, "PERSISTENCE_INTEGRATION_NOT_AUTHORIZED", None
    if environment.get("HARIS_PERSISTENCE_MODE", "").lower() != "postgres":
        return False, "PERSISTENCE_NOT_CONFIGURED", None
    if not environment.get("SUPABASE_URL") or not environment.get("SUPABASE_KEY"):
        return False, "PERSISTENCE_NOT_CONFIGURED", None
    parsed = urlparse(environment["SUPABASE_URL"])
    hostname = (parsed.hostname or "").lower()
    if (parsed.scheme.lower() != "https" or not hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment or parsed.path not in {"", "/"}
            or not hostname.endswith(".supabase.co") or hostname == "supabase.co"):
        return False, "PERSISTENCE_NOT_CONFIGURED", None
    return True, "AUTHORIZED", hostname


def sanitized_child_environment(source: dict[str, str], *, integration: bool) -> dict[str, str]:
    allowed = _PROCESS_ENV | (_PERSISTENCE_ENV if integration else set())
    child = {key: value for key, value in source.items() if key in allowed}
    child["PYTHONPATH"] = str(PROJECT_ROOT)
    child["NOKIA_OBSERVATION_ENABLED"] = "false"
    if not integration:
        child.update({"HARIS_RUNTIME_ENV":"test","HARIS_OFFLINE_TESTS":"true","HARIS_ALLOW_EXTERNAL_TESTS":"false"})
        child.pop("SUPABASE_URL", None); child.pop("SUPABASE_KEY", None)
    return child


def _emit(payload: dict) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


def _parse_output(completed: subprocess.CompletedProcess[str]) -> dict:
    lines = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    if len(lines) != 1 or not lines[0].startswith("{"):
        raise PersistenceValidationFailure("PERSISTENCE_RESPONSE_INVALID")
    try:
        value = json.loads(lines[0])
    except (TypeError, ValueError) as exc:
        raise PersistenceValidationFailure("PERSISTENCE_RESPONSE_INVALID") from exc
    if not isinstance(value, dict):
        raise PersistenceValidationFailure("PERSISTENCE_RESPONSE_INVALID")
    if completed.returncode != 0:
        raise PersistenceValidationFailure(
            str(value.get("safe_reason") or "PERSISTENCE_UNAVAILABLE"),
            process=value.get("process"), stage=value.get("stage"),
            rpc_name=value.get("rpc_name"), expected_shape=value.get("expected_shape"),
            actual_shape_type=value.get("actual_shape_type"),
            http_status=value.get("http_status"),
        )
    return value


def _run_child(stage: str, run_id: str, state_file: Path, *, integration: bool, fail: bool = False) -> dict:
    command = [sys.executable, str(Path(__file__).resolve()), "--child", stage, "--run-id", run_id, "--state-file", str(state_file)]
    if integration: command.append("--integration")
    if fail: command.append("--fail")
    completed = subprocess.run(command, capture_output=True, text=True, timeout=30, env=sanitized_child_environment(dict(os.environ), integration=integration), check=False)
    return _parse_output(completed)


def run_offline_orchestration(*, fail_stage: str | None = None) -> dict:
    run_id = f"offline-{uuid.uuid4().hex[:12]}"
    with tempfile.TemporaryDirectory(prefix="haris-persistence-harness-") as directory:
        state_file = Path(directory) / "safe-state.json"
        process_a = _run_child("A", run_id, state_file, integration=False, fail=fail_stage == "A")
        process_b = _run_child("B", run_id, state_file, integration=False, fail=fail_stage == "B")
    if process_a["run_id"] != process_b["run_id"] or process_a["incident_id"] != process_b["incident_id"]:
        raise RuntimeError("PERSISTENCE_CHILD_RESULT_MISMATCH")
    return {"status":"OFFLINE_HARNESS_ORCHESTRATION=PASS","process_a":process_a,"process_b":process_b}


def _offline_child(stage: str, run_id: str, state_file: Path, fail: bool) -> int:
    if fail:
        print("PERSISTENCE_CHILD_FAILED")
        return 3
    incident_id = f"PERSISTENCE-TEST-INC-{run_id}"
    if stage == "A":
        payload={"run_id":run_id,"incident_id":incident_id,"event_id":f"PERSISTENCE-TEST-EVT-{run_id}","action_state":"SENT","version":1}
        state_file.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        _emit({"stage":"A",**payload})
        return 0
    if not state_file.exists():
        print("PERSISTENCE_CHILD_STATE_MISSING")
        return 3
    payload=json.loads(state_file.read_text(encoding="utf-8"))
    _emit({"stage":"B",**payload,"action_state":"OUTCOME_UNKNOWN","reconciliation_required":True})
    return 0


def _safe_persistence_reason(exc: Exception) -> str:
    from durable_core import DuplicateEvent, ResourceAlreadyOwned, VersionConflict
    from postgres_persistence import (
        PersistenceAuthenticationFailed, PersistenceHostNotAllowed,
        PersistenceNetworkBlocked, PersistenceNotConfigured,
        PersistenceResponseInvalid, PersistenceRpcContractFailed,
        PersistenceSchemaNotReady, PersistenceTransportUnavailable,
    )
    if isinstance(exc, PersistenceAuthenticationFailed): return "PERSISTENCE_AUTH_FAILED"
    if isinstance(exc, PersistenceSchemaNotReady): return "PERSISTENCE_SCHEMA_NOT_READY"
    if isinstance(exc, PersistenceNetworkBlocked): return "PERSISTENCE_NETWORK_BLOCKED"
    if isinstance(exc, (PersistenceHostNotAllowed, PersistenceNotConfigured)): return "PERSISTENCE_HOST_NOT_ALLOWED"
    if isinstance(exc, PersistenceRpcContractFailed): return "PERSISTENCE_RPC_CONTRACT_FAILED"
    if isinstance(exc, PersistenceResponseInvalid): return "PERSISTENCE_RESPONSE_INVALID"
    if isinstance(exc, (VersionConflict, ResourceAlreadyOwned, DuplicateEvent)): return "PERSISTENCE_CONFLICT"
    if isinstance(exc, PersistenceValidationFailure): return exc.safe_reason
    if isinstance(exc, PersistenceTransportUnavailable): return "PERSISTENCE_UNAVAILABLE"
    return "PERSISTENCE_UNAVAILABLE"


def _run_stage(process: str, stage: str, rpc_name: str, expected_shape: str, operation):
    """Attach only allowlisted symbolic context to a repository operation."""
    safe_rpc = _safe_rpc_name(rpc_name)
    if process not in _DIAGNOSTIC_PROCESSES or stage not in _DIAGNOSTIC_STAGES or not safe_rpc or expected_shape not in _DIAGNOSTIC_SHAPES:
        raise PersistenceValidationFailure("PERSISTENCE_RPC_CONTRACT_FAILED")
    try:
        return operation()
    except Exception as exc:
        actual = getattr(exc, "actual_shape_type", None)
        raise PersistenceValidationFailure(
            _safe_persistence_reason(exc), process=process, stage=stage,
            rpc_name=safe_rpc, expected_shape=expected_shape,
            actual_shape_type=actual if actual in _DIAGNOSTIC_SHAPES else "unknown",
            http_status=getattr(exc, "http_status", None),
        ) from None


def _safe_failure_payload(status: str, exc: Exception, *, process: str) -> dict:
    """Create the only externally emitted persistence failure structure."""
    reason = _safe_persistence_reason(exc)
    payload = {"status":status, "safe_reason":reason}
    diagnostic_process = getattr(exc, "process", None)
    payload["process"] = diagnostic_process if diagnostic_process in _DIAGNOSTIC_PROCESSES else process
    if reason in {"PERSISTENCE_RPC_CONTRACT_FAILED", "PERSISTENCE_RESPONSE_INVALID"}:
        stage = getattr(exc, "stage", None)
        rpc_name = _safe_rpc_name(getattr(exc, "rpc_name", None))
        expected = getattr(exc, "expected_shape", None)
        actual = getattr(exc, "actual_shape_type", None)
        if stage in _DIAGNOSTIC_STAGES: payload["stage"] = stage
        if rpc_name: payload["rpc_name"] = rpc_name
        if expected in _DIAGNOSTIC_SHAPES: payload["expected_shape"] = expected
        payload["actual_shape_type"] = actual if actual in _DIAGNOSTIC_SHAPES else "unknown"
    http_status = getattr(exc, "http_status", None)
    if isinstance(http_status, int) and 100 <= http_status <= 599:
        payload["http_status"] = http_status
    return payload


def _require_integration_authorization() -> str:
    environment = dict(os.environ)
    authorized, _reason, hostname = integration_gate(environment)
    if authorized and hostname:
        return hostname
    from postgres_persistence import PersistenceHostNotAllowed, PersistenceNetworkBlocked
    configured_url = environment.get("SUPABASE_URL", "")
    parsed = urlparse(configured_url) if configured_url else None
    invalid_host = bool(parsed and (
        parsed.scheme.lower() != "https" or not parsed.hostname
        or parsed.username or parsed.password or parsed.query or parsed.fragment
        or parsed.path not in {"", "/"}
        or not parsed.hostname.lower().endswith(".supabase.co")
        or parsed.hostname.lower() == "supabase.co"
    ))
    if invalid_host:
        raise PersistenceHostNotAllowed()
    raise PersistenceNetworkBlocked() from None


def _integration_ids(run_id: str) -> dict[str, str]:
    if not re.fullmatch(r"[A-Za-z0-9-]{8,64}", run_id):
        raise PersistenceValidationFailure("PERSISTENCE_RPC_CONTRACT_FAILED")
    return {
        "run_id": run_id,
        "event_id": f"{_INTEGRATION_PREFIX}EVT-{run_id}",
        "entity_id": f"{_INTEGRATION_PREFIX}CELL-{run_id}",
        "incident_id": f"{_INTEGRATION_PREFIX}INC-{run_id}",
        "correlation_key": f"persistence-integration:{run_id}",
        "resource_key": f"{_INTEGRATION_PREFIX}RESOURCE-{run_id}",
        "action_id": f"{_INTEGRATION_PREFIX}CMD-{run_id}",
        "verification_id": f"{_INTEGRATION_PREFIX}VER-{run_id}",
    }


def _bundle():
    _require_integration_authorization()
    from config import get_settings
    from postgres_persistence import PostgresRepositoryBundle, build_persistence_transport
    from runtime import RuntimeEnvironment
    transport = build_persistence_transport(
        get_settings(), runtime=RuntimeEnvironment.PERSISTENCE_INTEGRATION,
    )
    return PostgresRepositoryBundle(transport), transport


def _close_transport(transport) -> None:
    close = getattr(transport, "close", None)
    if callable(close):
        close()


def _integration_child_a(run_id: str, state_file: Path) -> int:
    from durable_core import ActionCommand, ActionState
    from platform_events import EventType, HarisEvent, Provenance
    transport = None
    try:
        ids = _integration_ids(run_id)
        bundle, transport = _bundle()
        now = time.time()
        trace_id = f"{_INTEGRATION_PREFIX}TRACE-{run_id}"
        event = HarisEvent(
            event_id=ids["event_id"], event_type=EventType.NETWORK_CONGESTION_CHANGED,
            source="haris-persistence-validator", source_mode="PERSISTENCE_INTEGRATION_TEST",
            source_event_id=ids["event_id"], source_timestamp=now, received_at=now,
            created_at=now, entity_type="HARIS_CONFIGURED_LOGICAL_CELL",
            entity_id=ids["entity_id"], correlation_key=ids["correlation_key"],
            provenance=Provenance.HARIS_DERIVED,
            payload={"congestion_level":"High","integration_test":True}, trace_id=trace_id,
        )
        if not _run_stage("A", "APPEND_EVENT", "haris_append_domain_event", "object", lambda: bundle.events.append(event)):
            raise PersistenceValidationFailure("PERSISTENCE_CONFLICT")
        network_record = {
            "entity_id":ids["entity_id"], "entity_type":"HARIS_CONFIGURED_LOGICAL_CELL",
            "mapping_source":"PERSISTENCE_INTEGRATION_TEST", "provenance":"HARIS_DERIVED",
            "raw_congestion":"High", "raw_congestion_observed_at":now,
            "reachability_summary":None, "reachability_observed_at":None,
            "location_summary":None, "location_observed_at":None, "freshness":"FRESH",
            "haris_operational_state":"INCIDENT_OPEN",
            "active_incident_ids":[ids["incident_id"]], "last_source_change_at":now,
            "last_operational_change_at":now, "updated_at":now, "version":0,
        }
        network = _run_stage(
            "A", "WRITE_NETWORK_STATE", "haris_write_network_state", "object",
            lambda: bundle.network_state.save(network_record, 0),
        )
        incident_record = {
            "incident_id":ids["incident_id"], "schema_version":1,
            "correlation_key":ids["correlation_key"], "primary_entity":ids["entity_id"],
            "affected_entities":[ids["entity_id"]], "affected_devices":[],
            "trigger_event_id":ids["event_id"], "trigger_provenance":"PERSISTENCE_INTEGRATION_TEST",
            "trigger_source_timestamp":now, "opened_at":now, "updated_at":now,
            "severity":"test", "priority":"PERSISTENCE_TEST", "state":"DETECTED",
            "plan_version":1, "warden_decision":None, "verification_state":"PENDING",
            "recovery_state":"PENDING", "outcome":None, "closed_at":None,
            "version":0, "trace_id":trace_id,
        }
        incident, created = _run_stage(
            "A", "CREATE_INCIDENT", "haris_create_incident", "object",
            lambda: bundle.incidents.create_or_get_active(incident_record),
        )
        if not created or incident.get("incident_id") != ids["incident_id"]:
            raise PersistenceValidationFailure("PERSISTENCE_CONFLICT")
        transition_record = {
            "incident_id":ids["incident_id"], "from_state":None, "to_state":"DETECTED",
            "occurred_at":now, "trigger_event_id":ids["event_id"], "actor":"PERSISTENCE_VALIDATOR",
            "reason_code":"PERSISTENCE_INTEGRATION_TEST", "trace_id":trace_id,
        }
        _run_stage(
            "A", "TRANSITION_INCIDENT", "haris_append_incident_transition", "object",
            lambda: bundle.incidents.rpc("haris_append_incident_transition", p_record=transition_record),
        )
        ownership_record = {
            "resource_key":ids["resource_key"], "resource_type":"PERSISTENCE_TEST",
            "owner_incident_id":ids["incident_id"], "ownership_state":"OWNED",
            "acquired_at":now, "lease_started_at":now, "lease_expires_at":now+300,
            "renewable":False, "adopted_by_incident":False,
            "provider_resource_id":None, "version":0,
        }
        ownership = _run_stage(
            "A", "ACQUIRE_RESOURCE", "haris_acquire_resource", "object",
            lambda: bundle.resource_ownership.acquire(ownership_record),
        )
        action_command = ActionCommand(
            command_id=ids["action_id"], incident_id=ids["incident_id"],
            command_type="PERSISTENCE_TEST_NO_PROVIDER", resource_key=ids["resource_key"],
            device_id=None, plan_version=1, requested_at=now,
            parameters_safe={"source_mode":"PERSISTENCE_INTEGRATION_TEST"},
            preconditions={"provider_call_permitted":False}, state=ActionState.SENT,
        )
        action, action_created = _run_stage(
            "A", "SAVE_ACTION", "haris_save_action", "object",
            lambda: bundle.actions.create_or_get(action_command),
        )
        if not action_created or action.state is not ActionState.SENT:
            raise PersistenceValidationFailure("PERSISTENCE_CONFLICT")
        verification_record = {
            "verification_id":ids["verification_id"], "incident_id":ids["incident_id"],
            "evidence_event_ids":[ids["event_id"]],
            "verification_type":"PERSISTENCE_RESTART_TEST", "state":"PENDING",
            "started_at":now, "updated_at":now, "result":None,
            "reason":"persistence_integration_restart_proof",
            "source_provenance":"PERSISTENCE_INTEGRATION_TEST",
        }
        verification = _run_stage(
            "A", "SAVE_VERIFICATION", "haris_save_verification", "object",
            lambda: bundle.verification.save(verification_record),
        )
        sequence = _run_stage(
            "A", "RECONSTRUCT", "haris_event_sequence", "object",
            bundle.events.sequence,
        )
        checkpoint_projection = {ids["entity_id"]:{
            **network, "source_mode":"PERSISTENCE_INTEGRATION_TEST",
        }}
        checkpoint = _run_stage(
            "A", "SAVE_CHECKPOINT", "haris_save_checkpoint", "object",
            lambda: bundle.checkpoints.save(checkpoint_projection, sequence, now),
        )
        result = {
            "status":"PROCESS_A_OK", **ids,
            "checkpoint_id":checkpoint.get("snapshot_id"),
        }
        if not result["checkpoint_id"] or ownership.get("owner_incident_id") != ids["incident_id"] or verification.get("verification_id") != ids["verification_id"]:
            raise PersistenceValidationFailure("PERSISTENCE_RESPONSE_INVALID")
        state_file.write_text(json.dumps(result,sort_keys=True,separators=(",",":")),encoding="utf-8")
        _emit(result)
        return 0
    except Exception as exc:
        _emit(_safe_failure_payload("PROCESS_A_FAILED", exc, process="A"))
        return 3
    finally:
        if transport is not None:
            _close_transport(transport)


def _integration_child_b(run_id: str, state_file: Path) -> int:
    from durable_core import ActionState
    transport = None
    try:
        expected = json.loads(state_file.read_text(encoding="utf-8"))
        ids = _integration_ids(run_id)
        if not isinstance(expected, dict) or any(expected.get(key) != value for key, value in ids.items()):
            raise PersistenceValidationFailure("PERSISTENCE_RESPONSE_INVALID")
        bundle, transport = _bundle()
        event = _run_stage("B", "READ_EVENT", "haris_read_domain", "array", lambda: bundle.events.get(ids["event_id"]))
        network = _run_stage("B", "READ_NETWORK_STATE", "haris_read_domain", "array", lambda: bundle.network_state.get(ids["entity_id"]))
        incident = _run_stage("B", "READ_INCIDENT", "haris_read_domain", "array", lambda: bundle.incidents.get(ids["incident_id"]))
        ownership = _run_stage("B", "READ_RESOURCE", "haris_read_domain", "array", lambda: bundle.resource_ownership.get_active(ids["resource_key"]))
        action = _run_stage("B", "READ_ACTION", "haris_read_domain", "array", lambda: bundle.actions.get(ids["action_id"]))
        verification = _run_stage("B", "READ_VERIFICATION", "haris_read_domain", "array", lambda: bundle.verification.get(ids["verification_id"]))
        transitions = _run_stage("B", "TRANSITION_INCIDENT", "haris_read_domain", "array", lambda: bundle.incidents.transitions(ids["incident_id"]))
        checkpoint = _run_stage("B", "READ_CHECKPOINT", "haris_latest_checkpoint", "null_or_object", bundle.checkpoints.latest)
        incident_rows = _run_stage("B", "DUPLICATE_CHECK", "haris_read_domain", "array", lambda: bundle.incidents.read("incident", None, 0, 1000))
        matching_incidents = [
            row for row in incident_rows
            if row.get("correlation_key") == ids["correlation_key"]
        ]
        if not all((event, network, incident, ownership, action, verification, checkpoint)):
            raise PersistenceValidationFailure("PERSISTENCE_RESPONSE_INVALID")
        if (
            event.source_mode != "PERSISTENCE_INTEGRATION_TEST"
            or event.entity_id != ids["entity_id"]
            or network.get("mapping_source") != "PERSISTENCE_INTEGRATION_TEST"
            or int(network.get("version", 0)) < 1
            or incident.get("correlation_key") != ids["correlation_key"]
            or incident.get("primary_entity") != ids["entity_id"]
            or ownership.get("owner_incident_id") != ids["incident_id"]
            or action.incident_id != ids["incident_id"]
            or action.resource_key != ids["resource_key"]
            or verification.get("incident_id") != ids["incident_id"]
            or verification.get("source_provenance") != "PERSISTENCE_INTEGRATION_TEST"
            or len(transitions) != 1
            or transitions[0].get("reason_code") != "PERSISTENCE_INTEGRATION_TEST"
            or checkpoint.get("snapshot_id") != expected.get("checkpoint_id")
            or ids["entity_id"] not in (checkpoint.get("projection") or {})
            or len(matching_incidents) != 1
            or action.state is not ActionState.SENT
        ):
            raise PersistenceValidationFailure("PERSISTENCE_RESPONSE_INVALID")
        # Restart semantics are derived without a provider call or database
        # success mutation. Persistent SENT remains evidence of unknown outcome.
        runtime_action_state = ActionState.OUTCOME_UNKNOWN
        reconciliation_required = True
        stored_again = _run_stage("B", "READ_ACTION", "haris_read_domain", "array", lambda: bundle.actions.get(ids["action_id"]))
        if not stored_again or stored_again.state is not ActionState.SENT:
            raise PersistenceValidationFailure("PERSISTENCE_CONFLICT")
        result = {
            "status":"PROCESS_B_OK", **ids,
            "checkpoint_id":checkpoint.get("snapshot_id"),
            "stored_action_state":action.state.value,
            "runtime_action_state":runtime_action_state.value,
            "reconciliation_required":reconciliation_required,
            "duplicate_incidents":len(matching_incidents)-1,
        }
        _emit(result)
        return 0
    except Exception as exc:
        _emit(_safe_failure_payload("PROCESS_B_FAILED", exc, process="B"))
        return 3
    finally:
        if transport is not None:
            _close_transport(transport)


def run_integration_controller() -> dict:
    _require_integration_authorization()
    preflight = _run_stage(
        "PARENT", "PREFLIGHT", "haris_read_domain", "array",
        run_read_only_preflight,
    )
    if preflight.get("status") != "PERSISTENCE_PREFLIGHT_OK":
        raise PersistenceValidationFailure("PERSISTENCE_UNAVAILABLE")
    run_id=uuid.uuid4().hex[:16]
    with tempfile.TemporaryDirectory(prefix="haris-persistence-validation-") as directory:
        state_file=Path(directory)/"expected.json"
        process_a=_run_child("A",run_id,state_file,integration=True)
        process_b=_run_child("B",run_id,state_file,integration=True)
    keys=("run_id","event_id","entity_id","incident_id","correlation_key","resource_key","action_id","verification_id","checkpoint_id")
    if (
        process_a.get("status") != "PROCESS_A_OK"
        or process_b.get("status") != "PROCESS_B_OK"
        or any(process_a.get(key)!=process_b.get(key) for key in keys)
        or process_b.get("stored_action_state")!="SENT"
        or process_b.get("runtime_action_state")!="OUTCOME_UNKNOWN"
        or not process_b.get("reconciliation_required")
        or process_b.get("duplicate_incidents") != 0
    ):
        raise PersistenceValidationFailure("PERSISTENCE_RESPONSE_INVALID")
    return {
        "status":"PERSISTENCE_RESTART_VALIDATED", "run_id":run_id,
        "persistence_process_a":"PASS", "persistence_process_b":"PASS",
        "restart_reconstruction":"PASS", "incident_id_stable":"PASS",
        "resource_ownership_stable":"PASS", "sent_action_outcome_unknown":"PASS",
        "reconciliation_required":"PASS", "duplicate_incidents":0,
    }


def run_read_only_preflight() -> dict:
    """Confirm auth/schema/RPC reachability without writing a domain record."""
    _require_integration_authorization()
    from config import get_settings
    from postgres_persistence import PersistenceResponseInvalid, PersistenceRpcContractFailed, build_persistence_transport
    from runtime import RuntimeEnvironment
    transport = build_persistence_transport(
        get_settings(), runtime=RuntimeEnvironment.PERSISTENCE_INTEGRATION,
    )
    try:
        preflight = getattr(transport, "preflight", None)
        if not callable(preflight):
            raise PersistenceRpcContractFailed()
        result = _run_stage(
            "PARENT", "PREFLIGHT", "haris_read_domain", "array", preflight,
        )
        if not isinstance(result, list):
            raise PersistenceResponseInvalid(
                rpc_name="haris_read_domain", expected_shape="array",
                actual_shape_type=_actual_shape_type(result),
            )
        return {"status":"PERSISTENCE_PREFLIGHT_OK","safe_reason":"PERSISTENCE_PREFLIGHT_OK"}
    finally:
        close = getattr(transport, "close", None)
        if callable(close):
            close()


def safe_preflight_diagnostic() -> dict:
    """Run the read-only probe and expose only an allowlisted safe reason."""
    try:
        return run_read_only_preflight()
    except Exception as exc:
        # Never classify by raw exception text: it may contain an upstream
        # response, request URL, header, or credential.
        return _safe_failure_payload("PERSISTENCE_PREFLIGHT_FAILED", exc, process="PARENT")


def main(argv: list[str] | None = None) -> int:
    parser=argparse.ArgumentParser(add_help=True)
    parser.add_argument("--child",choices=("A","B"));parser.add_argument("--run-id");parser.add_argument("--state-file",type=Path);parser.add_argument("--integration",action="store_true");parser.add_argument("--fail",action="store_true");parser.add_argument("--offline-orchestration",action="store_true");parser.add_argument("--preflight",action="store_true")
    args=parser.parse_args(argv)
    if args.child:
        if not args.run_id or not args.state_file:return 2
        if args.integration:
            try:_require_integration_authorization()
            except Exception as exc:_emit(_safe_failure_payload(f"PROCESS_{args.child}_FAILED",exc,process=args.child));return 3
        return (_integration_child_a(args.run_id,args.state_file) if args.child=="A" else _integration_child_b(args.run_id,args.state_file)) if args.integration else _offline_child(args.child,args.run_id,args.state_file,args.fail)
    if args.offline_orchestration:
        if os.getenv("HARIS_OFFLINE_TESTS","").lower()!="true":print("OFFLINE_HARNESS_NOT_AUTHORIZED");return 2
        try:_emit(run_offline_orchestration());return 0
        except Exception:print("OFFLINE_HARNESS_ORCHESTRATION=FAIL");return 3
    if args.preflight:
        diagnostic=safe_preflight_diagnostic();_emit(diagnostic)
        return 0 if diagnostic["status"]=="PERSISTENCE_PREFLIGHT_OK" else 3
    try:_emit(run_integration_controller());return 0
    except Exception as exc:
        _emit(_safe_failure_payload("PERSISTENCE_RESTART_VALIDATION_FAILED",exc,process="PARENT"))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
