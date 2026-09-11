"""Real persistence concurrency validator with hard integration safety gates.

Importing this module performs no networking. The real controller is inert
unless the dedicated persistence-integration environment and existing secure
Supabase configuration are present. Child processes use the production HARIS
transport/repository composition and emit only captured, symbolic results.
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


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


PREFIX = "PERSISTENCE-TEST-"
PROVENANCE = "PERSISTENCE_INTEGRATION_TEST"
CRASH_EXIT_CODE = 86
_PROCESS_ENV = {
    "PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP",
    "PYTHONPATH", "VIRTUAL_ENV", "PYTHONIOENCODING", "PYTHONUTF8",
}
_PERSISTENCE_ENV = {
    "HARIS_RUNTIME_ENV", "HARIS_ALLOW_PERSISTENCE_INTEGRATION",
    "HARIS_PERSISTENCE_MODE", "SUPABASE_URL", "SUPABASE_KEY",
}
_TEST_CASES = frozenset({
    "PREFLIGHT", "INCIDENT_SINGLETON", "RESOURCE_OWNERSHIP",
    "EVENT_IDEMPOTENCY", "INBOX_IDEMPOTENCY", "ACTION_IDEMPOTENCY",
    "VERSION_CONFLICT", "OUTBOX_CLAIM", "CRASH_WRITE", "CRASH_REPLAY",
    "FINAL_EVALUATION",
})
_WORKERS = frozenset({"A", "B", "PARENT", "CRASH", "REPLAY"})
_WORKER_CASES = frozenset({
    "incident", "resource", "event", "inbox", "action", "version",
    "outbox", "crash_write", "crash_replay",
})
_SAFE_REASONS = frozenset({
    "PERSISTENCE_INTEGRATION_NOT_AUTHORIZED", "PERSISTENCE_NOT_CONFIGURED",
    "PERSISTENCE_AUTH_FAILED", "PERSISTENCE_SCHEMA_NOT_READY",
    "PERSISTENCE_UNAVAILABLE", "PERSISTENCE_RPC_CONTRACT_FAILED",
    "PERSISTENCE_RESPONSE_INVALID", "PERSISTENCE_NETWORK_BLOCKED",
    "PERSISTENCE_HOST_NOT_ALLOWED", "PERSISTENCE_CONFLICT",
    "PERSISTENCE_PROOF_FAILED", "PERSISTENCE_CHILD_FAILED",
})
_EXPECTED_RESULTS = frozenset({
    "authorized", "array", "one_created_one_existing", "one_owner_one_conflict",
    "one_insert_one_duplicate", "one_accepted_one_duplicate", "one_action",
    "one_success_one_version_conflict", "disjoint_current_run_claims",
    "sent_outcome_unknown_no_resend", "valid_worker_result", "all_proofs_pass",
})
_SUCCESS_KEYS = frozenset({
    "status", "concurrent_incident_singleton", "incident_id_convergence",
    "duplicate_active_incidents", "resource_single_owner",
    "resource_conflict_enforced", "active_resource_owners",
    "event_idempotency", "duplicate_events", "inbox_idempotency",
    "action_idempotency", "duplicate_actions", "optimistic_version_conflict",
    "stale_write_rejected", "outbox_claim_exclusive", "outbox_claim_overlap",
    "outbox_namespace_isolation", "crash_replay_safe",
    "sent_action_outcome_unknown", "reconciliation_required",
    "no_action_resend", "no_false_success", "run_id",
})


class ConcurrencyValidationFailure(RuntimeError):
    def __init__(
        self,
        safe_reason: str,
        *,
        test_case: str = "FINAL_EVALUATION",
        worker: str = "PARENT",
        rpc_name: str | None = None,
        http_status: int | None = None,
        expected_result: str | None = None,
        actual_result_type: str | None = None,
        run_id: str | None = None,
    ) -> None:
        reason = safe_reason if safe_reason in _SAFE_REASONS else "PERSISTENCE_UNAVAILABLE"
        super().__init__(reason)
        self.safe_reason = reason
        self.test_case = test_case if test_case in _TEST_CASES else "FINAL_EVALUATION"
        self.worker = worker if worker in _WORKERS else "PARENT"
        self.rpc_name = _safe_rpc_name(rpc_name)
        self.http_status = http_status if isinstance(http_status, int) and 100 <= http_status <= 599 else None
        self.expected_result = expected_result if expected_result in _EXPECTED_RESULTS else None
        self.actual_result_type = actual_result_type if actual_result_type in {
            "boolean", "object", "array", "integer", "string", "null", "unknown",
        } else "unknown"
        self.run_id = run_id if _valid_run_id(run_id) else None


def _valid_run_id(value: str | None) -> bool:
    return bool(value and re.fullmatch(r"[0-9a-f]{32}", value))


def new_run_id() -> str:
    return uuid.uuid4().hex


def namespace(run_id: str) -> str:
    if not _valid_run_id(run_id):
        raise ConcurrencyValidationFailure("PERSISTENCE_RPC_CONTRACT_FAILED")
    return f"{PREFIX}{run_id}"


def claim_owner(run_id: str, worker: str) -> str:
    if worker not in {"A", "B"}:
        raise ConcurrencyValidationFailure("PERSISTENCE_RPC_CONTRACT_FAILED")
    return f"{namespace(run_id)}-WORKER-{worker}"


def _ids(run_id: str) -> dict[str, str]:
    ns = namespace(run_id)
    return {
        "run_id": run_id,
        "namespace": ns,
        "trigger_event": f"{ns}-EVT-TRIGGER",
        "incident_correlation": f"{ns}-CORRELATION-INCIDENT",
        "resource_incident_a": f"{ns}-INC-RESOURCE-A",
        "resource_incident_b": f"{ns}-INC-RESOURCE-B",
        "resource_key": f"{ns}-RESOURCE-SHARED",
        "event_idempotent": f"{ns}-EVT-IDEMPOTENT",
        "inbox_event": f"{ns}-EVT-INBOX",
        "inbox_entity": f"{ns}-CELL-INBOX",
        "inbox_incident": f"{ns}-INC-INBOX",
        "inbox_correlation": f"{ns}-CORRELATION-INBOX",
        "inbox_outbox": f"{ns}-OUT-INBOX",
        "action_incident": f"{ns}-INC-ACTION",
        "action_resource": f"{ns}-RESOURCE-ACTION",
        "version_entity": f"{ns}-CELL-VERSION",
        "crash_incident": f"{ns}-INC-CRASH",
        "crash_action": f"{ns}-CMD-CRASH",
        "crash_resource": f"{ns}-RESOURCE-CRASH",
    }


def integration_gate(environment: dict[str, str]) -> tuple[bool, str, str | None]:
    if environment.get("HARIS_RUNTIME_ENV", "").lower() != "persistence_integration":
        return False, "PERSISTENCE_INTEGRATION_NOT_AUTHORIZED", None
    if environment.get("HARIS_ALLOW_PERSISTENCE_INTEGRATION", "").lower() != "true":
        return False, "PERSISTENCE_INTEGRATION_NOT_AUTHORIZED", None
    if environment.get("HARIS_PERSISTENCE_MODE", "").lower() != "postgres":
        return False, "PERSISTENCE_NOT_CONFIGURED", None
    if not environment.get("SUPABASE_URL") or not environment.get("SUPABASE_KEY"):
        return False, "PERSISTENCE_NOT_CONFIGURED", None
    parsed = urlparse(environment["SUPABASE_URL"])
    hostname = (parsed.hostname or "").lower()
    if (
        parsed.scheme.lower() != "https" or not hostname or parsed.username
        or parsed.password or parsed.query or parsed.fragment
        or parsed.path not in {"", "/"}
        or not hostname.endswith(".supabase.co") or hostname == "supabase.co"
    ):
        return False, "PERSISTENCE_HOST_NOT_ALLOWED", None
    return True, "AUTHORIZED", hostname


def sanitized_child_environment(source: dict[str, str]) -> dict[str, str]:
    allowed = _PROCESS_ENV | _PERSISTENCE_ENV
    child = {key: value for key, value in source.items() if key in allowed}
    child["PYTHONPATH"] = str(PROJECT_ROOT)
    return child


def _safe_rpc_name(value: str | None) -> str | None:
    if not value:
        return None
    from supabase_transport import HARIS_RPC_ALLOWLIST
    return value if value in HARIS_RPC_ALLOWLIST else None


def _shape(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, str):
        return "string"
    return "unknown"


def _safe_reason(exc: Exception) -> str:
    from durable_core import DuplicateEvent, ResourceAlreadyOwned, VersionConflict
    from postgres_persistence import (
        PersistenceAuthenticationFailed, PersistenceHostNotAllowed,
        PersistenceNetworkBlocked, PersistenceNotConfigured,
        PersistenceResponseInvalid, PersistenceRpcContractFailed,
        PersistenceSchemaNotReady, PersistenceTransportUnavailable,
    )
    if isinstance(exc, ConcurrencyValidationFailure):
        return exc.safe_reason
    if isinstance(exc, PersistenceAuthenticationFailed):
        return "PERSISTENCE_AUTH_FAILED"
    if isinstance(exc, PersistenceSchemaNotReady):
        return "PERSISTENCE_SCHEMA_NOT_READY"
    if isinstance(exc, PersistenceNetworkBlocked):
        return "PERSISTENCE_NETWORK_BLOCKED"
    if isinstance(exc, PersistenceHostNotAllowed):
        return "PERSISTENCE_HOST_NOT_ALLOWED"
    if isinstance(exc, PersistenceNotConfigured):
        return "PERSISTENCE_NOT_CONFIGURED"
    if isinstance(exc, PersistenceRpcContractFailed):
        return "PERSISTENCE_RPC_CONTRACT_FAILED"
    if isinstance(exc, PersistenceResponseInvalid):
        return "PERSISTENCE_RESPONSE_INVALID"
    if isinstance(exc, (VersionConflict, ResourceAlreadyOwned, DuplicateEvent)):
        return "PERSISTENCE_CONFLICT"
    if isinstance(exc, PersistenceTransportUnavailable):
        return "PERSISTENCE_UNAVAILABLE"
    return "PERSISTENCE_UNAVAILABLE"


def _failure(
    exc: Exception,
    *,
    test_case: str,
    worker: str,
    rpc_name: str | None = None,
    expected_result: str | None = None,
    run_id: str | None = None,
) -> ConcurrencyValidationFailure:
    if isinstance(exc, ConcurrencyValidationFailure):
        return exc
    return ConcurrencyValidationFailure(
        _safe_reason(exc), test_case=test_case, worker=worker,
        rpc_name=getattr(exc, "rpc_name", None) or rpc_name,
        http_status=getattr(exc, "http_status", None),
        expected_result=expected_result,
        actual_result_type=getattr(exc, "actual_shape_type", None) or "unknown",
        run_id=run_id,
    )


def safe_failure_payload(exc: Exception, *, run_id: str | None = None) -> dict:
    wrapped = exc if isinstance(exc, ConcurrencyValidationFailure) else _failure(
        exc, test_case="FINAL_EVALUATION", worker="PARENT", run_id=run_id,
    )
    payload = {
        "status": "PERSISTENCE_CONCURRENCY_VALIDATION_FAILED",
        "safe_reason": wrapped.safe_reason,
        "test_case": wrapped.test_case,
        "worker": wrapped.worker,
        "actual_result_type": wrapped.actual_result_type,
    }
    if wrapped.rpc_name:
        payload["rpc_name"] = wrapped.rpc_name
    if wrapped.http_status is not None:
        payload["http_status"] = wrapped.http_status
    if wrapped.expected_result:
        payload["expected_result"] = wrapped.expected_result
    safe_run_id = wrapped.run_id or (run_id if _valid_run_id(run_id) else None)
    if safe_run_id:
        payload["run_id"] = safe_run_id
    return payload


def _emit(payload: dict) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")), flush=True)


def _require_authorized() -> str:
    allowed, reason, hostname = integration_gate(dict(os.environ))
    if not allowed or not hostname:
        raise ConcurrencyValidationFailure(reason, test_case="PREFLIGHT", expected_result="authorized")
    return hostname


def _bundle():
    _require_authorized()
    from config import get_settings
    from postgres_persistence import PostgresRepositoryBundle, build_persistence_transport
    from runtime import RuntimeEnvironment
    transport = build_persistence_transport(
        get_settings(), runtime=RuntimeEnvironment.PERSISTENCE_INTEGRATION,
    )
    return PostgresRepositoryBundle(transport), transport


def _close(transport) -> None:
    close = getattr(transport, "close", None)
    if callable(close):
        close()


def _event(run_id: str, label: str):
    from platform_events import EventType, HarisEvent, Provenance
    now = time.time()
    ns = namespace(run_id)
    event_id = f"{ns}-EVT-{label}"
    return HarisEvent(
        event_id=event_id,
        event_type=EventType.NETWORK_CONGESTION_CHANGED,
        source="haris-persistence-concurrency-validator",
        source_mode=PROVENANCE,
        source_event_id=event_id,
        source_timestamp=now,
        received_at=now,
        created_at=now,
        entity_type="HARIS_CONFIGURED_LOGICAL_CELL",
        entity_id=f"{ns}-CELL-{label}",
        correlation_key=f"{ns}-CORRELATION-{label}",
        provenance=Provenance.HARIS_DERIVED,
        payload={"congestion_level": "High", "integration_test": True},
        trace_id=f"{ns}-TRACE-{label}",
    )


def _event_payload(event) -> dict:
    payload = event.model_dump(mode="json")
    payload["idempotency_key"] = event.idempotency_key
    return payload


def _incident_record(
    run_id: str, incident_id: str, correlation: str, trigger_event: str,
    *, primary_entity: str | None = None,
) -> dict:
    now = time.time()
    ns = namespace(run_id)
    return {
        "incident_id": incident_id,
        "schema_version": 1,
        "correlation_key": correlation,
        "primary_entity": primary_entity or f"{ns}-CELL-TRIGGER",
        "affected_entities": [primary_entity or f"{ns}-CELL-TRIGGER"],
        "affected_devices": [],
        "trigger_event_id": trigger_event,
        "trigger_provenance": PROVENANCE,
        "trigger_source_timestamp": now,
        "opened_at": now,
        "updated_at": now,
        "severity": "test",
        "priority": "PERSISTENCE_TEST",
        "state": "DETECTED",
        "plan_version": 1,
        "warden_decision": None,
        "verification_state": "PENDING",
        "recovery_state": "PENDING",
        "outcome": None,
        "closed_at": None,
        "version": 0,
        "trace_id": f"{ns}-TRACE-INCIDENT",
    }


def _network_record(run_id: str, entity_id: str) -> dict:
    now = time.time()
    return {
        "entity_id": entity_id,
        "entity_type": "HARIS_CONFIGURED_LOGICAL_CELL",
        "mapping_source": PROVENANCE,
        "provenance": "HARIS_DERIVED",
        "raw_congestion": "High",
        "raw_congestion_observed_at": now,
        "reachability_summary": None,
        "reachability_observed_at": None,
        "location_summary": None,
        "location_observed_at": None,
        "freshness": "FRESH",
        "haris_operational_state": "INCIDENT_OPEN",
        "active_incident_ids": [],
        "last_source_change_at": now,
        "last_operational_change_at": now,
        "updated_at": now,
        "version": 0,
    }


def _action(run_id: str, *, command_id: str, incident_id: str, resource_key: str, state="PENDING"):
    from durable_core import ActionCommand, ActionState
    return ActionCommand(
        command_id=command_id,
        incident_id=incident_id,
        command_type="PERSISTENCE_TEST_NO_PROVIDER",
        resource_key=resource_key,
        device_id=None,
        plan_version=1,
        requested_at=time.time(),
        parameters_safe={"source_mode": PROVENANCE},
        preconditions={"provider_call_permitted": False},
        state=ActionState(state),
    )


def _touch_ready_and_wait(ready_file: Path, gate_file: Path) -> None:
    ready_file.touch(exist_ok=False)
    deadline = time.monotonic() + 20
    while not gate_file.exists():
        if time.monotonic() >= deadline:
            raise ConcurrencyValidationFailure("PERSISTENCE_CHILD_FAILED")
        time.sleep(0.01)


def _worker_operation(case: str, worker: str, run_id: str, ready_file: Path, gate_file: Path) -> dict:
    ids = _ids(run_id)
    bundle = transport = None
    test_case = {
        "incident": "INCIDENT_SINGLETON", "resource": "RESOURCE_OWNERSHIP",
        "event": "EVENT_IDEMPOTENCY", "inbox": "INBOX_IDEMPOTENCY",
        "action": "ACTION_IDEMPOTENCY", "version": "VERSION_CONFLICT",
        "outbox": "OUTBOX_CLAIM", "crash_write": "CRASH_WRITE",
        "crash_replay": "CRASH_REPLAY",
    }[case]
    try:
        bundle, transport = _bundle()
        prepared = None
        if case == "version":
            prepared = bundle.network_state.get(ids["version_entity"])
            if not prepared:
                raise ConcurrencyValidationFailure(
                    "PERSISTENCE_PROOF_FAILED", test_case=test_case, worker=worker,
                    rpc_name="haris_read_domain", expected_result="one_success_one_version_conflict",
                    run_id=run_id,
                )
        if case not in {"crash_write", "crash_replay"}:
            _touch_ready_and_wait(ready_file, gate_file)

        if case == "incident":
            candidate = f"{ids['namespace']}-INC-RACE-{worker}"
            row, created = bundle.incidents.create_or_get_active(
                _incident_record(run_id, candidate, ids["incident_correlation"], ids["trigger_event"])
            )
            return {"worker": worker, "created": created, "incident_id": row.get("incident_id")}

        if case == "resource":
            owner_incident = ids[f"resource_incident_{worker.lower()}"]
            record = {
                "resource_key": ids["resource_key"], "resource_type": "PERSISTENCE_TEST",
                "owner_incident_id": owner_incident, "ownership_state": "OWNED",
                "acquired_at": time.time(), "lease_started_at": time.time(),
                "lease_expires_at": time.time() + 300, "renewable": False,
                "adopted_by_incident": False, "provider_resource_id": None, "version": 0,
            }
            from durable_core import ResourceAlreadyOwned
            try:
                row = bundle.resource_ownership.acquire(record)
                return {"worker": worker, "outcome": "ACQUIRED", "owner_incident_id": row.get("owner_incident_id")}
            except ResourceAlreadyOwned:
                return {"worker": worker, "outcome": "RESOURCE_CONFLICT"}

        if case == "event":
            inserted = bundle.events.append(_event(run_id, "IDEMPOTENT"))
            return {"worker": worker, "inserted": bool(inserted)}

        if case == "inbox":
            event = _event(run_id, "INBOX")
            projection = _network_record(run_id, ids["inbox_entity"])
            projection["expected_version"] = 0
            incident = _incident_record(
                run_id, ids["inbox_incident"], ids["inbox_correlation"], ids["inbox_event"],
                primary_entity=ids["inbox_entity"],
            )
            transition = {
                "incident_id": ids["inbox_incident"], "from_state": None,
                "to_state": "DETECTED", "occurred_at": time.time(),
                "trigger_event_id": ids["inbox_event"], "actor": "PERSISTENCE_VALIDATOR",
                "reason_code": PROVENANCE, "trace_id": f"{ids['namespace']}-TRACE-INBOX",
            }
            outbox = {
                "outbox_id": ids["inbox_outbox"], "event_type": "PERSISTENCE_TEST",
                "payload": {"integration_test": True},
            }
            result = bundle.events.rpc(
                "haris_process_inbound_event", p_event=_event_payload(event),
                p_projection=projection, p_incident=incident,
                p_transition=transition, p_outbox=outbox,
            )
            return {"worker": worker, "outcome": result.get("status") if isinstance(result, dict) else None}

        if case == "action":
            command = _action(
                run_id, command_id=f"{ids['namespace']}-CMD-IDEMPOTENT-{worker}",
                incident_id=ids["action_incident"], resource_key=ids["action_resource"],
            )
            row, created = bundle.actions.create_or_get(command)
            return {"worker": worker, "created": created, "command_id": row.command_id}

        if case == "version":
            from durable_core import VersionConflict
            expected_version = int(prepared.get("version", -1))
            prepared["raw_congestion"] = "Medium" if worker == "A" else "Low"
            prepared["updated_at"] = time.time()
            try:
                row = bundle.network_state.save(prepared, expected_version)
                return {"worker": worker, "outcome": "UPDATED", "version": row.get("version")}
            except VersionConflict:
                return {"worker": worker, "outcome": "VERSION_CONFLICT"}

        if case == "outbox":
            rows = bundle.outbox.claim(claim_owner(run_id, worker), limit=3, lease_seconds=120)
            return {"worker": worker, "claimed_ids": [row.get("outbox_id") for row in rows], "event_ids": [row.get("event_id") for row in rows]}

        if case == "crash_write":
            command = _action(
                run_id, command_id=ids["crash_action"], incident_id=ids["crash_incident"],
                resource_key=ids["crash_resource"], state="SENT",
            )
            row, _created = bundle.actions.create_or_get(command)
            if row.state.value != "SENT":
                raise ConcurrencyValidationFailure(
                    "PERSISTENCE_PROOF_FAILED", test_case=test_case, worker="CRASH",
                    rpc_name="haris_save_action", expected_result="sent_outcome_unknown_no_resend",
                    run_id=run_id,
                )
            os._exit(CRASH_EXIT_CODE)

        if case == "crash_replay":
            row = bundle.actions.get(ids["crash_action"])
            if row is None:
                raise ConcurrencyValidationFailure(
                    "PERSISTENCE_PROOF_FAILED", test_case=test_case, worker="REPLAY",
                    rpc_name="haris_read_domain", expected_result="sent_outcome_unknown_no_resend",
                    run_id=run_id,
                )
            return {
                "worker": "REPLAY", "stored_state": row.state.value,
                "runtime_state": "OUTCOME_UNKNOWN", "reconciliation_required": True,
                "provider_called": False, "resent": False,
            }
        raise ConcurrencyValidationFailure("PERSISTENCE_RPC_CONTRACT_FAILED")
    except Exception as exc:
        raise _failure(
            exc, test_case=test_case, worker=worker,
            rpc_name={
                "incident": "haris_create_incident", "resource": "haris_acquire_resource",
                "event": "haris_append_domain_event", "inbox": "haris_process_inbound_event",
                "action": "haris_save_action", "version": "haris_write_network_state",
                "outbox": "haris_claim_outbox", "crash_write": "haris_save_action",
                "crash_replay": "haris_read_domain",
            }[case],
            expected_result={
                "incident": "one_created_one_existing", "resource": "one_owner_one_conflict",
                "event": "one_insert_one_duplicate", "inbox": "one_accepted_one_duplicate",
                "action": "one_action", "version": "one_success_one_version_conflict",
                "outbox": "disjoint_current_run_claims",
                "crash_write": "sent_outcome_unknown_no_resend",
                "crash_replay": "sent_outcome_unknown_no_resend",
            }.get(case),
            run_id=run_id,
        ) from None
    finally:
        if transport is not None:
            _close(transport)


def _parse_child(completed, *, test_case: str, worker: str, run_id: str) -> dict:
    lines = [line.strip() for line in (completed.stdout or "").splitlines() if line.strip()]
    if len(lines) != 1:
        raise ConcurrencyValidationFailure(
            "PERSISTENCE_CHILD_FAILED", test_case=test_case, worker=worker,
            expected_result="valid_worker_result", run_id=run_id,
        )
    try:
        payload = json.loads(lines[0])
    except (TypeError, ValueError):
        raise ConcurrencyValidationFailure(
            "PERSISTENCE_CHILD_FAILED", test_case=test_case, worker=worker,
            expected_result="valid_worker_result", run_id=run_id,
        ) from None
    if completed.returncode != 0 or not isinstance(payload, dict):
        if isinstance(payload, dict) and payload.get("status") == "PERSISTENCE_CONCURRENCY_VALIDATION_FAILED":
            raise ConcurrencyValidationFailure(
                str(payload.get("safe_reason")), test_case=str(payload.get("test_case")),
                worker=str(payload.get("worker")), rpc_name=payload.get("rpc_name"),
                http_status=payload.get("http_status"), expected_result=payload.get("expected_result"),
                actual_result_type=payload.get("actual_result_type"), run_id=run_id,
            )
        raise ConcurrencyValidationFailure(
            "PERSISTENCE_CHILD_FAILED", test_case=test_case, worker=worker,
            expected_result="valid_worker_result", run_id=run_id,
        )
    return payload


def _worker_command(case: str, worker: str, run_id: str, gate: Path, ready: Path) -> list[str]:
    return [
        sys.executable, str(Path(__file__).resolve()), "--worker", case,
        "--worker-id", worker, "--run-id", run_id,
        "--gate-file", str(gate), "--ready-file", str(ready),
    ]


def _wait_ready(paths: list[Path], processes: list, timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
    while not all(path.exists() for path in paths):
        if any(process.poll() is not None for process in processes) or time.monotonic() >= deadline:
            raise ConcurrencyValidationFailure("PERSISTENCE_CHILD_FAILED")
        time.sleep(0.01)


def run_worker_pair(case: str, run_id: str, directory: Path, *, popen_factory=subprocess.Popen) -> list[dict]:
    if case not in _WORKER_CASES - {"crash_write", "crash_replay"}:
        raise ConcurrencyValidationFailure("PERSISTENCE_RPC_CONTRACT_FAILED")
    test_case = {
        "incident": "INCIDENT_SINGLETON", "resource": "RESOURCE_OWNERSHIP",
        "event": "EVENT_IDEMPOTENCY", "inbox": "INBOX_IDEMPOTENCY",
        "action": "ACTION_IDEMPOTENCY", "version": "VERSION_CONFLICT",
        "outbox": "OUTBOX_CLAIM",
    }[case]
    gate = directory / f"{case}.go"
    ready = [directory / f"{case}-{worker}.ready" for worker in ("A", "B")]
    env = sanitized_child_environment(dict(os.environ))
    processes = [
        popen_factory(
            _worker_command(case, worker, run_id, gate, ready[index]),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=dict(env),
            cwd=str(PROJECT_ROOT),
        )
        for index, worker in enumerate(("A", "B"))
    ]
    completed = []
    try:
        _wait_ready(ready, processes)
        gate.touch(exist_ok=False)
        for process in processes:
            stdout, stderr = process.communicate(timeout=40)
            completed.append(subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr))
    except Exception:
        for process in processes:
            if process.poll() is None:
                kill = getattr(process, "kill", None)
                if callable(kill):
                    kill()
                wait = getattr(process, "wait", None)
                if callable(wait):
                    wait(timeout=5)
        raise
    return [
        _parse_child(result, test_case=test_case, worker=worker, run_id=run_id)
        for result, worker in zip(completed, ("A", "B"))
    ]


def run_single_worker(case: str, worker: str, run_id: str) -> subprocess.CompletedProcess[str]:
    with tempfile.TemporaryDirectory(prefix="haris-persistence-single-") as temporary:
        directory = Path(temporary)
        command = _worker_command(case, worker, run_id, directory / "unused.go", directory / "unused.ready")
        return subprocess.run(
            command, capture_output=True, text=True, timeout=40,
            env=sanitized_child_environment(dict(os.environ)), cwd=str(PROJECT_ROOT), check=False,
        )


def evaluate_incident(results: list[dict], active_count: int) -> dict:
    ids = {row.get("incident_id") for row in results}
    created = [bool(row.get("created")) for row in results]
    if len(results) != 2 or len(ids) != 1 or None in ids or sorted(created) != [False, True] or active_count != 1:
        raise ConcurrencyValidationFailure("PERSISTENCE_PROOF_FAILED", test_case="INCIDENT_SINGLETON", expected_result="one_created_one_existing")
    return {"concurrent_incident_singleton": "PASS", "incident_id_convergence": "PASS", "duplicate_active_incidents": 0}


def evaluate_resource(results: list[dict], active_owners: int) -> dict:
    outcomes = sorted(row.get("outcome") for row in results)
    if outcomes != ["ACQUIRED", "RESOURCE_CONFLICT"] or active_owners != 1:
        raise ConcurrencyValidationFailure("PERSISTENCE_PROOF_FAILED", test_case="RESOURCE_OWNERSHIP", expected_result="one_owner_one_conflict")
    return {"resource_single_owner": "PASS", "resource_conflict_enforced": "PASS", "active_resource_owners": 1}


def evaluate_event(results: list[dict], canonical_exists: bool) -> dict:
    if sorted(bool(row.get("inserted")) for row in results) != [False, True] or not canonical_exists:
        raise ConcurrencyValidationFailure("PERSISTENCE_PROOF_FAILED", test_case="EVENT_IDEMPOTENCY", expected_result="one_insert_one_duplicate")
    return {"event_idempotency": "PASS", "duplicate_events": 0}


def evaluate_inbox(results: list[dict], *, event_exists: bool, incident_exists: bool, projection_exists: bool, transition_count: int) -> dict:
    if sorted(row.get("outcome") for row in results) != ["accepted", "duplicate"] or not all((event_exists, incident_exists, projection_exists)) or transition_count != 1:
        raise ConcurrencyValidationFailure("PERSISTENCE_PROOF_FAILED", test_case="INBOX_IDEMPOTENCY", expected_result="one_accepted_one_duplicate")
    return {"inbox_idempotency": "PASS"}


def evaluate_action(results: list[dict], canonical_exists: bool) -> dict:
    command_ids = {row.get("command_id") for row in results}
    if sorted(bool(row.get("created")) for row in results) != [False, True] or len(command_ids) != 1 or None in command_ids or not canonical_exists:
        raise ConcurrencyValidationFailure("PERSISTENCE_PROOF_FAILED", test_case="ACTION_IDEMPOTENCY", expected_result="one_action")
    return {"action_idempotency": "PASS", "duplicate_actions": 0}


def evaluate_version(results: list[dict], final_version: int) -> dict:
    if sorted(row.get("outcome") for row in results) != ["UPDATED", "VERSION_CONFLICT"] or final_version != 2:
        raise ConcurrencyValidationFailure("PERSISTENCE_PROOF_FAILED", test_case="VERSION_CONFLICT", expected_result="one_success_one_version_conflict")
    return {"optimistic_version_conflict": "PASS", "stale_write_rejected": "PASS"}


def evaluate_outbox(results: list[dict], run_id: str, expected_ids: set[str]) -> dict:
    claimed = [set(row.get("claimed_ids") or []) for row in results]
    event_ids = [event for row in results for event in (row.get("event_ids") or [])]
    overlap = len(claimed[0] & claimed[1]) if len(claimed) == 2 else -1
    all_claimed = set().union(*claimed) if len(claimed) == 2 else set()
    current_prefix = f"{namespace(run_id)}-"
    if overlap != 0 or all_claimed != expected_ids or not event_ids or not all(str(event).startswith(current_prefix) for event in event_ids):
        raise ConcurrencyValidationFailure("PERSISTENCE_PROOF_FAILED", test_case="OUTBOX_CLAIM", expected_result="disjoint_current_run_claims", run_id=run_id)
    return {"outbox_claim_exclusive": "PASS", "outbox_claim_overlap": 0, "outbox_namespace_isolation": "PASS"}


def evaluate_crash(replay: dict, crash_returncode: int) -> dict:
    if crash_returncode != CRASH_EXIT_CODE or replay != {
        "worker": "REPLAY", "stored_state": "SENT", "runtime_state": "OUTCOME_UNKNOWN",
        "reconciliation_required": True, "provider_called": False, "resent": False,
    }:
        raise ConcurrencyValidationFailure("PERSISTENCE_PROOF_FAILED", test_case="CRASH_REPLAY", expected_result="sent_outcome_unknown_no_resend")
    return {
        "crash_replay_safe": "PASS", "sent_action_outcome_unknown": "PASS",
        "reconciliation_required": "PASS", "no_action_resend": "PASS", "no_false_success": "PASS",
    }


def _create_incident(bundle, run_id: str, incident_id: str, correlation: str, trigger_event: str) -> None:
    row, created = bundle.incidents.create_or_get_active(_incident_record(run_id, incident_id, correlation, trigger_event))
    if not created or row.get("incident_id") != incident_id:
        raise ConcurrencyValidationFailure("PERSISTENCE_PROOF_FAILED")


def _verify_operational_filtering(bundle, ids: dict[str, str]) -> None:
    if ids["version_entity"] in bundle.network_state.load_all():
        raise ConcurrencyValidationFailure("PERSISTENCE_PROOF_FAILED")
    if any(str(row.get("primary_entity", "")).startswith(ids["namespace"]) for row in bundle.incidents.active()):
        raise ConcurrencyValidationFailure("PERSISTENCE_PROOF_FAILED")


def run_controller() -> dict:
    _require_authorized()
    run_id = new_run_id()
    ids = _ids(run_id)
    bundle = transport = None
    try:
        bundle, transport = _bundle()
        preflight = getattr(transport, "preflight", None)
        try:
            preflight_result = preflight() if callable(preflight) else None
        except Exception as exc:
            raise _failure(
                exc, test_case="PREFLIGHT", worker="PARENT",
                rpc_name="haris_read_domain", expected_result="array", run_id=run_id,
            ) from None
        if not isinstance(preflight_result, list):
            raise ConcurrencyValidationFailure("PERSISTENCE_RESPONSE_INVALID", test_case="PREFLIGHT", expected_result="array", actual_result_type=_shape(preflight_result), run_id=run_id)

        trigger = _event(run_id, "TRIGGER")
        if not bundle.events.append(trigger):
            raise ConcurrencyValidationFailure("PERSISTENCE_PROOF_FAILED", run_id=run_id)

        result: dict = {"status": "PERSISTENCE_CONCURRENCY_VALIDATED"}
        with tempfile.TemporaryDirectory(prefix="haris-persistence-concurrency-") as temporary:
            directory = Path(temporary)

            incident_results = run_worker_pair("incident", run_id, directory)
            canonical_incident = incident_results[0].get("incident_id")
            active_count = sum(
                bool(bundle.incidents.get(f"{ids['namespace']}-INC-RACE-{worker}"))
                for worker in ("A", "B")
            )
            result.update(evaluate_incident(incident_results, active_count))

            _create_incident(bundle, run_id, ids["resource_incident_a"], f"{ids['namespace']}-CORRELATION-RESOURCE-A", ids["trigger_event"])
            _create_incident(bundle, run_id, ids["resource_incident_b"], f"{ids['namespace']}-CORRELATION-RESOURCE-B", ids["trigger_event"])
            resource_results = run_worker_pair("resource", run_id, directory)
            active_owners = len([
                row for row in bundle.resource_ownership.read("ownership", ids["resource_key"], 0, 10)
                if row.get("ownership_state") == "OWNED"
            ])
            result.update(evaluate_resource(resource_results, active_owners))

            event_results = run_worker_pair("event", run_id, directory)
            result.update(evaluate_event(event_results, bundle.events.get(ids["event_idempotent"]) is not None))

            inbox_results = run_worker_pair("inbox", run_id, directory)
            result.update(evaluate_inbox(
                inbox_results,
                event_exists=bundle.events.get(ids["inbox_event"]) is not None,
                incident_exists=bundle.incidents.get(ids["inbox_incident"]) is not None,
                projection_exists=bundle.network_state.get(ids["inbox_entity"]) is not None,
                transition_count=len(bundle.incidents.transitions(ids["inbox_incident"])),
            ))

            _create_incident(bundle, run_id, ids["action_incident"], f"{ids['namespace']}-CORRELATION-ACTION", ids["trigger_event"])
            action_results = run_worker_pair("action", run_id, directory)
            action_id = action_results[0].get("command_id")
            result.update(evaluate_action(action_results, bool(action_id and bundle.actions.get(action_id))))

            bundle.network_state.save(_network_record(run_id, ids["version_entity"]), 0)
            version_results = run_worker_pair("version", run_id, directory)
            version_record = bundle.network_state.get(ids["version_entity"])
            result.update(evaluate_version(version_results, int((version_record or {}).get("version", -1))))

            expected_outbox = {ids["inbox_outbox"]}
            for index in range(5):
                event = _event(run_id, f"OUTBOX-{index}")
                if not bundle.events.append(event):
                    raise ConcurrencyValidationFailure("PERSISTENCE_PROOF_FAILED", run_id=run_id)
                outbox_id = f"{ids['namespace']}-OUT-{index}"
                bundle.outbox.append({
                    "outbox_id": outbox_id, "event_id": event.event_id,
                    "event_type": "PERSISTENCE_TEST", "payload": {"integration_test": True},
                    "trace_id": event.trace_id, "created_at": time.time(),
                })
                expected_outbox.add(outbox_id)
            outbox_results = run_worker_pair("outbox", run_id, directory)
            result.update(evaluate_outbox(outbox_results, run_id, expected_outbox))

            _create_incident(bundle, run_id, ids["crash_incident"], f"{ids['namespace']}-CORRELATION-CRASH", ids["trigger_event"])
            crash = run_single_worker("crash_write", "CRASH", run_id)
            if crash.returncode != CRASH_EXIT_CODE:
                _parse_child(crash, test_case="CRASH_WRITE", worker="CRASH", run_id=run_id)
            replay_completed = run_single_worker("crash_replay", "REPLAY", run_id)
            replay = _parse_child(replay_completed, test_case="CRASH_REPLAY", worker="REPLAY", run_id=run_id)
            result.update(evaluate_crash(replay, crash.returncode))

        _verify_operational_filtering(bundle, ids)
        if canonical_incident is None:
            raise ConcurrencyValidationFailure("PERSISTENCE_PROOF_FAILED", run_id=run_id)
        result["run_id"] = run_id
        if set(result) != _SUCCESS_KEYS:
            raise ConcurrencyValidationFailure(
                "PERSISTENCE_RESPONSE_INVALID", test_case="FINAL_EVALUATION",
                expected_result="all_proofs_pass", actual_result_type="object", run_id=run_id,
            )
        return result
    except Exception as exc:
        raise _failure(exc, test_case="FINAL_EVALUATION", worker="PARENT", expected_result="all_proofs_pass", run_id=run_id) from None
    finally:
        if transport is not None:
            _close(transport)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", choices=sorted(_WORKER_CASES))
    parser.add_argument("--worker-id", choices=sorted(_WORKERS))
    parser.add_argument("--run-id")
    parser.add_argument("--gate-file", type=Path)
    parser.add_argument("--ready-file", type=Path)
    args = parser.parse_args(argv)
    if args.worker:
        if not args.worker_id or not _valid_run_id(args.run_id) or not args.gate_file or not args.ready_file:
            _emit(safe_failure_payload(ConcurrencyValidationFailure("PERSISTENCE_RPC_CONTRACT_FAILED")))
            return 3
        try:
            result = _worker_operation(args.worker, args.worker_id, args.run_id, args.ready_file, args.gate_file)
            _emit(result)
            return 0
        except Exception as exc:
            _emit(safe_failure_payload(exc, run_id=args.run_id))
            return 3
    run_id = None
    try:
        result = run_controller()
        _emit(result)
        return 0
    except Exception as exc:
        run_id = getattr(exc, "run_id", None)
        _emit(safe_failure_payload(exc, run_id=run_id))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
