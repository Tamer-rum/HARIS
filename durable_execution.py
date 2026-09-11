"""Phase 7C durable controlled actuation, verification, and recovery.

The durable action repository is authoritative.  A provider mutation may be
invoked only after a READY action wins an optimistic READY -> SENT update and
durable resource ownership is confirmed.  SENT is never interpreted as
success and ambiguous results are never resent blindly.
"""
from __future__ import annotations

import copy
import hashlib
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Protocol

from network_as_code.errors import NotFound as NokiaNotFound
from pydantic import BaseModel, ConfigDict, Field

from durable_core import (
    ActionCommand, ActionState, IncidentState, RecoveryState, RepositoryUnavailable,
    ResourceAlreadyOwned, VerificationState, VersionConflict,
)
from recovery_consistency import save_recovery_and_repair


ACTION_KIND = {
    "QOD_PLAN": "qos",
    "GEOFENCE_PLAN": "geofence",
    "SLICE_ATTACH_PLAN": "slice_attach",
}
ROLLBACK_COMMAND = {
    "QOD_PLAN": "QOD_RELEASE",
    "GEOFENCE_PLAN": "GEOFENCE_DELETE",
    "SLICE_ATTACH_PLAN": "SLICE_DETACH",
}
ACTIONABLE_INCIDENT_STATES = {
    IncidentState.APPROVED, IncidentState.MITIGATING, IncidentState.VERIFYING,
}
TERMINAL_ACTION_STATES = {
    ActionState.SUCCESS, ActionState.FAILED, ActionState.ROLLED_BACK,
}


class ProviderAmbiguousOutcome(RuntimeError):
    """The provider may have received the request; retry is prohibited."""


class ProviderExplicitFailure(RuntimeError):
    """The provider conclusively rejected or failed the request."""


class ProviderMutationResult(BaseModel):
    model_config = ConfigDict(extra="forbid")
    outcome: str
    provider_resource_id: Optional[str] = None
    provider_state: Optional[str] = None
    reason: str
    provenance: str


class ActionVerificationResult(BaseModel):
    model_config = ConfigDict(extra="forbid")
    outcome: str
    reason: str
    provenance: str
    mitigation_improved: bool = False
    evidence: Dict[str, Any] = Field(default_factory=dict)


class DurableExecutionResult(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: str
    action_id: str
    incident_id: Optional[str] = None
    action_state: Optional[str] = None
    provider_invoked: bool = False
    provider_execution_provenance: str = "UNAVAILABLE"
    verification_state: Optional[str] = None
    rollback_state: Optional[str] = None
    reason: str
    duplicate: bool = False

    def public(self) -> Dict[str, Any]:
        return self.model_dump()


class DurableProviderAdapter(Protocol):
    execution_provenance: str

    def capability(self, action: ActionCommand, context: Dict[str, Any]) -> Dict[str, Any]: ...
    async def execute(self, action: ActionCommand) -> ProviderMutationResult: ...
    async def reconcile(self, action: ActionCommand) -> ProviderMutationResult: ...
    async def verify(
        self, action: ActionCommand, provider_resource_id: Optional[str],
        baseline: Dict[str, Any],
    ) -> ActionVerificationResult: ...
    async def rollback(
        self, action: ActionCommand, provider_resource_id: str,
    ) -> ProviderMutationResult: ...
    async def verify_rollback(
        self, action: ActionCommand, provider_resource_id: str,
    ) -> ActionVerificationResult: ...


class ExistingNokiaActuatorAdapter:
    """Thin adapter over the existing HARIS Nokia client implementation."""

    def __init__(self, client: Any, settings: Any) -> None:
        self.client = client
        self.settings = settings
        self.execution_provenance = (
            "FIXTURE_SIMULATED" if settings.nac_mode == "fixture" else "NOKIA_LIVE"
        )

    def capability(self, action: ActionCommand, context: Dict[str, Any]) -> Dict[str, Any]:
        kind = ACTION_KIND.get(action.command_type)
        if kind is None:
            return self._capability(False, "unsupported_action", False, False, False)
        error = self.client.action_safety_error(kind, action.parameters_safe)
        if error:
            return self._capability(False, "adapter_preconditions_unavailable", False, False, False)
        if kind == "slice_attach" and context.get("slice_status") != "OPERATING":
            return self._capability(False, "slice_not_operating", False, False, False)
        return {
            "action_type": action.command_type,
            "provider_adapter": type(self.client).__name__,
            "mutation_supported": True,
            "live_write_eligible": kind in {"qos", "geofence", "slice_attach"},
            "required_preconditions": ["CURRENT_WARDEN_ALLOW", "DURABLE_OWNERSHIP", "LIVE_WRITE_GATE"],
            "provider_native_idempotency": False,
            "reconciliation_read_supported": kind in {"qos", "geofence"},
            "verification_supported": kind in {"qos", "geofence"},
            "rollback_supported": kind in {"qos", "geofence"},
            "current_evidence_status": "AVAILABLE",
            "reason": "capability_preconditions_satisfied",
        }

    def _capability(self, supported: bool, reason: str, reconcile: bool, verify: bool, rollback: bool) -> Dict[str, Any]:
        return {
            "action_type": "UNSUPPORTED", "provider_adapter": type(self.client).__name__,
            "mutation_supported": supported, "live_write_eligible": supported,
            "required_preconditions": [], "provider_native_idempotency": False,
            "reconciliation_read_supported": reconcile,
            "verification_supported": verify, "rollback_supported": rollback,
            "current_evidence_status": "UNAVAILABLE", "reason": reason,
        }

    async def execute(self, action: ActionCommand) -> ProviderMutationResult:
        try:
            if action.command_type == "QOD_PLAN":
                result = await self.client.request_qos(
                    action.device_id, action.parameters_safe["profile"],
                    int(action.parameters_safe["duration_seconds"]),
                )
                return ProviderMutationResult(
                    outcome="ACCEPTED", provider_resource_id=result.session_id,
                    provider_state="REQUESTED", reason="provider_request_accepted",
                    provenance=self.execution_provenance,
                )
            if action.command_type == "GEOFENCE_PLAN":
                result = await self.client.create_geofence(
                    action.device_id, action.parameters_safe["polygon_id"],
                )
                return ProviderMutationResult(
                    outcome="ACCEPTED" if result.active else "FAILED",
                    provider_resource_id=result.subscription_id,
                    provider_state="ACTIVE" if result.active else "INACTIVE",
                    reason="provider_request_accepted" if result.active else "provider_explicit_failure",
                    provenance=self.execution_provenance,
                )
            if action.command_type == "SLICE_ATTACH_PLAN":
                result = await self.client.attach_slice(
                    action.device_id, action.parameters_safe["slice_id"],
                )
                return ProviderMutationResult(
                    outcome="ACCEPTED" if result.attached else "FAILED",
                    provider_state="ATTACHED" if result.attached else "NOT_ATTACHED",
                    reason="provider_request_accepted" if result.attached else "provider_explicit_failure",
                    provenance=self.execution_provenance,
                )
            raise ProviderExplicitFailure("unsupported_action")
        except ProviderExplicitFailure:
            raise
        except Exception as exc:
            # Provider exception details may contain URLs or credentials.
            raise ProviderAmbiguousOutcome("provider_outcome_ambiguous") from None

    async def reconcile(self, action: ActionCommand) -> ProviderMutationResult:
        resource_id = action.provider_resource_id
        if not resource_id:
            return ProviderMutationResult(
                outcome="UNKNOWN", reason="provider_resource_identity_unavailable",
                provenance=self.execution_provenance,
            )
        try:
            if self.settings.nac_mode == "fixture":
                if action.command_type == "QOD_PLAN":
                    item = self.client.state.get("qos", {}).get(resource_id)
                    return ProviderMutationResult(
                        outcome="ACCEPTED" if item else "FAILED",
                        provider_resource_id=resource_id,
                        provider_state="ACTIVE" if item and item.get("active") else "TERMINAL",
                        reason="provider_resource_found" if item else "provider_resource_absent",
                        provenance="FIXTURE_SIMULATED",
                    )
                if action.command_type == "GEOFENCE_PLAN":
                    item = self.client.state.get("geofences", {}).get(resource_id)
                    return ProviderMutationResult(
                        outcome="ACCEPTED" if item else "FAILED",
                        provider_resource_id=resource_id,
                        provider_state="ACTIVE" if item and item.get("active") else "TERMINAL",
                        reason="provider_resource_found" if item else "provider_resource_absent",
                        provenance="FIXTURE_SIMULATED",
                    )
            # The existing live SDK wrapper proves GET support for QoD and
            # geofencing resources only when their provider ID is known.
            if action.command_type == "QOD_PLAN" and hasattr(self.client, "client"):
                session = await __import__("asyncio").to_thread(self.client.client.sessions.get, resource_id)
                state = str(getattr(session, "status", "UNKNOWN")).upper()
                return ProviderMutationResult(
                    outcome="ACCEPTED", provider_resource_id=resource_id,
                    provider_state=state, reason="provider_resource_found",
                    provenance="NOKIA_LIVE",
                )
            if action.command_type == "GEOFENCE_PLAN" and hasattr(self.client, "client"):
                subscription = await __import__("asyncio").to_thread(self.client.client.geofencing.get, resource_id)
                return ProviderMutationResult(
                    outcome="ACCEPTED", provider_resource_id=resource_id,
                    provider_state="ACTIVE" if subscription is not None else "UNKNOWN",
                    reason="provider_resource_found" if subscription is not None else "provider_resource_unknown",
                    provenance="NOKIA_LIVE",
                )
        except Exception:
            return ProviderMutationResult(
                outcome="UNKNOWN", provider_resource_id=resource_id,
                reason="provider_reconciliation_unavailable", provenance=self.execution_provenance,
            )
        return ProviderMutationResult(
            outcome="UNKNOWN", provider_resource_id=resource_id,
            reason="provider_reconciliation_unsupported", provenance=self.execution_provenance,
        )

    async def verify(
        self, action: ActionCommand, provider_resource_id: Optional[str],
        baseline: Dict[str, Any],
    ) -> ActionVerificationResult:
        if action.command_type == "GEOFENCE_PLAN":
            reconciled = await self.reconcile(action)
            active = reconciled.outcome == "ACCEPTED" and reconciled.provider_state == "ACTIVE"
            return ActionVerificationResult(
                outcome="VERIFIED_RESOURCE" if active else "VERIFICATION_UNAVAILABLE",
                reason="geofence_resource_active" if active else "geofence_resource_unverified",
                provenance=reconciled.provenance, mitigation_improved=False,
                evidence={"resource_state": reconciled.provider_state or "UNAVAILABLE"},
            )
        if action.command_type != "QOD_PLAN":
            return ActionVerificationResult(
                outcome="VERIFICATION_UNAVAILABLE", reason="verification_contract_unavailable",
                provenance="UNAVAILABLE",
            )
        lifecycle = await self.reconcile(action)
        if lifecycle.outcome != "ACCEPTED" or lifecycle.provider_state not in {"ACTIVE", "AVAILABLE"}:
            return ActionVerificationResult(
                outcome=("VERIFICATION_PENDING" if lifecycle.outcome == "ACCEPTED"
                         else "VERIFICATION_UNAVAILABLE"),
                reason=("qod_lifecycle_not_available" if lifecycle.outcome == "ACCEPTED"
                        else "qod_lifecycle_unverified"),
                provenance=lifecycle.provenance,
                evidence={"resource_state": lifecycle.provider_state or "UNAVAILABLE"},
            )
        cell_id = baseline.get("cell_id")
        before = baseline.get("congestion_level")
        if not cell_id or before not in {"None", "Low", "Medium", "High"}:
            return ActionVerificationResult(
                outcome="VERIFICATION_UNAVAILABLE", reason="categorical_baseline_unavailable",
                provenance="UNAVAILABLE",
            )
        try:
            readings = await self.client.congestion_insights([cell_id])
        except Exception:
            return ActionVerificationResult(
                outcome="VERIFICATION_UNAVAILABLE", reason="categorical_readback_unavailable",
                provenance="UNAVAILABLE",
            )
        reading = next((item for item in readings if item.cell_id == cell_id), None)
        if reading is None:
            return ActionVerificationResult(
                outcome="VERIFICATION_UNAVAILABLE", reason="categorical_readback_unavailable",
                provenance="UNAVAILABLE",
            )
        rank = {"None": 0, "Low": 1, "Medium": 2, "High": 3}
        after = reading.congestion_level
        if after not in rank:
            return ActionVerificationResult(
                outcome="VERIFICATION_UNAVAILABLE", reason="categorical_readback_invalid",
                provenance="UNAVAILABLE",
            )
        baseline_stop = baseline.get("interval_stop")
        readback_stop = getattr(reading, "interval_stop", None)
        if baseline_stop:
            try:
                before_time = datetime.fromisoformat(str(baseline_stop).replace("Z", "+00:00"))
                after_time = datetime.fromisoformat(str(readback_stop).replace("Z", "+00:00"))
            except (TypeError, ValueError):
                return ActionVerificationResult(
                    outcome="VERIFICATION_UNAVAILABLE", reason="verification_interval_invalid",
                    provenance="UNAVAILABLE",
                )
            if after_time <= before_time:
                return ActionVerificationResult(
                    outcome="VERIFICATION_UNAVAILABLE", reason="verification_interval_not_newer",
                    provenance="UNAVAILABLE",
                )
        outcome = "VERIFIED_IMPROVED" if rank[after] < rank[before] else (
            "VERIFIED_NO_IMPROVEMENT" if rank[after] == rank[before] else "VERIFIED_DEGRADED"
        )
        return ActionVerificationResult(
            outcome=outcome, reason="categorical_congestion_compared",
            provenance=self.execution_provenance,
            mitigation_improved=outcome == "VERIFIED_IMPROVED",
            evidence={
                "cell_id": cell_id, "before_level": before, "after_level": after,
                "before_interval_stop": baseline_stop,
                "after_interval_stop": readback_stop,
            },
        )

    async def rollback(self, action: ActionCommand, provider_resource_id: str) -> ProviderMutationResult:
        try:
            if action.command_type == "QOD_PLAN":
                ok = await self.client.release_qos(provider_resource_id)
            elif action.command_type == "GEOFENCE_PLAN":
                ok = await self.client.delete_geofence(provider_resource_id)
            else:
                raise ProviderExplicitFailure("rollback_unsupported")
            return ProviderMutationResult(
                outcome="ACCEPTED" if ok else "FAILED",
                provider_resource_id=provider_resource_id,
                provider_state="RELEASED" if ok else "UNKNOWN",
                reason="rollback_accepted" if ok else "rollback_explicit_failure",
                provenance=self.execution_provenance,
            )
        except ProviderExplicitFailure:
            raise
        except Exception:
            raise ProviderAmbiguousOutcome("rollback_outcome_ambiguous") from None

    async def verify_rollback(self, action: ActionCommand, provider_resource_id: str) -> ActionVerificationResult:
        if self.settings.nac_mode == "fixture":
            collection = "qos" if action.command_type == "QOD_PLAN" else "geofences"
            item = self.client.state.get(collection, {}).get(provider_resource_id)
            inactive = bool(item) and item.get("active") is False
            return ActionVerificationResult(
                outcome="VERIFIED_ROLLBACK" if inactive else "VERIFICATION_UNAVAILABLE",
                reason="provider_resource_inactive" if inactive else "rollback_readback_unavailable",
                provenance="FIXTURE_SIMULATED", evidence={"resource_inactive": inactive},
            )
        if action.command_type == "QOD_PLAN" and hasattr(self.client, "client"):
            try:
                session = await __import__("asyncio").to_thread(
                    self.client.client.sessions.get, provider_resource_id,
                )
            except NokiaNotFound:
                # Nokia's typed 404 is authoritative absence for the
                # known-existing resource after an already-accepted rollback.
                return ActionVerificationResult(
                    outcome="VERIFIED_ROLLBACK",
                    reason="provider_resource_absent",
                    provenance="NOKIA_LIVE",
                    evidence={"resource_state": "ABSENT"},
                )
            except Exception:
                # A transport/provider error cannot safely prove absence.
                return ActionVerificationResult(
                    outcome="VERIFICATION_UNAVAILABLE",
                    reason="live_rollback_readback_unavailable",
                    provenance="UNAVAILABLE",
                )
            state = str(getattr(session, "status", "UNKNOWN")).upper() if session is not None else "ABSENT"
            inactive = state in {"ABSENT", "TERMINATED", "RELEASED", "DELETED", "INACTIVE"}
            return ActionVerificationResult(
                outcome="VERIFIED_ROLLBACK" if inactive else "VERIFICATION_UNAVAILABLE",
                reason=("provider_resource_inactive" if inactive
                        else "live_rollback_not_yet_terminal"),
                provenance="NOKIA_LIVE",
                evidence={"resource_state": state},
            )
        return ActionVerificationResult(
            outcome="VERIFICATION_UNAVAILABLE", reason="live_rollback_readback_not_proven",
            provenance="UNAVAILABLE",
        )


class DurableActionExecutionService:
    """Canonical action-ID-only execution boundary."""

    def __init__(
        self, *, bundle: Any, adapter: DurableProviderAdapter, settings: Any,
        is_ready: Callable[[], bool], metrics: Optional[Any] = None,
        failure_hook: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.bundle = bundle
        self.adapter = adapter
        self.settings = settings
        self.is_ready = is_ready
        self.metrics = metrics
        self.failure_hook = failure_hook

    def _metric(self, name: str) -> None:
        if self.metrics is not None:
            self.metrics.increment(name)

    async def handle_durable_decision_ready(self, row: Dict[str, Any]) -> list[DurableExecutionResult]:
        """Consume only durable action identities from a committed decision.

        The outbox payload is a wake-up/reference, never execution authority;
        ``execute_ready_action`` reloads every action, incident and guardrail.
        """
        if row.get("event_type") != "DURABLE_DECISION_READY":
            raise RepositoryUnavailable("invalid durable decision reference")
        payload = row.get("payload") or {}
        action_ids = payload.get("action_ids") or []
        if not isinstance(action_ids, list) or any(not isinstance(item, str) or not item for item in action_ids):
            raise RepositoryUnavailable("invalid durable action references")
        return [await self.execute_ready_action(action_id) for action_id in action_ids]

    async def handle_durable_action_ready(self, row: Dict[str, Any]) -> DurableExecutionResult:
        """Execute one already-durable action emitted by recovery policy."""
        if row.get("event_type") != "DURABLE_ACTION_EXECUTION_READY":
            raise RepositoryUnavailable("invalid durable action work reference")
        action_id = str((row.get("payload") or {}).get("action_id") or "")
        if not action_id:
            raise RepositoryUnavailable("durable action work reference is missing")
        return await self.execute_ready_action(action_id)

    def _failpoint(self, stage: str) -> None:
        if self.failure_hook:
            self.failure_hook(stage)

    def capability_matrix(self, action_id: str) -> Dict[str, Any]:
        action = self.bundle.actions.get(action_id)
        if action is None:
            return {"status": "UNAVAILABLE", "reason": "action_not_found"}
        return self.adapter.capability(action, self._capability_context(action))

    def resolve_if_verified(self, incident_id: str, plan_version: int) -> None:
        """Public deterministic resolution gate shared with Phase 7D."""
        self._maybe_resolve(incident_id, plan_version)

    def prepare_validation_cleanup(self, action_id: str, run_id: str) -> ActionCommand:
        """Durably authorize cleanup only for this harness-created QoD action.

        This method performs no provider operation. The returned
        ROLLBACK_REQUIRED action must still pass through execute_ready_action,
        which preserves the Phase 7C SENT/outcome/verification boundary.
        """
        action = self.bundle.actions.get(action_id)
        if action is None:
            raise RepositoryUnavailable("action_not_found")
        expected_incident = f"{run_id}-INCIDENT"
        if (
            not run_id.startswith("REAL-QOD-TEST-")
            or action.incident_id != expected_incident
            or action.command_type != "QOD_PLAN"
            or not action.provider_resource_id
            or action.state not in {
                ActionState.ACKNOWLEDGED, ActionState.SUCCESS,
                ActionState.RECONCILIATION_REQUIRED, ActionState.ROLLBACK_REQUIRED,
            }
        ):
            raise RepositoryUnavailable("validation_cleanup_not_authorized")
        owner = self.bundle.resource_ownership.get_active(action.resource_key)
        if owner and owner.get("owner_incident_id") != action.incident_id:
            raise ResourceAlreadyOwned("validation_cleanup_ownership_conflict")
        if owner is None:
            now = time.time()
            self.bundle.resource_ownership.acquire({
                "resource_key": action.resource_key, "resource_type": "DEVICE",
                "owner_incident_id": action.incident_id, "acquired_at": now,
                "lease_started_at": now, "lease_expires_at": now + 300,
                "renewable": False, "adopted_by_incident": False,
                "provider_resource_id": action.provider_resource_id,
            })
        if action.state is ActionState.ROLLBACK_REQUIRED:
            return action
        return self._update_action(
            action, ActionState.ROLLBACK_REQUIRED,
            "controlled_real_qod_validation_cleanup",
        )

    def complete_verified_validation_cleanup(
        self, original_action_id: str, cleanup_action_id: str,
    ) -> ActionCommand:
        """Persist a cleanup only after Phase 7D proves provider inactivity.

        This method performs no provider operation.  It accepts only the
        existing QoD validation action and its already-created QOD_RELEASE
        command, preventing either a new CREATE or a replacement DELETE.
        """
        original = self.bundle.actions.get(original_action_id)
        cleanup = self.bundle.actions.get(cleanup_action_id)
        if original is None or cleanup is None:
            raise RepositoryUnavailable("validation_cleanup_action_missing")
        if (
            original.command_type != "QOD_PLAN"
            or cleanup.command_type != "QOD_RELEASE"
            or cleanup.incident_id != original.incident_id
            or cleanup.resource_key != original.resource_key
            or cleanup.device_id != original.device_id
            or cleanup.parameters_safe.get("original_command_id") != original.command_id
            or not original.provider_resource_id
        ):
            raise RepositoryUnavailable("validation_cleanup_binding_invalid")
        if cleanup.state not in {
            ActionState.ACKNOWLEDGED, ActionState.OUTCOME_UNKNOWN,
            ActionState.RECONCILIATION_REQUIRED, ActionState.ROLLED_BACK,
        }:
            raise RepositoryUnavailable("validation_cleanup_not_reconcilable")
        self._finalize_superseded_provider_reconciliation(original, cleanup)
        if cleanup.state is ActionState.ROLLED_BACK:
            return original

        completed_cleanup = self._update_action(cleanup, ActionState.ROLLED_BACK)
        current_original = self.bundle.actions.get(original.command_id) or original
        if current_original.state is not ActionState.ROLLED_BACK:
            current_original = self._update_action(current_original, ActionState.ROLLED_BACK)
        now = time.time()
        self._save_recovery({
            "recovery_id": f"recovery-{original.incident_id}",
            "incident_id": original.incident_id,
            "resource_keys": [original.resource_key],
            "state": RecoveryState.COMPLETE.value,
            "started_at": now, "completed_at": now, "failure_reason": None,
        })
        incident = self.bundle.incidents.get(original.incident_id)
        if incident and IncidentState(incident["state"]) is IncidentState.VERIFYING:
            self._transition(original.incident_id, IncidentState.RECOVERING, "PHASE_7D_CLEANUP_VERIFIED")
            incident = self.bundle.incidents.get(original.incident_id)
        if incident and IncidentState(incident["state"]) in {IncidentState.RECOVERING, IncidentState.ESCALATED}:
            if IncidentState(incident["state"]) is IncidentState.ESCALATED:
                self._transition(original.incident_id, IncidentState.RECOVERING, "PHASE_7D_CLEANUP_VERIFIED")
            self._transition(original.incident_id, IncidentState.RESOLVED, "PHASE_7D_CLEANUP_VERIFIED")
            self._update_incident_fields(
                original.incident_id, outcome="ROLLED_BACK_SAFELY",
                recovery_state=RecoveryState.COMPLETE.value,
            )
        owner = self.bundle.resource_ownership.get_active(original.resource_key)
        if owner and owner.get("owner_incident_id") == original.incident_id:
            self.bundle.resource_ownership.release(original.resource_key, original.incident_id, now, int(owner.get("version", 0)))
        self._append_outbox(completed_cleanup, "ROLLED_BACK", "rollback_reconciled_and_verified")
        return current_original

    def _finalize_superseded_provider_reconciliation(
        self, original: ActionCommand, cleanup: ActionCommand,
    ) -> None:
        """Close intermediate attempts superseded by proven cleanup."""
        records = self.bundle.verification.for_incident(original.incident_id)
        cleanup_proven = any(
            row.get("verification_type") == "ROLLBACK_RECONCILIATION"
            and row.get("state") == VerificationState.IMPROVED.value
            and (row.get("result") or {}).get("command_id") == cleanup.command_id
            and (row.get("result") or {}).get("outcome") == "VERIFIED_ROLLBACK"
            and (row.get("result") or {}).get("resource_inactive") is True
            for row in records
        )
        if not cleanup_proven:
            raise RepositoryUnavailable("verified_cleanup_evidence_missing")
        now = time.time()
        for row in records:
            result = row.get("result") or {}
            if (
                row.get("verification_type") == "PROVIDER_RECONCILIATION"
                and row.get("state") == VerificationState.PENDING.value
                and result.get("command_id") == original.command_id
                and result.get("outcome") in {"PROVIDER_AVAILABLE", "WAITING_FOR_PROVIDER"}
                and result.get("terminal") is False
            ):
                finalized = copy.deepcopy(row)
                finalized["state"] = VerificationState.INSUFFICIENT_EVIDENCE.value
                finalized["updated_at"] = now
                finalized["reason"] = "superseded_by_verified_terminal_cleanup"
                self.bundle.verification.save(finalized)

    def _capability_context(self, action: ActionCommand) -> Dict[str, Any]:
        slice_status = "UNAVAILABLE"
        for row in self.bundle.network_state.load_all().values():
            candidate = row.get("slice_status") or (row.get("slice_summary") or {}).get("status")
            if candidate:
                slice_status = str(candidate).upper()
                break
        return {"slice_status": slice_status, "mode": self.settings.nac_mode}

    def _result(self, action: ActionCommand, status: str, reason: str, **changes: Any) -> DurableExecutionResult:
        return DurableExecutionResult(
            status=status, action_id=action.command_id, incident_id=action.incident_id,
            action_state=action.state.value, reason=reason, **changes,
        )

    def _update_action(
        self, action: ActionCommand, state: ActionState, reason: Optional[str] = None,
        provider_resource_id: Optional[str] = None,
    ) -> ActionCommand:
        updated = copy.deepcopy(action)
        updated.state = state
        updated.failure_reason = reason
        if provider_resource_id is not None:
            updated.provider_resource_id = provider_resource_id
        if state is ActionState.SENT:
            updated.attempt_count += 1
            updated.last_attempt_at = time.time()
        if state in TERMINAL_ACTION_STATES:
            updated.completed_at = time.time()
        return self.bundle.actions.update(updated, expected_version=action.version)

    def _update_incident_fields(self, incident_id: str, **fields: Any) -> Dict[str, Any]:
        current = self.bundle.incidents.get(incident_id)
        if current is None:
            raise RepositoryUnavailable("incident_not_found")
        updated = copy.deepcopy(current)
        updated.update(fields)
        updated["updated_at"] = time.time()
        return self.bundle.incidents.update(updated, expected_version=int(current.get("version", 0)))

    def _transition(self, incident_id: str, target: IncidentState, reason: str) -> Dict[str, Any]:
        current = self.bundle.incidents.get(incident_id)
        if current is None:
            raise RepositoryUnavailable("incident_not_found")
        if IncidentState(current["state"]) is target:
            return current
        return self.bundle.incidents.transition(
            incident_id, target, actor="ACTUATOR", reason_code=reason,
            trace_id=str(current.get("trace_id") or "durable-execution"), at=time.time(),
        )

    def _cost(self, action: ActionCommand) -> float:
        if action.command_type != "QOD_PLAN":
            return 0.0
        return 0.75 if action.parameters_safe.get("profile") == "guaranteed" else 0.20

    def _cost_recorded(self, action: ActionCommand) -> bool:
        ledger_id = f"cost-{action.command_id}"
        return any(row.get("ledger_id") == ledger_id for row in self.bundle.cost_ledger.for_incident(action.incident_id))

    def _record_cost_once(self, action: ActionCommand) -> None:
        cost = self._cost(action)
        if cost <= 0 or self._cost_recorded(action):
            return
        now = datetime.now(timezone.utc)
        self.bundle.cost_ledger.append({
            "ledger_id": f"cost-{action.command_id}", "incident_id": action.incident_id,
            "command_id": action.command_id, "estimated_policy_cost": cost,
            "committed_at": now.timestamp(), "released_at": None,
            "day_bucket": now.date().isoformat(),
        })

    def _current_trust_allows(self, action: ActionCommand) -> bool:
        if not action.preconditions.get("trusted_dispatch_required"):
            return True
        records = [
            row for row in self.bundle.verification.for_incident(action.incident_id)
            if row.get("verification_type") == "TRUSTED_DISPATCH"
        ]
        if not records:
            return False
        def timestamp(row: Dict[str, Any]) -> float:
            value = row.get("verified_at") or row.get("updated_at") or row.get("created_at")
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return float(value)
            if isinstance(value, str):
                try:
                    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
                except ValueError:
                    return 0.0
            return 0.0
        latest = max(records, key=timestamp)
        age = time.time() - timestamp(latest)
        fresh = 0 <= age <= self.settings.trusted_dispatch_verification_ttl_seconds
        return fresh and latest.get("number_verified") is True and latest.get("recent_sim_swap") is False

    def _eligibility_reason(self, action: ActionCommand, incident: Dict[str, Any]) -> Optional[str]:
        if IncidentState(incident["state"]) not in ACTIONABLE_INCIDENT_STATES:
            return "incident_not_actionable"
        authorization_age = time.time() - float(action.requested_at)
        if authorization_age < 0 or authorization_age > self.settings.guardrails.rollback_seconds:
            return "action_authorization_stale"
        if incident.get("warden_decision") != "ALLOW":
            return "warden_authorization_not_current"
        if int(incident.get("plan_version", -1)) != action.plan_version:
            return "action_plan_superseded"
        pre = action.preconditions
        confidence = pre.get("confidence")
        blast = pre.get("blast_radius")
        selected = pre.get("selected_device_count")
        if not isinstance(confidence, (int, float)) or confidence < self.settings.guardrails.minimum_confidence:
            return "confidence_guard_failed"
        if not isinstance(blast, (int, float)) or blast > self.settings.guardrails.human_approval_blast_radius:
            return "blast_radius_guard_failed"
        if (not isinstance(selected, int) or isinstance(selected, bool) or selected < 0
                or selected > self.settings.guardrails.max_devices_reconfigured_per_cycle):
            return "protected_device_guard_failed"
        if pre.get("warden_decision") != "ALLOW":
            return "action_warden_precondition_invalid"
        if not self._current_trust_allows(action):
            return "trusted_dispatch_guard_failed"
        cost = self._cost(action)
        total = float(self.bundle.cost_ledger.incident_total(action.incident_id))
        if not self._cost_recorded(action) and total + cost > self.settings.guardrails.qos_spend_ceiling_usd:
            return "cost_guard_failed"
        newer = [
            item for item in self.bundle.actions.for_incident(action.incident_id)
            if item.plan_version > action.plan_version
        ]
        if newer:
            return "newer_action_plan_exists"
        conflicting = [
            item for item in self.bundle.actions.for_incident(action.incident_id)
            if item.command_id != action.command_id
            and item.resource_key == action.resource_key
            and item.plan_version < action.plan_version
            and item.state not in TERMINAL_ACTION_STATES
        ]
        if conflicting:
            return "conflicting_nonterminal_action"
        return None

    def _mode_reason(self) -> Optional[str]:
        if self.settings.nac_mode == "fixture":
            return None
        if self.settings.nac_mode == "live_read_only":
            return "live_read_only_mutation_prohibited"
        if self.settings.nac_mode != "live_write":
            return "runtime_mode_invalid"
        if not self.settings.enable_live_write_loop:
            return "explicit_live_write_gate_disabled"
        if self.adapter.execution_provenance != "NOKIA_LIVE":
            return "live_provider_adapter_not_configured"
        return None

    def _ensure_ownership(self, action: ActionCommand) -> Dict[str, Any]:
        current = self.bundle.resource_ownership.get_active(action.resource_key)
        now = time.time()
        if current and current.get("owner_incident_id") != action.incident_id:
            expires = current.get("lease_expires_at")
            if expires is None or float(expires) > now:
                raise ResourceAlreadyOwned("resource_owned_by_other_incident")
            # Do not release another incident's expired lease. The database
            # acquisition RPC performs the only authorized atomic reclaim.
            current = None
        if current:
            expires = current.get("lease_expires_at")
            if expires is None or float(expires) > now:
                return current
            # Expired ownership is reclaimed atomically by the repository/RPC.
            # The new incident must never pretend to own and release the old lease.
        return self.bundle.resource_ownership.acquire({
            "resource_key": action.resource_key, "resource_type": "DEVICE",
            "owner_incident_id": action.incident_id, "acquired_at": now,
            "lease_started_at": now, "lease_expires_at": now + 300,
            "renewable": False, "adopted_by_incident": False,
            "provider_resource_id": None,
        })

    @staticmethod
    def _ownership_is_current(action: ActionCommand, ownership: Optional[Dict[str, Any]]) -> bool:
        """Validate the durable lease immediately before a provider mutation."""
        if not ownership or ownership.get("ownership_state", "OWNED") != "OWNED":
            return False
        if ownership.get("owner_incident_id") != action.incident_id:
            return False
        expires_at = ownership.get("lease_expires_at")
        return expires_at is not None and float(expires_at) > time.time()

    def _persist_block(self, action: ActionCommand, reason: str) -> DurableExecutionResult:
        current = self.bundle.actions.get(action.command_id) or action
        if current.state is ActionState.READY:
            current = self._update_action(current, ActionState.FAILED, reason)
        incident = self.bundle.incidents.get(action.incident_id)
        if incident and IncidentState(incident["state"]) is IncidentState.APPROVED:
            self._transition(action.incident_id, IncidentState.BLOCKED, "PHASE_7C_EXECUTION_BLOCKED")
        self._append_outbox(current, "BLOCKED", reason)
        self._metric("execution_blocked")
        return self._result(current, "BLOCKED", reason)

    def _append_outbox(self, action: ActionCommand, status: str, reason: str) -> None:
        incident = self.bundle.incidents.get(action.incident_id) or {}
        event_id = str(incident.get("trigger_event_id") or "")
        if not event_id:
            raise RepositoryUnavailable("execution_outbox_event_unavailable")
        identity = hashlib.sha256(f"{action.command_id}|{action.version}|{status}".encode()).hexdigest()[:24]
        self.bundle.outbox.append({
            "outbox_id": f"out-execution-{identity}", "event_id": event_id,
            "event_type": "DURABLE_ACTION_STATE_CHANGED",
            "payload": {
                "incident_id": action.incident_id, "action_id": action.command_id,
                "action_state": action.state.value, "status": status,
                "reason": reason, "provider_execution_provenance": self.adapter.execution_provenance,
            },
            "trace_id": str(incident.get("trace_id") or "durable-execution"),
            "created_at": time.time(),
        })

    async def execute_ready_action(self, action_id: str) -> DurableExecutionResult:
        if not self.is_ready():
            raise RepositoryUnavailable("durable_execution_runtime_not_ready")
        self._metric("execution_evaluated")
        action = self.bundle.actions.get(action_id)
        if action is None:
            raise RepositoryUnavailable("action_not_found")
        if action.state in TERMINAL_ACTION_STATES:
            return self._result(action, action.state.value, action.failure_reason or "terminal_action", duplicate=True)
        if action.state is ActionState.SENT:
            # A concurrent worker may still be between its durable SENT commit
            # and provider-result commit.  Restart reconstruction is the only
            # generic boundary that converts an orphaned SENT to UNKNOWN.
            return self._result(action, "EXECUTION_CLAIMED", "sent_attempt_in_progress", duplicate=True)
        if action.state in {ActionState.OUTCOME_UNKNOWN, ActionState.RECONCILIATION_REQUIRED}:
            return await self.reconcile_action(action_id)
        if action.state is ActionState.ACKNOWLEDGED:
            return await self._verify_known(action)
        if action.state is ActionState.ROLLBACK_REQUIRED:
            return await self._rollback(action)
        if action.state is not ActionState.READY:
            return self._result(action, "NOT_ELIGIBLE", "action_not_ready")

        incident = self.bundle.incidents.get(action.incident_id)
        if incident is None:
            return self._persist_block(action, "incident_not_found")
        reason = self._eligibility_reason(action, incident) or self._mode_reason()
        capability = self.adapter.capability(action, self._capability_context(action))
        if reason is None and not capability.get("mutation_supported"):
            reason = str(capability.get("reason") or "capability_unavailable")
        if reason:
            return self._persist_block(action, reason)

        self._failpoint("BEFORE_OWNERSHIP")
        try:
            self._ensure_ownership(action)
        except (ResourceAlreadyOwned, VersionConflict):
            return self._persist_block(action, "resource_ownership_conflict")
        self._failpoint("AFTER_OWNERSHIP_BEFORE_SENT")

        # Re-read every authority after ownership and before the CAS claim.
        current = self.bundle.actions.get(action_id)
        incident = self.bundle.incidents.get(action.incident_id)
        if current is None or incident is None or current.state is not ActionState.READY:
            return self._result(current or action, "NOT_ELIGIBLE", "execution_claim_lost")
        reason = self._eligibility_reason(current, incident)
        if reason:
            return self._persist_block(current, reason)
        if IncidentState(incident["state"]) is IncidentState.APPROVED:
            self._transition(action.incident_id, IncidentState.MITIGATING, "PHASE_7C_EXECUTION_STARTED")
        sent = self._update_action(current, ActionState.SENT)
        self._metric("execution_sent")
        self._failpoint("AFTER_SENT_BEFORE_PROVIDER")

        # Ownership is checked again after the durable SENT commit.  A lost
        # lease never causes a second provider invocation.
        ownership = self.bundle.resource_ownership.get_active(sent.resource_key)
        if not self._ownership_is_current(sent, ownership):
            unknown = self._update_action(sent, ActionState.OUTCOME_UNKNOWN, "ownership_lost_before_provider")
            self._metric("execution_unknown")
            self._append_outbox(unknown, "OUTCOME_UNKNOWN", "ownership_lost_before_provider")
            return self._result(unknown, "OUTCOME_UNKNOWN", "ownership_lost_before_provider")

        # Reload deterministic authority at the final possible point.  The
        # action CAS remains the execution claim; a later incident/plan/warden
        # change blocks the provider call without consulting an advisory model.
        latest_action = self.bundle.actions.get(action_id)
        latest_incident = self.bundle.incidents.get(sent.incident_id)
        if (
            latest_action is None
            or latest_action.state is not ActionState.SENT
            or latest_action.version != sent.version
            or latest_incident is None
            or self._eligibility_reason(latest_action, latest_incident) is not None
        ):
            if latest_action is not None and latest_action.state is ActionState.SENT:
                latest_action = self._update_action(
                    latest_action, ActionState.FAILED,
                    "execution_authority_lost_before_provider",
                )
                self._append_outbox(
                    latest_action, "BLOCKED",
                    "execution_authority_lost_before_provider",
                )
            self._metric("execution_blocked")
            return self._result(
                latest_action or sent, "BLOCKED",
                "execution_authority_lost_before_provider",
            )
        try:
            self._record_cost_once(sent)
        except Exception:
            failed = self._update_action(sent, ActionState.FAILED, "cost_reservation_failed")
            self._append_outbox(failed, "BLOCKED", "cost_reservation_failed")
            return self._result(failed, "BLOCKED", "cost_reservation_failed")

        try:
            provider = await self.adapter.execute(copy.deepcopy(sent))
            self._failpoint("AFTER_PROVIDER_BEFORE_RESULT")
        except ProviderExplicitFailure:
            failed = self._update_action(sent, ActionState.FAILED, "provider_explicit_failure")
            self._save_provider_failure_verification(failed)
            self._append_outbox(failed, "PROVIDER_FAILED", "provider_explicit_failure")
            return self._result(
                failed, "PROVIDER_FAILED", "provider_explicit_failure", provider_invoked=True,
                provider_execution_provenance=self.adapter.execution_provenance,
                verification_state=VerificationState.FAILED.value,
            )
        except Exception:
            unknown = self._update_action(sent, ActionState.OUTCOME_UNKNOWN, "provider_outcome_ambiguous")
            self._metric("execution_unknown")
            self._append_outbox(unknown, "OUTCOME_UNKNOWN", "provider_outcome_ambiguous")
            return self._result(
                unknown, "OUTCOME_UNKNOWN", "provider_outcome_ambiguous", provider_invoked=True,
                provider_execution_provenance=self.adapter.execution_provenance,
            )
        if provider.outcome != "ACCEPTED":
            failed = self._update_action(sent, ActionState.FAILED, "provider_explicit_failure")
            self._save_provider_failure_verification(failed)
            self._append_outbox(failed, "PROVIDER_FAILED", "provider_explicit_failure")
            return self._result(failed, "PROVIDER_FAILED", "provider_explicit_failure", provider_invoked=True,
                                provider_execution_provenance=provider.provenance,
                                verification_state=VerificationState.FAILED.value)
        accepted = self._update_action(
            sent, ActionState.ACKNOWLEDGED, provider_resource_id=provider.provider_resource_id,
        )
        self._failpoint("AFTER_RESULT_BEFORE_VERIFICATION")
        return await self._verify_known(accepted, provider_invoked=True)

    def _save_provider_failure_verification(self, action: ActionCommand) -> None:
        self.bundle.verification.save({
            "verification_id": f"verify-{action.command_id}", "incident_id": action.incident_id,
            "evidence_event_ids": [], "verification_type": "NETWORK_ACTION",
            "state": VerificationState.FAILED.value, "started_at": time.time(),
            "updated_at": time.time(), "result": {"command_id": action.command_id,
                "mitigation_improved": False, "outcome": "PROVIDER_FAILED"},
            "reason": "provider_explicit_failure", "source_provenance": self.adapter.execution_provenance,
        })

    async def reconcile_action(self, action_id: str) -> DurableExecutionResult:
        action = self.bundle.actions.get(action_id)
        if action is None:
            raise RepositoryUnavailable("action_not_found")
        if action.state is ActionState.SENT:
            action = self._update_action(action, ActionState.OUTCOME_UNKNOWN, "sent_outcome_requires_reconciliation")
        if action.state not in {ActionState.OUTCOME_UNKNOWN, ActionState.RECONCILIATION_REQUIRED}:
            return self._result(action, "NOT_ELIGIBLE", "reconciliation_not_required")
        try:
            result = await self.adapter.reconcile(copy.deepcopy(action))
        except Exception:
            result = ProviderMutationResult(
                outcome="UNKNOWN", provider_resource_id=action.provider_resource_id,
                reason="provider_reconciliation_unavailable", provenance="UNAVAILABLE",
            )
        if result.outcome == "ACCEPTED":
            accepted = self._update_action(
                action, ActionState.ACKNOWLEDGED,
                provider_resource_id=result.provider_resource_id or action.provider_resource_id,
            )
            return await self._verify_known(accepted)
        if result.outcome == "FAILED":
            failed = self._update_action(action, ActionState.FAILED, "provider_reconciliation_failed")
            self._append_outbox(failed, "PROVIDER_FAILED", "provider_reconciliation_failed")
            return self._result(failed, "PROVIDER_FAILED", "provider_reconciliation_failed")
        pending = action if action.state is ActionState.RECONCILIATION_REQUIRED else self._update_action(
            action, ActionState.RECONCILIATION_REQUIRED, "provider_outcome_unknown",
        )
        self._append_outbox(pending, "OUTCOME_UNKNOWN", "provider_outcome_unknown")
        self._metric("execution_unknown")
        return self._result(pending, "OUTCOME_UNKNOWN", "provider_outcome_unknown")

    async def _verify_known(self, action: ActionCommand, provider_invoked: bool = False) -> DurableExecutionResult:
        incident = self.bundle.incidents.get(action.incident_id)
        if incident and IncidentState(incident["state"]) is IncidentState.MITIGATING:
            self._transition(action.incident_id, IncidentState.VERIFYING, "PHASE_7C_PROVIDER_RESULT_PERSISTED")
        baseline = dict(action.preconditions.get("verification_baseline") or {})
        try:
            verification = await self.adapter.verify(
                copy.deepcopy(action), action.provider_resource_id, baseline,
            )
        except Exception:
            verification = ActionVerificationResult(
                outcome="VERIFICATION_UNAVAILABLE", reason="verification_adapter_unavailable",
                provenance="UNAVAILABLE",
            )
        state = {
            "VERIFIED_IMPROVED": VerificationState.IMPROVED,
            "VERIFIED_RESOURCE": VerificationState.IMPROVED,
            "VERIFIED_NO_IMPROVEMENT": VerificationState.UNCHANGED,
            "VERIFIED_DEGRADED": VerificationState.DEGRADED,
        }.get(verification.outcome, VerificationState.INSUFFICIENT_EVIDENCE)
        self.bundle.verification.save({
            "verification_id": f"verify-{action.command_id}", "incident_id": action.incident_id,
            "evidence_event_ids": [], "verification_type": "NETWORK_ACTION",
            "state": state.value, "started_at": time.time(), "updated_at": time.time(),
            "result": {"command_id": action.command_id, "outcome": verification.outcome,
                       "mitigation_improved": verification.mitigation_improved,
                       "evidence": verification.evidence},
            "reason": verification.reason, "source_provenance": verification.provenance,
        })
        self._update_incident_fields(action.incident_id, verification_state=state.value)
        self._failpoint("AFTER_VERIFICATION_BEFORE_PUBLICATION")
        if state is VerificationState.IMPROVED:
            success = self._update_action(action, ActionState.SUCCESS)
            self._metric("execution_verified")
            self._maybe_resolve(action.incident_id, action.plan_version)
            self._append_outbox(success, "VERIFIED", verification.reason)
            return self._result(
                success, "VERIFIED", verification.reason, provider_invoked=provider_invoked,
                provider_execution_provenance=verification.provenance,
                verification_state=state.value,
            )
        if state in {VerificationState.UNCHANGED, VerificationState.DEGRADED}:
            rollback = self._update_action(action, ActionState.ROLLBACK_REQUIRED, verification.reason)
            return await self._rollback(rollback, provider_invoked=provider_invoked)
        pending = self._update_action(action, ActionState.RECONCILIATION_REQUIRED, verification.reason)
        self._append_outbox(pending, "VERIFICATION_UNAVAILABLE", verification.reason)
        return self._result(
            pending, "VERIFICATION_UNAVAILABLE", verification.reason,
            provider_invoked=provider_invoked,
            provider_execution_provenance=verification.provenance,
            verification_state=state.value,
        )

    def _maybe_resolve(self, incident_id: str, plan_version: int) -> None:
        plan_actions = [
            item for item in self.bundle.actions.for_incident(incident_id)
            if item.plan_version == plan_version and item.command_type in ACTION_KIND
        ]
        if not plan_actions or any(item.state is not ActionState.SUCCESS for item in plan_actions):
            return
        verifications = self.bundle.verification.for_incident(incident_id)
        improved = any(
            (row.get("result") or {}).get("mitigation_improved") is True
            for row in verifications if row.get("verification_type") == "NETWORK_ACTION"
        )
        if not improved:
            return
        incident = self.bundle.incidents.get(incident_id)
        if incident and IncidentState(incident["state"]) is IncidentState.VERIFYING:
            self._transition(incident_id, IncidentState.RESOLVED, "PHASE_7C_MITIGATION_VERIFIED")
            self._update_incident_fields(
                incident_id, outcome="VERIFIED_IMPROVED",
                verification_state=VerificationState.IMPROVED.value,
            )
        for ownership in self.bundle.resource_ownership.owned_by(incident_id):
            try:
                self.bundle.resource_ownership.release(ownership["resource_key"], incident_id, time.time(), int(ownership.get("version", 0)))
            except (ResourceAlreadyOwned, VersionConflict):
                pass

    async def _rollback(self, action: ActionCommand, provider_invoked: bool = False) -> DurableExecutionResult:
        capability = self.adapter.capability(action, self._capability_context(action))
        mode_reason = self._mode_reason()
        if mode_reason:
            self._append_outbox(action, "RECOVERY_REQUIRED", mode_reason)
            return self._result(
                action, "RECOVERY_REQUIRED", mode_reason,
                provider_invoked=provider_invoked,
                verification_state=VerificationState.UNCHANGED.value,
                rollback_state="BLOCKED_BY_RUNTIME_MODE",
            )
        if not capability.get("rollback_supported") or not action.provider_resource_id:
            self._save_recovery({
                "recovery_id": f"recovery-{action.incident_id}", "incident_id": action.incident_id,
                "resource_keys": [action.resource_key], "state": RecoveryState.FAILED.value,
                "started_at": time.time(), "completed_at": time.time(),
                "failure_reason": "rollback_unsupported",
            })
            incident = self.bundle.incidents.get(action.incident_id)
            if incident and IncidentState(incident["state"]) is IncidentState.VERIFYING:
                self._transition(action.incident_id, IncidentState.ESCALATED, "PHASE_7C_ROLLBACK_UNSUPPORTED")
            self._append_outbox(action, "RECOVERY_REQUIRED", "rollback_unsupported")
            return self._result(
                action, "RECOVERY_REQUIRED", "rollback_unsupported",
                provider_invoked=provider_invoked, verification_state=VerificationState.UNCHANGED.value,
                rollback_state="UNSUPPORTED",
            )
        rollback_type = ROLLBACK_COMMAND[action.command_type]
        command = ActionCommand(
            incident_id=action.incident_id, command_type=rollback_type,
            resource_key=action.resource_key, device_id=action.device_id,
            plan_version=action.plan_version, requested_at=time.time(),
            parameters_safe={"original_command_id": action.command_id},
            preconditions={"phase": "7C_ROLLBACK", "provider_resource_id_known": True},
            state=ActionState.READY,
        )
        rollback_action, _ = self.bundle.actions.create_or_get(command)
        if rollback_action.state is ActionState.SENT:
            rollback_action = self._update_action(
                rollback_action, ActionState.OUTCOME_UNKNOWN, "rollback_outcome_unknown",
            )
            return self._result(action, "OUTCOME_UNKNOWN", "rollback_outcome_unknown", rollback_state="OUTCOME_UNKNOWN")
        if rollback_action.state in {ActionState.OUTCOME_UNKNOWN, ActionState.RECONCILIATION_REQUIRED}:
            return self._result(action, "OUTCOME_UNKNOWN", "rollback_reconciliation_required", rollback_state=rollback_action.state.value)
        if rollback_action.state is ActionState.ROLLED_BACK:
            return self._result(action, "ROLLED_BACK", "rollback_already_verified", duplicate=True, rollback_state="VERIFIED")
        if rollback_action.state is not ActionState.READY:
            return self._result(action, "RECOVERY_REQUIRED", "rollback_not_eligible", rollback_state=rollback_action.state.value)
        try:
            rollback_sent = self._update_action(rollback_action, ActionState.SENT)
        except VersionConflict:
            current = self.bundle.actions.get(rollback_action.command_id)
            return self._result(action, "OUTCOME_UNKNOWN", "rollback_claim_lost", rollback_state=current.state.value if current else "UNKNOWN")
        self._save_recovery({
            "recovery_id": f"recovery-{action.incident_id}", "incident_id": action.incident_id,
            "resource_keys": [action.resource_key], "state": RecoveryState.RELEASING.value,
            "started_at": time.time(), "completed_at": None, "failure_reason": None,
        })
        self._failpoint("AFTER_ROLLBACK_SENT_BEFORE_PROVIDER")
        ownership = self.bundle.resource_ownership.get_active(action.resource_key)
        if not self._ownership_is_current(action, ownership):
            unknown = self._update_action(
                rollback_sent, ActionState.OUTCOME_UNKNOWN,
                "rollback_ownership_lost_before_provider",
            )
            self._append_outbox(unknown, "OUTCOME_UNKNOWN", "rollback_ownership_lost_before_provider")
            self._metric("execution_unknown")
            return self._result(
                action, "OUTCOME_UNKNOWN", "rollback_ownership_lost_before_provider",
                rollback_state=unknown.state.value,
            )
        try:
            outcome = await self.adapter.rollback(copy.deepcopy(action), action.provider_resource_id)
            self._failpoint("AFTER_ROLLBACK_PROVIDER_BEFORE_RESULT")
        except ProviderExplicitFailure:
            failed = self._update_action(rollback_sent, ActionState.FAILED, "rollback_explicit_failure")
            self._save_recovery({
                "recovery_id": f"recovery-{action.incident_id}", "incident_id": action.incident_id,
                "resource_keys": [action.resource_key], "state": RecoveryState.FAILED.value,
                "started_at": time.time(), "completed_at": time.time(),
                "failure_reason": "rollback_explicit_failure",
            })
            return self._result(action, "RECOVERY_REQUIRED", "rollback_explicit_failure", rollback_state=failed.state.value)
        except Exception:
            unknown = self._update_action(rollback_sent, ActionState.OUTCOME_UNKNOWN, "rollback_outcome_ambiguous")
            incident = self.bundle.incidents.get(action.incident_id)
            if incident and IncidentState(incident["state"]) is IncidentState.VERIFYING:
                self._transition(action.incident_id, IncidentState.RECOVERING, "PHASE_7C_ROLLBACK_UNKNOWN")
            self._save_recovery({
                "recovery_id": f"recovery-{action.incident_id}", "incident_id": action.incident_id,
                "resource_keys": [action.resource_key], "state": RecoveryState.PARTIAL.value,
                "started_at": time.time(), "completed_at": None,
                "failure_reason": "rollback_outcome_ambiguous",
            })
            self._append_outbox(unknown, "OUTCOME_UNKNOWN", "rollback_outcome_ambiguous")
            self._metric("execution_unknown")
            return self._result(action, "OUTCOME_UNKNOWN", "rollback_outcome_ambiguous", rollback_state=unknown.state.value)
        if outcome.outcome != "ACCEPTED":
            failed = self._update_action(rollback_sent, ActionState.FAILED, "rollback_explicit_failure")
            return self._result(action, "RECOVERY_REQUIRED", "rollback_explicit_failure", rollback_state=failed.state.value)
        accepted = self._update_action(rollback_sent, ActionState.ACKNOWLEDGED)
        try:
            verification = await self.adapter.verify_rollback(
                copy.deepcopy(action), action.provider_resource_id,
            )
        except Exception:
            verification = ActionVerificationResult(
                outcome="VERIFICATION_UNAVAILABLE", reason="rollback_verification_unavailable",
                provenance="UNAVAILABLE",
            )
        incident = self.bundle.incidents.get(action.incident_id)
        if incident and IncidentState(incident["state"]) is IncidentState.VERIFYING:
            self._transition(action.incident_id, IncidentState.RECOVERING, "PHASE_7C_ROLLBACK_STARTED")
        if verification.outcome != "VERIFIED_ROLLBACK":
            pending = self._update_action(accepted, ActionState.RECONCILIATION_REQUIRED, verification.reason)
            self._save_recovery({
                "recovery_id": f"recovery-{action.incident_id}", "incident_id": action.incident_id,
                "resource_keys": [action.resource_key], "state": RecoveryState.PARTIAL.value,
                "started_at": time.time(), "completed_at": None,
                "failure_reason": verification.reason,
            })
            self._append_outbox(pending, "RECOVERY_REQUIRED", verification.reason)
            return self._result(action, "RECOVERY_REQUIRED", verification.reason, rollback_state=pending.state.value)
        completed = self._update_action(accepted, ActionState.ROLLED_BACK)
        original = self.bundle.actions.get(action.command_id) or action
        if original.state is ActionState.ROLLBACK_REQUIRED:
            original = self._update_action(original, ActionState.ROLLED_BACK)
        self._save_recovery({
            "recovery_id": f"recovery-{action.incident_id}", "incident_id": action.incident_id,
            "resource_keys": [action.resource_key], "state": RecoveryState.COMPLETE.value,
            "started_at": time.time(), "completed_at": time.time(), "failure_reason": None,
        })
        incident = self.bundle.incidents.get(action.incident_id)
        if incident and IncidentState(incident["state"]) is IncidentState.RECOVERING:
            self._transition(action.incident_id, IncidentState.RESOLVED, "PHASE_7C_ROLLBACK_VERIFIED")
            self._update_incident_fields(
                action.incident_id, outcome="ROLLED_BACK_SAFELY",
                recovery_state=RecoveryState.COMPLETE.value,
            )
        self._append_outbox(completed, "ROLLED_BACK", "rollback_verified")
        self._metric("execution_rolled_back")
        return self._result(
            original, "ROLLED_BACK", "rollback_verified", provider_invoked=provider_invoked,
            provider_execution_provenance=verification.provenance,
            verification_state=VerificationState.UNCHANGED.value,
            rollback_state="VERIFIED",
        )
    def _save_recovery(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Advance recovery authority, then repair its incident projection."""
        return save_recovery_and_repair(self.bundle, record)
