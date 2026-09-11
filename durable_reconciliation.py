"""Phase 7D durable, read-only provider reconciliation and reverification.

Reconciliation consumes durable action identities and can only call adapter
read methods. It never invokes execute(), rollback(), or any mutation helper.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import time
from typing import Any, Callable, Dict, Optional

from pydantic import BaseModel, ConfigDict

from durable_core import (
    ActionCommand, ActionState, IncidentState, RepositoryUnavailable,
    VerificationState, VersionConflict,
)
from durable_execution import (
    ACTION_KIND, ActionVerificationResult, DurableProviderAdapter,
    ProviderMutationResult,
)


ELIGIBLE_RECONCILIATION_STATES = {
    ActionState.OUTCOME_UNKNOWN,
    ActionState.RECONCILIATION_REQUIRED,
    ActionState.ACKNOWLEDGED,
}
TERMINAL_RECONCILIATION_STATES = {
    ActionState.SUCCESS, ActionState.FAILED, ActionState.ROLLED_BACK,
}
PENDING_PROVIDER_STATES = {"REQUESTED", "PENDING", "CREATING", "PROCESSING"}
AVAILABLE_PROVIDER_STATES = {"AVAILABLE", "ACTIVE"}


class DurableReconciliationResult(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: str
    action_id: str
    incident_id: Optional[str] = None
    action_state: Optional[str] = None
    attempt_count: int = 0
    max_attempts: int = 0
    next_eligible_at: Optional[float] = None
    deadline_at: Optional[float] = None
    provider_state: Optional[str] = None
    verification_state: Optional[str] = None
    provenance: str = "UNAVAILABLE"
    reason: str
    provider_reads: int = 0
    mutation_retries: int = 0
    duplicate: bool = False

    def public(self) -> Dict[str, Any]:
        return self.model_dump()


class DurableActionReconciliationService:
    """Canonical action-ID-only continuation for safe reads and verification."""

    def __init__(
        self, *, bundle: Any, adapter: DurableProviderAdapter, settings: Any,
        execution_service: Any, is_ready: Callable[[], bool],
        clock: Callable[[], float] = time.time, metrics: Optional[Any] = None,
        failure_hook: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.bundle = bundle
        self.adapter = adapter
        self.settings = settings
        self.execution_service = execution_service
        self.is_ready = is_ready
        self.clock = clock
        self.metrics = metrics
        self.failure_hook = failure_hook

    def _metric(self, name: str) -> None:
        if self.metrics is not None:
            self.metrics.increment(name)

    def _failpoint(self, stage: str) -> None:
        if self.failure_hook is not None:
            self.failure_hook(stage)

    def _attempts(self, action: ActionCommand) -> list[Dict[str, Any]]:
        rows = [
            row for row in self.bundle.verification.for_incident(action.incident_id)
            if row.get("verification_type") == "PROVIDER_RECONCILIATION"
            and (row.get("result") or {}).get("command_id") == action.command_id
        ]
        return sorted(rows, key=lambda row: (
            int((row.get("result") or {}).get("attempt_number", 0)),
            float(row.get("updated_at") or 0),
        ))

    def _deadline(self, action: ActionCommand) -> float:
        anchor = float(action.last_attempt_at or action.requested_at)
        return anchor + self.settings.durable_reconciliation_deadline_seconds

    def _backoff(self, attempt_count: int) -> float:
        if attempt_count <= 0:
            return 0.0
        base = self.settings.durable_reconciliation_base_backoff_seconds
        maximum = self.settings.durable_reconciliation_max_backoff_seconds
        return float(min(maximum, base * (2 ** (attempt_count - 1))))

    def _network_verification(self, action: ActionCommand, attempt_number: int) -> Optional[Dict[str, Any]]:
        return self.bundle.verification.get(
            f"reverify-{action.command_id}-{attempt_number}"
        )

    def work_status(self, action_id: str) -> DurableReconciliationResult:
        action = self.bundle.actions.get(action_id)
        if action is None:
            raise RepositoryUnavailable("action_not_found")
        attempts = self._attempts(action)
        count = len([row for row in attempts if (row.get("result") or {}).get("terminal") is not True])
        deadline = self._deadline(action)
        last_at = float(attempts[-1].get("updated_at") or 0) if attempts else None
        next_at = (last_at + self._backoff(count)) if last_at is not None else self.clock()
        last = (attempts[-1].get("result") or {}) if attempts else {}
        if action.state in TERMINAL_RECONCILIATION_STATES:
            status, reason = "TERMINAL", "terminal_action"
        elif action.state not in ELIGIBLE_RECONCILIATION_STATES:
            status, reason = "NOT_ELIGIBLE", "invalid_action_state"
        elif last.get("terminal") is True:
            status, reason = "ESCALATED", str(last.get("outcome") or "manual_reconciliation_required")
        elif self.clock() >= deadline or count >= self.settings.durable_reconciliation_max_attempts:
            status, reason = "DEADLINE_REACHED", "manual_reconciliation_required"
        # A durable provider observation is sufficient authority to resume
        # reverification. If the process crashed before persisting the ACK,
        # do not repeat the external provider read after restart.
        elif last.get("outcome") == "PROVIDER_AVAILABLE":
            latest_verification = self._network_verification(action, count)
            if latest_verification is not None and latest_verification.get("state") == VerificationState.INSUFFICIENT_EVIDENCE.value:
                last_at = float(latest_verification.get("updated_at") or last_at or 0)
                next_at = last_at + self._backoff(count)
                if self.clock() < next_at:
                    status, reason = "BACKOFF", "reverification_backoff_active"
                else:
                    status, reason = "READY_FOR_REVERIFICATION", "provider_available_reverification_due"
            else:
                status, reason = "READY_FOR_REVERIFICATION", "provider_available_verification_pending"
                next_at = self.clock()
        elif self.clock() < next_at:
            status, reason = "BACKOFF", "reconciliation_backoff_active"
        else:
            status, reason = "DUE", "reconciliation_due"
        return DurableReconciliationResult(
            status=status, action_id=action.command_id, incident_id=action.incident_id,
            action_state=action.state.value, attempt_count=count,
            max_attempts=self.settings.durable_reconciliation_max_attempts,
            next_eligible_at=next_at, deadline_at=deadline,
            provider_state=last.get("provider_state"),
            verification_state=last.get("verification_state"),
            provenance=str(last.get("provenance") or "UNAVAILABLE"), reason=reason,
        )

    def _update_action(
        self, action: ActionCommand, state: ActionState, reason: Optional[str] = None,
        provider_resource_id: Optional[str] = None,
    ) -> ActionCommand:
        current = self.bundle.actions.get(action.command_id)
        if current is None:
            raise RepositoryUnavailable("action_not_found")
        updated = copy.deepcopy(current)
        updated.state = state
        updated.failure_reason = reason
        if provider_resource_id is not None:
            updated.provider_resource_id = provider_resource_id
        if state in TERMINAL_RECONCILIATION_STATES:
            updated.completed_at = self.clock()
        return self.bundle.actions.update(updated, expected_version=current.version)

    def _update_incident(self, incident_id: str, **fields: Any) -> Dict[str, Any]:
        current = self.bundle.incidents.get(incident_id)
        if current is None:
            raise RepositoryUnavailable("incident_not_found")
        updated = copy.deepcopy(current)
        updated.update(fields)
        updated["updated_at"] = self.clock()
        return self.bundle.incidents.update(updated, expected_version=int(current.get("version", 0)))

    def _transition_to_escalated(self, incident_id: str, reason: str) -> None:
        current = self.bundle.incidents.get(incident_id)
        if current is None:
            raise RepositoryUnavailable("incident_not_found")
        state = IncidentState(current["state"])
        if state is IncidentState.APPROVED:
            current = self.bundle.incidents.transition(
                incident_id, IncidentState.MITIGATING, actor="RECONCILER",
                reason_code="PHASE_7D_RECONCILIATION", trace_id=str(current.get("trace_id") or "phase-7d"),
                at=self.clock(),
            )
            state = IncidentState(current["state"])
        if state is IncidentState.MITIGATING:
            current = self.bundle.incidents.transition(
                incident_id, IncidentState.VERIFYING, actor="RECONCILER",
                reason_code="PHASE_7D_RECONCILIATION", trace_id=str(current.get("trace_id") or "phase-7d"),
                at=self.clock(),
            )
            state = IncidentState(current["state"])
        if state is IncidentState.VERIFYING:
            self.bundle.incidents.transition(
                incident_id, IncidentState.ESCALATED, actor="RECONCILER",
                reason_code=reason, trace_id=str(current.get("trace_id") or "phase-7d"),
                at=self.clock(),
            )

    def _save_attempt(
        self, action: ActionCommand, attempt_number: int, *, outcome: str,
        provider_state: Optional[str], reason: str, provenance: str,
        terminal: bool = False,
    ) -> Dict[str, Any]:
        state = VerificationState.FAILED if outcome == "PROVIDER_NOT_FOUND" else (
            VerificationState.INSUFFICIENT_EVIDENCE
            if outcome in {"STILL_UNKNOWN", "MALFORMED_RESPONSE"}
            else VerificationState.PENDING
        )
        return self.bundle.verification.save({
            "verification_id": f"reconcile-{action.command_id}-{attempt_number}",
            "incident_id": action.incident_id, "evidence_event_ids": [],
            "verification_type": "PROVIDER_RECONCILIATION", "state": state.value,
            "started_at": self.clock(), "updated_at": self.clock(),
            "result": {
                "command_id": action.command_id, "attempt_number": attempt_number,
                "outcome": outcome, "provider_state": provider_state,
                "provenance": provenance, "terminal": terminal,
                "mutation_retry_performed": False,
            },
            "reason": reason, "source_provenance": provenance,
        })

    def _save_terminal_escalation(self, action: ActionCommand, attempt_count: int) -> DurableReconciliationResult:
        existing = next((
            row for row in self._attempts(action)
            if (row.get("result") or {}).get("terminal") is True
        ), None)
        if existing is None:
            self.bundle.verification.save({
                "verification_id": f"reconcile-{action.command_id}-terminal",
                "incident_id": action.incident_id, "evidence_event_ids": [],
                "verification_type": "PROVIDER_RECONCILIATION",
                "state": VerificationState.INSUFFICIENT_EVIDENCE.value,
                "started_at": self.clock(), "updated_at": self.clock(),
                "result": {
                    "command_id": action.command_id, "attempt_number": attempt_count,
                    "outcome": "MANUAL_RECONCILIATION_REQUIRED", "provider_state": None,
                    "provenance": "UNAVAILABLE", "terminal": True,
                    "mutation_retry_performed": False,
                },
                "reason": "manual_reconciliation_required", "source_provenance": "UNAVAILABLE",
            })
            self._transition_to_escalated(action.incident_id, "PHASE_7D_RECONCILIATION_DEADLINE")
            self._update_incident(
                action.incident_id, outcome="MANUAL_RECONCILIATION_REQUIRED",
                verification_state=VerificationState.INSUFFICIENT_EVIDENCE.value,
            )
        self._metric("reconciliation_escalated")
        return DurableReconciliationResult(
            status="ESCALATED", action_id=action.command_id, incident_id=action.incident_id,
            action_state=action.state.value, attempt_count=attempt_count,
            max_attempts=self.settings.durable_reconciliation_max_attempts,
            deadline_at=self._deadline(action), reason="manual_reconciliation_required",
        )

    def _read_capability(self, action: ActionCommand) -> tuple[bool, str]:
        if action.command_type not in {"QOD_PLAN", "GEOFENCE_PLAN"}:
            return False, "provider_reconciliation_unsupported"
        if not action.provider_resource_id:
            return False, "provider_resource_identity_unavailable"
        if self.settings.nac_mode not in {"fixture", "live_read_only", "live_write"}:
            return False, "runtime_mode_invalid"
        if self.settings.nac_mode != "fixture" and self.adapter.execution_provenance != "NOKIA_LIVE":
            return False, "provider_read_adapter_unavailable"
        return True, "provider_safe_read_available"

    def _reload_authority_context(self, action: ActionCommand) -> Optional[str]:
        """Reload durable execution context; return only a fail-closed conflict."""
        ownership = self.bundle.resource_ownership.get_active(action.resource_key)
        # These reads intentionally occur even when they do not block. They
        # ensure reconciliation never proceeds from process-local recovery or
        # capability assumptions after a restart.
        self.bundle.recovery.for_incident(action.incident_id)
        self.bundle.network_state.load_all()
        if ownership and ownership.get("owner_incident_id") != action.incident_id:
            return "resource_ownership_conflict"
        return None

    async def handle_durable_reconciliation_ready(self, row: Dict[str, Any]) -> DurableReconciliationResult:
        if row.get("event_type") != "DURABLE_RECONCILIATION_READY":
            raise RepositoryUnavailable("invalid durable reconciliation reference")
        action_id = str((row.get("payload") or {}).get("action_id") or "")
        if not action_id:
            raise RepositoryUnavailable("durable reconciliation action is missing")
        return await self.reconcile_action(action_id)

    async def reconcile_action(self, action_id: str) -> DurableReconciliationResult:
        if not self.is_ready():
            raise RepositoryUnavailable("durable_reconciliation_runtime_not_ready")
        action = self.bundle.actions.get(action_id)
        if action is None:
            raise RepositoryUnavailable("action_not_found")
        incident = self.bundle.incidents.get(action.incident_id)
        if incident is None:
            raise RepositoryUnavailable("incident_not_found")
        authority_conflict = self._reload_authority_context(action)
        if authority_conflict:
            return DurableReconciliationResult(
                status="NOT_ELIGIBLE", action_id=action.command_id,
                incident_id=action.incident_id, action_state=action.state.value,
                max_attempts=self.settings.durable_reconciliation_max_attempts,
                reason=authority_conflict,
            )
        status = self.work_status(action_id)
        if status.status == "TERMINAL":
            status.duplicate = True
            return status
        if status.status == "NOT_ELIGIBLE":
            return status
        if status.status == "ESCALATED":
            status.duplicate = True
            return status
        if status.status == "DEADLINE_REACHED":
            return self._save_terminal_escalation(action, status.attempt_count)
        if status.status == "BACKOFF":
            return status
        if status.status == "READY_FOR_REVERIFICATION":
            current = action
            if current.state in {ActionState.OUTCOME_UNKNOWN, ActionState.RECONCILIATION_REQUIRED}:
                current = self._update_action(
                    current, ActionState.ACKNOWLEDGED,
                    provider_resource_id=current.provider_resource_id,
                )
            attempt_number = status.attempt_count
            previous = self._network_verification(current, attempt_number)
            if previous is not None and previous.get("state") == VerificationState.INSUFFICIENT_EVIDENCE.value:
                attempt_number += 1
                self._save_attempt(
                    current, attempt_number, outcome="PROVIDER_AVAILABLE",
                    provider_state=status.provider_state or "AVAILABLE",
                    reason="provider_available_reverification",
                    provenance=status.provenance,
                )
            can_read, unavailable_reason = self._read_capability(current)
            return await self._reverify(
                current, attempt_number,
                read_unavailable_reason=None if can_read else unavailable_reason,
            )

        attempt_number = status.attempt_count + 1
        self._metric("reconciliation_started")
        self._failpoint("AFTER_CLAIM_BEFORE_PROVIDER_READ")
        can_read, unavailable_reason = self._read_capability(action)
        provider_reads = 0
        if not can_read:
            provider = ProviderMutationResult(
                outcome="UNKNOWN", provider_resource_id=action.provider_resource_id,
                provider_state=None, reason=unavailable_reason, provenance="UNAVAILABLE",
            )
        else:
            provider_reads = 1
            self._metric("provider_safe_read")
            try:
                provider = await self.adapter.reconcile(copy.deepcopy(action))
            except Exception:
                provider = ProviderMutationResult(
                    outcome="UNKNOWN", provider_resource_id=action.provider_resource_id,
                    provider_state=None, reason="provider_safe_read_unavailable",
                    provenance="UNAVAILABLE",
                )
            self._failpoint("AFTER_PROVIDER_READ_BEFORE_RECONCILIATION_PERSIST")
        if not isinstance(provider, ProviderMutationResult):
            provider = ProviderMutationResult(
                outcome="UNKNOWN", provider_resource_id=action.provider_resource_id,
                provider_state=None, reason="provider_response_invalid", provenance="UNAVAILABLE",
            )
            classification = "MALFORMED_RESPONSE"
        else:
            classification = ""
        provider_state = str(provider.provider_state or "").upper() or None
        if provider_state and (len(provider_state) > 64 or not provider_state.replace("_", "").isalnum()):
            provider_state = None
            classification = "MALFORMED_RESPONSE"

        if not classification and provider.outcome == "ACCEPTED" and provider_state in AVAILABLE_PROVIDER_STATES:
            classification = "PROVIDER_AVAILABLE"
        elif not classification and provider.outcome == "ACCEPTED" and provider_state in PENDING_PROVIDER_STATES:
            classification = "WAITING_FOR_PROVIDER"
        elif not classification and provider.outcome == "FAILED" and action.provider_resource_id:
            classification = "PROVIDER_NOT_FOUND"
        elif not classification:
            classification = "STILL_UNKNOWN"

        self._save_attempt(
            action, attempt_number, outcome=classification,
            provider_state=provider_state, reason=str(provider.reason)[:128],
            provenance=str(provider.provenance),
        )
        self._failpoint("AFTER_RECONCILIATION_PERSIST_BEFORE_VERIFICATION")
        if classification == "PROVIDER_AVAILABLE":
            current = self.bundle.actions.get(action_id) or action
            if current.state in {ActionState.OUTCOME_UNKNOWN, ActionState.RECONCILIATION_REQUIRED}:
                current = self._update_action(
                    current, ActionState.ACKNOWLEDGED,
                    provider_resource_id=provider.provider_resource_id or current.provider_resource_id,
                )
            result = await self._reverify(current, attempt_number)
            result.provider_reads += provider_reads
            return result
        if classification == "PROVIDER_NOT_FOUND":
            failed = self._update_action(action, ActionState.FAILED, "provider_resource_not_found")
            self._transition_to_escalated(action.incident_id, "PHASE_7D_PROVIDER_RESOURCE_NOT_FOUND")
            self._update_incident(action.incident_id, outcome="RECOVERY_REQUIRED")
            self._metric("reconciliation_completed")
            return DurableReconciliationResult(
                status="RECOVERY_REQUIRED", action_id=failed.command_id,
                incident_id=failed.incident_id, action_state=failed.state.value,
                attempt_count=attempt_number,
                max_attempts=self.settings.durable_reconciliation_max_attempts,
                deadline_at=self._deadline(failed), provider_state=provider_state,
                provenance=provider.provenance, reason="provider_resource_not_found",
                provider_reads=provider_reads,
            )
        current = self.bundle.actions.get(action_id) or action
        if current.state is ActionState.OUTCOME_UNKNOWN:
            current = self._update_action(current, ActionState.RECONCILIATION_REQUIRED, provider.reason)
        self._metric("reconciliation_unknown" if classification != "WAITING_FOR_PROVIDER" else "reconciliation_pending")
        return DurableReconciliationResult(
            status=("WAITING_FOR_PROVIDER" if classification == "WAITING_FOR_PROVIDER" else "STILL_UNKNOWN"),
            action_id=current.command_id, incident_id=current.incident_id,
            action_state=current.state.value, attempt_count=attempt_number,
            max_attempts=self.settings.durable_reconciliation_max_attempts,
            next_eligible_at=self.clock() + self._backoff(attempt_number),
            deadline_at=self._deadline(current), provider_state=provider_state,
            provenance=provider.provenance, reason=provider.reason,
            provider_reads=provider_reads,
        )

    async def reconcile_validation_cleanup(
        self, original_action_id: str, run_id: str,
    ) -> DurableReconciliationResult:
        """Read-reconcile one existing real-QoD cleanup; never send DELETE.

        The original CREATE and existing QOD_RELEASE action are reloaded from
        durable authority.  Only provider readback is permitted.  A terminal
        provider state finalizes the existing cleanup; active or unavailable
        evidence remains fail-closed and reconciliation-required.
        """
        if not self.is_ready():
            raise RepositoryUnavailable("durable_reconciliation_runtime_not_ready")
        original = self.bundle.actions.get(original_action_id)
        if original is None:
            raise RepositoryUnavailable("action_not_found")
        if (
            not run_id.startswith("REAL-QOD-TEST-")
            or original.incident_id != f"{run_id}-INCIDENT"
            or original.command_type != "QOD_PLAN"
            or not original.provider_resource_id
        ):
            raise RepositoryUnavailable("validation_cleanup_reference_invalid")
        cleanup_probe = ActionCommand(
            incident_id=original.incident_id, command_type="QOD_RELEASE",
            resource_key=original.resource_key, device_id=original.device_id,
            plan_version=original.plan_version, requested_at=0.0,
        )
        cleanup = self.bundle.actions.get_by_idempotency_key(cleanup_probe.idempotency_key)
        if cleanup is None:
            return DurableReconciliationResult(
                status="BLOCKED_SAFE", action_id=original.command_id,
                incident_id=original.incident_id, action_state=original.state.value,
                reason="authorized_cleanup_action_missing",
            )
        if (
            cleanup.parameters_safe.get("original_command_id") != original.command_id
            or cleanup.incident_id != original.incident_id
            or cleanup.resource_key != original.resource_key
        ):
            raise RepositoryUnavailable("validation_cleanup_binding_invalid")
        if cleanup.state is ActionState.ROLLED_BACK:
            return DurableReconciliationResult(
                status="CLEANUP_VERIFIED", action_id=cleanup.command_id,
                incident_id=cleanup.incident_id, action_state=cleanup.state.value,
                reason="cleanup_already_verified", duplicate=True,
            )
        if cleanup.state is ActionState.READY:
            return DurableReconciliationResult(
                status="BLOCKED_SAFE", action_id=cleanup.command_id,
                incident_id=cleanup.incident_id, action_state=cleanup.state.value,
                reason="cleanup_not_previously_attempted",
            )
        if cleanup.state not in {
            ActionState.SENT, ActionState.ACKNOWLEDGED, ActionState.OUTCOME_UNKNOWN,
            ActionState.RECONCILIATION_REQUIRED,
        }:
            return DurableReconciliationResult(
                status="BLOCKED_SAFE", action_id=cleanup.command_id,
                incident_id=cleanup.incident_id, action_state=cleanup.state.value,
                reason="cleanup_action_not_reconcilable",
            )
        authority_conflict = self._reload_authority_context(original)
        if authority_conflict:
            return DurableReconciliationResult(
                status="BLOCKED_SAFE", action_id=cleanup.command_id,
                incident_id=cleanup.incident_id, action_state=cleanup.state.value,
                reason=authority_conflict,
            )
        attempts = [
            row for row in self.bundle.verification.for_incident(original.incident_id)
            if row.get("verification_type") == "ROLLBACK_RECONCILIATION"
            and (row.get("result") or {}).get("command_id") == cleanup.command_id
        ]
        verified_attempt = next((
            row for row in attempts
            if (row.get("result") or {}).get("outcome") == "VERIFIED_ROLLBACK"
            and (row.get("result") or {}).get("resource_inactive") is True
        ), None)
        if verified_attempt is not None:
            completed = self.execution_service.complete_verified_validation_cleanup(
                original.command_id, cleanup.command_id,
            )
            return DurableReconciliationResult(
                status="CLEANUP_VERIFIED", action_id=cleanup.command_id,
                incident_id=cleanup.incident_id, action_state=completed.state.value,
                attempt_count=len(attempts),
                max_attempts=self.settings.durable_reconciliation_max_attempts,
                provider_state="INACTIVE",
                verification_state=VerificationState.IMPROVED.value,
                provenance=str(verified_attempt.get("source_provenance") or "UNAVAILABLE"),
                reason="durable_cleanup_readback_already_verified",
                provider_reads=0, duplicate=True,
            )
        attempt_number = len(attempts) + 1
        if attempt_number > self.settings.durable_reconciliation_max_attempts:
            return DurableReconciliationResult(
                status="RECONCILIATION_REQUIRED", action_id=cleanup.command_id,
                incident_id=cleanup.incident_id, action_state=cleanup.state.value,
                attempt_count=len(attempts), max_attempts=self.settings.durable_reconciliation_max_attempts,
                reason="manual_reconciliation_required",
            )
        self._metric("provider_safe_read")
        try:
            verification = await self.adapter.verify_rollback(
                copy.deepcopy(original), original.provider_resource_id,
            )
        except Exception:
            verification = ActionVerificationResult(
                outcome="VERIFICATION_UNAVAILABLE",
                reason="rollback_readback_unavailable", provenance="UNAVAILABLE",
            )
        if not isinstance(verification, ActionVerificationResult):
            verification = ActionVerificationResult(
                outcome="VERIFICATION_UNAVAILABLE",
                reason="rollback_readback_invalid", provenance="UNAVAILABLE",
            )
        state = (
            VerificationState.IMPROVED
            if verification.outcome == "VERIFIED_ROLLBACK"
            else VerificationState.INSUFFICIENT_EVIDENCE
        )
        self.bundle.verification.save({
            "verification_id": f"reconcile-cleanup-{cleanup.command_id}-{attempt_number}",
            "incident_id": original.incident_id, "evidence_event_ids": [],
            "verification_type": "ROLLBACK_RECONCILIATION", "state": state.value,
            "started_at": self.clock(), "updated_at": self.clock(),
            "result": {
                "command_id": cleanup.command_id, "attempt_number": attempt_number,
                "outcome": verification.outcome,
                "resource_inactive": verification.outcome == "VERIFIED_ROLLBACK",
            },
            "reason": verification.reason,
            "source_provenance": verification.provenance,
        })
        if verification.outcome == "VERIFIED_ROLLBACK":
            completed = self.execution_service.complete_verified_validation_cleanup(
                original.command_id, cleanup.command_id,
            )
            return DurableReconciliationResult(
                status="CLEANUP_VERIFIED", action_id=cleanup.command_id,
                incident_id=cleanup.incident_id, action_state=completed.state.value,
                attempt_count=attempt_number,
                max_attempts=self.settings.durable_reconciliation_max_attempts,
                provider_state="INACTIVE", verification_state=state.value,
                provenance=verification.provenance, reason=verification.reason,
                provider_reads=1,
            )
        current = self.bundle.actions.get(cleanup.command_id) or cleanup
        if current.state in {ActionState.SENT, ActionState.ACKNOWLEDGED, ActionState.OUTCOME_UNKNOWN}:
            current = self._update_action(
                current, ActionState.RECONCILIATION_REQUIRED,
                verification.reason,
            )
        active = verification.reason == "live_rollback_not_yet_terminal"
        return DurableReconciliationResult(
            status="RESOURCE_STILL_ACTIVE" if active else "RECONCILIATION_REQUIRED",
            action_id=current.command_id, incident_id=current.incident_id,
            action_state=current.state.value, attempt_count=attempt_number,
            max_attempts=self.settings.durable_reconciliation_max_attempts,
            provider_state="ACTIVE" if active else None,
            verification_state=state.value, provenance=verification.provenance,
            reason=verification.reason, provider_reads=1,
        )

    async def _reverify(
        self, action: ActionCommand, attempt_number: int,
        read_unavailable_reason: Optional[str] = None,
    ) -> DurableReconciliationResult:
        self._metric("reverification_started")
        baseline = dict(action.preconditions.get("verification_baseline") or {})
        if read_unavailable_reason is not None:
            verification = ActionVerificationResult(
                outcome="VERIFICATION_UNAVAILABLE", reason=read_unavailable_reason,
                provenance="UNAVAILABLE",
            )
        else:
            self._metric("provider_safe_read")
            try:
                verification = await self.adapter.verify(
                    copy.deepcopy(action), action.provider_resource_id, baseline,
                )
            except Exception:
                verification = ActionVerificationResult(
                    outcome="VERIFICATION_UNAVAILABLE", reason="verification_adapter_unavailable",
                    provenance="UNAVAILABLE",
                )
        if not isinstance(verification, ActionVerificationResult):
            verification = ActionVerificationResult(
                outcome="VERIFICATION_UNAVAILABLE", reason="verification_response_invalid",
                provenance="UNAVAILABLE",
            )
        expected_provenance = str(action.preconditions.get("provenance") or "UNAVAILABLE")
        if (
            expected_provenance != "UNAVAILABLE"
            and verification.provenance != "UNAVAILABLE"
            and verification.provenance != expected_provenance
        ):
            verification = ActionVerificationResult(
                outcome="VERIFICATION_UNAVAILABLE",
                reason="verification_provenance_mismatch",
                provenance="UNAVAILABLE",
            )
        state = {
            "VERIFIED_IMPROVED": VerificationState.IMPROVED,
            "VERIFIED_NO_IMPROVEMENT": VerificationState.UNCHANGED,
            "VERIFIED_DEGRADED": VerificationState.DEGRADED,
        }.get(verification.outcome, VerificationState.INSUFFICIENT_EVIDENCE)
        verification_id = f"reverify-{action.command_id}-{attempt_number}"
        self.bundle.verification.save({
            "verification_id": verification_id, "incident_id": action.incident_id,
            "evidence_event_ids": [], "verification_type": "NETWORK_ACTION",
            "state": state.value, "started_at": self.clock(), "updated_at": self.clock(),
            "result": {
                "command_id": action.command_id, "attempt_number": attempt_number,
                "outcome": verification.outcome,
                "mitigation_improved": verification.mitigation_improved,
                "evidence": verification.evidence,
            },
            "reason": verification.reason, "source_provenance": verification.provenance,
        })
        self._failpoint("AFTER_VERIFICATION_PERSIST_BEFORE_ACTION_UPDATE")
        self._update_incident(action.incident_id, verification_state=state.value)
        current = self.bundle.actions.get(action.command_id) or action
        if state is VerificationState.IMPROVED:
            success = self._update_action(current, ActionState.SUCCESS)
            self.execution_service.resolve_if_verified(success.incident_id, success.plan_version)
            self._metric("reverification_improved")
            self._metric("reconciliation_completed")
            return DurableReconciliationResult(
                status="RESOLVED" if self.bundle.incidents.get(action.incident_id).get("state") == "RESOLVED" else "VERIFIED_IMPROVED",
                action_id=success.command_id, incident_id=success.incident_id,
                action_state=success.state.value, attempt_count=attempt_number,
                max_attempts=self.settings.durable_reconciliation_max_attempts,
                deadline_at=self._deadline(success), provider_state="AVAILABLE",
                verification_state=state.value, provenance=verification.provenance,
                reason=verification.reason,
            )
        if state in {VerificationState.UNCHANGED, VerificationState.DEGRADED}:
            rollback = self._update_action(current, ActionState.ROLLBACK_REQUIRED, verification.reason)
            self._enqueue_phase7c_rollback(rollback)
            self._metric("reverification_unchanged" if state is VerificationState.UNCHANGED else "reverification_degraded")
            self._metric("reconciliation_completed")
            return DurableReconciliationResult(
                status="RECOVERY_REQUIRED", action_id=rollback.command_id,
                incident_id=rollback.incident_id, action_state=rollback.state.value,
                attempt_count=attempt_number,
                max_attempts=self.settings.durable_reconciliation_max_attempts,
                deadline_at=self._deadline(rollback), provider_state="AVAILABLE",
                verification_state=state.value, provenance=verification.provenance,
                reason=verification.reason,
            )
        pending = current
        if current.state is not ActionState.RECONCILIATION_REQUIRED:
            pending = self._update_action(current, ActionState.RECONCILIATION_REQUIRED, verification.reason)
        self._metric("reconciliation_pending")
        return DurableReconciliationResult(
            status="WAIT_AND_REVERIFY", action_id=pending.command_id,
            incident_id=pending.incident_id, action_state=pending.state.value,
            attempt_count=attempt_number,
            max_attempts=self.settings.durable_reconciliation_max_attempts,
            next_eligible_at=self.clock() + self._backoff(attempt_number),
            deadline_at=self._deadline(pending), provider_state="AVAILABLE",
            verification_state=state.value, provenance=verification.provenance,
            reason=verification.reason,
        )

    def _enqueue_phase7c_rollback(self, action: ActionCommand) -> None:
        incident = self.bundle.incidents.get(action.incident_id)
        if incident is None:
            raise RepositoryUnavailable("incident_not_found")
        identity = hashlib.sha256(f"{action.command_id}|ROLLBACK_REQUIRED".encode()).hexdigest()[:24]
        self.bundle.outbox.append({
            "outbox_id": f"out-action-{identity}",
            "event_id": str(incident.get("trigger_event_id") or ""),
            "event_type": "DURABLE_ACTION_EXECUTION_READY",
            "payload": {"incident_id": action.incident_id, "action_id": action.command_id},
            "trace_id": str(incident.get("trace_id") or "phase-7d"),
            "created_at": self.clock(),
        })


class DurableReconciliationScheduler:
    """Schedules due reconciliation from durable truth; it performs no reads itself."""

    def __init__(
        self, *, bundle: Any, service: DurableActionReconciliationService,
        is_ready: Callable[[], bool], clock: Callable[[], float] = time.time,
    ) -> None:
        self.bundle = bundle
        self.service = service
        self.is_ready = is_ready
        self.clock = clock
        self._running = False

    def _candidates(self) -> list[ActionCommand]:
        result: Dict[str, ActionCommand] = {}
        for incident in self.bundle.incidents.active():
            for action in self.bundle.actions.for_incident(incident["incident_id"]):
                if action.state in ELIGIBLE_RECONCILIATION_STATES:
                    result[action.command_id] = action
        return list(result.values())

    def enqueue_due(self) -> int:
        if not self.is_ready():
            return 0
        unsent = {row.get("outbox_id") for row in self.bundle.outbox.unsent()}
        enqueued = 0
        for action in self._candidates():
            status = self.service.work_status(action.command_id)
            if status.status not in {"DUE", "READY_FOR_REVERIFICATION", "DEADLINE_REACHED"}:
                continue
            attempt = status.attempt_count + 1
            identity = hashlib.sha256(f"{action.command_id}|{attempt}|RECONCILE".encode()).hexdigest()[:24]
            outbox_id = f"out-reconcile-{identity}"
            if outbox_id in unsent:
                continue
            incident = self.bundle.incidents.get(action.incident_id)
            if incident is None:
                continue
            try:
                self.bundle.outbox.append({
                    "outbox_id": outbox_id,
                    "event_id": str(incident.get("trigger_event_id") or ""),
                    "event_type": "DURABLE_RECONCILIATION_READY",
                    "payload": {"incident_id": action.incident_id, "action_id": action.command_id},
                    "trace_id": str(incident.get("trace_id") or "phase-7d"),
                    "created_at": self.clock(),
                })
                enqueued += 1
                unsent.add(outbox_id)
            except RepositoryUnavailable:
                # Another scheduler may have won the deterministic outbox ID,
                # or persistence may be unavailable. Either case is fail-safe.
                continue
        return enqueued

    async def run(self) -> None:
        self._running = True
        interval = max(1, int(self.service.settings.durable_reconciliation_scan_seconds))
        while self._running and self.is_ready():
            self.enqueue_due()
            await asyncio.sleep(interval)

    def stop(self) -> None:
        self._running = False

    def status(self) -> Dict[str, Any]:
        return {"state": "RUNNING" if self._running else "STOPPED", "authority": "DURABLE_REPOSITORY"}


def reconciliation_public_view(bundle: Any, settings: Any, now: float) -> list[Dict[str, Any]]:
    """Build a safe NOC view without constructing or calling a provider."""
    rows = []
    for incident in bundle.incidents.active():
        for action in bundle.actions.for_incident(incident["incident_id"]):
            attempts = [
                row for row in bundle.verification.for_incident(action.incident_id)
                if row.get("verification_type") == "PROVIDER_RECONCILIATION"
                and (row.get("result") or {}).get("command_id") == action.command_id
            ]
            if action.state not in ELIGIBLE_RECONCILIATION_STATES and not attempts:
                continue
            attempts.sort(key=lambda row: float(row.get("updated_at") or 0))
            latest = attempts[-1] if attempts else {}
            result = latest.get("result") or {}
            network_checks = [
                row for row in bundle.verification.for_incident(action.incident_id)
                if row.get("verification_type") == "NETWORK_ACTION"
                and (row.get("result") or {}).get("command_id") == action.command_id
            ]
            network_checks.sort(key=lambda row: float(row.get("updated_at") or 0))
            latest_network = network_checks[-1] if network_checks else {}
            count = len([row for row in attempts if (row.get("result") or {}).get("terminal") is not True])
            anchor = float(action.last_attempt_at or action.requested_at)
            deadline = anchor + settings.durable_reconciliation_deadline_seconds
            status = (
                "ESCALATED" if result.get("terminal") else
                "RESOLVED" if action.state is ActionState.SUCCESS else
                "RECOVERY_REQUIRED" if action.state in {ActionState.ROLLBACK_REQUIRED, ActionState.FAILED} else
                "WAIT_AND_REVERIFY" if latest_network.get("state") == VerificationState.INSUFFICIENT_EVIDENCE.value else
                "VERIFYING" if action.state is ActionState.ACKNOWLEDGED else
                "WAITING_FOR_PROVIDER" if result.get("outcome") == "WAITING_FOR_PROVIDER" else
                "RECONCILIATION_REQUIRED"
            )
            rows.append({
                "action_id": action.command_id, "incident_id": action.incident_id,
                "status": status,
                "safe_reason": str(latest_network.get("reason") or latest.get("reason") or "reconciliation_pending")[:128],
                "attempt_count": count,
                "max_attempts": settings.durable_reconciliation_max_attempts,
                "deadline_at": deadline, "deadline_status": "EXPIRED" if now >= deadline else "ACTIVE",
                "last_verification_outcome": latest_network.get("state"),
                "provenance": str(
                    latest_network.get("source_provenance")
                    or result.get("provenance") or latest.get("source_provenance")
                    or "UNAVAILABLE"
                ),
                "mutation_retry_performed": False,
            })
    return rows
