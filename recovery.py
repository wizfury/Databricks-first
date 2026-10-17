"""
Guarded Recovery Execution Engine.
Orchestrates allowlisted Databricks SDK operations under strict policy enforcement,
durable concurrency locks, and comprehensive audit logging.
"""

import logging
import uuid
from typing import Any, Dict, List, Optional
from pydantic import BaseModel

from audit import AuditAndStateStore, audit_store
from databricks_client import DatabricksClientWrapper, databricks_client
from models import (
    ApprovalStatus,
    DiagnosisResult,
    JobRunInfo,
    RecoveryActionType,
    RecoveryEvent,
    RecoveryStatus,
    VerificationStatus,
)
from policy_engine import PolicyEngine, policy_engine

logger = logging.getLogger(__name__)


class RecoveryExecutionResult(BaseModel):
    """Result of a recovery action execution."""
    success: bool
    action: RecoveryActionType
    repair_id: Optional[int] = None
    triggered_run_id: Optional[int] = None
    message: str
    event_id: str
    requires_approval: bool = False


class RecoveryEngine:
    """Executes safe, allowlisted recovery actions with server-side validation."""

    def __init__(
        self,
        client: Optional[DatabricksClientWrapper] = None,
        store: Optional[AuditAndStateStore] = None,
        policy: Optional[PolicyEngine] = None,
    ):
        self.client = client or databricks_client
        self.store = store or audit_store
        self.policy = policy or policy_engine

    def execute_recovery(
        self,
        job_id: int,
        run_info: JobRunInfo,
        action: RecoveryActionType,
        diagnosis: DiagnosisResult,
        approved_by: Optional[str] = None,
        failed_tasks: Optional[List[str]] = None,
    ) -> RecoveryExecutionResult:
        """
        Execute an approved recovery action with end-to-end policy checks and locking.
        """
        event_id = f"evt-{uuid.uuid4().hex[:12]}"
        is_approved = bool(approved_by)

        # 1. Server-Side Policy Evaluation Gate
        eval_result = self.policy.evaluate_action(
            job_id=job_id,
            run_info=run_info,
            proposed_action=action,
            diagnosis=diagnosis,
            is_approved=is_approved,
        )

        if not eval_result.allowed:
            # Audit the blocked attempt
            blocked_event = RecoveryEvent(
                event_id=event_id,
                workspace_id="databricks-workspace",
                job_id=job_id,
                run_id=run_info.run_id,
                task_key=failed_tasks[0] if failed_tasks else None,
                failure_category=diagnosis.failure_category,
                root_cause=diagnosis.root_cause,
                confidence=diagnosis.confidence,
                recommended_action=diagnosis.recommended_action,
                executed_action=None,
                approval_status=ApprovalStatus.PENDING if eval_result.requires_approval else ApprovalStatus.NOT_REQUIRED,
                approved_by=approved_by,
                recovery_status=RecoveryStatus.FAILED,
                error_summary=f"Blocked by Policy: {eval_result.reason}",
            )
            self.store.log_event(blocked_event)

            return RecoveryExecutionResult(
                success=False,
                action=action,
                message=eval_result.reason,
                event_id=event_id,
                requires_approval=eval_result.requires_approval,
            )

        # 2. Acquire Durable Concurrency Lock & Idempotency Key
        idempotency_key = f"rec-{job_id}-{run_info.run_id}-{action.value}-att{eval_result.attempt_count + 1}"
        lock_token = self.store.acquire_lock(job_id=job_id, run_id=run_info.run_id, idempotency_key=idempotency_key)

        if not lock_token:
            return RecoveryExecutionResult(
                success=False,
                action=action,
                message="Concurrency conflict or duplicate action: Another recovery operation is in-flight for this run.",
                event_id=event_id,
                requires_approval=False,
            )

        # 3. Create active audit event
        audit_event = RecoveryEvent(
            event_id=event_id,
            workspace_id="databricks-workspace",
            job_id=job_id,
            run_id=run_info.run_id,
            task_key=failed_tasks[0] if failed_tasks else None,
            failure_category=diagnosis.failure_category,
            root_cause=diagnosis.root_cause,
            confidence=diagnosis.confidence,
            recommended_action=diagnosis.recommended_action,
            executed_action=action,
            approval_status=ApprovalStatus.APPROVED if approved_by else ApprovalStatus.NOT_REQUIRED,
            approved_by=approved_by,
            recovery_status=RecoveryStatus.IN_PROGRESS,
            verification_status=VerificationStatus.NOT_STARTED,
            error_summary=None,
        )
        self.store.log_event(audit_event)

        # 4. Dispatch Allowlisted Action
        try:
            if action == RecoveryActionType.REPAIR_FAILED_TASKS:
                result = self._repair_tasks(run_info.run_id, failed_tasks)
                repair_id = result.get("repair_id")
                audit_event.repair_id = repair_id
                audit_event.recovery_status = RecoveryStatus.IN_PROGRESS
                self.store.release_lock(job_id, run_info.run_id, lock_token, RecoveryStatus.IN_PROGRESS, action)
                self.store.log_event(audit_event)
                return RecoveryExecutionResult(
                    success=True,
                    action=action,
                    repair_id=repair_id,
                    message=f"Task repair initiated successfully via Jobs API (Repair ID: {repair_id}).",
                    event_id=event_id,
                )

            elif action == RecoveryActionType.RETRY_RUN:
                result = self.client.run_now(job_id=job_id)
                new_run_id = result.get("run_id")
                audit_event.repair_id = new_run_id
                audit_event.recovery_status = RecoveryStatus.IN_PROGRESS
                self.store.release_lock(job_id, run_info.run_id, lock_token, RecoveryStatus.IN_PROGRESS, action)
                self.store.log_event(audit_event)
                return RecoveryExecutionResult(
                    success=True,
                    action=action,
                    triggered_run_id=new_run_id,
                    message=f"New run triggered successfully (Run ID: {new_run_id}).",
                    event_id=event_id,
                )

            elif action == RecoveryActionType.CANCEL_RUN:
                canceled = self.client.cancel_run(run_id=run_info.run_id)
                status = RecoveryStatus.SUCCEEDED if canceled else RecoveryStatus.FAILED
                audit_event.recovery_status = status
                self.store.release_lock(job_id, run_info.run_id, lock_token, status, action)
                self.store.log_event(audit_event)
                return RecoveryExecutionResult(
                    success=canceled,
                    action=action,
                    message="Job run cancellation requested.",
                    event_id=event_id,
                )

            elif action in (
                RecoveryActionType.ESCALATE_TO_HUMAN,
                RecoveryActionType.RECOMMEND_COMPUTE_ADJUSTMENT,
                RecoveryActionType.WAIT_FOR_DEPENDENCY,
                RecoveryActionType.TRIGGER_VALIDATION,
            ):
                # Non-mutative advisory actions
                audit_event.recovery_status = RecoveryStatus.SUCCEEDED
                self.store.release_lock(job_id, run_info.run_id, lock_token, RecoveryStatus.SUCCEEDED, action, increment_attempt=False)
                self.store.log_event(audit_event)
                return RecoveryExecutionResult(
                    success=True,
                    action=action,
                    message=f"Advisory action '{action.value}' recorded. Awaiting operator resolution.",
                    event_id=event_id,
                )

            else:
                raise ValueError(f"Unsupported recovery action: {action}")

        except Exception as e:
            logger.error("Failed to execute recovery action %s on run %s: %s", action, run_info.run_id, e)
            audit_event.recovery_status = RecoveryStatus.FAILED
            audit_event.error_summary = str(e)
            self.store.release_lock(job_id, run_info.run_id, lock_token, RecoveryStatus.FAILED, action)
            self.store.log_event(audit_event)
            return RecoveryExecutionResult(
                success=False,
                action=action,
                message=f"Execution error: {str(e)}",
                event_id=event_id,
            )

    def _repair_tasks(self, run_id: int, tasks: Optional[List[str]]) -> Dict[str, Any]:
        """Dispatch repair_run API call."""
        if tasks:
            return self.client.repair_run(run_id=run_id, rerun_tasks=tasks)
        return self.client.repair_run(run_id=run_id, rerun_all_failed_tasks=True)


# Singleton recovery engine
recovery_engine = RecoveryEngine()
