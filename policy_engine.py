"""
Recovery Policy Engine.
Enforces safety boundaries, allowlists, retry budgets, cooldown windows,
concurrency locks, and human approval gates before any recovery action is executed.
"""

import logging
import random
import time
from datetime import datetime, timezone
from typing import Optional, Tuple
from pydantic import BaseModel

from audit import AuditAndStateStore, audit_store
from models import (
    ApprovalStatus,
    DiagnosisResult,
    JobRunInfo,
    RecoveryActionType,
    RecoveryPolicy,
    RecoveryState,
)

logger = logging.getLogger(__name__)


class PolicyEvaluation(BaseModel):
    """Result of policy evaluation for a proposed recovery action."""
    allowed: bool
    reason: str
    requires_approval: bool
    backoff_seconds: int = 0
    attempt_count: int = 0
    max_retries: int = 3


class PolicyEngine:
    """Evaluates safety policies and enforces recovery budgets."""

    def __init__(self, store: Optional[AuditAndStateStore] = None):
        self.store = store or audit_store

    def evaluate_action(
        self,
        job_id: int,
        run_info: JobRunInfo,
        proposed_action: RecoveryActionType,
        diagnosis: Optional[DiagnosisResult] = None,
        is_approved: bool = False,
    ) -> PolicyEvaluation:
        """
        Evaluate whether a proposed recovery action is permissible.
        Returns PolicyEvaluation with boolean decision and explanatory reason.
        """
        policy: RecoveryPolicy = self.store.get_policy(job_id)
        state: RecoveryState = self.store.get_state(job_id, run_info.run_id)

        # 1. Job Allowlist Gate
        if not policy.allowlisted:
            return PolicyEvaluation(
                allowed=False,
                reason=f"Job {job_id} is not in the recovery allowlist.",
                requires_approval=True,
                attempt_count=state.attempt_count,
                max_retries=policy.max_retries,
            )

        # 2. Run Eligibility Gate
        if run_info.result_state == "SUCCESS":
            return PolicyEvaluation(
                allowed=False,
                reason="Run already completed successfully. Recovery not applicable.",
                requires_approval=False,
                attempt_count=state.attempt_count,
                max_retries=policy.max_retries,
            )

        if run_info.state == "RUNNING" and proposed_action != RecoveryActionType.CANCEL_RUN:
            return PolicyEvaluation(
                allowed=False,
                reason="Run is currently executing. Wait for completion or cancel before recovering.",
                requires_approval=True,
                attempt_count=state.attempt_count,
                max_retries=policy.max_retries,
            )

        # 3. Action Permitted in Policy
        if proposed_action not in policy.allowed_actions:
            return PolicyEvaluation(
                allowed=False,
                reason=f"Action '{proposed_action.value}' is not permitted by policy for Job {job_id}.",
                requires_approval=True,
                attempt_count=state.attempt_count,
                max_retries=policy.max_retries,
            )

        # 4. Retry Budget Gate
        if proposed_action in (RecoveryActionType.RETRY_RUN, RecoveryActionType.REPAIR_FAILED_TASKS):
            if state.attempt_count >= policy.max_retries:
                return PolicyEvaluation(
                    allowed=False,
                    reason=f"Retry budget exhausted: {state.attempt_count}/{policy.max_retries} attempts made. Human escalation required.",
                    requires_approval=True,
                    attempt_count=state.attempt_count,
                    max_retries=policy.max_retries,
                )

        # 5. Cooldown Window & Exponential Backoff
        if state.last_attempt_time:
            # Exponential backoff base: cooldown * (1.5 ** attempt) + small jitter
            factor = 1.5 ** state.attempt_count
            jitter = random.uniform(0.5, 2.0)
            required_cooldown = int((policy.cooldown_seconds * factor) + jitter)
            
            elapsed = (datetime.now(timezone.utc) - state.last_attempt_time).total_seconds()
            if elapsed < required_cooldown:
                remaining = int(required_cooldown - elapsed)
                return PolicyEvaluation(
                    allowed=False,
                    reason=f"Cooldown active. {remaining}s remaining before next attempt is permitted.",
                    requires_approval=False,
                    backoff_seconds=remaining,
                    attempt_count=state.attempt_count,
                    max_retries=policy.max_retries,
                )

        # 6. Concurrency / Lock Gate
        if state.is_locked:
            return PolicyEvaluation(
                allowed=False,
                reason=f"Active recovery is currently in progress for Run {run_info.run_id}.",
                requires_approval=True,
                attempt_count=state.attempt_count,
                max_retries=policy.max_retries,
            )

        # 7. Non-executable actions (Recommendations / Escalations)
        if proposed_action in (
            RecoveryActionType.ESCALATE_TO_HUMAN,
            RecoveryActionType.RECOMMEND_COMPUTE_ADJUSTMENT,
            RecoveryActionType.WAIT_FOR_DEPENDENCY,
        ):
            return PolicyEvaluation(
                allowed=True,
                reason=f"Action '{proposed_action.value}' is an advisory / escalation action.",
                requires_approval=False,
                attempt_count=state.attempt_count,
                max_retries=policy.max_retries,
            )

        # 8. Human Approval Gate
        requires_approval = (
            policy.requires_human_approval
            or (diagnosis and diagnosis.requires_approval)
            or not policy.auto_recovery_eligible
        )

        if requires_approval and not is_approved:
            return PolicyEvaluation(
                allowed=False,
                reason="Policy requires explicit human operator approval before execution.",
                requires_approval=True,
                attempt_count=state.attempt_count,
                max_retries=policy.max_retries,
            )

        return PolicyEvaluation(
            allowed=True,
            reason="All policy checks passed. Action is permitted.",
            requires_approval=requires_approval,
            attempt_count=state.attempt_count,
            max_retries=policy.max_retries,
        )


# Singleton policy engine instance
policy_engine = PolicyEngine()
