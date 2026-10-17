"""
Unit tests for the recovery policy engine.
Verifies allowlist enforcement, retry budgets, cooldown windows,
concurrency locks, and approval gates.
"""

from datetime import datetime, timezone, timedelta
import pytest

from audit import AuditAndStateStore
from models import (
    DiagnosisResult,
    FailureCategory,
    JobRunInfo,
    RecoveryActionType,
    RecoveryPolicy,
    RecoveryState,
    RecoveryStatus,
)
from policy_engine import PolicyEngine


@pytest.fixture
def policy_engine_setup(tmp_path):
    store = AuditAndStateStore(db_path=str(tmp_path / "policy_test.db"))
    engine = PolicyEngine(store=store)
    return engine, store


def test_reject_non_allowlisted_job(policy_engine_setup):
    engine, store = policy_engine_setup
    store.save_policy(RecoveryPolicy(job_id=200, allowlisted=False))

    run = JobRunInfo(job_id=200, run_id=1, run_name="Job", state="TERMINATED", result_state="FAILED")
    res = engine.evaluate_action(200, run, RecoveryActionType.RETRY_RUN)

    assert res.allowed is False
    assert "not in the recovery allowlist" in res.reason


def test_reject_already_successful_run(policy_engine_setup):
    engine, store = policy_engine_setup
    store.save_policy(RecoveryPolicy(job_id=101, allowlisted=True))

    run = JobRunInfo(job_id=101, run_id=1, run_name="Job", state="TERMINATED", result_state="SUCCESS")
    res = engine.evaluate_action(101, run, RecoveryActionType.RETRY_RUN)

    assert res.allowed is False
    assert "already completed successfully" in res.reason


def test_reject_active_running_job(policy_engine_setup):
    engine, store = policy_engine_setup
    store.save_policy(RecoveryPolicy(job_id=101, allowlisted=True))

    run = JobRunInfo(job_id=101, run_id=1, run_name="Job", state="RUNNING", result_state=None)
    res = engine.evaluate_action(101, run, RecoveryActionType.RETRY_RUN)

    assert res.allowed is False
    assert "currently executing" in res.reason


def test_retry_budget_exhaustion(policy_engine_setup):
    engine, store = policy_engine_setup
    store.save_policy(RecoveryPolicy(job_id=101, allowlisted=True, max_retries=2))

    # Simulate 2 previous attempts
    token = store.acquire_lock(101, 1, "k1")
    store.release_lock(101, 1, token, RecoveryStatus.FAILED, increment_attempt=True)
    token = store.acquire_lock(101, 1, "k2")
    store.release_lock(101, 1, token, RecoveryStatus.FAILED, increment_attempt=True)

    run = JobRunInfo(job_id=101, run_id=1, run_name="Job", state="TERMINATED", result_state="FAILED")
    res = engine.evaluate_action(101, run, RecoveryActionType.RETRY_RUN)

    assert res.allowed is False
    assert "Retry budget exhausted" in res.reason


def test_cooldown_window_enforcement(policy_engine_setup):
    engine, store = policy_engine_setup
    store.save_policy(RecoveryPolicy(job_id=101, allowlisted=True, max_retries=5, cooldown_seconds=300))

    # Record an attempt that just occurred
    token = store.acquire_lock(101, 1, "k1")
    store.release_lock(101, 1, token, RecoveryStatus.FAILED, increment_attempt=True)

    run = JobRunInfo(job_id=101, run_id=1, run_name="Job", state="TERMINATED", result_state="FAILED")
    res = engine.evaluate_action(101, run, RecoveryActionType.RETRY_RUN)

    assert res.allowed is False
    assert "Cooldown active" in res.reason
    assert res.backoff_seconds > 0


def test_human_approval_gate(policy_engine_setup):
    engine, store = policy_engine_setup
    # Requires human approval
    store.save_policy(RecoveryPolicy(job_id=102, allowlisted=True, requires_human_approval=True, auto_recovery_eligible=False))

    run = JobRunInfo(job_id=102, run_id=1, run_name="Job", state="TERMINATED", result_state="FAILED")
    diag = DiagnosisResult(
        failure_category=FailureCategory.SCHEMA_COMPATIBILITY,
        root_cause="Schema mismatch",
        confidence=0.9,
        recommended_action=RecoveryActionType.REPAIR_FAILED_TASKS,
        requires_approval=True,
        reasoning_summary="Schema issue",
    )

    # 1. Unapproved should be rejected
    res_unapproved = engine.evaluate_action(102, run, RecoveryActionType.REPAIR_FAILED_TASKS, diagnosis=diag, is_approved=False)
    assert res_unapproved.allowed is False
    assert res_unapproved.requires_approval is True
    assert "requires explicit human operator approval" in res_unapproved.reason

    # 2. Approved should pass
    res_approved = engine.evaluate_action(102, run, RecoveryActionType.REPAIR_FAILED_TASKS, diagnosis=diag, is_approved=True)
    assert res_approved.allowed is True
