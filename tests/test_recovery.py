"""
Unit tests for the guarded recovery execution engine.
Verifies API dispatches, concurrency locks, audit events, and idempotency protection.
"""

from unittest.mock import MagicMock
import pytest

from audit import AuditAndStateStore
from models import (
    DiagnosisResult,
    FailureCategory,
    JobRunInfo,
    RecoveryActionType,
    RecoveryPolicy,
    RecoveryStatus,
    TaskRunInfo,
)
from policy_engine import PolicyEngine
from recovery import RecoveryEngine


@pytest.fixture
def recovery_setup(tmp_path):
    store = AuditAndStateStore(db_path=str(tmp_path / "rec_test.db"))
    policy = PolicyEngine(store=store)
    client = MagicMock()
    engine = RecoveryEngine(client=client, store=store, policy=policy)
    return engine, client, store


def test_execute_repair_failed_tasks_success(recovery_setup):
    engine, client, store = recovery_setup
    store.save_policy(
        RecoveryPolicy(
            job_id=101,
            allowlisted=True,
            allowed_actions=[RecoveryActionType.REPAIR_FAILED_TASKS],
            auto_recovery_eligible=True,
            requires_human_approval=False,
        )
    )
    client.repair_run.return_value = {"repair_id": 9999, "status": "SUBMITTED"}

    run_info = JobRunInfo(
        job_id=101,
        run_id=5001,
        run_name="Bronze Ingest",
        state="TERMINATED",
        result_state="FAILED",
        tasks=[TaskRunInfo(task_key="ingest_s3", run_id=5001, state="TERMINATED", result_state="FAILED")],
    )
    diag = DiagnosisResult(
        failure_category=FailureCategory.TRANSIENT_INFRASTRUCTURE,
        root_cause="Network glitch",
        confidence=0.9,
        recommended_action=RecoveryActionType.REPAIR_FAILED_TASKS,
        requires_approval=False,
        reasoning_summary="Transient",
    )

    result = engine.execute_recovery(
        job_id=101,
        run_info=run_info,
        action=RecoveryActionType.REPAIR_FAILED_TASKS,
        diagnosis=diag,
        failed_tasks=["ingest_s3"],
    )

    assert result.success is True
    assert result.repair_id == 9999
    client.repair_run.assert_called_once_with(run_id=5001, rerun_tasks=["ingest_s3"])

    # Verify audit event was logged
    history = store.list_history(job_id=101)
    assert len(history) == 1
    assert history[0].repair_id == 9999
    assert history[0].recovery_status == RecoveryStatus.IN_PROGRESS


def test_execute_retry_run_success(recovery_setup):
    engine, client, store = recovery_setup
    store.save_policy(
        RecoveryPolicy(
            job_id=101,
            allowlisted=True,
            allowed_actions=[RecoveryActionType.RETRY_RUN],
            auto_recovery_eligible=True,
            requires_human_approval=False,
        )
    )
    client.run_now.return_value = {"run_id": 7001, "status": "TRIGGERED"}

    run_info = JobRunInfo(job_id=101, run_id=5001, run_name="Job", state="TERMINATED", result_state="FAILED")
    diag = DiagnosisResult(
        failure_category=FailureCategory.TRANSIENT_INFRASTRUCTURE,
        root_cause="Timeout",
        confidence=0.9,
        recommended_action=RecoveryActionType.RETRY_RUN,
        requires_approval=False,
        reasoning_summary="Retry",
    )

    result = engine.execute_recovery(
        job_id=101,
        run_info=run_info,
        action=RecoveryActionType.RETRY_RUN,
        diagnosis=diag,
    )

    assert result.success is True
    assert result.triggered_run_id == 7001
    client.run_now.assert_called_once_with(job_id=101)


def test_reject_concurrent_recovery_requests(recovery_setup):
    engine, client, store = recovery_setup
    store.save_policy(RecoveryPolicy(job_id=101, allowlisted=True, auto_recovery_eligible=True, requires_human_approval=False))
    
    # Pre-lock run 5001
    store.acquire_lock(101, 5001, "active-lock")

    run_info = JobRunInfo(job_id=101, run_id=5001, run_name="Job", state="TERMINATED", result_state="FAILED")
    diag = DiagnosisResult(
        failure_category=FailureCategory.TRANSIENT_INFRASTRUCTURE,
        root_cause="Glitch",
        confidence=0.9,
        recommended_action=RecoveryActionType.RETRY_RUN,
        requires_approval=False,
        reasoning_summary="Test",
    )

    result = engine.execute_recovery(
        job_id=101,
        run_info=run_info,
        action=RecoveryActionType.RETRY_RUN,
        diagnosis=diag,
    )

    assert result.success is False
    assert "in progress" in result.message.lower() or "active recovery" in result.message.lower()
    client.run_now.assert_not_called()


def test_recovery_blocked_when_unapproved(recovery_setup):
    engine, client, store = recovery_setup
    # Job requires human approval
    store.save_policy(RecoveryPolicy(job_id=102, allowlisted=True, requires_human_approval=True, auto_recovery_eligible=False))

    run_info = JobRunInfo(job_id=102, run_id=5002, run_name="Job", state="TERMINATED", result_state="FAILED")
    diag = DiagnosisResult(
        failure_category=FailureCategory.MISSING_OR_EMPTY_SOURCE,
        root_cause="Missing file",
        confidence=0.9,
        recommended_action=RecoveryActionType.REPAIR_FAILED_TASKS,
        requires_approval=True,
        reasoning_summary="Test",
    )

    result = engine.execute_recovery(
        job_id=102,
        run_info=run_info,
        action=RecoveryActionType.REPAIR_FAILED_TASKS,
        diagnosis=diag,
        approved_by=None,  # Not approved
    )

    assert result.success is False
    assert result.requires_approval is True
    assert "human operator approval" in result.message
    client.repair_run.assert_not_called()
