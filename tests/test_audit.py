"""
Unit tests for audit history, state locks, and policy storage.
Verifies durability, concurrency lock semantics, and state recovery.
"""

import os
import uuid
import pytest

from audit import AuditAndStateStore
from models import (
    ApprovalStatus,
    FailureCategory,
    RecoveryActionType,
    RecoveryEvent,
    RecoveryPolicy,
    RecoveryStatus,
    VerificationStatus,
)


@pytest.fixture
def temp_store(tmp_path):
    db_file = str(tmp_path / "test_audit.db")
    return AuditAndStateStore(db_path=db_file)


def test_policy_save_and_retrieve(temp_store):
    policy = RecoveryPolicy(
        job_id=999,
        allowlisted=True,
        allowed_actions=[RecoveryActionType.RETRY_RUN],
        max_retries=5,
        cooldown_seconds=45,
        auto_recovery_eligible=True,
        requires_human_approval=False,
    )
    temp_store.save_policy(policy)

    retrieved = temp_store.get_policy(999)
    assert retrieved.job_id == 999
    assert retrieved.max_retries == 5
    assert retrieved.cooldown_seconds == 45
    assert retrieved.allowed_actions == [RecoveryActionType.RETRY_RUN]
    assert retrieved.auto_recovery_eligible is True


def test_concurrency_lock_acquisition_and_release(temp_store):
    job_id = 101
    run_id = 5001
    key1 = "rec-key-1"

    # 1. Acquire lock
    token1 = temp_store.acquire_lock(job_id, run_id, key1)
    assert token1 is not None

    # 2. Concurrent acquisition should fail
    token2 = temp_store.acquire_lock(job_id, run_id, "rec-key-2")
    assert token2 is None

    # 3. State should reflect locked
    state = temp_store.get_state(job_id, run_id)
    assert state.is_locked is True
    assert state.status == RecoveryStatus.IN_PROGRESS

    # 4. Release lock
    released = temp_store.release_lock(job_id, run_id, token1, RecoveryStatus.SUCCEEDED, RecoveryActionType.RETRY_RUN)
    assert released is True

    # 5. State after release
    updated_state = temp_store.get_state(job_id, run_id)
    assert updated_state.is_locked is False
    assert updated_state.attempt_count == 1
    assert updated_state.status == RecoveryStatus.SUCCEEDED


def test_state_persistence_after_restart(tmp_path):
    # Scenario 14: Recovery service restart during active recovery
    db_file = str(tmp_path / "restart_audit.db")
    store1 = AuditAndStateStore(db_path=db_file)

    token = store1.acquire_lock(101, 777, "key-restart")
    assert token is not None

    # Simulate restart by instantiating new store on same file
    store2 = AuditAndStateStore(db_path=db_file)
    recovered_state = store2.get_state(101, 777)

    assert recovered_state.is_locked is True
    assert recovered_state.lock_token == token
    assert recovered_state.idempotency_key == "key-restart"


def test_audit_event_logging_and_filtering(temp_store):
    event = RecoveryEvent(
        event_id="evt-100",
        workspace_id="test-ws",
        job_id=101,
        run_id=5001,
        task_key="task_a",
        failure_category=FailureCategory.TRANSIENT_INFRASTRUCTURE,
        root_cause="Network timeout",
        confidence=0.92,
        recommended_action=RecoveryActionType.RETRY_RUN,
        executed_action=RecoveryActionType.RETRY_RUN,
        approval_status=ApprovalStatus.NOT_REQUIRED,
        recovery_status=RecoveryStatus.SUCCEEDED,
        verification_status=VerificationStatus.PASSED,
    )
    temp_store.log_event(event)

    events = temp_store.list_history(job_id=101)
    assert len(events) == 1
    assert events[0].event_id == "evt-100"
    assert events[0].failure_category == FailureCategory.TRANSIENT_INFRASTRUCTURE
    assert events[0].recovery_status == RecoveryStatus.SUCCEEDED
