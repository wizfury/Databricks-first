"""
Unit tests for the post-recovery verification engine.
Verifies terminal state assertions, task completion gates,
data quality/volume assertions, and duplicate prevention.
"""

from unittest.mock import MagicMock
import pytest

from audit import AuditAndStateStore
from models import (
    DataQualityThresholds,
    JobRunInfo,
    RecoveryPolicy,
    TaskRunInfo,
    VerificationStatus,
)
from verification import RecoveryVerifier


@pytest.fixture
def verifier_setup(tmp_path):
    store = AuditAndStateStore(db_path=str(tmp_path / "verify_test.db"))
    client = MagicMock()
    verifier = RecoveryVerifier(client=client, store=store)
    return verifier, client, store


def test_verify_recovery_success(verifier_setup):
    verifier, client, store = verifier_setup
    store.save_policy(
        RecoveryPolicy(
            job_id=101,
            data_quality_thresholds=DataQualityThresholds(min_row_count=100, max_duplicate_ratio=0.01),
        )
    )

    client.get_run.return_value = JobRunInfo(
        job_id=101,
        run_id=5001,
        run_name="Bronze Ingest",
        state="TERMINATED",
        result_state="SUCCESS",
        execution_duration_ms=60000,
        tasks=[
            TaskRunInfo(task_key="t1", run_id=5001, state="TERMINATED", result_state="SUCCESS"),
            TaskRunInfo(task_key="t2", run_id=5001, state="TERMINATED", result_state="SUCCESS"),
        ],
    )

    result = verifier.verify_recovery(job_id=101, run_id=5001)

    assert result.passed is True
    assert result.failure_reason is None
    assert all(c.passed for c in result.checks)


def test_verify_fails_if_run_still_running(verifier_setup):
    verifier, client, store = verifier_setup
    store.save_policy(RecoveryPolicy(job_id=101))

    client.get_run.return_value = JobRunInfo(
        job_id=101,
        run_id=5001,
        run_name="Bronze Ingest",
        state="RUNNING",
        result_state=None,
        tasks=[TaskRunInfo(task_key="t1", run_id=5001, state="RUNNING", result_state=None)],
    )

    result = verifier.verify_recovery(job_id=101, run_id=5001)

    assert result.passed is False
    assert "Terminal State Check" in result.failure_reason


def test_verify_fails_if_task_failed(verifier_setup):
    verifier, client, store = verifier_setup
    store.save_policy(RecoveryPolicy(job_id=101))

    client.get_run.return_value = JobRunInfo(
        job_id=101,
        run_id=5001,
        run_name="Bronze Ingest",
        state="TERMINATED",
        result_state="FAILED",
        execution_duration_ms=45000,
        tasks=[
            TaskRunInfo(task_key="t1", run_id=5001, state="TERMINATED", result_state="SUCCESS"),
            TaskRunInfo(task_key="t2", run_id=5001, state="TERMINATED", result_state="FAILED"),
        ],
    )

    result = verifier.verify_recovery(job_id=101, run_id=5001)

    assert result.passed is False
    assert "Task Completion Check" in result.failure_reason


def test_verify_fails_on_missing_run(verifier_setup):
    verifier, client, store = verifier_setup
    store.save_policy(RecoveryPolicy(job_id=101))
    client.get_run.return_value = None

    result = verifier.verify_recovery(job_id=101, run_id=9999)

    assert result.passed is False
    assert "not found" in result.failure_reason
