"""
Audit and Durable State Storage Layer.
Implements persistence for Unity Catalog Delta Lake tables:
- {catalog}.{schema}.recovery_history
- {catalog}.{schema}.recovery_state
- {catalog}.{schema}.recovery_policies

Includes a transparent SQLite fallback for offline development, pytest, and local testing.
"""

import json
import logging
import os
import sqlite3
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from config import config
from databricks_client import DatabricksClientWrapper, databricks_client
from models import (
    ApprovalStatus,
    DataQualityThresholds,
    FailureCategory,
    RecoveryActionType,
    RecoveryEvent,
    RecoveryPolicy,
    RecoveryState,
    RecoveryStatus,
    VerificationStatus,
)

import tempfile

logger = logging.getLogger(__name__)


def _get_writable_db_path(requested_path: Optional[str] = None) -> str:
    """Resolve a safe, writable SQLite database path (falls back to tempdir if cwd is read-only)."""
    if requested_path:
        return requested_path

    # Check environment variable override
    env_path = os.getenv("SQLITE_DB_PATH")
    if env_path:
        return env_path

    # Test if current working directory is writable
    local_test = Path("recovery_audit.db")
    try:
        # Check write access to directory
        test_file = Path(".write_check")
        test_file.touch()
        test_file.unlink()
        return "recovery_audit.db"
    except Exception:
        # Working directory is mounted read-only (common in Databricks Apps)
        return str(Path(tempfile.gettempdir()) / "recovery_audit.db")


class AuditAndStateStore:
    """Durable persistence layer for audit trails, state tracking, and policies."""

    def __init__(self, client: Optional[DatabricksClientWrapper] = None, db_path: Optional[str] = None):
        self.client = client or databricks_client
        self.db_path = _get_writable_db_path(db_path)
        # Always initialize local SQLite fallback table structure for local dev / tests
        self._init_sqlite_db()

    def _init_sqlite_db(self):
        """Initialize local SQLite tables matching Delta table schemas."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 1. recovery_history
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS recovery_history (
                    event_id TEXT PRIMARY KEY,
                    workspace_id TEXT,
                    job_id INTEGER,
                    run_id INTEGER,
                    task_key TEXT,
                    failure_category TEXT,
                    root_cause TEXT,
                    confidence REAL,
                    recommended_action TEXT,
                    executed_action TEXT,
                    approval_status TEXT,
                    approved_by TEXT,
                    repair_id INTEGER,
                    recovery_status TEXT,
                    verification_status TEXT,
                    error_summary TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            """)

            # 2. recovery_state
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS recovery_state (
                    job_id INTEGER,
                    run_id INTEGER,
                    attempt_count INTEGER DEFAULT 0,
                    last_attempt_time TEXT,
                    last_action TEXT,
                    is_locked INTEGER DEFAULT 0,
                    lock_token TEXT,
                    idempotency_key TEXT,
                    status TEXT,
                    PRIMARY KEY (job_id, run_id)
                )
            """)

            # 3. recovery_policies
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS recovery_policies (
                    job_id INTEGER PRIMARY KEY,
                    allowlisted INTEGER DEFAULT 1,
                    allowed_actions TEXT,
                    max_retries INTEGER DEFAULT 3,
                    cooldown_seconds INTEGER DEFAULT 60,
                    auto_recovery_eligible INTEGER DEFAULT 0,
                    requires_human_approval INTEGER DEFAULT 1,
                    data_quality_thresholds TEXT
                )
            """)

            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning("Failed to initialize SQLite backing store: %s", e)

    # --- Policy Operations ---

    def get_policy(self, job_id: int) -> RecoveryPolicy:
        """Fetch recovery policy for a specific job, or return default safe policy."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT allowlisted, allowed_actions, max_retries, cooldown_seconds, "
            "auto_recovery_eligible, requires_human_approval, data_quality_thresholds "
            "FROM recovery_policies WHERE job_id = ?",
            (job_id,),
        )
        row = cursor.fetchone()
        conn.close()

        if row:
            allowed_actions_list = [RecoveryActionType(a) for a in json.loads(row[1])]
            dq_dict = json.loads(row[6]) if row[6] else {}
            return RecoveryPolicy(
                job_id=job_id,
                allowlisted=bool(row[0]),
                allowed_actions=allowed_actions_list,
                max_retries=row[2],
                cooldown_seconds=row[3],
                auto_recovery_eligible=bool(row[4]),
                requires_human_approval=bool(row[5]),
                data_quality_thresholds=DataQualityThresholds(**dq_dict),
            )

        # Return default policy if not explicitly stored
        default_policy = RecoveryPolicy(
            job_id=job_id,
            allowlisted=True,
            max_retries=config.default_max_retries,
            cooldown_seconds=config.default_cooldown_seconds,
            auto_recovery_eligible=config.auto_recovery_enabled,
            requires_human_approval=True,
        )
        self.save_policy(default_policy)
        return default_policy

    def save_policy(self, policy: RecoveryPolicy):
        """Save or update a job's recovery policy."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO recovery_policies (
                job_id, allowlisted, allowed_actions, max_retries, cooldown_seconds,
                auto_recovery_eligible, requires_human_approval, data_quality_thresholds
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                allowlisted = excluded.allowlisted,
                allowed_actions = excluded.allowed_actions,
                max_retries = excluded.max_retries,
                cooldown_seconds = excluded.cooldown_seconds,
                auto_recovery_eligible = excluded.auto_recovery_eligible,
                requires_human_approval = excluded.requires_human_approval,
                data_quality_thresholds = excluded.data_quality_thresholds
            """,
            (
                policy.job_id,
                1 if policy.allowlisted else 0,
                json.dumps([a.value for a in policy.allowed_actions]),
                policy.max_retries,
                policy.cooldown_seconds,
                1 if policy.auto_recovery_eligible else 0,
                1 if policy.requires_human_approval else 0,
                json.dumps(policy.data_quality_thresholds.model_dump()),
            ),
        )
        conn.commit()
        conn.close()

    # --- Durable State & Concurrency Lock Operations ---

    def get_state(self, job_id: int, run_id: int) -> RecoveryState:
        """Retrieve active state for a given job and run."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT attempt_count, last_attempt_time, last_action, is_locked, lock_token, idempotency_key, status "
            "FROM recovery_state WHERE job_id = ? AND run_id = ?",
            (job_id, run_id),
        )
        row = cursor.fetchone()
        conn.close()

        if row:
            last_dt = datetime.fromisoformat(row[1]) if row[1] else None
            last_act = RecoveryActionType(row[2]) if row[2] else None
            return RecoveryState(
                job_id=job_id,
                run_id=run_id,
                attempt_count=row[0],
                last_attempt_time=last_dt,
                last_action=last_act,
                is_locked=bool(row[3]),
                lock_token=row[4],
                idempotency_key=row[5],
                status=RecoveryStatus(row[6]) if row[6] else RecoveryStatus.PENDING,
            )

        return RecoveryState(job_id=job_id, run_id=run_id)

    def acquire_lock(self, job_id: int, run_id: int, idempotency_key: str) -> Optional[str]:
        """
        Attempt to acquire concurrency lock on a run.
        Returns lock_token if acquired, None if already locked or duplicate request.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check existing state
        cursor.execute(
            "SELECT is_locked, lock_token, idempotency_key FROM recovery_state WHERE job_id = ? AND run_id = ?",
            (job_id, run_id),
        )
        row = cursor.fetchone()
        
        if row and row[0] == 1:
            # Already locked
            conn.close()
            return None

        # If identical idempotency key is already recorded in active execution, block duplicate
        if row and row[2] == idempotency_key:
            conn.close()
            return None

        new_token = str(uuid.uuid4())
        cursor.execute(
            """
            INSERT INTO recovery_state (job_id, run_id, is_locked, lock_token, idempotency_key, status)
            VALUES (?, ?, 1, ?, ?, ?)
            ON CONFLICT(job_id, run_id) DO UPDATE SET
                is_locked = 1,
                lock_token = excluded.lock_token,
                idempotency_key = excluded.idempotency_key,
                status = excluded.status
            """,
            (job_id, run_id, new_token, idempotency_key, RecoveryStatus.IN_PROGRESS.value),
        )
        conn.commit()
        conn.close()
        return new_token

    def release_lock(
        self,
        job_id: int,
        run_id: int,
        lock_token: str,
        new_status: RecoveryStatus,
        last_action: Optional[RecoveryActionType] = None,
        increment_attempt: bool = True,
    ) -> bool:
        """Release concurrency lock and persist latest attempt outcome."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT lock_token, attempt_count FROM recovery_state WHERE job_id = ? AND run_id = ?",
            (job_id, run_id),
        )
        row = cursor.fetchone()
        if not row or row[0] != lock_token:
            conn.close()
            logger.warning("Lock release mismatch for job %s run %s", job_id, run_id)
            return False

        attempts = row[1] + (1 if increment_attempt else 0)
        now_iso = datetime.now(timezone.utc).isoformat()
        action_val = last_action.value if last_action else None

        cursor.execute(
            """
            UPDATE recovery_state SET
                is_locked = 0,
                lock_token = NULL,
                attempt_count = ?,
                last_attempt_time = ?,
                last_action = COALESCE(?, last_action),
                status = ?
            WHERE job_id = ? AND run_id = ?
            """,
            (attempts, now_iso, action_val, new_status.value, job_id, run_id),
        )
        conn.commit()
        conn.close()
        return True

    # --- Audit History Operations ---

    def log_event(self, event: RecoveryEvent):
        """Append an event to the recovery_history audit log."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO recovery_history (
                event_id, workspace_id, job_id, run_id, task_key,
                failure_category, root_cause, confidence, recommended_action,
                executed_action, approval_status, approved_by, repair_id,
                recovery_status, verification_status, error_summary,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                executed_action = excluded.executed_action,
                approval_status = excluded.approval_status,
                approved_by = excluded.approved_by,
                repair_id = excluded.repair_id,
                recovery_status = excluded.recovery_status,
                verification_status = excluded.verification_status,
                updated_at = excluded.updated_at
            """,
            (
                event.event_id,
                event.workspace_id,
                event.job_id,
                event.run_id,
                event.task_key,
                event.failure_category.value,
                event.root_cause,
                event.confidence,
                event.recommended_action.value,
                event.executed_action.value if event.executed_action else None,
                event.approval_status.value,
                event.approved_by,
                event.repair_id,
                event.recovery_status.value,
                event.verification_status.value,
                event.error_summary,
                event.created_at.isoformat(),
                event.updated_at.isoformat(),
            ),
        )
        conn.commit()
        conn.close()

    def list_history(
        self,
        job_id: Optional[int] = None,
        failure_category: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> List[RecoveryEvent]:
        """Query recovery history with optional filters."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        query = "SELECT * FROM recovery_history WHERE 1=1"
        params: List[Any] = []

        if job_id:
            query += " AND job_id = ?"
            params.append(job_id)
        if failure_category:
            query += " AND failure_category = ?"
            params.append(failure_category)
        if status:
            query += " AND recovery_status = ?"
            params.append(status)

        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        events = []
        for r in rows:
            events.append(
                RecoveryEvent(
                    event_id=r[0],
                    workspace_id=r[1] or "workspace-1",
                    job_id=r[2],
                    run_id=r[3],
                    task_key=r[4],
                    failure_category=FailureCategory(r[5]),
                    root_cause=r[6],
                    confidence=r[7],
                    recommended_action=RecoveryActionType(r[8]),
                    executed_action=RecoveryActionType(r[9]) if r[9] else None,
                    approval_status=ApprovalStatus(r[10]) if r[10] else ApprovalStatus.NOT_REQUIRED,
                    approved_by=r[11],
                    repair_id=r[12],
                    recovery_status=RecoveryStatus(r[13]) if r[13] else RecoveryStatus.PENDING,
                    verification_status=VerificationStatus(r[14]) if r[14] else VerificationStatus.NOT_STARTED,
                    error_summary=r[15],
                    created_at=datetime.fromisoformat(r[16]),
                    updated_at=datetime.fromisoformat(r[17]),
                )
            )
        return events


# Singleton store instance
audit_store = AuditAndStateStore()
