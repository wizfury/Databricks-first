"""
Domain models and schema definitions for the Databricks Self-Healing Pipeline Manager.
Uses Pydantic V2 for strict runtime validation.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class FailureCategory(str, Enum):
    """Enterprise failure categories as specified in system objectives."""
    TRANSIENT_INFRASTRUCTURE = "TRANSIENT_INFRASTRUCTURE"
    MISSING_OR_EMPTY_SOURCE = "MISSING_OR_EMPTY_SOURCE"
    SCHEMA_COMPATIBILITY = "SCHEMA_COMPATIBILITY"
    COMPUTE_OR_MEMORY = "COMPUTE_OR_MEMORY"
    UPSTREAM_DEPENDENCY = "UPSTREAM_DEPENDENCY"
    DATA_QUALITY_VIOLATION = "DATA_QUALITY_VIOLATION"
    AUTHENTICATION_OR_PERMISSION = "AUTHENTICATION_OR_PERMISSION"
    CODE_OR_TRANSFORMATION = "CODE_OR_TRANSFORMATION"
    INTERRUPTED_WRITE_OR_DUPLICATE = "INTERRUPTED_WRITE_OR_DUPLICATE"
    SILENT_FAILURE = "SILENT_FAILURE"
    UNKNOWN = "UNKNOWN"


class RecoveryActionType(str, Enum):
    """Controlled allowlist of recovery operations."""
    RETRY_RUN = "RETRY_RUN"
    REPAIR_FAILED_TASKS = "REPAIR_FAILED_TASKS"
    WAIT_FOR_DEPENDENCY = "WAIT_FOR_DEPENDENCY"
    TRIGGER_VALIDATION = "TRIGGER_VALIDATION"
    CANCEL_RUN = "CANCEL_RUN"
    RECOMMEND_COMPUTE_ADJUSTMENT = "RECOMMEND_COMPUTE_ADJUSTMENT"
    ESCALATE_TO_HUMAN = "ESCALATE_TO_HUMAN"


class ApprovalStatus(str, Enum):
    """Approval lifecycle states."""
    NOT_REQUIRED = "NOT_REQUIRED"
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class RecoveryStatus(str, Enum):
    """Execution status of a recovery attempt."""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    SKIPPED = "SKIPPED"


class VerificationStatus(str, Enum):
    """Status of post-recovery data and pipeline verification."""
    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    PASSED = "PASSED"
    FAILED = "FAILED"


# --- Pipeline Metadata Models ---

class TaskRunInfo(BaseModel):
    """Details for a specific task run within a Lakeflow job."""
    task_key: str
    run_id: int
    task_run_id: Optional[int] = None
    state: str = "PENDING"  # e.g., SUCCESS, FAILED, RUNNING
    result_state: Optional[str] = None
    error_message: Optional[str] = None
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    execution_duration_ms: Optional[int] = None
    attempt_number: int = 1
    depends_on: List[str] = Field(default_factory=list)


class JobRunInfo(BaseModel):
    """High-level summary of a Lakeflow Job run."""
    job_id: int
    run_id: int
    run_name: str
    state: str  # TERMINATED, RUNNING, PENDING
    result_state: Optional[str] = None  # SUCCESS, FAILED, TIMEDOUT, CANCELED
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    execution_duration_ms: Optional[int] = None
    tasks: List[TaskRunInfo] = Field(default_factory=list)
    error_summary: Optional[str] = None
    trigger: str = "PERIODIC"
    run_page_url: Optional[str] = None


# --- Diagnosis & AI Models ---

class DiagnosisResult(BaseModel):
    """Structured output from failure diagnosis engine."""
    failure_category: FailureCategory
    root_cause: str
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence estimate between 0.0 and 1.0")
    recommended_action: RecoveryActionType
    requires_approval: bool = True
    evidence: List[str] = Field(default_factory=list)
    reasoning_summary: str
    missing_information: List[str] = Field(default_factory=list)
    potential_risks: List[str] = Field(default_factory=list)
    is_ai_generated: bool = False


# --- Policy & State Storage Models ---

class DataQualityThresholds(BaseModel):
    """Configurable data-quality boundaries for recovery verification."""
    min_row_count: int = 1
    max_duplicate_ratio: float = 0.01
    max_null_ratio_primary_keys: float = 0.0
    max_staleness_minutes: int = 120


class RecoveryPolicy(BaseModel):
    """Policy rules governing automated or manual recovery for a job."""
    job_id: int
    allowlisted: bool = True
    allowed_actions: List[RecoveryActionType] = Field(
        default_factory=lambda: [
            RecoveryActionType.REPAIR_FAILED_TASKS,
            RecoveryActionType.RETRY_RUN,
            RecoveryActionType.ESCALATE_TO_HUMAN,
        ]
    )
    max_retries: int = 3
    cooldown_seconds: int = 60
    auto_recovery_eligible: bool = False
    requires_human_approval: bool = True
    data_quality_thresholds: DataQualityThresholds = Field(default_factory=DataQualityThresholds)


class RecoveryState(BaseModel):
    """Durable state tracking recovery attempts, cooldowns, and locks."""
    job_id: int
    run_id: int
    attempt_count: int = 0
    last_attempt_time: Optional[datetime] = None
    last_action: Optional[RecoveryActionType] = None
    is_locked: bool = False
    lock_token: Optional[str] = None
    idempotency_key: Optional[str] = None
    status: RecoveryStatus = RecoveryStatus.PENDING


class RecoveryEvent(BaseModel):
    """Audit log entry for diagnosis and recovery actions."""
    event_id: str
    workspace_id: str
    job_id: int
    run_id: int
    task_key: Optional[str] = None
    failure_category: FailureCategory
    root_cause: str
    confidence: float
    recommended_action: RecoveryActionType
    executed_action: Optional[RecoveryActionType] = None
    approval_status: ApprovalStatus = ApprovalStatus.NOT_REQUIRED
    approved_by: Optional[str] = None
    repair_id: Optional[int] = None
    recovery_status: RecoveryStatus = RecoveryStatus.PENDING
    verification_status: VerificationStatus = VerificationStatus.NOT_STARTED
    error_summary: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# --- Verification Models ---

class VerificationCheck(BaseModel):
    """Specific assertion evaluated during verification."""
    name: str
    passed: bool
    details: str
    expected_value: Optional[str] = None
    actual_value: Optional[str] = None


class VerificationResult(BaseModel):
    """Comprehensive recovery verification assessment."""
    passed: bool
    checks: List[VerificationCheck] = Field(default_factory=list)
    failure_reason: Optional[str] = None
    verified_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
