"""
Seed Initial Policies Script for Databricks Self-Healing Pipeline Manager.
Configures baseline recovery budgets, allowlists, and approval policies for Lakeflow Jobs.
"""

import logging
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from audit import audit_store
from models import DataQualityThresholds, RecoveryActionType, RecoveryPolicy

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


DEFAULT_POLICIES = [
    RecoveryPolicy(
        job_id=101,
        allowlisted=True,
        allowed_actions=[
            RecoveryActionType.RETRY_RUN,
            RecoveryActionType.REPAIR_FAILED_TASKS,
            RecoveryActionType.CANCEL_RUN,
        ],
        max_retries=3,
        cooldown_seconds=60,
        auto_recovery_eligible=True,
        requires_human_approval=False,
        data_quality_thresholds=DataQualityThresholds(min_row_count=100, max_duplicate_ratio=0.01),
    ),
    RecoveryPolicy(
        job_id=102,
        allowlisted=True,
        allowed_actions=[
            RecoveryActionType.REPAIR_FAILED_TASKS,
            RecoveryActionType.WAIT_FOR_DEPENDENCY,
            RecoveryActionType.ESCALATE_TO_HUMAN,
        ],
        max_retries=2,
        cooldown_seconds=120,
        auto_recovery_eligible=False,
        requires_human_approval=True,
        data_quality_thresholds=DataQualityThresholds(min_row_count=500, max_duplicate_ratio=0.005),
    ),
    RecoveryPolicy(
        job_id=103,
        allowlisted=True,
        allowed_actions=[
            RecoveryActionType.REPAIR_FAILED_TASKS,
            RecoveryActionType.RECOMMEND_COMPUTE_ADJUSTMENT,
            RecoveryActionType.TRIGGER_VALIDATION,
            RecoveryActionType.ESCALATE_TO_HUMAN,
        ],
        max_retries=2,
        cooldown_seconds=180,
        auto_recovery_eligible=False,
        requires_human_approval=True,
        data_quality_thresholds=DataQualityThresholds(min_row_count=1000, max_duplicate_ratio=0.001),
    ),
    RecoveryPolicy(
        job_id=104,
        allowlisted=True,
        allowed_actions=[
            RecoveryActionType.ESCALATE_TO_HUMAN,
        ],
        max_retries=1,
        cooldown_seconds=300,
        auto_recovery_eligible=False,
        requires_human_approval=True,
        data_quality_thresholds=DataQualityThresholds(min_row_count=50, max_duplicate_ratio=0.0),
    ),
]


def seed_policies():
    logger.info("Seeding default recovery policies...")
    for policy in DEFAULT_POLICIES:
        audit_store.save_policy(policy)
        logger.info(
            "Seeded policy for Job %s: max_retries=%s, auto_eligible=%s, requires_approval=%s",
            policy.job_id,
            policy.max_retries,
            policy.auto_recovery_eligible,
            policy.requires_human_approval,
        )
    logger.info("Successfully seeded all %d policies.", len(DEFAULT_POLICIES))
    return True


if __name__ == "__main__":
    success = seed_policies()
    sys.exit(0 if success else 1)
