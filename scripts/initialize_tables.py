"""
Table Initialization Script for Databricks Self-Healing Pipeline Manager.
Safely creates Delta Lake tables in Unity Catalog using idempotent DDL.
Also initializes local SQLite backing store for offline testing.
"""

import logging
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import config
from databricks_client import databricks_client
from audit import audit_store

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


DDL_STATEMENTS = [
    # 1. Ensure Schema exists
    f"CREATE SCHEMA IF NOT EXISTS {config.catalog}.{config.schema} COMMENT 'Ops schema for AI self-healing pipeline manager';",

    # 2. recovery_history Delta table
    f"""
    CREATE TABLE IF NOT EXISTS {config.history_table} (
        event_id STRING NOT NULL,
        workspace_id STRING,
        job_id BIGINT NOT NULL,
        run_id BIGINT NOT NULL,
        task_key STRING,
        failure_category STRING NOT NULL,
        root_cause STRING,
        confidence DOUBLE,
        recommended_action STRING NOT NULL,
        executed_action STRING,
        approval_status STRING,
        approved_by STRING,
        repair_id BIGINT,
        recovery_status STRING NOT NULL,
        verification_status STRING,
        error_summary STRING,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Audit log recording all failure diagnoses, approvals, recovery attempts, and verification results'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
    """,

    # 3. recovery_state Delta table
    f"""
    CREATE TABLE IF NOT EXISTS {config.state_table} (
        job_id BIGINT NOT NULL,
        run_id BIGINT NOT NULL,
        attempt_count INT DEFAULT 0,
        last_attempt_time TIMESTAMP,
        last_action STRING,
        is_locked BOOLEAN DEFAULT FALSE,
        lock_token STRING,
        idempotency_key STRING,
        status STRING
    )
    USING DELTA
    COMMENT 'Durable recovery state tracking attempt budgets, cooldowns, and concurrency locks';
    """,

    # 4. recovery_policies Delta table
    f"""
    CREATE TABLE IF NOT EXISTS {config.policies_table} (
        job_id BIGINT NOT NULL,
        allowlisted BOOLEAN DEFAULT TRUE,
        allowed_actions STRING,
        max_retries INT DEFAULT 3,
        cooldown_seconds INT DEFAULT 60,
        auto_recovery_eligible BOOLEAN DEFAULT FALSE,
        requires_human_approval BOOLEAN DEFAULT TRUE,
        data_quality_thresholds STRING
    )
    USING DELTA
    COMMENT 'Configurable safety policies and recovery budgets per Lakeflow Job';
    """,
]


def initialize_tables():
    logger.info("Initializing tables in Unity Catalog (%s.%s)...", config.catalog, config.schema)

    if databricks_client.is_mock or not config.sql_warehouse_id:
        logger.info(
            "SQL Warehouse ID not set or running in mock mode. "
            "Initializing local durable SQLite store at '%s'...",
            audit_store.db_path,
        )
        audit_store._init_sqlite_db()
        logger.info("Local SQLite store initialized successfully.")
        return True

    try:
        for stmt in DDL_STATEMENTS:
            logger.info("Executing DDL statement:\n%s", stmt.strip()[:100] + "...")
            databricks_client.execute_sql(stmt)
        logger.info("All Unity Catalog Delta tables verified and ready.")
        return True
    except Exception as e:
        logger.error("Failed to initialize Unity Catalog tables: %s", e)
        return False


if __name__ == "__main__":
    success = initialize_tables()
    sys.exit(0 if success else 1)
