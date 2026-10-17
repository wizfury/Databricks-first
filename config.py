"""
Configuration module for Databricks Self-Healing Pipeline Manager.
Loads settings from environment variables and provides sensible defaults.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class AppConfig:
    """Application configuration parameters."""
    app_name: str = os.getenv("DATABRICKS_APP_NAME", "ai-self-healing-pipeline")
    
    # Unity Catalog Configuration
    catalog: str = os.getenv("DEFAULT_CATALOG", "main")
    schema: str = os.getenv("DEFAULT_SCHEMA", "ops")
    sql_warehouse_id: str = os.getenv("SQL_WAREHOUSE_ID", "")
    
    # AI / Model Serving Configuration
    model_serving_endpoint: str = os.getenv(
        "MODEL_SERVING_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"
    )
    
    # Operational Safety Settings
    mock_mode: bool = os.getenv("MOCK_MODE", "false").lower() in ("true", "1", "yes")
    auto_recovery_enabled: bool = os.getenv("AUTO_RECOVERY_ENABLED", "false").lower() in ("true", "1", "yes")
    default_max_retries: int = int(os.getenv("DEFAULT_MAX_RETRIES", "3"))
    default_cooldown_seconds: int = int(os.getenv("DEFAULT_COOLDOWN_SECONDS", "60"))
    poll_interval_seconds: int = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
    
    # Table naming helpers
    @property
    def history_table(self) -> str:
        return f"{self.catalog}.{self.schema}.recovery_history"

    @property
    def state_table(self) -> str:
        return f"{self.catalog}.{self.schema}.recovery_state"

    @property
    def policies_table(self) -> str:
        return f"{self.catalog}.{self.schema}.recovery_policies"


# Global singleton configuration
config = AppConfig()
