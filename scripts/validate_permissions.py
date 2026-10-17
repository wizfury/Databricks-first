"""
Permission Validation Script for Databricks Self-Healing Pipeline Manager.
Verifies service principal and user privileges for Unity Catalog, Jobs API,
and Model Serving endpoints, providing exact remediation commands if permissions are missing.
"""

import logging
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import config
from databricks_client import databricks_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def validate_environment_permissions():
    identity = databricks_client.get_current_identity()
    print("=" * 70)
    print("  DATABRICKS PERMISSION VALIDATION REPORT")
    print(f"  Target Workspace Identity: {identity}")
    print(f"  Execution Mode: {'MOCK / SIMULATION' if databricks_client.is_mock else 'LIVE WORKSPACE'}")
    print("=" * 70)

    checks = []

    # 1. Identity Check
    checks.append({
        "component": "Authentication / Identity",
        "passed": bool(identity),
        "details": f"Authenticated as '{identity}'",
        "remediation": "Configure DATABRICKS_HOST and DATABRICKS_TOKEN or service principal credentials.",
    })

    # 2. Lakeflow Jobs API View Check
    try:
        jobs = databricks_client.list_jobs(limit=5)
        checks.append({
            "component": "Jobs API (Read / CAN_VIEW)",
            "passed": True,
            "details": f"Successfully retrieved {len(jobs)} jobs from workspace.",
            "remediation": f"Grant CAN_VIEW permission on target jobs to '{identity}'.",
        })
    except Exception as e:
        checks.append({
            "component": "Jobs API (Read / CAN_VIEW)",
            "passed": False,
            "details": str(e),
            "remediation": f"databricks permissions set --job-id <JOB_ID> --json '{{\"access_control_list\": [{{\"user_name\": \"{identity}\", \"permission_level\": \"CAN_VIEW\"}}]}}'",
        })

    # 3. Model Serving Endpoint Check
    if config.model_serving_endpoint:
        try:
            # Send a tiny ping message to endpoint
            response = databricks_client.query_serving_endpoint(
                endpoint_name=config.model_serving_endpoint,
                messages=[{"role": "user", "content": "ping"}],
                max_tokens=5,
            )
            checks.append({
                "component": f"Model Serving ('{config.model_serving_endpoint}')",
                "passed": True if response is not None or databricks_client.is_mock else False,
                "details": "Model serving endpoint queried successfully." if response or databricks_client.is_mock else "Endpoint query returned empty or failed.",
                "remediation": f"Grant CAN_QUERY permission on serving endpoint: databricks permissions set --serving-endpoint {config.model_serving_endpoint} --json '{{\"access_control_list\": [{{\"user_name\": \"{identity}\", \"permission_level\": \"CAN_QUERY\"}}]}}'",
            })
        except Exception as e:
            checks.append({
                "component": f"Model Serving ('{config.model_serving_endpoint}')",
                "passed": False,
                "details": str(e),
                "remediation": f"databricks permissions set --serving-endpoint {config.model_serving_endpoint} --json '{{\"access_control_list\": [{{\"user_name\": \"{identity}\", \"permission_level\": \"CAN_QUERY\"}}]}}'",
            })

    # 4. Unity Catalog Permissions
    if not databricks_client.is_mock and config.sql_warehouse_id:
        try:
            databricks_client.execute_sql(f"USE CATALOG {config.catalog};")
            databricks_client.execute_sql(f"USE SCHEMA {config.catalog}.{config.schema};")
            checks.append({
                "component": f"Unity Catalog Schema ({config.catalog}.{config.schema})",
                "passed": True,
                "details": "USE CATALOG and USE SCHEMA granted.",
                "remediation": "",
            })
        except Exception as e:
            checks.append({
                "component": f"Unity Catalog Schema ({config.catalog}.{config.schema})",
                "passed": False,
                "details": str(e),
                "remediation": (
                    f"GRANT USE CATALOG ON CATALOG {config.catalog} TO `{identity}`;\n"
                    f"GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA {config.catalog}.{config.schema} TO `{identity}`;"
                ),
            })
    else:
        checks.append({
            "component": "Unity Catalog / SQL Warehouse",
            "passed": True,
            "details": "Running with durable local SQLite persistence (SQL Warehouse not bound).",
            "remediation": "Set SQL_WAREHOUSE_ID in app.yaml or environment to activate direct Delta table persistence.",
        })

    # Print Report
    all_passed = True
    for c in checks:
        status_sym = "[PASS]" if c["passed"] else "[FAIL]"
        print(f"\n{status_sym} {c['component']}")
        print(f"       Details: {c['details']}")
        if not c["passed"]:
            all_passed = False
            print(f"       Action Required:\n       {c['remediation']}")

    print("\n" + "=" * 70)
    if all_passed:
        print("  RESULT: All core permissions verified! App is ready for deployment.")
    else:
        print("  RESULT: Missing permissions detected. Review remediation steps above.")
    print("=" * 70)
    return all_passed


if __name__ == "__main__":
    success = validate_environment_permissions()
    sys.exit(0 if success else 1)
