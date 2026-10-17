"""
Databricks Client abstraction layer.
Handles WorkspaceClient lifecycle, authentication in Databricks Apps,
and provides a high-fidelity mock implementation for local offline development.
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from config import config
from models import JobRunInfo, TaskRunInfo

logger = logging.getLogger(__name__)

# Try importing Databricks SDK
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import Run, RunOutput
    from databricks.sdk.service.serving import ChatMessage
    HAS_DATABRICKS_SDK = True
except ImportError:
    HAS_DATABRICKS_SDK = False


class DatabricksClientWrapper:
    """Resilient wrapper for Databricks SDK operations."""

    def __init__(self, force_mock: Optional[bool] = None):
        self._mock_mode = (
            force_mock
            if force_mock is not None
            else (config.mock_mode or not self._has_valid_credentials())
        )
        self._client: Optional[Any] = None
        
        if not self._mock_mode and HAS_DATABRICKS_SDK:
            try:
                # In Databricks Apps, WorkspaceClient() automatically picks up
                # the managed identity / service principal injected by the runtime.
                self._client = WorkspaceClient()
                logger.info("Initialized live Databricks WorkspaceClient.")
            except Exception as e:
                logger.warning(
                    "Failed to initialize live Databricks WorkspaceClient: %s. Falling back to Mock mode.",
                    e,
                )
                self._mock_mode = True
        else:
            self._mock_mode = True

        if self._mock_mode:
            logger.info("DatabricksClientWrapper running in MOCK / SIMULATION mode.")
            self._init_mock_data()

    def _has_valid_credentials(self) -> bool:
        """Check if environment contains Databricks host, auth details, or is running inside Databricks Apps."""
        if os.getenv("DATABRICKS_APP_PORT") or os.getenv("DATABRICKS_HOST"):
            return True
        return bool(
            os.getenv("DATABRICKS_TOKEN")
            or (os.getenv("DATABRICKS_CLIENT_ID") and os.getenv("DATABRICKS_CLIENT_SECRET"))
        )

    @property
    def is_mock(self) -> bool:
        return self._mock_mode

    def get_current_identity(self) -> str:
        """Retrieve the identity of the current user or service principal."""
        if self._mock_mode or not self._client:
            return "app-service-principal-sp-mock@databricks.internal"
        try:
            user = self._client.current_user.me()
            return user.user_name or "unknown-service-principal"
        except Exception as e:
            logger.error("Error retrieving current user identity: %s", e)
            return "service-principal-unverified"

    # --- Lakeflow Jobs API Operations ---

    def list_jobs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """List configured jobs in the workspace."""
        if self._mock_mode or not self._client:
            return [
                {"job_id": j["job_id"], "name": j["name"], "creator": j.get("creator", "data-team")}
                for j in self._mock_jobs
            ]
        try:
            jobs = list(self._client.jobs.list(limit=limit))
            return [
                {
                    "job_id": j.job_id,
                    "name": j.settings.name if j.settings else f"Job {j.job_id}",
                    "creator": j.creator_user_name or "unknown",
                }
                for j in jobs
            ]
        except Exception as e:
            logger.error("Failed to list Databricks jobs: %s", e)
            return []

    def list_runs(self, job_id: int, limit: int = 20) -> List[JobRunInfo]:
        """List recent runs for a given job."""
        if self._mock_mode or not self._client:
            runs = [r for r in self._mock_runs if r.job_id == job_id]
            return sorted(runs, key=lambda x: x.run_id, reverse=True)[:limit]

        try:
            raw_runs = list(self._client.jobs.list_runs(job_id=job_id, limit=limit))
            results = []
            for r in raw_runs:
                tasks = []
                if r.tasks:
                    for t in r.tasks:
                        state_str = t.state.life_cycle_state.value if t.state and t.state.life_cycle_state else "UNKNOWN"
                        res_str = t.state.result_state.value if t.state and t.state.result_state else None
                        tasks.append(
                            TaskRunInfo(
                                task_key=t.task_key,
                                run_id=t.run_id or r.run_id,
                                state=state_str,
                                result_state=res_str,
                                error_message=t.state.state_message if t.state else None,
                                start_time=t.start_time,
                                end_time=t.end_time,
                                execution_duration_ms=t.execution_duration,
                                attempt_number=t.attempt_number or 1,
                            )
                        )
                
                state_str = r.state.life_cycle_state.value if r.state and r.state.life_cycle_state else "UNKNOWN"
                res_str = r.state.result_state.value if r.state and r.state.result_state else None
                err_summary = r.state.state_message if r.state else None

                results.append(
                    JobRunInfo(
                        job_id=job_id,
                        run_id=r.run_id,
                        run_name=r.run_name or f"Run {r.run_id}",
                        state=state_str,
                        result_state=res_str,
                        start_time=r.start_time,
                        end_time=r.end_time,
                        execution_duration_ms=r.execution_duration,
                        tasks=tasks,
                        error_summary=err_summary,
                        run_page_url=r.run_page_url,
                    )
                )
            return results
        except Exception as e:
            logger.error("Failed to list runs for job %s: %s", job_id, e)
            return []

    def get_run(self, run_id: int) -> Optional[JobRunInfo]:
        """Retrieve details for a specific run."""
        if self._mock_mode or not self._client:
            for r in self._mock_runs:
                if r.run_id == run_id:
                    return r
            return None

        try:
            r = self._client.jobs.get_run(run_id=run_id)
            tasks = []
            if r.tasks:
                for t in r.tasks:
                    state_str = t.state.life_cycle_state.value if t.state and t.state.life_cycle_state else "UNKNOWN"
                    res_str = t.state.result_state.value if t.state and t.state.result_state else None
                    tasks.append(
                        TaskRunInfo(
                            task_key=t.task_key,
                            run_id=t.run_id or r.run_id,
                            state=state_str,
                            result_state=res_str,
                            error_message=t.state.state_message if t.state else None,
                            start_time=t.start_time,
                            end_time=t.end_time,
                            execution_duration_ms=t.execution_duration,
                            attempt_number=t.attempt_number or 1,
                        )
                    )
            
            state_str = r.state.life_cycle_state.value if r.state and r.state.life_cycle_state else "UNKNOWN"
            res_str = r.state.result_state.value if r.state and r.state.result_state else None
            return JobRunInfo(
                job_id=r.job_id,
                run_id=r.run_id,
                run_name=r.run_name or f"Run {r.run_id}",
                state=state_str,
                result_state=res_str,
                start_time=r.start_time,
                end_time=r.end_time,
                execution_duration_ms=r.execution_duration,
                tasks=tasks,
                error_summary=r.state.state_message if r.state else None,
                run_page_url=r.run_page_url,
            )
        except Exception as e:
            logger.error("Failed to get run %s: %s", run_id, e)
            return None

    def get_run_output(self, run_id: int) -> Dict[str, Any]:
        """Fetch logs and task output for a run or task run."""
        if self._mock_mode or not self._client:
            for r in self._mock_runs:
                if r.run_id == run_id:
                    return {
                        "error": r.error_summary or "",
                        "error_trace": r.error_summary or "",
                        "logs": f"Task execution output for run {run_id}",
                    }
            return {"error": "", "logs": ""}

        try:
            output = self._client.jobs.get_run_output(run_id=run_id)
            return {
                "error": output.error or "",
                "error_trace": output.error_trace or "",
                "logs": output.logs or "",
            }
        except Exception as e:
            logger.error("Failed to fetch run output for %s: %s", run_id, e)
            return {"error": str(e), "logs": ""}

    def repair_run(
        self,
        run_id: int,
        rerun_tasks: Optional[List[str]] = None,
        rerun_all_failed_tasks: bool = True,
    ) -> Dict[str, Any]:
        """Execute repair on failed tasks in a run."""
        if self._mock_mode or not self._client:
            repair_id = int(time.time() * 1000) % 1000000
            logger.info("Mock repair executed for run %s. repair_id=%s", run_id, repair_id)
            # Update mock run status to reflect healing
            for r in self._mock_runs:
                if r.run_id == run_id:
                    r.state = "RUNNING"
                    r.result_state = None
                    for t in r.tasks:
                        if rerun_tasks and t.task_key in rerun_tasks:
                            t.state = "RUNNING"
                            t.result_state = None
                        elif rerun_all_failed_tasks and t.result_state == "FAILED":
                            t.state = "RUNNING"
                            t.result_state = None
            return {"repair_id": repair_id, "status": "SUBMITTED"}

        try:
            waiter = self._client.jobs.repair_run(
                run_id=run_id,
                rerun_tasks=rerun_tasks,
                rerun_all_failed_tasks=rerun_all_failed_tasks if not rerun_tasks else None,
            )
            repair_id = getattr(waiter.response, "repair_id", None)
            return {"repair_id": repair_id, "status": "SUBMITTED"}
        except Exception as e:
            logger.error("Failed to repair run %s: %s", run_id, e)
            raise

    def run_now(self, job_id: int, parameters: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Trigger a new run of a job."""
        if self._mock_mode or not self._client:
            new_run_id = int(time.time() * 1000) % 1000000
            new_run = JobRunInfo(
                job_id=job_id,
                run_id=new_run_id,
                run_name=f"Retry-Run-{new_run_id}",
                state="RUNNING",
                result_state=None,
                start_time=int(time.time() * 1000),
                tasks=[
                    TaskRunInfo(
                        task_key="main_task",
                        run_id=new_run_id,
                        state="RUNNING",
                        result_state=None,
                    )
                ],
            )
            self._mock_runs.insert(0, new_run)
            return {"run_id": new_run_id, "status": "TRIGGERED"}

        try:
            waiter = self._client.jobs.run_now(
                job_id=job_id,
                job_parameters=parameters,
            )
            run_id = getattr(waiter.response, "run_id", None)
            return {"run_id": run_id, "status": "TRIGGERED"}
        except Exception as e:
            logger.error("Failed to trigger job %s: %s", job_id, e)
            raise

    def cancel_run(self, run_id: int) -> bool:
        """Cancel a running job run."""
        if self._mock_mode or not self._client:
            for r in self._mock_runs:
                if r.run_id == run_id:
                    r.state = "TERMINATED"
                    r.result_state = "CANCELED"
                    return True
            return False

        try:
            self._client.jobs.cancel_run(run_id=run_id)
            return True
        except Exception as e:
            logger.error("Failed to cancel run %s: %s", run_id, e)
            return False

    # --- Model Serving API Operations ---

    def query_serving_endpoint(
        self, endpoint_name: str, messages: List[Dict[str, str]], max_tokens: int = 800
    ) -> Optional[str]:
        """Query a Databricks Model Serving endpoint with chat messages."""
        if self._mock_mode or not self._client:
            return None

        try:
            sdk_messages = [
                ChatMessage(role=m["role"], content=m["content"]) for m in messages
            ]
            response = self._client.serving_endpoints.query(
                name=endpoint_name,
                messages=sdk_messages,
                max_tokens=max_tokens,
                temperature=0.1,
            )
            # Inspect return choices
            if hasattr(response, "choices") and response.choices:
                first_choice = response.choices[0]
                if hasattr(first_choice, "message"):
                    return first_choice.message.content
            return None
        except Exception as e:
            logger.warning("Databricks Model Serving query failed on endpoint '%s': %s", endpoint_name, e)
            return None

    # --- SQL Statement Execution API Operations ---

    def execute_sql(self, statement: str, warehouse_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Execute SQL statement against Databricks SQL Warehouse."""
        wh_id = warehouse_id or config.sql_warehouse_id
        if self._mock_mode or not self._client or not wh_id:
            logger.debug("execute_sql bypassed in mock mode or warehouse not configured.")
            return []

        try:
            response = self._client.statement_execution.execute_statement(
                statement=statement,
                warehouse_id=wh_id,
                catalog=config.catalog,
                schema=config.schema,
            )
            # Format rows
            rows = []
            if response.result and response.result.data_array and response.manifest:
                cols = [col.name for col in response.manifest.schema.columns]
                for raw_row in response.result.data_array:
                    rows.append(dict(zip(cols, raw_row)))
            return rows
        except Exception as e:
            logger.error("SQL execution error: %s", e)
            raise

    # --- High-Fidelity Mock Fixtures ---

    def _init_mock_data(self):
        """Pre-populate realistic mock jobs and runs illustrating all 10 failure categories."""
        now_ms = int(time.time() * 1000)
        
        self._mock_jobs = [
            {"job_id": 101, "name": "Bronze Ingestion - IoT Telemetry Stream", "creator": "lakeflow-ingest@company.com"},
            {"job_id": 102, "name": "Silver ETL - Financial Transactions Cleaning", "creator": "fin-data-eng@company.com"},
            {"job_id": 103, "name": "Gold Reporting - Executive Daily KPI Mart", "creator": "bi-analytics@company.com"},
            {"job_id": 104, "name": "Customer 360 Dimension Sync", "creator": "crm-integrations@company.com"},
        ]

        self._mock_runs = [
            # 1. Transient infrastructure failure
            JobRunInfo(
                job_id=101,
                run_id=5001,
                run_name="Bronze Ingestion - IoT Telemetry Stream [Run 5001]",
                state="TERMINATED",
                result_state="FAILED",
                start_time=now_ms - 3600000,
                end_time=now_ms - 3300000,
                execution_duration_ms=300000,
                tasks=[
                    TaskRunInfo(
                        task_key="read_s3_stream",
                        run_id=5001,
                        state="TERMINATED",
                        result_state="FAILED",
                        error_message="HTTPConnectionPool(host='s3.us-west-2.amazonaws.com', port=443): Read timed out. (read timeout=60). Connection reset by peer.",
                        start_time=now_ms - 3600000,
                        end_time=now_ms - 3300000,
                        execution_duration_ms=300000,
                    ),
                    TaskRunInfo(
                        task_key="write_bronze_delta",
                        run_id=5001,
                        state="TERMINATED",
                        result_state="UPSTREAM_FAILED",
                        error_message="Upstream task 'read_s3_stream' failed.",
                        depends_on=["read_s3_stream"],
                    ),
                ],
                error_summary="Read timed out (read timeout=60). Connection reset by peer.",
                run_page_url="https://workspace.databricks.com/#job/101/run/5001",
            ),
            # 2. Missing source file
            JobRunInfo(
                job_id=102,
                run_id=5002,
                run_name="Silver ETL - Financial Transactions Cleaning [Run 5002]",
                state="TERMINATED",
                result_state="FAILED",
                start_time=now_ms - 7200000,
                end_time=now_ms - 7100000,
                execution_duration_ms=100000,
                tasks=[
                    TaskRunInfo(
                        task_key="validate_input_manifest",
                        run_id=5002,
                        state="TERMINATED",
                        result_state="FAILED",
                        error_message="FileNotFoundException: Path does not exist: abfss://raw@datalake.dfs.core.windows.net/daily_tx/2026-10-17/manifest.json",
                    )
                ],
                error_summary="FileNotFoundException: Path does not exist: manifest.json",
                run_page_url="https://workspace.databricks.com/#job/102/run/5002",
            ),
            # 3. Schema evolution error
            JobRunInfo(
                job_id=102,
                run_id=5003,
                run_name="Silver ETL - Financial Transactions Cleaning [Run 5003]",
                state="TERMINATED",
                result_state="FAILED",
                start_time=now_ms - 10800000,
                end_time=now_ms - 10600000,
                execution_duration_ms=200000,
                tasks=[
                    TaskRunInfo(
                        task_key="merge_into_silver",
                        run_id=5003,
                        state="TERMINATED",
                        result_state="FAILED",
                        error_message="org.apache.spark.sql.AnalysisException: A schema mismatch detected when writing to the Delta table. Source column 'merchant_tax_id' (StringType) does not exist in target Delta table schema.",
                    )
                ],
                error_summary="AnalysisException: A schema mismatch detected when writing to the Delta table.",
                run_page_url="https://workspace.databricks.com/#job/102/run/5003",
            ),
            # 4. Executor OOM / Compute failure
            JobRunInfo(
                job_id=103,
                run_id=5004,
                run_name="Gold Reporting - Executive Daily KPI Mart [Run 5004]",
                state="TERMINATED",
                result_state="FAILED",
                start_time=now_ms - 14400000,
                end_time=now_ms - 13800000,
                execution_duration_ms=600000,
                tasks=[
                    TaskRunInfo(
                        task_key="aggregate_kpis",
                        run_id=5004,
                        state="TERMINATED",
                        result_state="FAILED",
                        error_message="SparkException: Job aborted due to stage failure: Task 14 in stage 3.0 failed 4 times: ExecutorLostFailure (executor 2 exited caused by java.lang.OutOfMemoryError: Java heap space due to data skew on partition key).",
                    )
                ],
                error_summary="java.lang.OutOfMemoryError: Java heap space. ExecutorLostFailure.",
                run_page_url="https://workspace.databricks.com/#job/103/run/5004",
            ),
            # 5. Unity Catalog Permission error
            JobRunInfo(
                job_id=104,
                run_id=5005,
                run_name="Customer 360 Dimension Sync [Run 5005]",
                state="TERMINATED",
                result_state="FAILED",
                start_time=now_ms - 18000000,
                end_time=now_ms - 17950000,
                execution_duration_ms=50000,
                tasks=[
                    TaskRunInfo(
                        task_key="sync_salesforce_contacts",
                        run_id=5005,
                        state="TERMINATED",
                        result_state="FAILED",
                        error_message="[PERMISSION_DENIED] User 'app-service-principal' does not have USE SCHEMA privilege on Schema 'main.crm'. Unity Catalog authorization failure.",
                    )
                ],
                error_summary="[PERMISSION_DENIED] User does not have USE SCHEMA privilege on Schema 'main.crm'.",
                run_page_url="https://workspace.databricks.com/#job/104/run/5005",
            ),
            # 6. Data Quality Violation (silent or caught by assertion)
            JobRunInfo(
                job_id=103,
                run_id=5006,
                run_name="Gold Reporting - Executive Daily KPI Mart [Run 5006]",
                state="TERMINATED",
                result_state="FAILED",
                start_time=now_ms - 21600000,
                end_time=now_ms - 21400000,
                execution_duration_ms=200000,
                tasks=[
                    TaskRunInfo(
                        task_key="quality_gate",
                        run_id=5006,
                        state="TERMINATED",
                        result_state="FAILED",
                        error_message="DataQualityException: Assertion failed for table 'main.ops.daily_revenue': 1,420 duplicate records detected on primary key 'transaction_id'. Duplicate ratio 0.04 exceeds threshold 0.01.",
                    )
                ],
                error_summary="DataQualityException: 1,420 duplicate records detected on primary key.",
                run_page_url="https://workspace.databricks.com/#job/103/run/5006",
            ),
            # 7. Successful Run for baseline
            JobRunInfo(
                job_id=101,
                run_id=5000,
                run_name="Bronze Ingestion - IoT Telemetry Stream [Run 5000]",
                state="TERMINATED",
                result_state="SUCCESS",
                start_time=now_ms - 86400000,
                end_time=now_ms - 86100000,
                execution_duration_ms=300000,
                tasks=[
                    TaskRunInfo(
                        task_key="read_s3_stream",
                        run_id=5000,
                        state="TERMINATED",
                        result_state="SUCCESS",
                        start_time=now_ms - 86400000,
                        end_time=now_ms - 86250000,
                        execution_duration_ms=150000,
                    ),
                    TaskRunInfo(
                        task_key="write_bronze_delta",
                        run_id=5000,
                        state="TERMINATED",
                        result_state="SUCCESS",
                        start_time=now_ms - 86250000,
                        end_time=now_ms - 86100000,
                        execution_duration_ms=150000,
                        depends_on=["read_s3_stream"],
                    ),
                ],
                error_summary=None,
                run_page_url="https://workspace.databricks.com/#job/101/run/5000",
            ),
        ]


# Export client singleton
databricks_client = DatabricksClientWrapper()
