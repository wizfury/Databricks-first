"""
Monitoring and Failure Detection Service.
Interacts with Lakeflow Jobs API to identify failed runs, recurring anomalies,
and duration SLA breaches with bounded, cached polling.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from config import config
from databricks_client import DatabricksClientWrapper, databricks_client
from models import JobRunInfo, TaskRunInfo

logger = logging.getLogger(__name__)


class PipelineMonitor:
    """Service for discovering and monitoring Lakeflow Jobs and run states."""

    def __init__(self, client: Optional[DatabricksClientWrapper] = None):
        self.client = client or databricks_client
        self._cache: Dict[str, Tuple[float, Any]] = {}
        self._cache_ttl_seconds = config.poll_interval_seconds

    def _get_from_cache(self, key: str) -> Optional[Any]:
        now = time.time()
        if key in self._cache:
            ts, val = self._cache[key]
            if now - ts < self._cache_ttl_seconds:
                return val
        return None

    def _set_cache(self, key: str, val: Any):
        self._cache[key] = (time.time(), val)

    def clear_cache(self):
        """Force cache invalidation on user-initiated refresh."""
        self._cache.clear()

    def get_configured_jobs(self) -> List[Dict[str, Any]]:
        """List all configured Lakeflow jobs in workspace with caching."""
        cache_key = "configured_jobs"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return cached

        jobs = self.client.list_jobs(limit=100)
        self._set_cache(cache_key, jobs)
        return jobs

    def get_recent_runs(self, job_id: int, limit: int = 15) -> List[JobRunInfo]:
        """Fetch recent runs for a given job."""
        cache_key = f"job_runs_{job_id}_{limit}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return cached

        runs = self.client.list_runs(job_id=job_id, limit=limit)
        self._set_cache(cache_key, runs)
        return runs

    def get_all_failed_runs(self, limit_per_job: int = 10) -> List[JobRunInfo]:
        """Aggregate failed runs across all configured jobs."""
        cache_key = f"all_failed_runs_{limit_per_job}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return cached

        failed_runs = []
        jobs = self.get_configured_jobs()
        for j in jobs:
            runs = self.client.list_runs(job_id=j["job_id"], limit=limit_per_job)
            for r in runs:
                if r.result_state in ("FAILED", "TIMEDOUT") or r.state == "INTERNAL_ERROR":
                    failed_runs.append(r)

        # Sort newest first
        failed_runs.sort(key=lambda x: x.run_id, reverse=True)
        self._set_cache(cache_key, failed_runs)
        return failed_runs

    def get_task_failure_details(self, run_id: int) -> Tuple[Optional[TaskRunInfo], Dict[str, Any]]:
        """Identify the primary failed task in a run and fetch its output logs."""
        run = self.client.get_run(run_id=run_id)
        if not run:
            return None, {"error": f"Run {run_id} not found", "logs": ""}

        failed_task = None
        for t in run.tasks:
            if t.result_state == "FAILED":
                failed_task = t
                break

        # Fallback to the first non-successful task if no explicit FAILED status
        if not failed_task:
            for t in run.tasks:
                if t.state in ("FAILED", "INTERNAL_ERROR") or t.result_state in ("FAILED", "TIMEDOUT"):
                    failed_task = t
                    break

        output = self.client.get_run_output(run_id=run_id)
        return failed_task, output

    def detect_sla_breaches(self, multiplier_threshold: float = 2.0) -> List[Dict[str, Any]]:
        """Detect currently active runs that have exceeded normal historical run duration."""
        breaches = []
        jobs = self.get_configured_jobs()
        for j in jobs:
            runs = self.client.list_runs(job_id=j["job_id"], limit=10)
            successful_durations = [
                r.execution_duration_ms
                for r in runs
                if r.result_state == "SUCCESS" and r.execution_duration_ms
            ]
            if not successful_durations:
                continue
            
            avg_duration_ms = sum(successful_durations) / len(successful_durations)

            for r in runs:
                if r.state == "RUNNING" and r.start_time:
                    elapsed_ms = (int(time.time() * 1000) - r.start_time)
                    if elapsed_ms > avg_duration_ms * multiplier_threshold:
                        breaches.append({
                            "job_id": r.job_id,
                            "run_id": r.run_id,
                            "run_name": r.run_name,
                            "elapsed_minutes": round(elapsed_ms / 60000, 1),
                            "avg_duration_minutes": round(avg_duration_ms / 60000, 1),
                            "ratio": round(elapsed_ms / avg_duration_ms, 2),
                        })
        return breaches

    def get_failure_frequency(self, job_id: int, window_runs: int = 10) -> Dict[str, Any]:
        """Calculate failure rate and detect recurring pipeline failures."""
        runs = self.client.list_runs(job_id=job_id, limit=window_runs)
        if not runs:
            return {"failure_count": 0, "total_runs": 0, "failure_rate": 0.0, "is_recurring": False}

        failures = [r for r in runs if r.result_state in ("FAILED", "TIMEDOUT")]
        failure_count = len(failures)
        rate = failure_count / len(runs)
        
        # Check if last 2 consecutive runs failed
        is_consecutive = len(runs) >= 2 and all(r.result_state in ("FAILED", "TIMEDOUT") for r in runs[:2])

        return {
            "failure_count": failure_count,
            "total_runs": len(runs),
            "failure_rate": round(rate, 2),
            "is_recurring": failure_count >= 2 or is_consecutive,
        }

    def get_system_kpis(self) -> Dict[str, Any]:
        """Aggregate high-level telemetry for the overview dashboard."""
        jobs = self.get_configured_jobs()
        failed_runs = self.get_all_failed_runs(limit_per_job=5)
        
        total_jobs = len(jobs)
        total_failures = len(failed_runs)

        return {
            "total_jobs": total_jobs,
            "active_failures": total_failures,
            "sla_breaches": len(self.detect_sla_breaches()),
            "last_monitored_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        }


# Singleton monitor instance
pipeline_monitor = PipelineMonitor()
