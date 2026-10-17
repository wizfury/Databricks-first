# ⚡ AI-Powered Self-Healing Data Pipeline on Databricks

> A production-oriented autonomous recovery control plane built natively for **Databricks Apps**, **Lakeflow Jobs**, **Unity Catalog**, and **Databricks Model Serving**.

---

## 1. Executive Summary

Data pipelines fail daily across enterprise workspaces due to transient network glitches, schema drifts, executor out-of-memory errors, upstream file delays, and authorization changes. 

This application provides an **autonomous self-healing recovery control plane** hosted natively on **Databricks Apps**. It continuously monitors Lakeflow Jobs, classifies failures across **10 enterprise failure categories**, uses a **Databricks Model Serving LLM** (with a deterministic rule-based fallback) to diagnose root causes, enforces strict policy boundaries and approval gates, executes guarded repair operations via the Databricks Python SDK, and verifies data integrity before declaring recovery successful.

---

## 2. Architecture & Enterprise Failure Categories

```
┌────────────────────────────────────────────────────────────────────────┐
│                        DATABRICKS WORKSPACE                            │
│                                                                        │
│  ┌───────────────────────┐          ┌───────────────────────────────┐  │
│  │     Lakeflow Jobs     │◄─────────┤   Databricks App (Streamlit)  │  │
│  │ (Bronze / Silver /    │          │  - Overview Dashboard         │  │
│  │  Gold Pipelines)      │─────────►│  - Run Explorer               │  │
│  └──────────┬────────────┘          │  - AI Failure Diagnosis       │  │
│             │                       │  - Recovery & Approvals       │  │
│             │ Runs & Failure Logs   │  - Policy & Audit Log         │  │
│             ▼                       └──────┬────────────────┬───────┘  │
│  ┌───────────────────────┐                 │                │          │
│  │  Model Serving LLM    │◄────────────────┘                │          │
│  │ (Llama 3.3 / DBRX)    │  Structured Diagnosis JSON       │          │
│  └───────────────────────┘                                  ▼          │
│                                              ┌──────────────────────┐  │
│                                              │ Unity Catalog Delta  │  │
│                                              │ - recovery_history   │  │
│                                              │ - recovery_state     │  │
│                                              │ - recovery_policies  │  │
│                                              └──────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

### The 10 Enterprise Failure Categories Handled

| Category | Typical Error Signatures | Root Cause | Allowlisted Action | Requires Approval |
| :--- | :--- | :--- | :--- | :--- |
| **1. Transient Infrastructure** | `Read timed out`, `Connection reset`, `503 Service Unavailable` | Network glitch, temporary cloud endpoint timeout | `RETRY_RUN` | ❌ Auto-eligible (within budget) |
| **2. Missing / Delayed Source** | `FileNotFoundException`, `Path does not exist`, `manifest.json` missing | Upstream data provider delayed | `WAIT_FOR_DEPENDENCY` | ✅ Human Review |
| **3. Schema Compatibility** | `AnalysisException: schema mismatch`, `cannot cast without loss` | Incompatible upstream column changes or drift | `ESCALATE_TO_HUMAN` | ✅ Human Review |
| **4. Compute / Memory (OOM)** | `OutOfMemoryError: Java heap space`, `ExecutorLostFailure` | Executor memory exhaustion, severe data skew | `RECOMMEND_COMPUTE_ADJUSTMENT` | ✅ Human Review |
| **5. Upstream Dependency** | `Upstream task failed`, `Parent dependency not ready` | Cascade failure from parent task | `REPAIR_FAILED_TASKS` | ✅ Human Review |
| **6. Data Quality Violation** | `DataQualityException`, `Duplicate records detected on PK` | Record uniqueness breach, abnormal row volume | `TRIGGER_VALIDATION` | ✅ Human Review |
| **7. Unity Catalog Permission** | `[PERMISSION_DENIED]`, `AccessControlException`, `403 Forbidden` | Expired grants, missing `USE SCHEMA` / `MODIFY` | `ESCALATE_TO_HUMAN` | ✅ Human Review |
| **8. Code / Regression** | `NullPointerException`, `KeyError`, `ValueError`, `ZeroDivisionError` | Application bug in transformation notebook | `ESCALATE_TO_HUMAN` | ✅ Human Review |
| **9. Interrupted / Concurrent Write**| `DeltaConcurrentModificationException`, `ConcurrentAppendException` | Concurrent commit conflict on Delta table | `REPAIR_FAILED_TASKS` | ❌ Auto-eligible (with backoff) |
| **10. Silent Failure** | Job status `SUCCESS` but duration < 1s with 0 rows processed | Upstream empty stream or unhandled pass statement | `TRIGGER_VALIDATION` | ✅ Human Review |

---

## 3. Project Structure

```text
Databricks-first/
├── app.py                      # Main multi-page Streamlit application
├── app.yaml                    # Databricks Apps runtime configuration
├── requirements.txt            # Pinned dependencies (databricks-sdk, streamlit, etc.)
├── README.md                   # Complete architectural and deployment documentation
├── config.py                   # Environment configuration & table naming
├── models.py                   # Pydantic schemas for runs, diagnosis, and policies
├── databricks_client.py        # Resilient Databricks SDK client with simulation mode
├── monitoring.py               # Lakeflow Jobs monitoring & duration SLA detector
├── diagnosis.py                # Hybrid AI (Model Serving) + Deterministic rules engine
├── policy_engine.py            # Safety guardrails, retry budgets, cooldowns, & locks
├── recovery.py                 # Guarded Databricks SDK execution engine
├── verification.py             # Post-recovery data quality & terminal state validator
├── audit.py                    # Durable state storage & Unity Catalog Delta adapter
├── recovery_audit.db           # Local persistent SQLite store for dev & offline mode
├── scripts/
│   ├── initialize_tables.py    # Idempotent DDL script for Unity Catalog tables
│   ├── validate_permissions.py # Service Principal permissions auditor
│   └── seed_test_policies.py   # Baseline policy seeder for demo pipelines
└── tests/
    ├── test_diagnosis.py       # Tests all 10 failure categories & prompt injection
    ├── test_policy_engine.py   # Tests retry budgets, cooldowns, and approval gates
    ├── test_recovery.py        # Tests repair_run, run_now, and concurrency locks
    ├── test_verification.py    # Tests terminal state & data quality verification
    └── test_audit.py           # Tests state recovery across process restarts
```

---

## 4. Step-by-Step Deployment Guide to Databricks Apps

Assume you are a beginner with Databricks Apps. Follow these steps sequentially:

### Step 1: Prerequisites in Your Databricks Workspace

Before deploying the app, make sure you have:
1. A Databricks workspace with **Unity Catalog** enabled.
2. A **SQL Warehouse** (Serverless or Pro) to run Delta queries.
3. Access to a **Databricks Model Serving Endpoint** (e.g., `databricks-meta-llama-3-3-70b-instruct`).
4. Permission to create **Databricks Apps** in your workspace.

---

### Step 2: Initialize Unity Catalog Tables

Run the table initialization script to create the necessary Delta tables in Unity Catalog:

```bash
python scripts/initialize_tables.py
```

Or execute the following SQL statements in the Databricks SQL Query Editor:

```sql
CREATE SCHEMA IF NOT EXISTS main.ops 
COMMENT 'Operational telemetry and recovery storage for self-healing pipelines';

CREATE TABLE IF NOT EXISTS main.ops.recovery_history (
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
COMMENT 'Audit log recording all failure diagnoses, approvals, and recoveries';

CREATE TABLE IF NOT EXISTS main.ops.recovery_state (
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
COMMENT 'Durable recovery state tracking attempt budgets and concurrency locks';

CREATE TABLE IF NOT EXISTS main.ops.recovery_policies (
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
```

---

### Step 3: Configure Permissions for the Databricks App Service Principal

Databricks Apps runs under a managed **Service Principal**. Ensure this identity has least-privilege permissions:

```sql
-- Grant access to the Ops schema in Unity Catalog
GRANT USE CATALOG ON CATALOG main TO `app-service-principal-name`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA main.ops TO `app-service-principal-name`;

-- Grant permissions to monitor and repair target Lakeflow Jobs
-- In Databricks Workspace UI -> Workflows -> Select Job -> Permissions:
-- Add App Service Principal with 'CAN MANAGE RUN' privilege.
```

To verify permissions automatically, run:
```bash
python scripts/validate_permissions.py
```

---

### Step 4: Deploying via Databricks CLI

Ensure you have the Databricks CLI installed (`databricks --version >= 0.220.0`):

1. **Authenticate CLI with your workspace:**
   ```bash
   databricks auth login --host https://<your-databricks-instance>.azuredatabricks.net
   ```

2. **Create the Databricks App:**
   ```bash
   databricks apps create ai-self-healing-pipeline
   ```

3. **Sync Source Files:**
   ```bash
   databricks sync . /Workspace/Users/<your-user>/self_healing_databricks
   ```

4. **Deploy the App:**
   ```bash
   databricks apps deploy ai-self-healing-pipeline --source-code-path /Workspace/Users/<your-user>/self_healing_databricks
   ```

5. **Start and Open App:**
   ```bash
   databricks apps start ai-self-healing-pipeline
   ```

---

### Step 5: Deploying via the Databricks UI

1. Open your Databricks workspace and navigate to **Compute** -> **Apps** (or **Apps** in the left sidebar).
2. Click **Create App**.
3. Set the App Name: `ai-self-healing-pipeline`.
4. Choose **Custom / Streamlit**.
5. Select the workspace directory containing this repository.
6. Click **Deploy**.

---

## 5. Security & Safety Principles

1. **Untrusted Log Handling (Prompt Injection Resistance):**
   Error messages from failed pipelines can contain arbitrary text, user input, or malicious payloads. The system treats all logs as untrusted text within strict delimiter tags and instructs the model never to follow commands found within log content.
2. **Strict Schema Validation:**
   LLM output is strictly parsed and validated against the Pydantic `DiagnosisResult` schema. If the model returns invalid JSON, markdown, or unauthorized actions, the system silently and safely falls back to the deterministic rules engine.
3. **No Arbitrary Execution:**
   The model **never** receives direct execution tools or the Databricks SDK client. It only returns recommended actions from a controlled allowlist (`RETRY_RUN`, `REPAIR_FAILED_TASKS`, `CANCEL_RUN`, `WAIT_FOR_DEPENDENCY`, `TRIGGER_VALIDATION`, `RECOMMEND_COMPUTE_ADJUSTMENT`, `ESCALATE_TO_HUMAN`).
4. **Durable Concurrency Locks:**
   To prevent duplicate simultaneous recovery requests from running against the same failed run, a distributed lock is acquired in `main.ops.recovery_state`.
5. **Human Approval Gates:**
   Schema conflicts, permission errors, code regressions, and non-allowlisted actions **always** require explicit human confirmation.

---

## 6. Running Tests & Local Development

This application features a complete **high-fidelity simulation mode**. You can run the entire test suite and test the Streamlit UI on your local workstation without requiring live Databricks credentials:

### Running Unit Tests

```bash
python -m pytest tests/ -v
```

All 28 tests cover the 14 failure scenarios required in Section 10:
- Transient network timeouts and retries
- Missing source file detection
- Delta schema drift conflicts
- Executor OOM & data skew
- Unity Catalog permission errors
- Unknown / unclassified errors
- Silent 0-second failures
- Concurrency locks & idempotency
- Retry budget exhaustion
- Cooldown window & exponential backoff
- Post-recovery data verification
- Process restart state durability

### Running the App Locally

```bash
streamlit run app.py
```
Open your browser at `http://localhost:8501`.

---

## 7. Troubleshooting: "App Not Available" Error in Databricks Apps

If you encounter:
> **"App Not Available: Sorry, the Databricks app you are trying to access is currently unavailable. Please try again later."**

Follow these 4 diagnostic checks:

### 1. Dynamic Port Binding (`app.yaml`)
Databricks Apps assigns a dynamic listening port at runtime through the `DATABRICKS_APP_PORT` environment variable and automatically maps it to Streamlit's `STREAMLIT_SERVER_PORT`.
* **Fix applied:** `app.yaml` uses the standard command:
  ```yaml
  command:
    - "streamlit"
    - "run"
    - "app.py"
  ```
  *(Never pass `--server.port 8501` in `app.yaml` on Databricks Apps, as it overrides the dynamic proxy port).*

### 2. Grant `CAN USE` Permission in the Databricks Apps UI
If the app was deployed via a Service Principal, Git repo, or by a workspace admin, other users will receive this error until explicit access is granted:
1. Navigate to **Compute** -> **Apps** in Databricks.
2. Click your app: `ai-self-healing-pipeline`.
3. In the top right, click **Share / Permissions**.
4. Add your user account or group and set permission to **`CAN USE`** (or **`CAN MANAGE`**).

### 3. Inspect Container Logs
1. In the Databricks Apps UI, select `ai-self-healing-pipeline`.
2. Click the **Logs** tab.
3. Review the container startup output to confirm Streamlit started without errors.

### 4. Container Restart
After updating `app.yaml`, restart the app from the Databricks Apps UI or CLI:
```bash
databricks apps stop ai-self-healing-pipeline
databricks apps deploy ai-self-healing-pipeline --source-code-path /Workspace/Users/<your-user>/self_healing_databricks
databricks apps start ai-self-healing-pipeline
```
