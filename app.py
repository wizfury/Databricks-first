"""
AI-Powered Self-Healing Pipeline Manager for Databricks.
Native Databricks App built with Streamlit and Databricks SDK.
Provides automated failure detection, AI root-cause diagnosis, guarded recovery,
and post-recovery data integrity verification.
"""

import json
import logging
from datetime import datetime, timezone
import pandas as pd
import streamlit as st

from audit import audit_store
from config import config
from databricks_client import databricks_client
from diagnosis import diagnosis_engine
from models import (
    ApprovalStatus,
    DataQualityThresholds,
    FailureCategory,
    RecoveryActionType,
    RecoveryPolicy,
    RecoveryStatus,
)
from monitoring import pipeline_monitor
from policy_engine import policy_engine
from recovery import recovery_engine
from verification import recovery_verifier

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Streamlit Page Setup ---
st.set_page_config(
    page_title="Databricks AI Self-Healing Pipeline",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling
st.markdown(
    """
    <style>
    .main-header {
        font-size: 1.85rem;
        font-weight: 700;
        background: linear-gradient(90deg, #FF3621, #FF8000, #3B82F6);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.25rem;
    }
    .sub-header {
        color: #94A3B8;
        font-size: 0.95rem;
        margin-bottom: 1.25rem;
    }
    .metric-card {
        background-color: #1E293B;
        border-radius: 8px;
        padding: 16px;
        border-left: 4px solid #3B82F6;
        margin-bottom: 12px;
    }
    .badge-failed {
        background-color: #EF4444;
        color: white;
        padding: 2px 8px;
        border-radius: 4px;
        font-weight: 600;
        font-size: 0.8rem;
    }
    .badge-success {
        background-color: #10B981;
        color: white;
        padding: 2px 8px;
        border-radius: 4px;
        font-weight: 600;
        font-size: 0.8rem;
    }
    .badge-running {
        background-color: #3B82F6;
        color: white;
        padding: 2px 8px;
        border-radius: 4px;
        font-weight: 600;
        font-size: 0.8rem;
    }
    .badge-pending {
        background-color: #F59E0B;
        color: white;
        padding: 2px 8px;
        border-radius: 4px;
        font-weight: 600;
        font-size: 0.8rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def render_sidebar():
    """Sidebar navigation and workspace environment badge."""
    st.sidebar.markdown("### ⚡ Lakeflow Self-Healing")
    
    # Environment Status Card
    if databricks_client.is_mock:
        st.sidebar.warning("Mode: **SIMULATION / DEV**\n(No live Databricks credentials detected)")
    else:
        st.sidebar.success(f"Mode: **LIVE WORKSPACE**\nCatalog: `{config.catalog}` | Schema: `{config.schema}`")

    st.sidebar.caption(f"Identity: `{databricks_client.get_current_identity()}`")
    st.sidebar.divider()

    menu = st.sidebar.radio(
        "Navigation",
        [
            "Overview Dashboard",
            "Job & Run Explorer",
            "Failure Diagnosis",
            "Recovery & Approvals",
            "Recovery History & Audit",
            "Policies & Allowlist",
            "System Health & Diagnostics",
        ],
    )
    st.sidebar.divider()
    if st.sidebar.button("🔄 Refresh Telemetry Cache", use_container_width=True):
        pipeline_monitor.clear_cache()
        st.sidebar.success("Cache refreshed.")
        st.rerun()

    return menu


# ==============================================================================
# VIEW 1: Overview Dashboard
# ==============================================================================
def view_overview():
    st.markdown('<div class="main-header">Autonomous Pipeline Recovery Control Plane</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Real-time health, anomaly detection, and AI self-healing across Lakeflow Jobs</div>', unsafe_allow_html=True)

    kpis = pipeline_monitor.get_system_kpis()
    history = audit_store.list_history(limit=100)

    total_recoveries = len([h for h in history if h.executed_action is not None])
    successful_recoveries = len([h for h in history if h.recovery_status == RecoveryStatus.SUCCEEDED])
    pending_approvals = len([h for h in history if h.approval_status == ApprovalStatus.PENDING])

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Monitored Jobs", kpis["total_jobs"])
    col2.metric("Active Failures", kpis["active_failures"], delta=f"{kpis['active_failures']} failed", delta_color="inverse")
    col3.metric("SLA Breaches", kpis["sla_breaches"], delta=f"{kpis['sla_breaches']} overdue", delta_color="inverse")
    col4.metric("Recoveries Attempted", total_recoveries)
    col5.metric("Successful Heals", successful_recoveries, delta=f"{successful_recoveries} verified", delta_color="normal")

    st.divider()

    # Active Incidents and Failures
    col_left, col_right = st.columns([1.6, 1])

    with col_left:
        st.subheader("⚠️ Active Unresolved Pipeline Failures")
        failed_runs = pipeline_monitor.get_all_failed_runs(limit_per_job=5)
        
        if failed_runs:
            table_data = []
            for r in failed_runs:
                table_data.append({
                    "Job ID": r.job_id,
                    "Run ID": r.run_id,
                    "Run Name": r.run_name,
                    "State": r.result_state or r.state,
                    "Error Summary": (r.error_summary or "Unknown error")[:80] + "...",
                })
            st.dataframe(pd.DataFrame(table_data), use_container_width=True, hide_index=True)
            st.info("💡 Select 'Failure Diagnosis' or 'Recovery & Approvals' from the sidebar to inspect and heal these runs.")
        else:
            st.success("🎉 All monitored Lakeflow pipelines are healthy and meeting SLAs!")

    with col_right:
        st.subheader("📊 Failure Category Distribution")
        if history:
            cat_counts = {}
            for h in history:
                cat_counts[h.failure_category.value] = cat_counts.get(h.failure_category.value, 0) + 1
            df_cats = pd.DataFrame(list(cat_counts.items()), columns=["Category", "Incidents"])
            st.bar_chart(df_cats.set_index("Category"))
        else:
            st.caption("No historical failure records in audit table yet.")


# ==============================================================================
# VIEW 2: Job & Run Explorer
# ==============================================================================
def view_job_explorer():
    st.markdown("### 🔍 Lakeflow Job & Run Explorer")
    st.caption("Inspect live job runs, task trees, execution durations, and error traces.")

    jobs = pipeline_monitor.get_configured_jobs()
    if not jobs:
        st.warning("No jobs discovered in workspace.")
        return

    job_options = {f"{j['job_id']} - {j['name']}": j["job_id"] for j in jobs}
    selected_label = st.selectbox("Select Lakeflow Job", list(job_options.keys()))
    job_id = job_options[selected_label]

    runs = pipeline_monitor.get_recent_runs(job_id=job_id, limit=15)
    if not runs:
        st.info("No recent runs found for this job.")
        return

    # Recurring Failure Analysis
    freq = pipeline_monitor.get_failure_frequency(job_id=job_id)
    if freq["is_recurring"]:
        st.error(f"🚨 **Recurring Failure Detected:** This job has failed {freq['failure_count']} times ({int(freq['failure_rate']*100)}% failure rate). Automatic multi-retry should be restricted.")

    # Table of runs
    run_records = []
    for r in runs:
        duration_sec = (r.execution_duration_ms or 0) / 1000
        run_records.append({
            "Run ID": r.run_id,
            "Run Name": r.run_name,
            "Status": r.result_state or r.state,
            "Duration (s)": round(duration_sec, 1),
            "Tasks Count": len(r.tasks),
            "Error Summary": (r.error_summary or "None")[:60],
        })
    st.dataframe(pd.DataFrame(run_records), use_container_width=True, hide_index=True)

    st.divider()

    # Detailed Task Breakdown for selected run
    run_ids = [r.run_id for r in runs]
    selected_run_id = st.selectbox("Inspect Tasks for Run ID", run_ids)
    selected_run = next(r for r in runs if r.run_id == selected_run_id)

    st.markdown(f"#### Tasks for Run `{selected_run_id}` ({selected_run.run_name})")
    if selected_run.run_page_url:
        st.markdown(f"🔗 [Open in Databricks Workspace]({selected_run.run_page_url})")

    task_cols = st.columns(len(selected_run.tasks) if selected_run.tasks else 1)
    for idx, t in enumerate(selected_run.tasks):
        with task_cols[idx]:
            state = t.result_state or t.state
            border_color = "#10B981" if state == "SUCCESS" else ("#EF4444" if state == "FAILED" else "#3B82F6")
            st.markdown(
                f"""
                <div style="border: 2px solid {border_color}; border-radius: 6px; padding: 12px; margin-bottom: 8px;">
                    <strong>{t.task_key}</strong><br/>
                    <small>Status: {state}</small><br/>
                    <small>Attempt: #{t.attempt_number}</small>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # Show raw error trace if failed
    if selected_run.error_summary:
        with st.expander("📄 View Task Error Trace & Logs", expanded=True):
            st.code(selected_run.error_summary, language="text")


# ==============================================================================
# VIEW 3: Failure Diagnosis
# ==============================================================================
def view_diagnosis():
    st.markdown("### 🧠 AI & Deterministic Root-Cause Diagnosis")
    st.caption("Normalizes logs, classifies failure signatures into enterprise categories, and recommends safe recovery actions.")

    failed_runs = pipeline_monitor.get_all_failed_runs()
    if not failed_runs:
        st.success("No failed runs currently requiring diagnosis!")
        return

    run_labels = {f"Run {r.run_id} ({r.run_name})": r for r in failed_runs}
    selected_key = st.selectbox("Select Failed Run for Diagnosis", list(run_labels.keys()))
    run = run_labels[selected_key]

    failed_task, output = pipeline_monitor.get_task_failure_details(run.run_id)

    use_ai = st.toggle("Enable Databricks Model Serving LLM Endpoint", value=True, help="Queries configured endpoint with strict schema validation and prompt injection defenses.")

    if st.button("🚀 Run Diagnosis Engine", type="primary"):
        with st.spinner("Analyzing error stack traces, execution duration, and log signals..."):
            diagnosis = diagnosis_engine.diagnose_run(
                run_info=run,
                task_info=failed_task,
                task_logs=output.get("logs") or output.get("error"),
                prefer_ai=use_ai,
            )
            st.session_state[f"diag_{run.run_id}"] = diagnosis

    diagnosis = st.session_state.get(f"diag_{run.run_id}")
    if diagnosis:
        st.divider()

        # Diagnosis Banner
        engine_type = "🤖 Databricks Model Serving LLM" if diagnosis.is_ai_generated else "⚙️ Deterministic Knowledge Rules Engine"
        st.markdown(f"**Diagnosis Method:** {engine_type}")

        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Failure Category", diagnosis.failure_category.value)
        col_b.metric("Confidence Score", f"{int(diagnosis.confidence * 100)}%")
        col_c.metric("Recommended Action", diagnosis.recommended_action.value)

        # Root cause card
        st.markdown(
            f"""
            <div class="metric-card">
                <h4 style="margin-top:0;">Root Cause Analysis</h4>
                <p>{diagnosis.root_cause}</p>
                <strong>Reasoning Summary:</strong>
                <p>{diagnosis.reasoning_summary}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Supporting Evidence
        if diagnosis.evidence:
            st.markdown("##### 📌 Supporting Evidence")
            for ev in diagnosis.evidence:
                st.markdown(f"- `{ev}`")

        # Potential Risks & Missing Info
        col_risk, col_miss = st.columns(2)
        with col_risk:
            if diagnosis.potential_risks:
                st.markdown("##### ⚠️ Operational Risks")
                for rsk in diagnosis.potential_risks:
                    st.markdown(f"- {rsk}")
        with col_miss:
            if diagnosis.missing_information:
                st.markdown("##### ❓ Missing Diagnostic Information")
                for mis in diagnosis.missing_information:
                    st.markdown(f"- {mis}")

        # Human Approval Warning
        if diagnosis.requires_approval:
            st.warning("⚠️ **Human Operator Approval Required:** This failure mode or action cannot be executed automatically under current policy.")
        else:
            st.success("✅ **Auto-Recovery Eligible:** This action can safely be executed within retry budget.")


# ==============================================================================
# VIEW 4: Recovery & Approvals
# ==============================================================================
def view_recovery():
    st.markdown("### 🛡️ Guarded Recovery Execution & Approvals")
    st.caption("Execute allowlisted recovery actions under strict policy constraints, cooldowns, and audit logging.")

    failed_runs = pipeline_monitor.get_all_failed_runs()
    if not failed_runs:
        st.success("No active failed runs requiring recovery!")
        return

    run_options = {f"Run {r.run_id} - Job {r.job_id} ({r.run_name})": r for r in failed_runs}
    selected_option = st.selectbox("Select Target Run to Recover", list(run_options.keys()))
    run = run_options[selected_option]

    # Check for existing diagnosis in session state
    diag = st.session_state.get(f"diag_{run.run_id}")
    if not diag:
        failed_task, _ = pipeline_monitor.get_task_failure_details(run.run_id)
        diag = diagnosis_engine.diagnose_run(run_info=run, task_info=failed_task, prefer_ai=False)

    col1, col2 = st.columns([1.5, 1])

    with col1:
        st.markdown(f"#### Proposed Action: `{diag.recommended_action.value}`")
        st.caption(f"Diagnosed Category: **{diag.failure_category.value}** | Confidence: **{int(diag.confidence*100)}%**")

        action_override = st.selectbox(
            "Select Recovery Action (Allowlisted)",
            [a.value for a in RecoveryActionType],
            index=[a.value for a in RecoveryActionType].index(diag.recommended_action.value),
        )
        chosen_action = RecoveryActionType(action_override)

        # Policy Evaluation
        eval_result = policy_engine.evaluate_action(
            job_id=run.job_id,
            run_info=run,
            proposed_action=chosen_action,
            diagnosis=diag,
            is_approved=False,
        )

        st.markdown("##### Policy Checks & Guardrails")
        if eval_result.allowed:
            st.success(f"✅ {eval_result.reason}")
        else:
            st.error(f"🛑 **Action Blocked by Policy:** {eval_result.reason}")

        st.markdown(f"- **Attempt Count:** `{eval_result.attempt_count} / {eval_result.max_retries}`")
        if eval_result.backoff_seconds > 0:
            st.markdown(f"- **Active Cooldown:** `{eval_result.backoff_seconds}s remaining`")

    with col2:
        st.markdown("#### Operator Approval Gate")
        operator_name = st.text_input("Approving Engineer / Service Principal", value=databricks_client.get_current_identity())
        approval_confirmed = st.checkbox("I authorize execution of this recovery operation on Databricks Lakeflow.", value=not eval_result.requires_approval)

        execute_button = st.button("⚡ Execute Recovery Action", type="primary", disabled=(not approval_confirmed))

        if execute_button:
            with st.spinner("Submitting guarded recovery operation to Databricks SDK..."):
                failed_task, _ = pipeline_monitor.get_task_failure_details(run.run_id)
                failed_task_keys = [failed_task.task_key] if failed_task else None

                exec_result = recovery_engine.execute_recovery(
                    job_id=run.job_id,
                    run_info=run,
                    action=chosen_action,
                    diagnosis=diag,
                    approved_by=operator_name if approval_confirmed else None,
                    failed_tasks=failed_task_keys,
                )

                if exec_result.success:
                    st.success(f"🎉 **Success:** {exec_result.message}")
                    st.markdown(f"Audit Event ID: `{exec_result.event_id}`")
                    st.session_state[f"active_recovery_{run.run_id}"] = exec_result
                else:
                    st.error(f"❌ **Execution Blocked:** {exec_result.message}")


# ==============================================================================
# VIEW 5: Recovery History & Audit Logs
# ==============================================================================
def view_audit_history():
    st.markdown("### 📜 Recovery History & Audit Logs")
    st.caption("Complete, tamper-evident audit trail backed by Unity Catalog Delta Lake table.")

    col1, col2, col3 = st.columns(3)
    filter_category = col1.selectbox("Filter by Category", ["ALL"] + [c.value for c in FailureCategory])
    filter_status = col2.selectbox("Filter by Status", ["ALL"] + [s.value for s in RecoveryStatus])
    filter_job = col3.number_input("Filter by Job ID (0 for all)", min_value=0, value=0)

    events = audit_store.list_history(
        job_id=filter_job if filter_job > 0 else None,
        failure_category=filter_category if filter_category != "ALL" else None,
        status=filter_status if filter_status != "ALL" else None,
        limit=50,
    )

    if not events:
        st.info("No audit events match the selected criteria.")
        return

    table_data = []
    for ev in events:
        table_data.append({
            "Event ID": ev.event_id,
            "Timestamp (UTC)": ev.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "Job ID": ev.job_id,
            "Run ID": ev.run_id,
            "Category": ev.failure_category.value,
            "Action Executed": ev.executed_action.value if ev.executed_action else "BLOCKED",
            "Approved By": ev.approved_by or "N/A",
            "Recovery Status": ev.recovery_status.value,
            "Verification": ev.verification_status.value,
        })

    st.dataframe(pd.DataFrame(table_data), use_container_width=True, hide_index=True)


# ==============================================================================
# VIEW 6: Policies & Allowlist Management
# ==============================================================================
def view_policies():
    st.markdown("### ⚙️ Recovery Policy & Safety Controls")
    st.caption("Configure allowable recovery actions, retry budgets, cooldown periods, and data quality assertions per Lakeflow Job.")

    jobs = pipeline_monitor.get_configured_jobs()
    job_map = {f"{j['job_id']} - {j['name']}": j["job_id"] for j in jobs}
    selected = st.selectbox("Select Job to Configure", list(job_map.keys()))
    job_id = job_map[selected]

    policy = audit_store.get_policy(job_id)

    with st.form(f"policy_form_{job_id}"):
        col1, col2 = st.columns(2)
        allowlisted = col1.checkbox("Allowlist this Job for Recovery", value=policy.allowlisted)
        auto_eligible = col2.checkbox("Enable Automatic Recovery (Transient Failures)", value=policy.auto_recovery_eligible)

        max_retries = col1.slider("Max Retry Budget", min_value=1, max_value=10, value=policy.max_retries)
        cooldown = col2.number_input("Cooldown Period (Seconds)", min_value=10, max_value=3600, value=policy.cooldown_seconds)

        st.markdown("##### Permitted Recovery Actions")
        allowed_actions_str = st.multiselect(
            "Allowed Actions",
            [a.value for a in RecoveryActionType],
            default=[a.value for a in policy.allowed_actions],
        )

        st.markdown("##### Data Quality Verification Thresholds")
        dq_c1, dq_c2 = st.columns(2)
        min_rows = dq_c1.number_input("Minimum Output Rows", min_value=1, value=policy.data_quality_thresholds.min_row_count)
        max_dup = dq_c2.number_input("Max Allowed Duplicate Ratio", min_value=0.0, max_value=1.0, value=policy.data_quality_thresholds.max_duplicate_ratio, step=0.001)

        submitted = st.form_submit_button("💾 Save Policy Configuration")
        if submitted:
            updated_policy = RecoveryPolicy(
                job_id=job_id,
                allowlisted=allowlisted,
                allowed_actions=[RecoveryActionType(a) for a in allowed_actions_str],
                max_retries=max_retries,
                cooldown_seconds=cooldown,
                auto_recovery_eligible=auto_eligible,
                requires_human_approval=not auto_eligible,
                data_quality_thresholds=DataQualityThresholds(
                    min_row_count=min_rows,
                    max_duplicate_ratio=max_dup,
                ),
            )
            audit_store.save_policy(updated_policy)
            st.success(f"Policy for Job {job_id} updated and persisted.")


# ==============================================================================
# VIEW 7: System Health & Diagnostics
# ==============================================================================
def view_system_health():
    st.markdown("### 🩺 System Health & Post-Recovery Verification")
    st.caption("Verify recovery outcomes, validate Unity Catalog schemas, and verify Databricks SDK connectivity.")

    st.markdown("#### Post-Recovery Run Verification Tool")
    ver_job_id = st.number_input("Target Job ID", min_value=1, value=101)
    ver_run_id = st.number_input("Target Run ID", min_value=1, value=5000)

    if st.button("🧪 Conduct Recovery Verification"):
        with st.spinner("Checking terminal states, task completion, and data quality metrics..."):
            ver_result = recovery_verifier.verify_recovery(job_id=ver_job_id, run_id=ver_run_id)

            if ver_result.passed:
                st.success("🎉 **VERIFICATION PASSED:** The pipeline run and output data meet all integrity assertions!")
            else:
                st.error(f"❌ **VERIFICATION FAILED:** {ver_result.failure_reason}")

            st.markdown("##### Detailed Assertions")
            for chk in ver_result.checks:
                symbol = "✅" if chk.passed else "❌"
                st.markdown(f"- {symbol} **{chk.name}**: {chk.details}")


# ==============================================================================
# Main Router
# ==============================================================================
def main():
    selected_page = render_sidebar()

    if selected_page == "Overview Dashboard":
        view_overview()
    elif selected_page == "Job & Run Explorer":
        view_job_explorer()
    elif selected_page == "Failure Diagnosis":
        view_diagnosis()
    elif selected_page == "Recovery & Approvals":
        view_recovery()
    elif selected_page == "Recovery History & Audit":
        view_audit_history()
    elif selected_page == "Policies & Allowlist":
        view_policies()
    elif selected_page == "System Health & Diagnostics":
        view_system_health()


if __name__ == "__main__":
    main()
