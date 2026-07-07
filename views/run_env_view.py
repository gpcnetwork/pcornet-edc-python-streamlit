import datetime
import uuid

import streamlit as st

from services.dq_checks_service import load_dq_checks
from services.section_service import load_sections
from utils.constants import fmt_ms
from utils.db import get_meta_conn
from utils.run_repository import RunRepository


def render_run_env_view(session, schemas: list):
    active = st.session_state.get("active_site", {})

    st.title("PCORNet Empirical Data Curation Report")
    st.info(
        f"▶ **Site:** {active['site_name']}  ·  "
        f"`{active['database_name']}.{active['cdm_schema']}`  ·  "
        f"Network: {active['network_name']}"
    )

    st.subheader("Previous Sessions")
    _render_session_list(session, active)

    st.markdown("---")
    _render_new_run_expander(schemas, active)


def _render_session_list(session, active: dict):
    site_id = active.get("site_id", "")
    sessions = RunRepository(get_meta_conn()).load_sessions_for_site(site_id)

    if not sessions:
        st.caption("No previous sessions yet. Start a new run below.")
        return

    # Compute total expected runnable items from CSVs (cached — no extra DB cost)
    dq_df = load_dq_checks()
    total_dq = len(dq_df[
        dq_df["SQL_File_Ref"].notna()
        & (dq_df["SQL_File_Ref"].str.lower().str.strip() != "n/a")
    ])
    sec_df = load_sections()
    sec_run = sec_df[
        sec_df["SQL_File_Ref"].notna()
        & (sec_df["SQL_File_Ref"].str.lower().str.strip() != "n/a")
    ]
    total_table = len(sec_run[~sec_run["Table"].str.lower().str.startswith("chart")])
    total_chart = len(sec_run[sec_run["Table"].str.lower().str.startswith("chart")])
    total_expected = total_dq + total_table + total_chart

    # Header row
    h1, h2, h3, h4, h5, h6, h7, h8, h9 = st.columns([3, 2, 2, 2, 2, 2, 1, 1, 1])
    h1.markdown("**Schema / Prev**")
    h2.markdown("**Cutoff / Lookback**")
    h3.markdown("**Started**")
    h4.markdown("**Counts**")
    h5.markdown("**Results**")
    h6.markdown("**Duration**")
    h7.markdown("**Status**")
    h8.markdown("")
    h9.markdown("")
    st.markdown("---")

    for s in sessions:
        _render_session_row(session, s, total_expected)


def _render_session_row(session, s, total_expected: int = 0):
    pass_count       = int(getattr(s, "PASS_COUNT",        0) or 0)
    fail_count       = int(getattr(s, "FAIL_COUNT",        0) or 0)
    dq_count         = int(getattr(s, "DQ_COUNT",          0) or 0)
    table_count      = int(getattr(s, "TABLE_COUNT",       0) or 0)
    chart_count      = int(getattr(s, "CHART_COUNT",       0) or 0)
    total_duration   = int(getattr(s, "TOTAL_DURATION_MS", 0) or 0)
    not_run          = max(0, total_expected - (dq_count + table_count + chart_count))
    effective_status = "PARTIAL" if not_run > 0 else "COMPLETE"
    status_icon      = {"COMPLETE": "✅", "PARTIAL": "⚠️", "RUNNING": "⏳"}.get(effective_status, "❓")
    started = str(s.STARTED_AT)[:16] if s.STARTED_AT else "—"

    c1, c2, c3, c4, c5, c6, c7, c8, c9 = st.columns([3, 2, 2, 2, 2, 2, 1, 1, 1])
    c1.markdown(f"`{s.CDM_SCHEMA}`")
    prev = getattr(s, "PREV_SCHEMA", None)
    if prev:
        c1.caption(f"Prev: {prev}")
    lookback_years = int(getattr(s, "LOOKBACK_YEARS", 10) or 10)
    c2.markdown(str(s.CUTOFF_DATE) if s.CUTOFF_DATE else "—")
    c2.caption(f"{lookback_years}y lookback")
    c3.markdown(started)
    c4.markdown(f"DQ: {dq_count} · T: {table_count} · C: {chart_count}")
    c5.markdown(f"✅ {pass_count} · ❌ {fail_count} · ⬜ {not_run}")
    c6.markdown(fmt_ms(total_duration) if total_duration else "—")
    c7.markdown(f"{status_icon} {effective_status}")
    with c8:
        if st.button("Load", key=f"load_session_{s.SESSION_ID}",
                     width="stretch"):
            _clear_result_state()
            prev_schema = getattr(s, "PREV_SCHEMA", None) or None
            lookback_years = int(getattr(s, "LOOKBACK_YEARS", 10) or 10)
            st.session_state["run_env"] = {
                "current_schema":     s.CDM_SCHEMA,
                "prev_schema":        prev_schema,
                "cutoff_date":        s.CUTOFF_DATE,
                "lookback_years":     lookback_years,
                "loaded_run_id":      None,
                "session_id":         s.SESSION_ID,
                "restore_session_id": s.SESSION_ID,
            }
            st.query_params["schema"]     = str(s.CDM_SCHEMA)
            st.query_params["session_id"] = str(s.SESSION_ID)
            st.query_params["lookback"]   = str(lookback_years)
            st.query_params["cutoff"]     = str(s.CUTOFF_DATE)[:10] if s.CUTOFF_DATE else ""
            if prev_schema:
                st.query_params["prev_schema"] = prev_schema
            else:
                st.query_params.pop("prev_schema", None)
            st.rerun()
    with c9:
        if st.button("🗑", key=f"del_session_{s.SESSION_ID}",
                     width="stretch",
                     help="Delete this session and all its results"):
            RunRepository(get_meta_conn()).delete_session(s.SESSION_ID)
            st.rerun()


def _clear_result_state():
    """Wipe all result/status/duration/run-id buckets before loading a different session."""
    drop = [
        k for k in st.session_state
        if k in ("dq_results", "dq_statuses", "dq_durations", "dq_run_id")
        or k.endswith(("_results", "_statuses", "_durations", "_run_id"))
    ]
    for k in drop:
        del st.session_state[k]


def _render_new_run_expander(schemas: list, active: dict):
    with st.expander("＋ Start a New Run", expanded=False):
        _render_new_run(schemas, active)


def _render_new_run(schemas: list, active: dict):
    default_schema = active.get("cdm_schema", "")
    default_idx = schemas.index(default_schema) if default_schema in schemas else 0

    col1, col2, col3, col4 = st.columns([2, 2, 2, 2])

    with col1:
        current_schema = st.selectbox(
            "Current Schema",
            schemas,
            index=default_idx,
            key="run_env_current_schema",
        )

    with col2:
        prev_options = ["— Select —"] + schemas
        prev_schema = st.selectbox(
            "Previous Schema",
            prev_options,
            key="run_env_prev_schema",
        )

    with col3:
        cutoff_date = st.date_input(
            "Cutoff Date (optional)",
            value=None,
            key="run_env_cutoff_date",
        )

    with col4:
        lookback_years = st.number_input(
            "Lookback (years)",
            min_value=1,
            max_value=50,
            value=10,
            step=1,
            key="run_env_lookback_years",
            help="Reports run over [cutoff (or today) − lookback years, cutoff (or today)].",
        )

    st.write("")
    _, center, _ = st.columns([3, 2, 3])
    with center:
        st.markdown("""
        <style>
        [data-testid="stBaseButton-primary"] {
            background-color: #2ea043 !important;
            border-color: #2ea043 !important;
            font-size: 1.1rem !important;
            font-weight: 700 !important;
            padding: 0.65rem 1.5rem !important;
        }
        [data-testid="stBaseButton-primary"]:hover {
            background-color: #3fb950 !important;
            border-color: #3fb950 !important;
        }
        </style>
        """, unsafe_allow_html=True)

        if st.button("Begin Analysis", type="primary", width="stretch"):
            if prev_schema == "— Select —":
                st.error("Previous Schema is required. Select a schema from the dropdown.")
                st.stop()
            _clear_result_state()
            session_id  = str(uuid.uuid4())
            eff_prev    = prev_schema
            st.session_state["run_env"] = {
                "current_schema":     current_schema,
                "prev_schema":        eff_prev,
                "cutoff_date":        cutoff_date,
                "lookback_years":     int(lookback_years),
                "loaded_run_id":      None,
                "session_id":         session_id,
                "restore_session_id": None,
            }
            st.query_params["schema"]     = current_schema
            st.query_params["session_id"] = session_id
            st.query_params["lookback"]   = str(int(lookback_years))
            if cutoff_date:
                st.query_params["cutoff"] = str(cutoff_date)
            else:
                st.query_params.pop("cutoff", None)
            if eff_prev:
                st.query_params["prev_schema"] = eff_prev
            else:
                st.query_params.pop("prev_schema", None)
            st.rerun()
