import json

import streamlit as st

from services.dq_checks_service import load_dq_checks, run_single_check
from services.section_service import load_sections, run_section_item
from utils.constants import fmt_ms
from utils.db import get_meta_conn
from utils.run_repository import RunRepository
from views.dq_checks_view import render_dq_checks_view, _dq_status
from views.section_view import render_section_view


def _ensure_dq_run_id(session, schema, cutoff_date, network_id, site_id,
                      session_id=None, prev_schema=None, lookback_years=10) -> str:
    if not st.session_state.get("dq_run_id"):
        run_id = RunRepository(get_meta_conn()).begin_run(
            network_id, site_id, schema, cutoff_date=cutoff_date,
            triggered_by="dq_checks", session_id=session_id, prev_schema=prev_schema,
            lookback_years=lookback_years,
        )
        st.session_state["dq_run_id"] = run_id
    return st.session_state["dq_run_id"]


def _restore_session(session, session_id: str, sections_df):
    """Populate session state from stored DB results."""
    rows = RunRepository(get_meta_conn()).load_session_results(session_id)
    if not rows:
        st.warning("No results found for this session.")
        return

    for check_row in rows:
        triggered_by = check_row.TRIGGERED_BY or ""
        check_id     = check_row.CHECK_ID
        status      = "complete" if check_row.STATUS == "SUCCESS" else "error"
        result_data = check_row.RESULT_DATA
        if isinstance(result_data, list):
            deserialized = result_data
        elif isinstance(result_data, str):
            try:
                parsed = json.loads(result_data)
                deserialized = parsed if isinstance(parsed, list) else None
            except Exception:
                deserialized = None
        else:
            deserialized = None

        duration_ms = int(getattr(check_row, "DURATION_MS", 0) or 0)
        if triggered_by == "dq_checks":
            # For DQ checks, refine status from Pass/Fail in the stored result
            if check_row.STATUS == "SUCCESS" and deserialized:
                status = _dq_status(True, deserialized)
            st.session_state.setdefault("dq_results",   {})
            st.session_state.setdefault("dq_statuses",  {})
            st.session_state.setdefault("dq_durations", {})
            st.session_state["dq_results"][check_id]   = deserialized
            st.session_state["dq_statuses"][check_id]  = status
            st.session_state["dq_durations"][check_id] = duration_ms
        else:
            slug = triggered_by
            st.session_state.setdefault(f"{slug}_results",   {})
            st.session_state.setdefault(f"{slug}_statuses",  {})
            st.session_state.setdefault(f"{slug}_durations", {})
            st.session_state[f"{slug}_results"][check_id]   = deserialized
            st.session_state[f"{slug}_statuses"][check_id]  = status
            st.session_state[f"{slug}_durations"][check_id] = duration_ms


def _count_missing_results(sections_df) -> dict:
    dq_results  = st.session_state.get("dq_results",  {})
    dq_statuses = st.session_state.get("dq_statuses", {})

    dq_df = load_dq_checks()
    runnable_dq = dq_df[
        dq_df["SQL_File_Ref"].notna()
        & (dq_df["SQL_File_Ref"].str.lower().str.strip() != "n/a")
    ]
    dq_missing = sum(
        1 for _, r in runnable_dq.iterrows()
        if (
            f"{r['Data Check']}|{r['EDC Table']}" not in dq_results
            or dq_results.get(f"{r['Data Check']}|{r['EDC Table']}") is None
            or dq_statuses.get(f"{r['Data Check']}|{r['EDC Table']}") in ("error", "fail")
        )
    )

    table_missing = chart_missing = 0
    for section_name in sections_df["Section"].unique():
        slug  = section_name.split(":")[0].strip().lower().replace(" ", "_")
        items = sections_df[sections_df["Section"] == section_name]
        runnable = items[
            items["SQL_File_Ref"].notna()
            & (items["SQL_File_Ref"].str.lower().str.strip() != "n/a")
        ]
        results  = st.session_state.get(f"{slug}_results",  {})
        statuses = st.session_state.get(f"{slug}_statuses", {})
        for _, row in runnable.iterrows():
            key = row["Table"]
            if (
                key not in results
                or results.get(key) is None
                or statuses.get(key) == "error"
            ):
                if str(key).lower().startswith("chart"):
                    chart_missing += 1
                else:
                    table_missing += 1

    return {"dq": dq_missing, "table": table_missing, "chart": chart_missing}


def _execute_missing(session, run_env, sections_df):
    active      = st.session_state.get("active_site", {})
    network_id  = active.get("network_id", "")
    site_id     = active.get("site_id", "")
    db_name     = active.get("database_name", "")
    schema      = run_env["current_schema"]
    prev_schema    = run_env.get("prev_schema")
    cutoff_date    = run_env.get("cutoff_date")
    lookback_years = run_env.get("lookback_years", 10)
    session_id     = run_env.get("session_id")

    import time as _time
    st.session_state.setdefault("dq_results",   {})
    st.session_state.setdefault("dq_statuses",  {})
    st.session_state.setdefault("dq_durations", {})

    # ── DQ Checks — only checks with no stored result ─────────────────────────
    dq_df    = load_dq_checks()
    runnable = dq_df[
        dq_df["SQL_File_Ref"].notna()
        & (dq_df["SQL_File_Ref"].str.lower().str.strip() != "n/a")
    ]
    missing_dq = runnable[
        runnable.apply(
            lambda r: (
                st.session_state["dq_results"].get(f"{r['Data Check']}|{r['EDC Table']}") is None
                or st.session_state["dq_statuses"].get(f"{r['Data Check']}|{r['EDC Table']}") in ("error", "fail")
            ),
            axis=1,
        )
    ]
    if not missing_dq.empty:
        run_id = _ensure_dq_run_id(session, schema, cutoff_date, network_id, site_id,
                                   session_id, prev_schema, lookback_years)
        prog   = st.progress(0, text="Running missing DQ checks…")
        total  = len(missing_dq)
        for i, (_, row) in enumerate(missing_dq.iterrows()):
            rk = f"{row['Data Check']}|{row['EDC Table']}"
            t0 = _time.perf_counter()
            success, result = run_single_check(
                session, row, schema, db_name, run_id, network_id, site_id,
                cutoff_date=cutoff_date, prev_schema=prev_schema,
                lookback_years=lookback_years,
            )
            st.session_state["dq_durations"][rk] = int((_time.perf_counter() - t0) * 1000)
            st.session_state["dq_results"][rk]   = result
            st.session_state["dq_statuses"][rk]  = _dq_status(success, result)
            prog.progress((i + 1) / total, text=f"DQ {row['Data Check']} ({i+1}/{total})")
        RunRepository(get_meta_conn()).complete_run(run_id, "COMPLETE")

    # ── Section items — only items with no stored result ──────────────────────
    for section_name in sections_df["Section"].unique():
        slug  = section_name.split(":")[0].strip().lower().replace(" ", "_")
        items = sections_df[sections_df["Section"] == section_name]
        sec_runnable = items[
            items["SQL_File_Ref"].notna()
            & (items["SQL_File_Ref"].str.lower().str.strip() != "n/a")
        ]
        st.session_state.setdefault(f"{slug}_results",   {})
        st.session_state.setdefault(f"{slug}_statuses",  {})
        st.session_state.setdefault(f"{slug}_durations", {})

        missing_sec = sec_runnable[
            sec_runnable.apply(
                lambda r: (
                    st.session_state[f"{slug}_results"].get(r["Table"]) is None
                    or st.session_state[f"{slug}_statuses"].get(r["Table"]) == "error"
                ),
                axis=1,
            )
        ]
        if missing_sec.empty:
            continue

        sec_run_id = RunRepository(get_meta_conn()).begin_run(
            network_id, site_id, schema,
            cutoff_date=cutoff_date, triggered_by=slug, session_id=session_id,
            prev_schema=prev_schema, lookback_years=lookback_years,
        )
        st.session_state[f"{slug}_run_id"] = sec_run_id

        sec_prog  = st.progress(0, text=f"Section: {section_name}…")
        sec_total = len(missing_sec)
        for j, (_, row) in enumerate(missing_sec.iterrows()):
            item_key = row["Table"]
            t0 = _time.perf_counter()
            success, result = run_section_item(
                session, row["SQL_File_Ref"], schema, db_name,
                sec_run_id, item_key, network_id, site_id,
                prev_schema=prev_schema, cutoff_date=cutoff_date,
                lookback_years=lookback_years,
            )
            st.session_state[f"{slug}_durations"][item_key] = int((_time.perf_counter() - t0) * 1000)
            st.session_state[f"{slug}_results"][item_key]   = result
            st.session_state[f"{slug}_statuses"][item_key]  = "complete" if success else "error"
            sec_prog.progress((j + 1) / sec_total, text=f"{item_key} ({j+1}/{sec_total})")
        RunRepository(get_meta_conn()).complete_run(sec_run_id, "COMPLETE")


def render_analysis_view(session):
    active  = st.session_state.get("active_site", {})
    run_env = st.session_state["run_env"]

    schema         = run_env["current_schema"]
    prev_schema    = run_env.get("prev_schema")
    cutoff_date    = run_env.get("cutoff_date")
    lookback_years = run_env.get("lookback_years", 10)
    session_id     = run_env.get("session_id")

    sections_df = load_sections()

    # ── Restore previous session (instant, no re-execution) ───────────────────
    if run_env.get("restore_session_id"):
        restore_id = run_env["restore_session_id"]
        run_env["restore_session_id"] = None
        st.title("PCORNet Empirical Data Curation Report")
        with st.spinner("Restoring previous session…"):
            _restore_session(session, restore_id, sections_df)
        st.rerun()
        return

    # ── Execute missing checks (triggered from banner button) ─────────────────
    if run_env.get("auto_execute"):
        run_env["auto_execute"] = False
        st.title("PCORNet Empirical Data Curation Report")
        st.info("Running failed and missing checks — please wait…")
        _execute_missing(session, run_env, sections_df)
        st.rerun()
        return

    st.title("PCORNet Empirical Data Curation Report")

    # Initialise shared state buckets once
    for key in ("dq_results", "dq_statuses", "dq_durations"):
        if key not in st.session_state:
            st.session_state[key] = {}
    if "dq_run_id" not in st.session_state:
        st.session_state["dq_run_id"] = None

    total_ms = sum(
        int(v or 0)
        for key, val in st.session_state.items()
        if (key == "dq_durations" or key.endswith("_durations")) and isinstance(val, dict)
        for v in val.values()
    )

    info_parts = [f"▶ **Site:** {active['site_name']}", f"Schema: `{schema}`"]
    if prev_schema:
        info_parts.append(f"Prev: `{prev_schema}`")
    if cutoff_date:
        info_parts.append(f"Cutoff: `{cutoff_date}`")
    info_parts.append(f"Lookback: `{lookback_years}y`")
    if total_ms:
        info_parts.append(f"Total time: `{fmt_ms(total_ms)}`")
    st.info("  ·  ".join(info_parts))

    missing = _count_missing_results(sections_df)
    total_missing = missing["dq"] + missing["table"] + missing["chart"]
    if total_missing > 0:
        parts = []
        if missing["dq"]:    parts.append(f"DQ: {missing['dq']}")
        if missing["table"]: parts.append(f"Tables: {missing['table']}")
        if missing["chart"]: parts.append(f"Charts: {missing['chart']}")
        banner_col, btn_col = st.columns([6, 2])
        with banner_col:
            st.warning(
                f"**{total_missing} item(s)** failed or not yet run — "
                f"{' · '.join(parts)}. Click to run all failed and missing items."
            )
        with btn_col:
            if st.button(f"▶ Run Failed/Not Run ({total_missing})", key="rerun_session_btn", width="stretch"):
                run_env["auto_execute"] = True
                st.rerun()

    section_names = sections_df["Section"].unique().tolist()
    tab_labels    = ["DQ CHECKS"] + [s.split(":", 1)[1].strip() for s in section_names]

    # Restore active tab from URL on browser refresh (st.query_params survives refresh)
    if "active_tab" not in st.session_state:
        try:
            st.session_state["active_tab"] = int(st.query_params.get("tab", 0))
        except (ValueError, TypeError):
            st.session_state["active_tab"] = 0

    active_tab = max(0, min(st.session_state["active_tab"], len(section_names)))
    st.session_state["active_tab"] = active_tab

    # Custom tab navigation bar
    tab_cols = st.columns(len(tab_labels))
    for i, (col, label) in enumerate(zip(tab_cols, tab_labels)):
        with col:
            if st.button(
                label,
                key=f"nav_tab_{i}",
                type="primary" if i == active_tab else "secondary",
                width="stretch",
            ):
                st.session_state["active_tab"] = i
                st.query_params["tab"] = str(i)
                st.rerun()

    st.divider()

    # Render only the active view
    if active_tab == 0:
        render_dq_checks_view(session, schema, cutoff_date, prev_schema=prev_schema,
                              session_id=session_id, lookback_years=lookback_years)
    else:
        section_name = section_names[active_tab - 1]
        items = sections_df[sections_df["Section"] == section_name].reset_index(drop=True)
        render_section_view(session, section_name, items, schema, cutoff_date,
                            prev_schema, session_id=session_id,
                            lookback_years=lookback_years)
