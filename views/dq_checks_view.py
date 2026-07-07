import time

import pandas as pd
import streamlit as st

from services.dq_checks_service import load_dq_checks, run_single_check
from services.section_service import load_sections
from utils.constants import fmt_ms
from utils.db import get_meta_conn
from utils.run_repository import RunRepository
from utils.sql_loader import SqlLoader

_STATUS_BADGE = {"complete": "✅", "fail": "❌", "error": "❌", "pending": "⬜"}
_PAGE_SIZE = 10

# Maps the user-visible Pass/Fail labels to internal status values
# "fail"  = check ran OK, result STATUS = Fail
# "error" = SQL execution error
_RESULT_LABELS = {"Pass": "complete", "Fail": "fail", "Not Run": "pending"}


def _dq_status(success: bool, result) -> str:
    """Derive status from run outcome and the result STATUS column value."""
    if not success:
        return "error"
    try:
        row = result[0] if result else None
        if row is None:
            return "complete"
        val = row.get("STATUS") if isinstance(row, dict) else getattr(row, "STATUS", None)
        return "fail" if str(val).strip() == "Fail" else "complete"
    except Exception:
        return "complete"


@st.cache_data
def _build_check_to_sections() -> dict:
    """Reverse mapping: check_num → [(section_name, table_name, table_desc, tab_idx)]"""
    df = load_sections()
    section_names = df["Section"].unique().tolist()
    mapping: dict = {}
    for _, row in df.iterrows():
        checks_str = str(row.get("Data Check(s)", "")).strip()
        if checks_str.lower() in ("n/a", "nan", ""):
            continue
        tab_idx = section_names.index(row["Section"]) + 1  # tab 0 = DQ CHECKS
        for check_num in [c.strip() for c in checks_str.split(",")]:
            if check_num:
                mapping.setdefault(check_num, []).append(
                    (row["Section"], str(row["Table"]), str(row["Table Description"]), tab_idx)
                )
    return mapping


def _pagination_bar(page: int, total_pages: int, total_items: int, key_prefix: str, position: str = "top") -> int:
    """Render prev/next controls; return the (possibly updated) page index."""
    prev_col, info_col, next_col = st.columns([1, 4, 1])
    with prev_col:
        if st.button("← Prev", key=f"{key_prefix}_prev_{position}", disabled=page == 0):
            st.session_state[f"{key_prefix}_page"] = page - 1
            st.rerun()
    with info_col:
        start = page * _PAGE_SIZE + 1
        end = min((page + 1) * _PAGE_SIZE, total_items)
        st.caption(f"Page **{page + 1}** of {total_pages}  ·  showing {start}–{end} of {total_items}")
    with next_col:
        if st.button("Next →", key=f"{key_prefix}_next_{position}", disabled=page == total_pages - 1):
            st.session_state[f"{key_prefix}_page"] = page + 1
            st.rerun()
    return st.session_state.get(f"{key_prefix}_page", page)


def _row_key(row) -> str:
    """Unique key per CSV row — DC 3.03 appears twice with different EDC tables."""
    return f"{row['Data Check']}|{row['EDC Table']}"


def _ensure_run_id(session, schema: str, cutoff_date, network_id: str, site_id: str,
                   session_id: str | None = None,
                   prev_schema: str | None = None,
                   lookback_years: int = 10) -> str:
    if not st.session_state.get("dq_run_id"):
        run_id = RunRepository(get_meta_conn()).begin_run(
            network_id, site_id, schema, cutoff_date=cutoff_date,
            triggered_by="dq_checks", session_id=session_id, prev_schema=prev_schema,
            lookback_years=lookback_years,
        )
        st.session_state["dq_run_id"] = run_id
    return st.session_state["dq_run_id"]


def render_dq_checks_view(session, schema: str, cutoff_date,
                           prev_schema: str | None = None,
                           session_id: str | None = None,
                           lookback_years: int = 10):
    active = st.session_state.get("active_site", {})
    network_id = active.get("network_id", "")
    site_id = active.get("site_id", "")
    db_name = active.get("database_name", "")

    dq_df = load_dq_checks()

    # ── Filter controls (2 × 2 grid) ─────────────────────────────────────────
    col_cat, col_type = st.columns(2)
    col_edc, col_result = st.columns(2)

    with col_cat:
        all_categories = sorted(dq_df["Category"].dropna().unique().tolist())
        sel_categories = st.multiselect(
            "Category",
            options=all_categories,
            default=[],
            placeholder="All categories",
            key="dq_filter_category",
        )

    with col_type:
        all_types = sorted(dq_df["Type"].dropna().unique().tolist())
        sel_types = st.multiselect(
            "Type",
            options=all_types,
            default=[],
            placeholder="All types",
            key="dq_filter_type",
        )

    with col_edc:
        all_edc = sorted(dq_df["EDC Table"].dropna().unique().tolist())
        sel_edc = st.multiselect(
            "EDC Table",
            options=all_edc,
            default=[],
            placeholder="All EDC tables",
            key="dq_filter_edc",
        )

    with col_result:
        all_results = list(_RESULT_LABELS.keys())   # ["Pass", "Fail", "Not Run"]
        sel_results = st.multiselect(
            "Pass / Fail",
            options=all_results,
            default=[],
            placeholder="All results",
            key="dq_filter_result",
        )

    # Build a status series from current session state for vectorised filtering
    status_series = dq_df.apply(
        lambda r: st.session_state["dq_statuses"].get(_row_key(r), "pending"), axis=1
    )

    # Empty selection = no filter on that dimension (show all)
    filtered_df = dq_df.copy()
    if sel_categories:
        filtered_df = filtered_df[filtered_df["Category"].isin(sel_categories)]
    if sel_types:
        filtered_df = filtered_df[filtered_df["Type"].isin(sel_types)]
    if sel_edc:
        filtered_df = filtered_df[filtered_df["EDC Table"].isin(sel_edc)]
    if sel_results:
        sel_statuses = {_RESULT_LABELS[r] for r in sel_results}
        if "fail" in sel_statuses:   # also surface SQL-error checks under "Fail"
            sel_statuses.add("error")
        filtered_df = filtered_df[status_series.reindex(filtered_df.index).isin(sel_statuses)]

    # "Run All" always runs every check regardless of current filters
    all_runnable = dq_df[
        dq_df["SQL_File_Ref"].notna()
        & (dq_df["SQL_File_Ref"].str.lower().str.strip() != "n/a")
    ]

    # ── Summary + Run buttons ──────────────────────────────────────────────────
    total_done = sum(
        1 for _, r in dq_df.iterrows()
        if st.session_state["dq_statuses"].get(_row_key(r)) in ("complete", "fail")
    )

    # Pending = never run, SQL error, or DQ Fail result (all re-runnable)
    pending_runnable = all_runnable[
        all_runnable.apply(
            lambda r: st.session_state["dq_statuses"].get(_row_key(r), "pending")
                      != "complete",
            axis=1,
        )
    ]
    has_pending = len(pending_runnable) > 0

    st.caption(
        f"Showing **{len(filtered_df)}** of {len(dq_df)} checks  ·  "
        f"**{total_done}/{len(all_runnable)}** complete overall"
    )

    st.markdown("""
    <style>
    [data-testid="stBaseButton-primary"] {
        background-color: #2ea043 !important;
        border-color: #2ea043 !important;
        font-size: 1.1rem !important;
        font-weight: 700 !important;
        letter-spacing: 0.04em !important;
        padding: 0.65rem 1.5rem !important;
    }
    [data-testid="stBaseButton-primary"]:hover {
        background-color: #3fb950 !important;
        border-color: #3fb950 !important;
    }
    </style>
    """, unsafe_allow_html=True)

    _, lc, rc, _ = st.columns([2, 2, 2, 2])
    run_pending_clicked = lc.button(
        f"▶  Run Failed/Not Run ({len(pending_runnable)})",
        key="dq_run_pending_btn",
        type="primary",
        disabled=len(pending_runnable) == 0,
        width="stretch",
    )
    rerun_all_clicked = rc.button(
        "↺  Re-Run All",
        key="dq_rerun_all_btn",
        disabled=len(all_runnable) == 0,
        width="stretch",
    )

    def _run_checks(rows):
        run_id = _ensure_run_id(session, schema, cutoff_date, network_id, site_id,
                                session_id, prev_schema, lookback_years)
        progress = st.progress(0, text="Running checks…")
        total = len(rows)
        for i, (_, row) in enumerate(rows.iterrows()):
            rk = _row_key(row)
            t0 = time.perf_counter()
            success, result = run_single_check(
                session, row, schema, db_name, run_id, network_id, site_id,
                cutoff_date=cutoff_date, prev_schema=prev_schema,
                lookback_years=lookback_years,
            )
            st.session_state["dq_durations"][rk] = int((time.perf_counter() - t0) * 1000)
            st.session_state["dq_results"][rk] = result
            st.session_state["dq_statuses"][rk] = _dq_status(success, result)
            progress.progress((i + 1) / total, text=f"Ran {row['Data Check']} ({i + 1}/{total})")
        RunRepository(get_meta_conn()).complete_run(run_id, "COMPLETE")
        st.rerun()

    if run_pending_clicked:
        _run_checks(pending_runnable)
    if rerun_all_clicked:
        _run_checks(all_runnable)

    st.divider()

    # ── Pagination ─────────────────────────────────────────────────────────────
    filter_key = (tuple(sel_categories), tuple(sel_types), tuple(sel_edc), tuple(sel_results))
    if st.session_state.get("dq_filter_key") != filter_key:
        st.session_state["dq_filter_key"] = filter_key
        st.session_state["dq_page"] = 0

    total_filtered = len(filtered_df)
    total_pages = max(1, (total_filtered + _PAGE_SIZE - 1) // _PAGE_SIZE)
    page = min(st.session_state.get("dq_page", 0), total_pages - 1)
    st.session_state["dq_page"] = page

    if total_pages > 1:
        _pagination_bar(page, total_pages, total_filtered, "dq", "top")

    page_df = filtered_df.iloc[page * _PAGE_SIZE:(page + 1) * _PAGE_SIZE]

    check_to_sections = _build_check_to_sections()
    sql_params = SqlLoader.build_display_params(schema, db_name, cutoff_date,
                                                prev_schema or "", lookback_years)

    # ── Paged check list ───────────────────────────────────────────────────────
    for _, row in page_df.iterrows():
        rk = _row_key(row)
        check_id = row["Data Check"]
        status = st.session_state["dq_statuses"].get(rk, "pending")
        sql_file = str(row.get("SQL_File_Ref", "")).strip()
        has_sql = sql_file.lower() not in ("n/a", "nan", "")

        safe_rk = rk.replace(" ", "_").replace("|", "_")
        hdr_col, badge_col, sql_col, btn_col = st.columns([7, 1, 1, 1])
        with hdr_col:
            st.markdown(
                f"**{check_id}** · *{row['Category']}* · {row['Type']}\n\n"
                f"{row['Data Check Description']}\n\n"
                f"<small>EDC Table: {row['EDC Table']}</small>",
                unsafe_allow_html=True,
            )
        with badge_col:
            st.write(_STATUS_BADGE.get(status, "⬜"))
            dur = st.session_state["dq_durations"].get(rk)
            if dur is not None:
                st.caption(fmt_ms(dur))
        with sql_col:
            if has_sql:
                toggle_key = f"show_sql_{rk}"
                if st.button("SQL", key=f"sql_btn_{safe_rk}"):
                    st.session_state[toggle_key] = not st.session_state.get(toggle_key, False)
        with btn_col:
            if has_sql and st.button("Re-Run" if status != "pending" else "Run", key=f"dq_run_{safe_rk}"):
                run_id = _ensure_run_id(session, schema, cutoff_date,
                                        network_id, site_id, session_id, prev_schema,
                                        lookback_years)
                t0 = time.perf_counter()
                success, result = run_single_check(
                    session, row, schema, db_name, run_id, network_id, site_id,
                    cutoff_date=cutoff_date, prev_schema=prev_schema,
                    lookback_years=lookback_years,
                )
                st.session_state["dq_durations"][rk] = int((time.perf_counter() - t0) * 1000)
                st.session_state["dq_results"][rk] = result
                st.session_state["dq_statuses"][rk] = _dq_status(success, result)
                st.rerun()

        if has_sql and st.session_state.get(f"show_sql_{rk}"):
            try:
                st.code(SqlLoader.load_sql(sql_file, **sql_params), language="sql")
            except Exception:
                try:
                    st.code(SqlLoader.load_raw(sql_file), language="sql")
                except Exception:
                    st.warning("SQL file not found.")

        # ── Related section tables / charts ────────────────────────────────────
        check_num = check_id.replace("DC ", "").strip()
        associated = check_to_sections.get(check_num, [])
        if associated:
            n_cols = min(len(associated), 5)
            link_cols = st.columns(n_cols)
            for i, (sec_name, tbl_name, tbl_desc, tab_idx) in enumerate(associated):
                with link_cols[i % n_cols]:
                    if st.button(
                        f"→ {tbl_name}",
                        key=f"nav_{safe_rk}_{i}",
                        help=f"{sec_name}\n{tbl_desc}",
                    ):
                        st.session_state["active_tab"] = tab_idx
                        st.query_params["tab"] = str(tab_idx)
                        st.session_state["section_target"] = tbl_name
                        st.rerun()

        if rk in st.session_state["dq_results"]:
            result = st.session_state["dq_results"][rk]
            if result is None:
                st.caption("Result not stored — click Run to re-execute.")
            else:
                if result:
                    dicts = result if isinstance(result[0], dict) else \
                            [{col: getattr(r, col) for col in r._fields} for r in result]
                    detail_dicts = [d for d in dicts if str(d.get("ROW_TYPE", "")).upper() == "DETAIL"]
                    has_detail_schema = "ROW_TYPE" in dicts[0]
                    if has_detail_schema:
                        label = f"Exceptions ({len(detail_dicts)})"
                        with st.expander(label, expanded=bool(detail_dicts)):
                            if detail_dicts:
                                cols = [c for c in ("EXC_TABLE", "EXC_FIELD", "EXC_DETAIL", "EXC_COUNT")
                                        if c in detail_dicts[0]]
                                detail_df = pd.DataFrame(detail_dicts)[cols]
                                # Hide columns that are empty for this check
                                # (e.g. Field/Count for the missing-table checks).
                                detail_df = detail_df.replace("", pd.NA).dropna(axis=1, how="all")
                                detail_df = detail_df.rename(columns={
                                    "EXC_TABLE": "Table",
                                    "EXC_FIELD": "Field",
                                    "EXC_DETAIL": "Issue",
                                    "EXC_COUNT": "Count",
                                })
                                st.dataframe(detail_df, width="stretch", hide_index=True)
                            else:
                                st.info("No exceptions — check passed.")
                    else:
                        label = f"Results ({len(result)} rows)"
                        with st.expander(label, expanded=True):
                            st.dataframe(pd.DataFrame(dicts), width="stretch", hide_index=True)
                else:
                    with st.expander("Results (0 rows)", expanded=False):
                        st.info("No records returned.")

        st.divider()

    if total_pages > 1:
        _pagination_bar(page, total_pages, total_filtered, "dq", "bottom")
