import time

import pandas as pd
import streamlit as st

from services.section_service import run_section_item
from utils.constants import fmt_ms
from utils.db import get_meta_conn
from utils.run_repository import RunRepository
from utils.sql_loader import SqlLoader

_STATUS_BADGE = {"complete": "✅", "exception": "⚠️", "error": "❌", "pending": "⬜"}
_PAGE_SIZE = 10

_EXCEPTION_MARKERS = {
    "HAS_PK_ERROR":   "Yes",
    "REFRESH_STATUS": "Missing",
    "IS_OUTLIER":     "Yes",
}


def _pct_exception_styles(df: pd.DataFrame) -> pd.DataFrame:
    """Blue cell background for any PCT_CHANGE column where value < -5.0."""
    styles = pd.DataFrame("", index=df.index, columns=df.columns)
    pct_cols = [
        c for c in df.columns
        if "PCT_CHANGE" in (c[1] if isinstance(c, tuple) else c).upper()
    ]
    for col in pct_cols:
        for idx in df.index:
            try:
                if float(df.at[idx, col]) < -5.0:
                    styles.at[idx, col] = "background-color: #cfe2ff"
            except (ValueError, TypeError):
                pass
    return styles


def _slug(section_name: str) -> str:
    return section_name.split(":")[0].strip().lower().replace(" ", "_")


def _pagination_bar(page: int, total_pages: int, total_items: int, key_prefix: str, position: str = "top") -> None:
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


def _ensure_run_id(slug: str, session, schema: str, cutoff_date,
                   network_id: str, site_id: str,
                   session_id: str | None = None,
                   prev_schema: str | None = None) -> str:
    key = f"{slug}_run_id"
    if not st.session_state.get(key):
        run_id = RunRepository(get_meta_conn()).begin_run(
            network_id, site_id, schema, cutoff_date=cutoff_date,
            triggered_by=slug, session_id=session_id, prev_schema=prev_schema
        )
        st.session_state[key] = run_id
    return st.session_state[key]


def _count_exceptions(result: list) -> int:
    if not result:
        return 0
    total = 0
    for row in result:
        d = row if isinstance(row, dict) else {col: getattr(row, col) for col in row._fields}
        if any(str(d.get(col, "")) == val for col, val in _EXCEPTION_MARKERS.items()):
            total += 1
    return total


def _section_summary(slug: str, runnable: pd.DataFrame) -> None:
    statuses = st.session_state.get(f"{slug}_statuses", {})
    results  = st.session_state.get(f"{slug}_results",  {})
    ok = exc = err = pend = 0
    for _, row in runnable.iterrows():
        k = row["Table"]
        item_status = statuses.get(k, "pending")
        if item_status == "error":
            err += 1
        elif item_status in ("complete", "exception"):
            n_exc = _count_exceptions(results.get(k) or [])
            if n_exc:
                exc += 1
            else:
                ok += 1
        else:
            pend += 1
    parts = []
    if ok:   parts.append(f"✅ {ok} OK")
    if exc:  parts.append(f"⚠️ {exc} Exception{'s' if exc > 1 else ''}")
    if err:  parts.append(f"❌ {err} Error{'s' if err > 1 else ''}")
    if pend: parts.append(f"⬜ {pend} Not Run")
    if parts:
        color = "#fff3cd" if exc else ("#fde8e8" if err else "#d4edda")
        st.markdown(
            f'<div style="background:{color};padding:8px 12px;border-radius:6px;'
            f'margin-bottom:8px;">{" · ".join(parts)}</div>',
            unsafe_allow_html=True,
        )


def _render_chart(records: list, item_name: str):
    if not records:
        st.info("No data returned.")
        return
    dicts = records if isinstance(records[0], dict) else \
            [{col: getattr(r, col) for col in r._fields} for r in records]
    df = pd.DataFrame(dicts)
    date_cols = [
        c for c in df.columns
        if any(k in c.upper() for k in ("DATE", "MONTH", "YEAR", "PERIOD"))
    ]
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if date_cols and numeric_cols:
        st.line_chart(df.set_index(date_cols[0])[numeric_cols])
    elif len(df.columns) >= 2 and numeric_cols:
        st.bar_chart(df.set_index(df.columns[0])[numeric_cols])
    else:
        st.info("No chart available for this data layout.")
    with st.expander("Raw data", expanded=False):
        st.dataframe(df, width="stretch", hide_index=True)


def _render_result(result: list, item_key: str, is_chart: bool, force_expand: bool = False) -> None:
    n_exc = _count_exceptions(result)
    with st.expander("Results", expanded=force_expand or bool(n_exc or not result)):
        if not result:
            st.info("No data returned.")
            return
        if is_chart:
            _render_chart(result, item_key)
            return
        dicts = result if isinstance(result[0], dict) else \
                [{col: getattr(r, col) for col in r._fields} for r in result]
        column_order = None
        if dicts and isinstance(dicts[-1], dict) and "__columns__" in dicts[-1]:
            column_order = dicts[-1]["__columns__"]
            dicts = dicts[:-1]
        df = pd.DataFrame(dicts)
        if column_order:
            ordered = [c for c in column_order if c in df.columns]
            df = df[ordered + [c for c in df.columns if c not in ordered]]

        # Build MultiIndex display DataFrame if any column uses __ grouping separator
        if any("__" in c for c in df.columns):
            display_df = df.copy()
            display_df.columns = pd.MultiIndex.from_tuples([
                tuple(c.split("__", 1)) if "__" in c else ("", c)
                for c in df.columns
            ])
        else:
            display_df = df

        def _highlight(row):
            is_exc = any(
                str(row.get(col, "")) == val
                for col, val in _EXCEPTION_MARKERS.items()
                if col in row.index
            )
            return ["background-color: #fde8e8" if is_exc else ""] * len(row)

        has_pct_cols = any(
            "PCT_CHANGE" in (c[1] if isinstance(c, tuple) else c).upper()
            for c in display_df.columns
        )
        if n_exc or has_pct_cols:
            styled = display_df.style.apply(_highlight, axis=1)
            if has_pct_cols:
                styled = styled.apply(_pct_exception_styles, axis=None)
            styled = styled.format(na_rep="---")
        else:
            styled = display_df
        st.dataframe(styled, width="stretch", hide_index=True)
        st.download_button(
            "⬇ Download CSV",
            df.to_csv(index=False),
            file_name=f"{item_key.lower().replace(' ', '_')}.csv",
            mime="text/csv",
            key=f"dl_{item_key}",
        )


def render_section_view(session, section_name: str, items_df: pd.DataFrame,
                        schema: str, cutoff_date, prev_schema: str | None = None,
                        session_id: str | None = None):
    active = st.session_state.get("active_site", {})
    network_id = active.get("network_id", "")
    site_id = active.get("site_id", "")
    db_name = active.get("database_name", "")
    slug = _slug(section_name)

    for key in (f"{slug}_results", f"{slug}_statuses", f"{slug}_durations"):
        if key not in st.session_state:
            st.session_state[key] = {}
    if f"{slug}_run_id" not in st.session_state:
        st.session_state[f"{slug}_run_id"] = None

    # Handle navigation from DQ checks: clear filters and queue a page jump
    _incoming_target = st.session_state.pop("section_target", None)
    if _incoming_target:
        st.session_state[f"{slug}_filter_type"] = []
        st.session_state[f"{slug}_filter_status"] = []
        st.session_state[f"{slug}_pending_target"] = _incoming_target

    runnable = items_df[
        items_df["SQL_File_Ref"].notna()
        & (items_df["SQL_File_Ref"].str.lower().str.strip() != "n/a")
    ]

    done = sum(
        1 for _, r in runnable.iterrows()
        if st.session_state[f"{slug}_statuses"].get(r["Table"]) in ("complete", "exception")
    )

    # Pending = never run OR errored
    pending_items = runnable[
        runnable.apply(
            lambda r: st.session_state[f"{slug}_statuses"].get(r["Table"], "pending")
            not in ("complete", "exception"),
            axis=1,
        )
    ]

    _section_summary(slug, runnable)

    # ── Filters ────────────────────────────────────────────────────────────────
    _STATUS_FILTER_MAP = {"Pass": "complete", "Fail": "error", "Not Run": "pending"}
    col_type, col_status = st.columns(2)
    with col_type:
        sel_type = st.multiselect(
            "Type", ["Table", "Chart"],
            placeholder="All types",
            key=f"{slug}_filter_type",
        )
    with col_status:
        sel_status = st.multiselect(
            "Pass / Fail", list(_STATUS_FILTER_MAP.keys()),
            placeholder="All results",
            key=f"{slug}_filter_status",
        )

    filtered_items = items_df.copy()
    if sel_type:
        filtered_items = filtered_items[
            filtered_items["Table"].apply(
                lambda k: ("Chart" if str(k).lower().startswith("chart") else "Table") in sel_type
            )
        ]
    if sel_status:
        sel_statuses = {_STATUS_FILTER_MAP[r] for r in sel_status}
        filtered_items = filtered_items[
            filtered_items["Table"].apply(
                lambda k: st.session_state[f"{slug}_statuses"].get(k, "pending") in sel_statuses
            )
        ]

    # Reset page when filter changes
    filter_key = (tuple(sel_type), tuple(sel_status))
    if st.session_state.get(f"{slug}_filter_key") != filter_key:
        st.session_state[f"{slug}_filter_key"] = filter_key
        st.session_state[f"{slug}_page"] = 0

    # Apply pending navigation target (after filter reset, so it wins)
    _pending_nav = st.session_state.pop(f"{slug}_pending_target", None)
    if _pending_nav:
        _target_rows = items_df[items_df["Table"] == _pending_nav]
        if not _target_rows.empty:
            st.session_state[f"{slug}_page"] = _target_rows.index[0] // _PAGE_SIZE
        st.session_state[f"{slug}_highlight"] = _pending_nav

    # ── Run buttons (always both visible) ─────────────────────────────────────
    col_summary, col_pending, col_rerun = st.columns([5, 2, 2])
    col_summary.caption(
        f"Showing **{len(filtered_items)}** of {len(items_df)} items  ·  "
        f"{done}/{len(runnable)} complete"
    )
    run_pending_clicked = col_pending.button(
        f"▶ Run Pending ({len(pending_items)})",
        key=f"{slug}_run_pending_btn",
        type="primary",
        disabled=len(pending_items) == 0,
        width="stretch",
    )
    rerun_all_clicked = col_rerun.button(
        "↺ Re-Run All",
        key=f"{slug}_rerun_all_btn",
        disabled=len(runnable) == 0,
        width="stretch",
    )

    def _run_items(rows):
        run_id = _ensure_run_id(slug, session, schema, cutoff_date, network_id, site_id, session_id, prev_schema)
        progress = st.progress(0, text="Running…")
        total = len(rows)
        for i, (_, row) in enumerate(rows.iterrows()):
            item_key = row["Table"]
            t0 = time.perf_counter()
            success, result = run_section_item(
                session, row["SQL_File_Ref"], schema, db_name,
                run_id, item_key, network_id, site_id,
                prev_schema=prev_schema,
                cutoff_date=cutoff_date,
            )
            st.session_state[f"{slug}_durations"][item_key] = int((time.perf_counter() - t0) * 1000)
            st.session_state[f"{slug}_results"][item_key] = result
            n_exc = _count_exceptions(result)
            badge_status = "exception" if (success and n_exc > 0) else ("complete" if success else "error")
            st.session_state[f"{slug}_statuses"][item_key] = badge_status
            progress.progress((i + 1) / total, text=f"Ran {item_key} ({i + 1}/{total})")
        RunRepository(get_meta_conn()).complete_run(st.session_state[f"{slug}_run_id"], "COMPLETE")
        st.rerun()

    if run_pending_clicked and len(runnable) > 0:
        _run_items(pending_items)
    if rerun_all_clicked and len(runnable) > 0:
        _run_items(runnable)

    st.divider()

    # ── Pagination (on filtered items) ─────────────────────────────────────────
    total_items = len(filtered_items)
    total_pages = max(1, (total_items + _PAGE_SIZE - 1) // _PAGE_SIZE)
    page_key = f"{slug}_page"
    page = min(st.session_state.get(page_key, 0), total_pages - 1)
    st.session_state[page_key] = page

    if total_pages > 1:
        _pagination_bar(page, total_pages, total_items, slug, "top")

    page_items_df = filtered_items.iloc[page * _PAGE_SIZE:(page + 1) * _PAGE_SIZE]

    for _, row in page_items_df.iterrows():
        item_key = row["Table"]
        description = str(row["Table Description"])
        data_checks = str(row.get("Data Check(s)", "n/a"))
        sql_file = str(row.get("SQL_File_Ref", "n/a")).strip()
        has_sql = sql_file.lower() not in ("n/a", "nan", "")
        is_chart = item_key.lower().startswith("chart")

        status = st.session_state[f"{slug}_statuses"].get(item_key, "pending")
        badge = _STATUS_BADGE.get(status, "⬜")

        title_col, sql_col, btn_col = st.columns([8, 1, 1])
        with title_col:
            st.markdown(f"**{item_key}** — {description}")
            dur = st.session_state[f"{slug}_durations"].get(item_key)
            caption_parts = []
            if dur is not None:
                caption_parts.append(fmt_ms(dur))
            if data_checks.lower() not in ("n/a", "nan"):
                caption_parts.append(f"Data checks: {data_checks}")
            if caption_parts:
                st.caption("  ·  ".join(caption_parts))
        with sql_col:
            if has_sql:
                toggle_key = f"show_sql_{slug}_{item_key}"
                if st.button("SQL", key=f"sql_btn_{slug}_{item_key}"):
                    st.session_state[toggle_key] = not st.session_state.get(toggle_key, False)
        with btn_col:
            if has_sql and st.button("Re-Run" if status != "pending" else "Run", key=f"{slug}_{item_key}_run"):
                run_id = _ensure_run_id(slug, session, schema, cutoff_date,
                                        network_id, site_id, session_id, prev_schema)
                t0 = time.perf_counter()
                success, result = run_section_item(
                    session, sql_file, schema, db_name,
                    run_id, item_key, network_id, site_id,
                    prev_schema=prev_schema,
                    cutoff_date=cutoff_date,
                )
                st.session_state[f"{slug}_durations"][item_key] = int((time.perf_counter() - t0) * 1000)
                st.session_state[f"{slug}_results"][item_key] = result
                n_exc = _count_exceptions(result)
                badge_status = "exception" if (success and n_exc > 0) else ("complete" if success else "error")
                st.session_state[f"{slug}_statuses"][item_key] = badge_status
                st.rerun()

        _is_highlight = st.session_state.get(f"{slug}_highlight") == item_key
        if _is_highlight:
            st.session_state.pop(f"{slug}_highlight", None)
            st.info("↑ Navigated here from a DQ check")

        if has_sql and st.session_state.get(f"show_sql_{slug}_{item_key}"):
            try:
                _sql_params = SqlLoader.build_display_params(schema, db_name, cutoff_date, prev_schema or "")
                st.code(SqlLoader.load_sql(sql_file, **_sql_params), language="sql")
            except Exception:
                try:
                    st.code(SqlLoader.load_raw(sql_file), language="sql")
                except Exception:
                    st.warning("SQL file not found.")

        results = st.session_state[f"{slug}_results"]
        if item_key in results:
            result = results[item_key]
            if result is None:
                st.caption("Result not stored — click Run to re-execute.")
            else:
                _render_result(result, item_key, is_chart, force_expand=_is_highlight)

        st.divider()

    if total_pages > 1:
        _pagination_bar(page, total_pages, total_items, slug, "bottom")
