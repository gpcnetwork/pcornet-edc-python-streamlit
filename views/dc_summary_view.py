import pandas as pd
import streamlit as st

from services.dc_summary_service import (
    SEVERITY_BLUE,
    SEVERITY_RED,
    SUMMARY_COLUMNS,
    build_dc_summary,
)
from services.dq_checks_service import load_dq_checks
from services.report_text import get_report_text, load_report_text
from services.section_service import load_sections
from utils.constants import NETWORK_BENCHMARK_ASOF

_RED_CSS = "color: #b30000; font-weight: 700; background-color: #fde8e8"
_BLUE_CSS = "color: #0b5ed7; font-weight: 700; background-color: #cfe2ff"


@st.cache_data
def _report_texts() -> dict:
    return load_report_text()


@st.cache_data
def _table_to_tab() -> dict:
    """EDC table name -> (section tab index, section name). Tab 0 = DC SUMMARY, 1 = DQ CHECKS."""
    df = load_sections()
    section_names = df["Section"].unique().tolist()
    return {
        str(row["Table"]): (section_names.index(row["Section"]) + 2, row["Section"])
        for _, row in df.iterrows()
    }


def _style_exception(df: pd.DataFrame, severities: pd.Series) -> pd.DataFrame:
    styles = pd.DataFrame("", index=df.index, columns=df.columns)
    for idx in df.index:
        sev = severities.loc[idx]
        if sev == SEVERITY_RED:
            styles.at[idx, "Exception?"] = _RED_CSS
        elif sev == SEVERITY_BLUE:
            styles.at[idx, "Exception?"] = _BLUE_CSS
    return styles


def render_dc_summary_view():
    texts = _report_texts()
    dc_text = get_report_text(texts, "DC Summary")

    st.subheader(dc_text["Title"] or "Data Check Exception Summary")
    if dc_text["Description_Paragraph"]:
        st.markdown(dc_text["Description_Paragraph"])

    dq_df = load_dq_checks()
    statuses = st.session_state.get("dq_statuses", {})
    summary = build_dc_summary(dq_df, statuses)

    n_yes = int((summary["Exception?"] == "Yes").sum())
    n_no = int((summary["Exception?"] == "No").sum())
    n_na = int((summary["Exception?"] == "n/a").sum())
    n_other = len(summary) - n_yes - n_no - n_na
    st.caption(
        f"**{len(summary)}** data checks  ·  "
        f"🔴/🔵 Exceptions: **{n_yes}**  ·  No exception: **{n_no}**  ·  "
        f"n/a: **{n_na}**  ·  Not run / error: **{n_other}**"
    )

    display = summary[SUMMARY_COLUMNS]
    severities = summary["_severity"]
    styled = (
        display.style
        .apply(lambda _: _style_exception(display, severities), axis=None)
        .format(na_rep="---")
    )
    st.dataframe(styled, width="stretch", hide_index=True, height=600)

    if dc_text["Footnote"]:
        st.caption(dc_text["Footnote"])
    st.caption(
        f"“% DataMarts w/ exceptions” is the PCORnet network benchmark as of "
        f"{NETWORK_BENCHMARK_ASOF} (shipped with the v7.03 query package); "
        f"“---” = not comparable."
    )

    # ── Quick links for checks needing attention ──────────────────────────────
    attention = summary[summary["Exception?"].isin(["Yes", "Error"])]
    if attention.empty:
        return
    st.divider()
    st.markdown("**Checks needing attention**")
    table_to_tab = _table_to_tab()
    for _, row in attention.iterrows():
        check_label = row["Data Check"]
        edc_table = row["EDC Table"]
        c1, c2, c3 = st.columns([6, 1.5, 1.5])
        with c1:
            sev = "🔴" if row["_severity"] == SEVERITY_RED else "🔵"
            marker = sev if row["Exception?"] == "Yes" else "⚠️"
            st.markdown(f"{marker} **{check_label}** · {edc_table} · {row['Type']}")
        safe = f"{row['_check_num']}_{edc_table}".replace(" ", "_")
        with c2:
            if st.button("View check", key=f"dcsum_check_{safe}"):
                st.session_state["dq_filter_edc"] = [edc_table]
                st.session_state["active_tab"] = 1
                st.query_params["tab"] = "1"
                st.rerun()
        with c3:
            if edc_table in table_to_tab and st.button("View table", key=f"dcsum_tbl_{safe}"):
                tab_idx, _section = table_to_tab[edc_table]
                st.session_state["active_tab"] = tab_idx
                st.query_params["tab"] = str(tab_idx)
                st.session_state["section_target"] = edc_table
                st.rerun()
