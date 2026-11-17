import pandas as pd
import streamlit as st
import altair as alt
from views.renderer.charts.base import zero_rule, zscore_axes
from views.renderer.charts.common import (
    ENC_PANELS,
    IC_ENC_TYPES,
    enc_labels_palette,
    prepare_zscore_df,
)


def _render_zscore_trend(df: pd.DataFrame, date_col: str) -> None:
    """Single-series Z-score trend chart (e.g. Chart IA – VITAL)."""

    df = prepare_zscore_df(df, date_col)

    x, y = zscore_axes(date_col, x_title=date_col)
    tooltip = [
        alt.Tooltip(f"{date_col}:T", title=date_col, format="%b %Y"),
        alt.Tooltip("Z_SCORE:Q",     title="Z-Score",     format=".4f"),
    ]
    if "RECORD_COUNT" in df.columns:
        tooltip.append(alt.Tooltip("RECORD_COUNT:Q", title="Record Count", format=","))

    base  = alt.Chart(df)
    lines = base.mark_line(color="#2166ac", strokeWidth=2.5).encode(x=x, y=y, tooltip=tooltip)
    pts   = base.mark_point(color="#2166ac", size=40, filled=True, opacity=0.85).encode(
        x=x, y=y, tooltip=tooltip
    )
    chart = (zero_rule() + lines + pts).properties(height=380)
    st.altair_chart(chart.interactive(), width="stretch")


def _render_encounter_trend(df: pd.DataFrame, date_col: str) -> None:
    """3-panel Z-score trend chart for Chart IB (Encounter by ENC_TYPE)."""

    df = prepare_zscore_df(df, date_col)

    x, y = zscore_axes(date_col, x_title=date_col)
    x = x.copy()

    for panel_title, enc_types in ENC_PANELS:
        labels, palette = enc_labels_palette(enc_types)
        subset = df[df["ENC_TYPE"].isin(enc_types)]

        st.markdown(f"**{panel_title}**")
        if subset.empty:
            st.info("No data.")
            continue

        tooltip = [
            alt.Tooltip(f"{date_col}:T", title=date_col, format="%b %Y"),
            alt.Tooltip("ENC_LABEL:N",   title="Type"),
            alt.Tooltip("Z_SCORE:Q",     title="Z-Score",  format=".4f"),
            alt.Tooltip("RECORD_COUNT:Q", title="Records", format=","),
        ]

        lines = (
            alt.Chart(subset)
            .mark_line(strokeWidth=2,
                       point=alt.OverlayMarkDef(size=30, filled=True))
            .encode(
                x=x, y=y,
                color=alt.Color(
                    "ENC_LABEL:N",
                    title=None,
                    scale=alt.Scale(domain=labels, range=palette),
                    legend=alt.Legend(
                        orient="bottom",
                        columns=1,
                        labelFontSize=11,
                        symbolSize=80,
                    ),
                ),
                tooltip=tooltip,
            )
        )

        chart = (
            (zero_rule() + lines)
            .properties(height=340)
            .interactive()
        )
        st.altair_chart(chart, width="stretch")


def _render_institutional_encounter_trend(df: pd.DataFrame, date_col: str) -> None:
    """Single full-width multi-series Z-score chart for Chart IC (EI, IP, IS by DISCHARGE_DATE)."""

    df = prepare_zscore_df(df, date_col)

    labels, palette = enc_labels_palette(IC_ENC_TYPES)

    x, y = zscore_axes(date_col, x_title=date_col)

    tooltip = [
        alt.Tooltip(f"{date_col}:T", title=date_col, format="%b %Y"),
        alt.Tooltip("ENC_LABEL:N",   title="Type"),
        alt.Tooltip("Z_SCORE:Q",     title="Z-Score",         format=".4f"),
        alt.Tooltip("RECORD_COUNT:Q", title="Records",        format=","),
    ]

    lines = (
        alt.Chart(df)
        .mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=30, filled=True))
        .encode(
            x=x, y=y,
            color=alt.Color(
                "ENC_LABEL:N",
                title=None,
                scale=alt.Scale(domain=labels, range=palette),
                legend=alt.Legend(orient="bottom", columns=3, labelFontSize=11, symbolSize=80),
            ),
            tooltip=tooltip,
        )
    )

    chart = (zero_rule() + lines).properties(height=380).interactive()
    st.altair_chart(chart, width="stretch")


def _render_grouped_zscore_trend(df: pd.DataFrame, date_col: str, group_col: str) -> None:
    """Multi-series Z-score chart grouped by a categorical column (e.g. DEATH_SOURCE)."""

    df = prepare_zscore_df(df, date_col, group_col=group_col)
    label_col = f"{group_col}_LABEL"

    x, y = zscore_axes(date_col, x_title=date_col)
    tooltip = [
        alt.Tooltip(f"{date_col}:T",   title=date_col, format="%b %Y"),
        alt.Tooltip(f"{label_col}:N",  title=group_col),
        alt.Tooltip("Z_SCORE:Q",       title="Z-Score", format=".4f"),
        alt.Tooltip("RECORD_COUNT:Q",  title="Records", format=","),
    ]
    lines = (
        alt.Chart(df)
        .mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=30, filled=True))
        .encode(
            x=x, y=y,
            color=alt.Color(f"{label_col}:N", title=group_col,
                            legend=alt.Legend(orient="bottom", labelFontSize=11, symbolSize=80)),
            tooltip=tooltip,
        )
    )
    chart = (zero_rule() + lines).properties(height=380).interactive()
    st.altair_chart(chart, width="stretch")


def render(df: pd.DataFrame, date_cols: list, numeric_cols: list, item_name: str = "") -> None:
    """Entry point for all Section I charts."""
    name = item_name.upper()
    if name == "CHART IC" and "Z_SCORE" in df.columns and date_cols:
        _render_institutional_encounter_trend(df, date_cols[0])
    elif "Z_SCORE" in df.columns and "ENC_TYPE" in df.columns and date_cols:
        _render_encounter_trend(df, date_cols[0])
    elif "Z_SCORE" in df.columns and "DEATH_SOURCE" in df.columns and date_cols:
        _render_grouped_zscore_trend(df, date_cols[0], group_col="DEATH_SOURCE")
    elif "Z_SCORE" in df.columns and date_cols:
        _render_zscore_trend(df, date_cols[0])
    elif date_cols and numeric_cols:
        st.line_chart(df.set_index(date_cols[0])[numeric_cols])
    else:
        st.info("No chart available for this data layout.")
