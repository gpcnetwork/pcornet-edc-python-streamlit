import pandas as pd
import streamlit as st

from charts.base import zero_rule, zscore_axes

_ENC_COLORS = {
    "AV (Ambulatory Visit)":        "#1f77b4",
    "OA (Other Ambulatory Visit)":  "#aec7e8",
    "TH (Telehealth)":              "#17becf",
    "ED (Emergency Dept)":          "#d62728",
    "EI (Emergency + Inpatient)":   "#ff7f0e",
    "IP (Inpatient)":               "#ffbb78",
    "IS (Non-Acute Inpatient)":     "#9467bd",
    "IC (ICU)":                     "#c5b0d5",
    "OS (Observation Stay)":        "#8c564b",
}

_ENC_LABEL = {
    "AV": "AV (Ambulatory Visit)",
    "OA": "OA (Other Ambulatory Visit)",
    "TH": "TH (Telehealth)",
    "ED": "ED (Emergency Dept)",
    "EI": "EI (Emergency + Inpatient)",
    "IP": "IP (Inpatient)",
    "IS": "IS (Non-Acute Inpatient)",
    "IC": "IC (ICU)",
    "OS": "OS (Observation Stay)",
}

_ENC_PANELS = [
    ("Panel 1 — Ambulatory",          ["AV", "OA", "TH"]),
    ("Panel 2 — Emergency/Inpatient", ["ED", "EI", "IP"]),
    ("Panel 3 — Institutional/Other", ["IS", "IC", "OS"]),
]

def _render_zscore_trend(df: pd.DataFrame, date_col: str) -> None:
    """Single-series Z-score trend chart (e.g. Chart IA – VITAL)."""
    import altair as alt

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])

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
    st.altair_chart(chart.interactive(), use_container_width=True)


def _render_encounter_trend(df: pd.DataFrame, date_col: str) -> None:
    """3-panel Z-score trend chart for Chart IB (Encounter by ENC_TYPE)."""
    import altair as alt

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df["ENC_LABEL"] = df["ENC_TYPE"].map(lambda x: _ENC_LABEL.get(x, x))

    x, y = zscore_axes(date_col, x_title=date_col)
    x = x.copy()

    for panel_title, enc_types in _ENC_PANELS:
        labels  = [_ENC_LABEL[c] for c in enc_types if c in _ENC_LABEL]
        palette = [_ENC_COLORS[lb] for lb in labels]
        subset  = df[df["ENC_TYPE"].isin(enc_types)]

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
        st.altair_chart(chart, use_container_width=True)


_IC_ENC_TYPES = ["EI", "IP", "IS"]


def _render_institutional_encounter_trend(df: pd.DataFrame, date_col: str) -> None:
    """Single full-width multi-series Z-score chart for Chart IC (EI, IP, IS by DISCHARGE_DATE)."""
    import altair as alt

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df["ENC_LABEL"] = df["ENC_TYPE"].map(lambda x: _ENC_LABEL.get(x, x))

    labels  = [_ENC_LABEL[c] for c in _IC_ENC_TYPES if c in _ENC_LABEL]
    palette = [_ENC_COLORS[lb] for lb in labels]

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
    st.altair_chart(chart, use_container_width=True)


def _render_grouped_zscore_trend(df: pd.DataFrame, date_col: str, group_col: str) -> None:
    """Multi-series Z-score chart grouped by a categorical column (e.g. DEATH_SOURCE)."""
    import altair as alt

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])

    label_col = f"{group_col}_LABEL"
    df[label_col] = df[group_col]

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
    st.altair_chart(chart, use_container_width=True)


def render(df: pd.DataFrame, date_cols: list, numeric_cols: list, item_name: str = "") -> None:
    """Entry point for all Section I charts."""
    if "Z_SCORE" in df.columns:
        df = df.copy()
        df["Z_SCORE"] = df["Z_SCORE"].fillna(0)
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
