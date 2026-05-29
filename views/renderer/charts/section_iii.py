import pandas as pd
import streamlit as st
import altair as alt

_III_ENC_ORDER  = ["AV", "ED", "EI", "IP", "TH"]
_III_ENC_COLORS = {
    "AV": "#1f77b4",
    "ED": "#d62728",
    "EI": "#ff7f0e",
    "IP": "#ffbb78",
    "TH": "#17becf",
}


def _render_enc_type_monthly(df: pd.DataFrame) -> None:
    df = df.copy()
    df["MONTH_START"]   = pd.to_datetime(df["MONTH_START"])
    df["RECORD_COUNT"]  = pd.to_numeric(df["RECORD_COUNT"],  errors="coerce")
    df["PATIENT_COUNT"] = pd.to_numeric(df["PATIENT_COUNT"], errors="coerce")

    present = [e for e in _III_ENC_ORDER if e in df["ENC_TYPE"].unique()]
    palette = [_III_ENC_COLORS[e] for e in present]

    x = alt.X(
        "MONTH_START:T",
        title="Month",
        axis=alt.Axis(format="%b %Y", tickCount={"interval": "month", "step": 6},
                      labelAngle=-45, grid=False),
    )
    y = alt.Y(
        "RECORD_COUNT:Q",
        title="Record Count",
        scale=alt.Scale(zero=True),
        axis=alt.Axis(grid=True),
    )
    tooltip = [
        alt.Tooltip("MONTH_START:T",   title="Month",          format="%b %Y"),
        alt.Tooltip("ENC_TYPE:N",      title="Encounter Type"),
        alt.Tooltip("RECORD_COUNT:Q",  title="Record Count",   format=","),
        alt.Tooltip("PATIENT_COUNT:Q", title="Patient Count",  format=","),
    ]

    chart = (
        alt.Chart(df)
        .mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=30, filled=True))
        .encode(
            x=x, y=y,
            color=alt.Color(
                "ENC_TYPE:N", title="Encounter Type",
                scale=alt.Scale(domain=present, range=palette),
                legend=alt.Legend(orient="bottom", columns=len(present),
                                  labelFontSize=11, symbolSize=80),
            ),
            tooltip=tooltip,
        )
        .properties(height=380)
        .interactive()
    )
    st.altair_chart(chart, width="stretch")


def _render_single_monthly(df: pd.DataFrame) -> None:
    df = df.copy()
    df["MONTH_START"]   = pd.to_datetime(df["MONTH_START"])
    df["RECORD_COUNT"]  = pd.to_numeric(df["RECORD_COUNT"],  errors="coerce")
    df["PATIENT_COUNT"] = pd.to_numeric(df["PATIENT_COUNT"], errors="coerce")

    x = alt.X(
        "MONTH_START:T",
        title="Month",
        axis=alt.Axis(format="%b %Y", tickCount={"interval": "month", "step": 6},
                      labelAngle=-45, grid=False),
    )
    y = alt.Y(
        "RECORD_COUNT:Q",
        title="Record Count",
        scale=alt.Scale(zero=True),
        axis=alt.Axis(grid=True),
    )
    tooltip = [
        alt.Tooltip("MONTH_START:T",   title="Month",         format="%b %Y"),
        alt.Tooltip("RECORD_COUNT:Q",  title="Record Count",  format=","),
        alt.Tooltip("PATIENT_COUNT:Q", title="Patient Count", format=","),
    ]

    base  = alt.Chart(df)
    lines = base.mark_line(color="#2166ac", strokeWidth=2.5).encode(x=x, y=y, tooltip=tooltip)
    pts   = base.mark_point(color="#2166ac", size=40, filled=True, opacity=0.85).encode(
        x=x, y=y, tooltip=tooltip
    )
    chart = (lines + pts).properties(height=380)
    st.altair_chart(chart.interactive(), width="stretch")


def render(df: pd.DataFrame, date_cols: list, numeric_cols: list, item_name: str = "") -> None:
    """Entry point for all Section III monthly volume charts."""
    if "ENC_TYPE" in df.columns and "MONTH_START" in df.columns and "RECORD_COUNT" in df.columns:
        _render_enc_type_monthly(df)
    elif "MONTH_START" in df.columns and "RECORD_COUNT" in df.columns:
        _render_single_monthly(df)
    elif date_cols and numeric_cols:
        st.line_chart(df.set_index(date_cols[0])[numeric_cols])
    elif len(df.columns) >= 2 and numeric_cols:
        st.bar_chart(df.set_index(df.columns[0])[numeric_cols])
    else:
        st.info("No chart available for this data layout.")
