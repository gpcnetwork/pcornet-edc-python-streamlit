import pandas as pd
import streamlit as st
import altair as alt

_IV_ENC_ORDER  = ["AV", "ED", "EI", "IP", "TH"]
_IV_ENC_COLORS = {
    "AV": "#1f77b4",
    "ED": "#d62728",
    "EI": "#ff7f0e",
    "IP": "#ffbb78",
    "TH": "#17becf",
}

_IVA_ENC_ORDER  = _IV_ENC_ORDER
_IVA_ENC_COLORS = _IV_ENC_COLORS


def _enc_type_chart(df: pd.DataFrame, metric_col: str, y_title: str,
                    metric_tooltip: str, extra_tooltips: list) -> None:
    """Shared multi-series line chart for per-encounter metric by ENC_TYPE."""

    present = [e for e in _IV_ENC_ORDER if e in df["ENC_TYPE"].unique()]
    palette = [_IV_ENC_COLORS[e] for e in present]

    x = alt.X(
        "MONTH_START:T",
        title="ADMIT_DATE",
        axis=alt.Axis(format="%b %Y", tickCount={"interval": "month", "step": 6},
                      labelAngle=-45, grid=False),
    )
    y = alt.Y(
        f"{metric_col}:Q",
        title=y_title,
        scale=alt.Scale(zero=True),
        axis=alt.Axis(grid=True),
    )
    tooltip = [
        alt.Tooltip("MONTH_START:T",       title="ADMIT_DATE",     format="%b %Y"),
        alt.Tooltip("ENC_TYPE:N",          title="Encounter Type"),
        alt.Tooltip(f"{metric_col}:Q",     title=metric_tooltip,   format=".2f"),
    ] + extra_tooltips

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


def _render_chart_iva(df: pd.DataFrame) -> None:
    df = df.copy()
    df["MONTH_START"]     = pd.to_datetime(df["MONTH_START"])
    df["AVG_DX_PER_ENC"]  = pd.to_numeric(df["AVG_DX_PER_ENC"],  errors="coerce")
    df["ENCOUNTER_COUNT"] = pd.to_numeric(df["ENCOUNTER_COUNT"], errors="coerce")
    df["DX_COUNT"]        = pd.to_numeric(df["DX_COUNT"],        errors="coerce")

    _enc_type_chart(
        df, "AVG_DX_PER_ENC", "Diagnosis Records per Encounter", "Dx per Encounter",
        [alt.Tooltip("ENCOUNTER_COUNT:Q", title="Encounters", format=","),
         alt.Tooltip("DX_COUNT:Q",        title="Dx Records", format=",")],
    )


def _render_chart_ivb(df: pd.DataFrame) -> None:
    df = df.copy()
    df["MONTH_START"]     = pd.to_datetime(df["MONTH_START"])
    df["AVG_PX_PER_ENC"]  = pd.to_numeric(df["AVG_PX_PER_ENC"],  errors="coerce")
    df["ENCOUNTER_COUNT"] = pd.to_numeric(df["ENCOUNTER_COUNT"], errors="coerce")
    df["PX_COUNT"]        = pd.to_numeric(df["PX_COUNT"],        errors="coerce")

    _enc_type_chart(
        df, "AVG_PX_PER_ENC", "Procedure Records per Encounter", "Px per Encounter",
        [alt.Tooltip("ENCOUNTER_COUNT:Q", title="Encounters", format=","),
         alt.Tooltip("PX_COUNT:Q",        title="Px Records", format=",")],
    )


def render(df: pd.DataFrame, date_cols: list, numeric_cols: list, item_name: str = "") -> None:
    """Entry point for all Section IV charts."""
    name = item_name.upper()
    if name == "CHART IVA" and "AVG_DX_PER_ENC" in df.columns:
        _render_chart_iva(df)
    elif name == "CHART IVB" and "AVG_PX_PER_ENC" in df.columns:
        _render_chart_ivb(df)
    elif date_cols and numeric_cols:
        st.line_chart(df.set_index(date_cols[0])[numeric_cols])
    elif len(df.columns) >= 2 and numeric_cols:
        st.bar_chart(df.set_index(df.columns[0])[numeric_cols])
    else:
        st.info("No chart available for this data layout.")
