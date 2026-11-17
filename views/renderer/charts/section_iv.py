import pandas as pd
import streamlit as st
import altair as alt

from views.renderer.charts.common import (
    SECTION_IV_CHART_CONFIGS,
    SECTION_IV_ENC_COLORS,
    SECTION_IV_ENC_ORDER,
    prepare_ratio_df,
    present_series,
)


def _enc_type_chart(df: pd.DataFrame, metric_col: str, y_title: str,
                    metric_tooltip: str, extra_tooltips: list) -> None:
    """Shared multi-series line chart for per-encounter metric by ENC_TYPE."""

    present, palette = present_series(df, SECTION_IV_ENC_ORDER, SECTION_IV_ENC_COLORS)

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


def _render_config_chart(df: pd.DataFrame, chart_key: str) -> None:
    """Render one Section IV ratio chart from its SECTION_IV_CHART_CONFIGS entry."""
    cfg = SECTION_IV_CHART_CONFIGS[chart_key]
    df = prepare_ratio_df(df, chart_key)
    extra = [alt.Tooltip(f"{col}:Q", title=title, format=",")
             for col, title in cfg["count_cols"]]
    _enc_type_chart(df, cfg["metric_col"], cfg["y_title"], cfg["metric_tooltip"], extra)


def render(df: pd.DataFrame, date_cols: list, numeric_cols: list, item_name: str = "") -> None:
    """Entry point for all Section IV charts."""
    name = item_name.upper()
    cfg  = SECTION_IV_CHART_CONFIGS.get(name)
    if cfg and cfg["metric_col"] in df.columns:
        _render_config_chart(df, name)
    elif date_cols and numeric_cols:
        st.line_chart(df.set_index(date_cols[0])[numeric_cols])
    elif len(df.columns) >= 2 and numeric_cols:
        st.bar_chart(df.set_index(df.columns[0])[numeric_cols])
    else:
        st.info("No chart available for this data layout.")
