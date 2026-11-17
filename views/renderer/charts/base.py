import pandas as pd
import streamlit as st
import altair as alt

from views.renderer.frames import (
    EXCEPTION_MARKERS,
    count_exception_rows,
    exception_row_mask,
    has_pct_change,
    pct_change_mask,
    records_to_df,
    to_multiindex,
)


def render_generic(df, date_cols, numeric_cols) -> None:
    if date_cols and numeric_cols:
        st.line_chart(df.set_index(date_cols[0])[numeric_cols])
    elif len(df.columns) >= 2 and numeric_cols:
        st.bar_chart(df.set_index(df.columns[0])[numeric_cols])
    else:
        st.info("No chart available for this data layout.")


def zscore_axes(date_col: str, x_title: str = "Month"):
    """Shared Altair X / Y axis encodings for all Z-score charts."""

    x = alt.X(
        f"{date_col}:T",
        title=x_title,
        axis=alt.Axis(
            format="%b %Y",
            tickCount={"interval": "month", "step": 6},
            labelAngle=-45,
            grid=False,
        ),
    )
    y = alt.Y(
        "Z_SCORE:Q",
        title="Z-Score (Deviation from Mean)",
        scale=alt.Scale(domain=[-3, 3], clamp=False),
        axis=alt.Axis(values=[-3, -2, -1, 0, 1, 2, 3], grid=True),
    )
    return x, y


def zero_rule() -> "alt.Chart":
    """Dashed horizontal reference line at Z = 0."""

    return (
        alt.Chart(pd.DataFrame({"z": [0]}))
        .mark_rule(color="#444444", strokeDash=[5, 3], strokeWidth=1.5)
        .encode(y=alt.Y("z:Q"))
    )


# ── Table rendering utilities ──────────────────────────────────────────────
# Frame preparation and exception logic live in views/renderer/frames.py so the
# PDF exporter shares them; this module only wraps them in Streamlit widgets.

count_exceptions = count_exception_rows


def render_table_generic(records: list, item_key: str) -> None:
    """Generic table renderer: MultiIndex via __ separator, exception row highlight, PCT_CHANGE blue, CSV download."""
    df = records_to_df(records)
    display_df = to_multiindex(df)

    row_mask = exception_row_mask(df)
    pct_mask = pct_change_mask(display_df)

    def _highlight(_):
        styles = pd.DataFrame("", index=display_df.index, columns=display_df.columns)
        for idx in display_df.index[row_mask.reindex(display_df.index, fill_value=False)]:
            styles.loc[idx, :] = "background-color: #fde8e8"
        styles = styles.mask(pct_mask, "background-color: #cfe2ff")
        return styles

    if row_mask.any() or has_pct_change(display_df):
        styled = display_df.style.apply(_highlight, axis=None).format(na_rep="---")
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
