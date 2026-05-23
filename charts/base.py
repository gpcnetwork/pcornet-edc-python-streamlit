import pandas as pd
import streamlit as st

def render_generic(df, date_cols, numeric_cols) -> None:
    if date_cols and numeric_cols:
        st.line_chart(df.set_index(date_cols[0])[numeric_cols])
    elif len(df.columns) >= 2 and numeric_cols:
        st.bar_chart(df.set_index(df.columns[0])[numeric_cols])
    else:
        st.info("No chart available for this data layout.")


def zscore_axes(date_col: str, x_title: str = "Month"):
    """Shared Altair X / Y axis encodings for all Z-score charts."""
    import altair as alt

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
    import altair as alt

    return (
        alt.Chart(pd.DataFrame({"z": [0]}))
        .mark_rule(color="#444444", strokeDash=[5, 3], strokeWidth=1.5)
        .encode(y=alt.Y("z:Q"))
    )
