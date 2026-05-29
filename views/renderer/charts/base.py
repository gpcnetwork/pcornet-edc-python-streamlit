import pandas as pd
import streamlit as st
import altair as alt

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

EXCEPTION_MARKERS = {
    "HAS_PK_ERROR":   "Yes",
    "REFRESH_STATUS": "Missing",
    "IS_OUTLIER":     "Yes",
}


def count_exceptions(result: list) -> int:
    if not result:
        return 0
    total = 0
    for row in result:
        d = row if isinstance(row, dict) else {col: getattr(row, col) for col in row._fields}
        if any(str(d.get(col, "")) == val for col, val in EXCEPTION_MARKERS.items()):
            total += 1
    return total


def pct_exception_styles(df: pd.DataFrame) -> pd.DataFrame:
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


def render_table_generic(records: list, item_key: str) -> None:
    """Generic table renderer: MultiIndex via __ separator, exception row highlight, PCT_CHANGE blue, CSV download."""
    dicts = records if isinstance(records[0], dict) else \
            [{col: getattr(r, col) for col in r._fields} for r in records]

    column_order = None
    if dicts and isinstance(dicts[-1], dict) and "__columns__" in dicts[-1]:
        column_order = dicts[-1]["__columns__"]
        dicts = dicts[:-1]

    df = pd.DataFrame(dicts)
    if column_order:
        ordered = [c for c in column_order if c in df.columns]
        df = df[ordered + [c for c in df.columns if c not in ordered]]

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
            for col, val in EXCEPTION_MARKERS.items()
            if col in row.index
        )
        return ["background-color: #fde8e8" if is_exc else ""] * len(row)

    has_pct_cols = any(
        "PCT_CHANGE" in (c[1] if isinstance(c, tuple) else c).upper()
        for c in display_df.columns
    )
    n_exc = count_exceptions(records)
    if n_exc or has_pct_cols:
        styled = display_df.style.apply(_highlight, axis=1)
        if has_pct_cols:
            styled = styled.apply(pct_exception_styles, axis=None)
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
