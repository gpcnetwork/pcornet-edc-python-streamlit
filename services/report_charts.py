"""Matplotlib chart renderers for the PDF export.

The in-app charts are Altair, which cannot be rasterized in Streamlit-in-Snowflake
(no kaleido/vl-convert in the Snowflake Anaconda channel), so the PDF re-renders
the same stored dataframes with matplotlib (Agg). Visual conventions mirror the
Altair renderers: z-score y-domain [-3, 3], dashed zero rule, Mon-Year ticks.
"""

import io

import pandas as pd

_F2F_PANELS = [("AV", "OA", "TH"), ("ED", "EI", "IP"), ("IS", "IC", "OS")]

_ENC_COLORS = {
    "AV": "#1f77b4", "ED": "#d62728", "EI": "#9467bd", "IP": "#2ca02c",
    "IS": "#8c564b", "IC": "#e377c2", "OA": "#7f7f7f", "OS": "#bcbd22",
    "TH": "#17becf",
}


def _mpl():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    return plt, mdates


def _find_col(df, *keywords):
    for c in df.columns:
        cu = str(c).upper()
        if any(k in cu for k in keywords):
            return c
    return None


def _prep(records):
    from views.renderer.frames import records_to_df
    df = records_to_df(records)
    if df.empty:
        return None, None, None
    date_col = _find_col(df, "MONTH", "DATE", "PERIOD")
    if date_col is None:
        return None, None, None
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).sort_values(date_col)
    series_col = _find_col(df, "ENC_TYPE", "DEATH_SOURCE", "SERIES", "SOURCE")
    return df, date_col, series_col


def _fig_to_png(fig) -> bytes:
    plt, _ = _mpl()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


def _style_axes(ax, mdates, zscore: bool):
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    for lbl in ax.get_xticklabels():
        lbl.set_rotation(45)
        lbl.set_ha("right")
        lbl.set_fontsize(7)
    for lbl in ax.get_yticklabels():
        lbl.set_fontsize(7)
    if zscore:
        ax.set_ylim(-3, 3)
        ax.axhline(0, color="#444444", linestyle=(0, (5, 3)), linewidth=1.2)
        ax.set_ylabel("Z-Score (Deviation from Mean)", fontsize=8)
    ax.grid(axis="y", linewidth=0.3, alpha=0.5)


def chart_png(item_key: str, records: list) -> bytes | None:
    """Render a section chart to PNG bytes; None when the data layout is unusable."""
    df, date_col, series_col = _prep(records)
    if df is None:
        return None
    plt, mdates = _mpl()

    zscore = "Z_SCORE" in df.columns
    value_col = "Z_SCORE" if zscore else next(
        (c for c in df.columns
         if c != date_col and pd.api.types.is_numeric_dtype(df[c])), None)
    if value_col is None:
        return None

    key = item_key.strip().lower()
    if key == "chart ib" and series_col:
        # Chart IB: 3 panels by encounter-type group
        fig, axes = plt.subplots(3, 1, figsize=(9, 7.5), sharex=True)
        for ax, panel in zip(axes, _F2F_PANELS):
            plotted = False
            for enc in panel:
                sub = df[df[series_col].astype(str).str.upper() == enc]
                if not sub.empty:
                    ax.plot(sub[date_col], sub[value_col], label=enc,
                            color=_ENC_COLORS.get(enc), linewidth=1.2)
                    plotted = True
            if plotted:
                ax.legend(fontsize=6, loc="upper left", ncol=3, frameon=False)
            _style_axes(ax, mdates, zscore)
        fig.suptitle(item_key, fontsize=9)
        return _fig_to_png(fig)

    fig, ax = plt.subplots(figsize=(9, 3.4))
    if series_col:
        for name, sub in df.groupby(series_col):
            color = _ENC_COLORS.get(str(name).upper())
            ax.plot(sub[date_col], sub[value_col], label=str(name),
                    color=color, linewidth=1.2)
        ax.legend(fontsize=6, loc="best", ncol=4, frameon=False)
    else:
        ax.plot(df[date_col], df[value_col], color="#1f77b4", linewidth=1.2)
    if not zscore:
        ax.set_ylabel(str(value_col).replace("_", " ").title(), fontsize=8)
    _style_axes(ax, mdates, zscore)
    ax.set_title(item_key, fontsize=9)
    return _fig_to_png(fig)
