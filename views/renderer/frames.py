"""Pure dataframe preparation shared by the Streamlit renderers and the PDF exporter.

No streamlit/altair imports — everything here returns plain pandas objects and
boolean masks so the same logic drives both `st.dataframe` Stylers and
reportlab table styling.
"""

import pandas as pd

# Column -> value pairs that mark a row as an exception in generic tables
EXCEPTION_MARKERS = {
    "HAS_PK_ERROR":   "Yes",
    "REFRESH_STATUS": "Missing",
    "IS_OUTLIER":     "Yes",
}


def records_to_dicts(records: list) -> list:
    """Snowpark Rows or dicts -> list of dicts (without the __columns__ sentinel)."""
    if not records:
        return []
    dicts = records if isinstance(records[0], dict) else \
        [{col: getattr(r, col) for col in r._fields} for r in records]
    if dicts and isinstance(dicts[-1], dict) and "__columns__" in dicts[-1]:
        dicts = dicts[:-1]
    return dicts


def records_to_df(records: list) -> pd.DataFrame:
    """Records -> DataFrame, honoring the trailing __columns__ order sentinel."""
    if not records:
        return pd.DataFrame()
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
    return df


def to_multiindex(df: pd.DataFrame) -> pd.DataFrame:
    """Split `GROUP__COLUMN` names into a two-level header (no-op otherwise)."""
    if not any("__" in str(c) for c in df.columns):
        return df
    out = df.copy()
    out.columns = pd.MultiIndex.from_tuples([
        tuple(str(c).split("__", 1)) if "__" in str(c) else ("", str(c))
        for c in df.columns
    ])
    return out


def exception_row_mask(df: pd.DataFrame) -> pd.Series:
    """True for rows matching any EXCEPTION_MARKERS column/value pair."""
    mask = pd.Series(False, index=df.index)
    for col, val in EXCEPTION_MARKERS.items():
        if col in df.columns:
            mask |= df[col].astype(str) == val
    return mask


def count_exception_rows(records: list) -> int:
    df = records_to_df(records)
    return 0 if df.empty else int(exception_row_mask(df).sum())


def pct_change_mask(df: pd.DataFrame, threshold: float = -5.0) -> pd.DataFrame:
    """Boolean frame: True where a PCT_CHANGE column value < threshold."""
    mask = pd.DataFrame(False, index=df.index, columns=df.columns)
    for col in df.columns:
        name = col[1] if isinstance(col, tuple) else col
        if "PCT_CHANGE" not in str(name).upper():
            continue
        vals = pd.to_numeric(df[col], errors="coerce")
        mask[col] = vals < threshold
    return mask


def has_pct_change(df: pd.DataFrame) -> bool:
    return any(
        "PCT_CHANGE" in str(c[1] if isinstance(c, tuple) else c).upper()
        for c in df.columns
    )
