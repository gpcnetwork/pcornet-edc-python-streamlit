"""Chart constants and dataframe preparation shared by the Streamlit/Altair
chart renderers and the PDF exporter (reportlab + matplotlib).

Pure module: pandas only — no streamlit / altair / matplotlib imports.
Z_SCORE values come straight from SQL; nothing here computes statistics.
"""

import pandas as pd

# ── Section I — encounter-type palette, labels, panel groupings ─────────────

ENC_LABEL = {
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

ENC_COLORS = {
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

# Chart IB — three stacked panels, each a group of ENC_TYPE codes
ENC_PANELS = [
    ("Panel 1 — Ambulatory",          ["AV", "OA", "TH"]),
    ("Panel 2 — Emergency/Inpatient", ["ED", "EI", "IP"]),
    ("Panel 3 — Institutional/Other", ["IS", "IC", "OS"]),
]

# Chart IC — single full-width institutional chart
IC_ENC_TYPES = ["EI", "IP", "IS"]


def enc_labels_palette(enc_types: list) -> tuple:
    """(full labels, hex colors) for a list of ENC_TYPE codes (Section I)."""
    labels = [ENC_LABEL[c] for c in enc_types if c in ENC_LABEL]
    return labels, [ENC_COLORS[lb] for lb in labels]


def prepare_zscore_df(df: pd.DataFrame, date_col: str,
                      group_col=None) -> pd.DataFrame:
    """Prep a Z-score trend frame exactly as the Altair renderers expect.

    - parses ``date_col`` to datetime
    - fills missing Z_SCORE with 0 (Z_SCORE itself comes from SQL — no math)
    - adds ``ENC_LABEL`` (full encounter-type label) when ENC_TYPE is present
    - adds ``<group_col>_LABEL`` when ``group_col`` is given (e.g. DEATH_SOURCE)
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    if "Z_SCORE" in df.columns:
        df["Z_SCORE"] = df["Z_SCORE"].fillna(0)
    if "ENC_TYPE" in df.columns:
        df["ENC_LABEL"] = df["ENC_TYPE"].map(lambda x: ENC_LABEL.get(x, x))
    if group_col is not None:
        df[f"{group_col}_LABEL"] = df[group_col]
    return df


# ── Section III — monthly volume charts ─────────────────────────────────────

SECTION_III_ENC_ORDER  = ["AV", "ED", "EI", "IP", "TH"]
SECTION_III_ENC_COLORS = {
    "AV": "#1f77b4",
    "ED": "#d62728",
    "EI": "#ff7f0e",
    "IP": "#ffbb78",
    "TH": "#17becf",
}


def present_series(df: pd.DataFrame, order: list, colors: dict,
                   series_col: str = "ENC_TYPE") -> tuple:
    """(present series values in canonical order, matching hex palette)."""
    present = [e for e in order if e in df[series_col].unique()]
    return present, [colors[e] for e in present]


def prepare_monthly_volume_df(df: pd.DataFrame) -> pd.DataFrame:
    """Prep for Section III monthly volume charts: datetime + numeric parsing."""
    df = df.copy()
    df["MONTH_START"]   = pd.to_datetime(df["MONTH_START"])
    df["RECORD_COUNT"]  = pd.to_numeric(df["RECORD_COUNT"],  errors="coerce")
    df["PATIENT_COUNT"] = pd.to_numeric(df["PATIENT_COUNT"], errors="coerce")
    return df


# ── Section IV — per-encounter ratio charts ──────────────────────────────────

SECTION_IV_ENC_ORDER  = ["AV", "ED", "EI", "IP", "TH"]
SECTION_IV_ENC_COLORS = {
    "AV": "#1f77b4",
    "ED": "#d62728",
    "EI": "#ff7f0e",
    "IP": "#ffbb78",
    "TH": "#17becf",
}

# One config per chart: metric column, axis/tooltip titles, and the extra
# count columns shown as tooltips (column name, tooltip title).
SECTION_IV_CHART_CONFIGS = {
    "CHART IVA": {
        "metric_col":     "AVG_DX_PER_ENC",
        "y_title":        "Diagnosis Records per Encounter",
        "metric_tooltip": "Dx per Encounter",
        "count_cols":     [("ENCOUNTER_COUNT", "Encounters"),
                           ("DX_COUNT",        "Dx Records")],
    },
    "CHART IVB": {
        "metric_col":     "AVG_PX_PER_ENC",
        "y_title":        "Procedure Records per Encounter",
        "metric_tooltip": "Px per Encounter",
        "count_cols":     [("ENCOUNTER_COUNT", "Encounters"),
                           ("PX_COUNT",        "Px Records")],
    },
}


def prepare_ratio_df(df: pd.DataFrame, chart_key: str) -> pd.DataFrame:
    """Prep for a Section IV ratio chart (keyed by SECTION_IV_CHART_CONFIGS)."""
    cfg = SECTION_IV_CHART_CONFIGS[chart_key.upper()]
    df = df.copy()
    df["MONTH_START"]     = pd.to_datetime(df["MONTH_START"])
    df[cfg["metric_col"]] = pd.to_numeric(df[cfg["metric_col"]], errors="coerce")
    for col, _title in cfg["count_cols"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df
