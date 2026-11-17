"""Data Check Exception Summary — the EDCRPT summary artifact.

Builds the 46-row summary exactly as the SAS report prints it
(edc_report.sas %print_dcsummary, lines 11294-11441): one row per
(Data Check, EDC Table) in registry order, with the SAS red/blue
severity rule and the network benchmark column.

Pure (no Streamlit): consumed by both the UI view and the PDF exporter.
"""

import pandas as pd

from utils.constants import REQUIRED_RED_CHECKS

# Footnote letters as printed in the EDCRPT (superscripts on the check id)
_SUPERSCRIPT = {"a": "ᵃ", "b": "ᵇ", "c": "ᶜ"}

# Internal run status -> EDCRPT "Exception?" value
_STATUS_TO_EXCEPTION = {
    "complete": "No",
    "fail":     "Yes",
    "na":       "n/a",
    "error":    "Error",
    "pending":  "Not run",
}

# Severity flags (SAS `color` variable): 1 = red (must be corrected),
# 2 = blue (investigative, must be explained in the ETL ADD), 0 = none.
SEVERITY_NONE, SEVERITY_RED, SEVERITY_BLUE = 0, 1, 2

# EDCRPT page breaks for the summary table (SAS: pg assignment by _n_)
SUMMARY_PAGE_BREAKS = (14, 24, 30, 37, 46)

SUMMARY_COLUMNS = [
    "Data Check", "EDC Table", "Data Check Description",
    "Category", "Type", "Exception?", "% DataMarts w/ exceptions",
]


def _row_key(row) -> str:
    return f"{row['Data Check']}|{row['EDC Table']}"


def _severity(check_num: str, exception: str) -> int:
    if exception != "Yes":
        return SEVERITY_NONE
    return SEVERITY_RED if check_num in REQUIRED_RED_CHECKS else SEVERITY_BLUE


def build_dc_summary(dq_df: pd.DataFrame, dq_statuses: dict) -> pd.DataFrame:
    """One row per registry check, in registry (= EDCRPT print) order.

    dq_statuses: {"DC x.xx|Table YYY": "complete"|"fail"|"na"|"error"} —
    missing keys mean the check has not been run this session.

    Returns SUMMARY_COLUMNS plus hidden helper columns `_severity` (0/1/2)
    and `_check_num` (e.g. "3.03").
    """
    rows = []
    for _, r in dq_df.iterrows():
        check_num = str(r["Data Check"]).replace("DC ", "").strip()
        status = dq_statuses.get(_row_key(r), "pending")
        exception = _STATUS_TO_EXCEPTION.get(status, "Not run")
        letter = str(r.get("Footnote", "") or "").strip()
        rows.append({
            "Data Check": f"DC {check_num}{_SUPERSCRIPT.get(letter, '')}",
            "EDC Table": r["EDC Table"],
            "Data Check Description": r["Data Check Description"],
            "Category": r["Category"],
            "Type": r["Type"],
            "Exception?": exception,
            "% DataMarts w/ exceptions": str(r.get("Network_Pct", "---") or "---"),
            "_severity": _severity(check_num, exception),
            "_check_num": check_num,
        })
    return pd.DataFrame(rows, columns=SUMMARY_COLUMNS + ["_severity", "_check_num"])
