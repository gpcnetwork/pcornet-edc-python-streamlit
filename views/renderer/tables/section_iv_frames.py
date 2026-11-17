"""Pure data preparation for the custom Section IV tables.

Shared by the Streamlit renderer (views/renderer/tables/section_iv.py) and the
PDF exporter — NO streamlit imports here, pandas only.

Each ``build_<key>_frame(records)`` accepts the raw records list (Snowpark Rows
or dicts, optionally ending with the ``__columns__`` sentinel) and returns a
dict of plain pandas objects:

- Simple tables (IVA, IVB, IVC, IVD)::

      {"df": display DataFrame, "fmt": format dict or None,
       "masks": {"blue": bool DataFrame aligned to df}}

- Grouped tables (IVE, IVH, IVI) return one frame per group::

      {"groups": [{"title", "df", "fmt", "masks"}, ...]}

  IVH may instead return ``{"error": "<detail>"}``.

- Latency pivots (IVF, IVG, IVJ)::

      {"current":   {"title", "df", "fmt", "masks"},
       "benchmark": {"title", "df", "fmt", "masks"},
       "benchmark_row": index of the "Benchmark average" row in benchmark df}

The "blue" mask marks cells the Streamlit renderer paints ``#cfe2ff``.
"""

import re as _re

import pandas as pd

from views.renderer.frames import records_to_df, records_to_dicts

# ── Table configuration (moved verbatim from the Streamlit renderer) ─────────

LATENCY_CONFIG = {
    "TABLE IVF": {
        "domains": ["ENCOUNTER", "DIAGNOSIS", "PROCEDURES"],
        "labels": {
            "ENCOUNTER":  "Ambulatory, Telehealth, ED, Inpatient or ED-to-Inpatient encounters",
            "DIAGNOSIS":  "Ambulatory, Telehealth, ED, Inpatient or ED-to-Inpatient diagnoses",
            "PROCEDURES": "Ambulatory, ED, Inpatient or ED-to-Inpatient procedures",
        },
        "source":  {"ENCOUNTER": "ENC_L3_ADATE_YM", "DIAGNOSIS": "DX_L3_ADATE_YM", "PROCEDURES": "PX_L3_PXDATE_YM"},
    },
    "TABLE IVG": {
        "domains": ["VITAL", "PRESCRIBING", "LAB_RESULT_CM"],
        "source":  {"VITAL": "VITAL_L3_MDATE_YM", "PRESCRIBING": "PRESCRIBING_L3_RXODATE_YM", "LAB_RESULT_CM": "LAB_L3_RDATE_YM"},
    },
    "TABLE IVJ": {
        "domains": ["MED_ADMIN", "DISPENSING", "OBS_CLIN"],
        "source":  {"MED_ADMIN": "MEDADM_L3_SDATE_YM", "DISPENSING": "DISP_L3_DDATE_YM", "OBS_CLIN": "OBSCLIN_L3_SDATE_YM"},
    },
}

CURRENT_PERIOD_TITLE   = "Current Period — Months −0 to −11"
BENCHMARK_PERIOD_TITLE = "Benchmark Period — Months −12 to −23"

MISSING_HEADER = "Records with missing, NI, UN, or OT values"

DC301_ENC_TYPES = {"AV", "IP", "ED", "EI", "TH"}

IVE_INST_ENC_TYPES = {"EI", "IP"}  # DC 3.06 / 2.07 apply only to EI and IP

DC302_THRESHOLDS = {"AV": 0.75, "ED": 0.75, "EI": 1.00, "IP": 1.00}

DC303_IVD_FIELDS = frozenset({
    ("PRESCRIBING",   "RX_ORDER_DATE"),
    ("IMMUNIZATION",  "VX_RECORD_DATE"),
    ("DISPENSING",    "DISPENSE_SUP"),
    ("DEATH",         "DEATH_SOURCE"),
    ("DEATH_CAUSE",   "DEATH_CAUSE_CODE"),
    ("DEATH_CAUSE",   "DEATH_CAUSE_SOURCE"),
    ("DISPENSING",    "DISPENSE_SOURCE"),
    ("CONDITION",     "CONDITION_SOURCE"),
    ("LAB_RESULT_CM", "LAB_RESULT_SOURCE"),
    ("MED_ADMIN",     "MEDADMIN_SOURCE"),
    ("PRESCRIBING",   "RX_SOURCE"),
    ("VITAL",         "VITAL_SOURCE"),
    ("IMMUNIZATION",  "VX_SOURCE"),
    ("VITAL",         "ENCOUNTERID"),
    ("MED_ADMIN",     "ENCOUNTERID"),
    ("LAB_RESULT_CM", "ENCOUNTERID"),
    ("MED_ADMIN",     "MEDADMIN_CODE"),
    ("MED_ADMIN",     "MEDADMIN_TYPE"),
    ("OBS_CLIN",      "OBSCLIN_CODE"),
    ("OBS_CLIN",      "OBSCLIN_TYPE"),
    ("OBS_GEN",       "OBSGEN_CODE"),
    ("OBS_GEN",       "OBSGEN_TYPE"),
    ("EXTERNAL_MEDS", "EXT_RECORD_DATE"),
    ("EXTERNAL_MEDS", "EXTMED_SOURCE"),
    ("EXTERNAL_MEDS", "EXT_BASIS"),
})

DC303_FIELDS = frozenset({
    ("DEMOGRAPHIC", "BIRTH_DATE"),
    ("DEMOGRAPHIC", "SEX"),
    ("ENCOUNTER",   "DISCHARGE_DATE"),
    ("ENCOUNTER",   "DISCHARGE_DISPOSITION"),
    ("DIAGNOSIS",   "DX_ORIGIN"),
    ("DIAGNOSIS",   "DX_SOURCE"),
    ("DIAGNOSIS",   "DX_TYPE"),
    ("DIAGNOSIS",   "ENCOUNTERID"),
    ("PROCEDURES",  "PX_DATE"),
    ("PROCEDURES",  "PX_SOURCE"),
    ("PROCEDURES",  "PX_TYPE"),
    ("PROCEDURES",  "ENCOUNTERID"),
})


# ── Shared helpers ───────────────────────────────────────────────────────────

def _empty_mask(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(False, index=df.index, columns=df.columns)


def _row_mask(df: pd.DataFrame, flags: list) -> pd.DataFrame:
    """Whole-row boolean mask aligned to df from a per-row flag list."""
    mask = _empty_mask(df)
    for idx, flag in zip(df.index, flags):
        if flag:
            mask.loc[idx, :] = True
    return mask


def _pct_to_float(v):
    """'12.3%' / 12.3 -> 12.3; unparseable -> None (matches the old try/except)."""
    try:
        return float(str(v).replace("%", ""))
    except (ValueError, TypeError):
        return None


# ── Latency pivots: Tables IVF / IVG / IVJ ───────────────────────────────────

def build_latency_frame(records: list, cfg: dict) -> dict:
    domains = cfg["domains"]
    labels  = cfg.get("labels", {})

    df = records_to_df(records)
    df["MONTH_START"] = pd.to_datetime(df["MONTH_START"])

    report_month = df["MONTH_START"].max()
    df["OFFSET"] = df["MONTH_START"].apply(
        lambda d: (report_month.year - d.year) * 12 + (report_month.month - d.month)
    )
    df["CAL_MONTH"] = df["MONTH_START"].dt.strftime("%m/%Y")

    def _build_wide(offsets) -> pd.DataFrame:
        rows = []
        for off in offsets:
            sub = df[df["OFFSET"] == off]
            row = {
                "Month":          f"Month -{off}",
                "Calendar Month": sub["CAL_MONTH"].iloc[0] if not sub.empty else "",
            }
            for domain in domains:
                hdr = labels.get(domain, domain)
                d = sub[sub["DOMAIN"] == domain]
                row[(hdr, "Records")]                   = int(d["RECORD_COUNT"].iloc[0]) if not d.empty else None
                row[(hdr, "Percent of Benchmark Avg")] = d["COMPLETENESS_PCT"].iloc[0]   if not d.empty else None
            rows.append(row)
        result = pd.DataFrame(rows)
        result.columns = pd.MultiIndex.from_tuples([
            ("", c) if isinstance(c, str) else c for c in result.columns
        ])
        return result

    current_df = _build_wide(range(12))
    bench_df   = _build_wide(range(12, 24))

    bench_src = df[df["OFFSET"].between(12, 23)]
    avg_row = {("", "Month"): "Benchmark average", ("", "Calendar Month"): ""}
    for domain in domains:
        hdr = labels.get(domain, domain)
        d = bench_src[bench_src["DOMAIN"] == domain]
        avg_row[(hdr, "Records")]                   = int(round(d["RECORD_COUNT"].mean())) if not d.empty else None
        avg_row[(hdr, "Percent of Benchmark Avg")] = None
    bench_df = pd.concat([bench_df, pd.DataFrame([avg_row])], ignore_index=True)

    fmt = {}
    for domain in domains:
        hdr = labels.get(domain, domain)
        fmt[(hdr, "Records")] = (
            lambda x: f"{int(x):,}" if isinstance(x, (int, float)) and pd.notna(x) else ("—" if pd.isna(x) else str(x))
        )
        fmt[(hdr, "Percent of Benchmark Avg")] = (
            lambda x: f"{x:.1f}" if isinstance(x, (int, float)) and pd.notna(x) else ("—" if pd.isna(x) else str(x))
        )

    # Exception cells: % of benchmark < 75, or zero records (current period only)
    cur_mask = _empty_mask(current_df)
    for domain in domains:
        hdr     = labels.get(domain, domain)
        pct_col = (hdr, "Percent of Benchmark Avg")
        rec_col = (hdr, "Records")
        if pct_col not in current_df.columns:
            continue
        for idx in current_df.index:
            try:
                pct = float(current_df.at[idx, pct_col])
                rec = current_df.at[idx, rec_col]
                if pct < 75 or rec == 0:
                    cur_mask.at[idx, pct_col] = True
                    cur_mask.at[idx, rec_col] = True
            except (ValueError, TypeError):
                pass

    return {
        "current": {
            "title": CURRENT_PERIOD_TITLE,
            "df":    current_df,
            "fmt":   fmt,
            "masks": {"blue": cur_mask},
        },
        "benchmark": {
            "title": BENCHMARK_PERIOD_TITLE,
            "df":    bench_df,
            "fmt":   fmt,
            "masks": {"blue": _empty_mask(bench_df)},
        },
        "benchmark_row": int(bench_df.index[-1]),
    }


def build_table_ivf_frame(records: list) -> dict:
    return build_latency_frame(records, LATENCY_CONFIG["TABLE IVF"])


def build_table_ivg_frame(records: list) -> dict:
    return build_latency_frame(records, LATENCY_CONFIG["TABLE IVG"])


def build_table_ivj_frame(records: list) -> dict:
    return build_latency_frame(records, LATENCY_CONFIG["TABLE IVJ"])


LATENCY_BUILDERS = {
    "TABLE IVF": build_table_ivf_frame,
    "TABLE IVG": build_table_ivg_frame,
    "TABLE IVJ": build_table_ivj_frame,
}


# ── Table IVA — diagnosis records per encounter (DC 3.01) ────────────────────

def build_table_iva_frame(records: list) -> dict:
    df = records_to_df(records)

    for col in ["DX_RECORDS", "DX_RECORDS_KNOWN_DXTYPE", "ENC_RECORDS",
                "DX_PER_ENC", "DX_KNOWN_TYPE_PER_ENC"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    display = df[["ENC_TYPE", "DX_RECORDS", "DX_RECORDS_KNOWN_DXTYPE",
                  "ENC_RECORDS", "DX_PER_ENC", "DX_KNOWN_TYPE_PER_ENC"]].copy()
    display.columns = [
        "Encounter Type",
        "DIAGNOSIS records",
        "DIAGNOSIS records with known DX_TYPE",
        "ENCOUNTER records",
        "Diagnosis records per encounter",
        "Diagnosis records with known DX_TYPE per encounter",
    ]

    flags = [
        df.at[idx, "ENC_TYPE"] in DC301_ENC_TYPES
        and pd.notna(df.at[idx, "DX_KNOWN_TYPE_PER_ENC"])
        and float(df.at[idx, "DX_KNOWN_TYPE_PER_ENC"]) < 1.0
        for idx in df.index
    ]

    fmt = {
        "DIAGNOSIS records":                              lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "DIAGNOSIS records with known DX_TYPE":           lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "ENCOUNTER records":                              lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "Diagnosis records per encounter":                lambda x: f"{x:.2f}"    if pd.notna(x) else "—",
        "Diagnosis records with known DX_TYPE per encounter": lambda x: f"{x:.2f}" if pd.notna(x) else "—",
    }

    return {"df": display, "fmt": fmt, "masks": {"blue": _row_mask(display, flags)}}


# ── Table IVB — procedure records per encounter (DC 3.02) ────────────────────

def build_table_ivb_frame(records: list) -> dict:
    df = records_to_df(records)

    for col in ["PX_RECORDS", "PX_RECORDS_KNOWN_PXTYPE", "ENC_RECORDS",
                "PX_PER_ENC", "PX_KNOWN_TYPE_PER_ENC"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    display = df[["ENC_TYPE", "PX_RECORDS", "PX_RECORDS_KNOWN_PXTYPE",
                  "ENC_RECORDS", "PX_PER_ENC", "PX_KNOWN_TYPE_PER_ENC"]].copy()
    display.columns = [
        "Encounter Type",
        "PROCEDURES records",
        "PROCEDURES records with known PX_TYPE",
        "ENCOUNTER records",
        "Procedures records per encounter",
        "Procedures records with known PX_TYPE per encounter",
    ]

    flags = []
    for idx in df.index:
        threshold = DC302_THRESHOLDS.get(df.at[idx, "ENC_TYPE"])
        val       = df.at[idx, "PX_KNOWN_TYPE_PER_ENC"]
        flags.append(threshold is not None and pd.notna(val) and float(val) < threshold)

    fmt = {
        "PROCEDURES records":                                lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "PROCEDURES records with known PX_TYPE":             lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "ENCOUNTER records":                                 lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "Procedures records per encounter":                  lambda x: f"{x:.2f}"    if pd.notna(x) else "—",
        "Procedures records with known PX_TYPE per encounter": lambda x: f"{x:.2f}" if pd.notna(x) else "—",
    }

    return {"df": display, "fmt": fmt, "masks": {"blue": _row_mask(display, flags)}}


# ── Table IVC — missing/NI/UN/OT with encounter-type constraint (DC 3.03) ────

def build_table_ivc_frame(records: list) -> dict:
    df = records_to_df(records)

    display = pd.DataFrame({
        ("", "Table"):                          df["TABLE_NAME"].values,
        ("", "Field"):                          df["FIELD_NAME"].values,
        ("", "Encounter Type Constraint"):      df["ENC_TYPE_CONSTRAINT"].values,
        (MISSING_HEADER, "Numerator"):          df["NUMERATOR"].values,
        (MISSING_HEADER, "Denominator"):        df["DENOMINATOR"].values,
        (MISSING_HEADER, "%"):                  df["PCT"].values,
    })
    display.columns = pd.MultiIndex.from_tuples(display.columns)
    display = display.reset_index(drop=True)

    flags = []
    for idx in df.index:
        key = (df.at[idx, "TABLE_NAME"], df.at[idx, "FIELD_NAME"])
        pct = _pct_to_float(df.at[idx, "PCT"])
        flags.append(key in DC303_FIELDS and pct is not None and pct > 10)

    return {"df": display, "fmt": None, "masks": {"blue": _row_mask(display, flags)}}


# ── Table IVD — missing/NI/UN/OT (DC 3.03, extended field list) ──────────────

def build_table_ivd_frame(records: list) -> dict:
    df = records_to_df(records)

    display = pd.DataFrame({
        ("", "Table"):                    df["TABLE_NAME"].values,
        ("", "Field"):                    df["FIELD_NAME"].values,
        (MISSING_HEADER, "Numerator"):    df["NUMERATOR"].values,
        (MISSING_HEADER, "Denominator"):  df["DENOMINATOR"].values,
        (MISSING_HEADER, "%"):            df["PCT"].values,
    })
    display.columns = pd.MultiIndex.from_tuples(display.columns)
    display = display.reset_index(drop=True)

    flags = []
    for idx in df.index:
        key = (df.at[idx, "TABLE_NAME"], df.at[idx, "FIELD_NAME"])
        pct = _pct_to_float(df.at[idx, "PCT"])
        flags.append(key in DC303_IVD_FIELDS and pct is not None and pct > 10)

    return {"df": display, "fmt": None, "masks": {"blue": _row_mask(display, flags)}}


# ── Table IVE — principal diagnoses per encounter type ───────────────────────

def build_table_ive_frame(records: list) -> dict:
    df = records_to_df(records)

    for col in ["ENCOUNTERS_WITH_PRINCIPAL_DX", "ENCOUNTERS_WITHOUT_PRINCIPAL_DX",
                "TOTAL_PRINCIPAL_DX", "PCT_WITHOUT_PRINCIPAL_DX", "AVG_PRINCIPAL_DX_PER_ENC"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    fmt = {
        "Encounters with a principal diagnosis":    lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "Encounters without a principal diagnosis": lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "% without a principal diagnosis":          lambda x: f"{x:.2f}"    if pd.notna(x) else "—",
        "Principal diagnoses":                      lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "Principal diagnoses per encounter":        lambda x: f"{x:.2f}"    if pd.notna(x) else "—",
    }

    groups = []
    for enc_type, grp in df.groupby("ENCOUNTER_TYPE", sort=False):
        grp = grp.reset_index(drop=True)

        display = grp[[
            "DX_ORIGIN",
            "ENCOUNTERS_WITH_PRINCIPAL_DX", "ENCOUNTERS_WITHOUT_PRINCIPAL_DX",
            "PCT_WITHOUT_PRINCIPAL_DX", "TOTAL_PRINCIPAL_DX", "AVG_PRINCIPAL_DX_PER_ENC",
        ]].copy()
        display.columns = [
            "DX_ORIGIN",
            "Encounters with a principal diagnosis",
            "Encounters without a principal diagnosis",
            "% without a principal diagnosis",
            "Principal diagnoses",
            "Principal diagnoses per encounter",
        ]

        is_inst = str(enc_type)[:2] in IVE_INST_ENC_TYPES

        flags = []
        for idx in grp.index:
            if not is_inst:
                flags.append(False)
                continue
            try:
                pct      = float(grp.at[idx, "PCT_WITHOUT_PRINCIPAL_DX"])
                enc_with = float(grp.at[idx, "ENCOUNTERS_WITH_PRINCIPAL_DX"])
                avg      = float(grp.at[idx, "AVG_PRINCIPAL_DX_PER_ENC"])
                flags.append(pct > 10 or enc_with == 0 or avg > 2.0)
            except (ValueError, TypeError):
                flags.append(False)

        groups.append({
            "title": enc_type,
            "df":    display,
            "fmt":   fmt,
            "masks": {"blue": _row_mask(display, flags)},
        })

    return {"groups": groups}


# ── Table IVH — term type tiers per table ────────────────────────────────────

def build_table_ivh_frame(records: list) -> dict:
    dicts = records_to_dicts(records)
    if dicts and "STATUS" in dicts[0]:
        return {"error": dicts[0].get("DETAIL", "Unknown error running Table IVH.")}
    df = records_to_df(records)

    df["NUMERATOR"] = pd.to_numeric(df["NUMERATOR"], errors="coerce")
    df["PCT"]       = pd.to_numeric(df["PCT"],       errors="coerce")

    fmt = {
        "Numerator":  lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "Percentage": lambda x: f"{x:.2f}"    if pd.notna(x) else "—",
    }

    groups = []
    for table_name, grp in df.groupby("TABLE_NAME", sort=False):
        grp = grp.reset_index(drop=True)

        display = grp[["TIER", "TIER_DESCRIPTION", "TERM_TYPES", "NUMERATOR", "PCT"]].copy()
        display.columns = [
            "Term Type Tier", "Term Type Tier Description", "Term Types",
            "Numerator", "Percentage",
        ]

        flags = []
        for idx in grp.index:
            if grp.at[idx, "TIER"] != "Tier 1":
                flags.append(False)
                continue
            try:
                pct = float(grp.at[idx, "PCT"])
                num = float(grp.at[idx, "NUMERATOR"])
                flags.append(pct < 80 or num == 0)
            except (ValueError, TypeError):
                flags.append(False)

        groups.append({
            "title": table_name,
            "df":    display,
            "fmt":   fmt,
            "masks": {"blue": _row_mask(display, flags)},
        })

    return {"groups": groups}


# ── Table IVI — data-check results per table ─────────────────────────────────

def build_table_ivi_frame(records: list) -> dict:
    df = records_to_df(records)

    if "DATA_CHECK" not in df.columns and "RESULT_TYPE" in df.columns:
        def _dc(t):
            m = _re.search(r"\(DC\s*([\d.]+)\)", str(t))
            return m.group(1) if m else "n/a"
        df["DATA_CHECK"]  = df["RESULT_TYPE"].map(_dc)
        df["DESCRIPTION"] = df["RESULT_TYPE"].map(
            lambda t: _re.sub(r"\s*\(DC\s*[\d.]+\)", "", str(t)).strip()
        )

    groups = []
    for table_name, grp in df.groupby("TABLE_NAME", sort=False):
        grp = grp.reset_index(drop=True)

        display = grp[["DATA_CHECK", "DESCRIPTION", "NUMERATOR", "DENOMINATOR", "PCT"]].copy()
        display.columns = ["Data Check", "Description", "Numerator", "Denominator", "Percentage"]

        flags = []
        for idx in grp.index:
            if grp.at[idx, "DATA_CHECK"] == "n/a":
                flags.append(False)
                continue
            pct = _pct_to_float(grp.at[idx, "PCT"])
            flags.append(pct is not None and pct < 80)

        groups.append({
            "title": table_name,
            "df":    display,
            "fmt":   None,
            "masks": {"blue": _row_mask(display, flags)},
        })

    return {"groups": groups}
