import re as _re

import pandas as pd
import streamlit as st

_LATENCY_CONFIG = {
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


def _render_latency_table(records: list, cfg: dict) -> None:
    domains = cfg["domains"]
    labels  = cfg.get("labels", {})

    dicts = records if isinstance(records[0], dict) else \
            [{col: getattr(r, col) for col in r._fields} for r in records]
    df = pd.DataFrame(dicts)
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

    def _exc_style(d: pd.DataFrame) -> pd.DataFrame:
        styles = pd.DataFrame("", index=d.index, columns=d.columns)
        for domain in domains:
            hdr     = labels.get(domain, domain)
            pct_col = (hdr, "Percent of Benchmark Avg")
            rec_col = (hdr, "Records")
            if pct_col not in d.columns:
                continue
            for idx in d.index:
                try:
                    pct = float(d.at[idx, pct_col])
                    rec = d.at[idx, rec_col]
                    if pct < 75 or rec == 0:
                        styles.at[idx, pct_col] = "background-color: #cfe2ff"
                        styles.at[idx, rec_col] = "background-color: #cfe2ff"
                except (ValueError, TypeError):
                    pass
        return styles

    st.markdown("**Current Period — Months −0 to −11**")
    st.dataframe(
        current_df.style.apply(_exc_style, axis=None).format(fmt, na_rep="—"),
        width="stretch",
        hide_index=True,
    )

    st.markdown("**Benchmark Period — Months −12 to −23**")
    st.dataframe(
        bench_df.style.format(fmt, na_rep="—"),
        width="stretch",
        hide_index=True,
    )


def _render_table_ivi(records: list) -> None:
    dicts = records if isinstance(records[0], dict) else \
            [{col: getattr(r, col) for col in r._fields} for r in records]
    df = pd.DataFrame(dicts)

    if "DATA_CHECK" not in df.columns and "RESULT_TYPE" in df.columns:
        def _dc(t):
            m = _re.search(r"\(DC\s*([\d.]+)\)", str(t))
            return m.group(1) if m else "n/a"
        df["DATA_CHECK"]  = df["RESULT_TYPE"].map(_dc)
        df["DESCRIPTION"] = df["RESULT_TYPE"].map(
            lambda t: _re.sub(r"\s*\(DC\s*[\d.]+\)", "", str(t)).strip()
        )

    def _exc_style(d: pd.DataFrame) -> pd.DataFrame:
        styles = pd.DataFrame("", index=d.index, columns=d.columns)
        for idx in d.index:
            if d.at[idx, "Data Check"] == "n/a":
                continue
            try:
                pct = float(str(d.at[idx, "Percentage"]).replace("%", ""))
                if pct < 80:
                    styles.loc[idx, :] = "background-color: #cfe2ff"
            except (ValueError, TypeError):
                pass
        return styles

    for table_name, grp in df.groupby("TABLE_NAME", sort=False):
        st.markdown(f"**{table_name}**")
        display = grp[["DATA_CHECK", "DESCRIPTION", "NUMERATOR", "DENOMINATOR", "PCT"]].copy()
        display.columns = ["Data Check", "Description", "Numerator", "Denominator", "Percentage"]
        st.dataframe(
            display.style.apply(_exc_style, axis=None),
            width="stretch",
            hide_index=True,
        )


_DC303_IVD_FIELDS = frozenset({
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

_MISSING_HEADER = "Records with missing, NI, UN, or OT values"

_DC301_ENC_TYPES = {"AV", "IP", "ED", "EI", "TH"}

_IVE_INST_ENC_TYPES = {"EI", "IP"}  # DC 3.06 / 2.07 apply only to EI and IP

_DC302_THRESHOLDS = {"AV": 0.75, "ED": 0.75, "EI": 1.00, "IP": 1.00}

_DC303_FIELDS = frozenset({
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


def _render_table_ivd(records: list) -> None:
    dicts = records if isinstance(records[0], dict) else \
            [{col: getattr(r, col) for col in r._fields} for r in records]
    df = pd.DataFrame(dicts)

    display = pd.DataFrame({
        ("", "Table"):                    df["TABLE_NAME"].values,
        ("", "Field"):                    df["FIELD_NAME"].values,
        (_MISSING_HEADER, "Numerator"):   df["NUMERATOR"].values,
        (_MISSING_HEADER, "Denominator"): df["DENOMINATOR"].values,
        (_MISSING_HEADER, "%"):           df["PCT"].values,
    })
    display.columns = pd.MultiIndex.from_tuples(display.columns)
    display = display.reset_index(drop=True)

    def _exc_style(d: pd.DataFrame) -> pd.DataFrame:
        styles = pd.DataFrame("", index=d.index, columns=d.columns)
        for idx in d.index:
            key = (df.at[idx, "TABLE_NAME"], df.at[idx, "FIELD_NAME"])
            if key not in _DC303_IVD_FIELDS:
                continue
            try:
                pct = float(str(df.at[idx, "PCT"]).replace("%", ""))
                if pct > 10:
                    styles.loc[idx, :] = "background-color: #cfe2ff"
            except (ValueError, TypeError):
                pass
        return styles

    st.dataframe(
        display.style.apply(_exc_style, axis=None),
        width="stretch",
        hide_index=True,
    )


def _render_table_ivc(records: list) -> None:
    dicts = records if isinstance(records[0], dict) else \
            [{col: getattr(r, col) for col in r._fields} for r in records]
    df = pd.DataFrame(dicts)

    display = pd.DataFrame({
        ("", "Table"):                          df["TABLE_NAME"].values,
        ("", "Field"):                          df["FIELD_NAME"].values,
        ("", "Encounter Type Constraint"):      df["ENC_TYPE_CONSTRAINT"].values,
        (_MISSING_HEADER, "Numerator"):         df["NUMERATOR"].values,
        (_MISSING_HEADER, "Denominator"):       df["DENOMINATOR"].values,
        (_MISSING_HEADER, "%"):                 df["PCT"].values,
    })
    display.columns = pd.MultiIndex.from_tuples(display.columns)
    display = display.reset_index(drop=True)

    def _exc_style(d: pd.DataFrame) -> pd.DataFrame:
        styles = pd.DataFrame("", index=d.index, columns=d.columns)
        for idx in d.index:
            key = (df.at[idx, "TABLE_NAME"], df.at[idx, "FIELD_NAME"])
            if key not in _DC303_FIELDS:
                continue
            try:
                pct = float(str(df.at[idx, "PCT"]).replace("%", ""))
                if pct > 10:
                    styles.loc[idx, :] = "background-color: #cfe2ff"
            except (ValueError, TypeError):
                pass
        return styles

    st.dataframe(
        display.style.apply(_exc_style, axis=None),
        width="stretch",
        hide_index=True,
    )


def _render_table_iva(records: list) -> None:
    dicts = records if isinstance(records[0], dict) else \
            [{col: getattr(r, col) for col in r._fields} for r in records]
    df = pd.DataFrame(dicts)

    for col in ["DX_RECORDS", "DX_RECORDS_KNOWN_DXTYPE", "ENC_RECORDS"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["DX_PER_ENC", "DX_KNOWN_TYPE_PER_ENC"]:
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

    def _exc_style(d: pd.DataFrame) -> pd.DataFrame:
        styles = pd.DataFrame("", index=d.index, columns=d.columns)
        for idx in d.index:
            enc = df.at[idx, "ENC_TYPE"]
            if enc not in _DC301_ENC_TYPES:
                continue
            try:
                if float(df.at[idx, "DX_KNOWN_TYPE_PER_ENC"]) < 1.0:
                    styles.loc[idx, :] = "background-color: #cfe2ff"
            except (ValueError, TypeError):
                pass
        return styles

    fmt = {
        "DIAGNOSIS records":                              lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "DIAGNOSIS records with known DX_TYPE":           lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "ENCOUNTER records":                              lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "Diagnosis records per encounter":                lambda x: f"{x:.2f}"    if pd.notna(x) else "—",
        "Diagnosis records with known DX_TYPE per encounter": lambda x: f"{x:.2f}" if pd.notna(x) else "—",
    }

    st.dataframe(
        display.style.apply(_exc_style, axis=None).format(fmt, na_rep="—"),
        width="stretch",
        hide_index=True,
    )


def _render_table_ivb(records: list) -> None:
    dicts = records if isinstance(records[0], dict) else \
            [{col: getattr(r, col) for col in r._fields} for r in records]
    df = pd.DataFrame(dicts)

    for col in ["PX_RECORDS", "PX_RECORDS_KNOWN_PXTYPE", "ENC_RECORDS"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["PX_PER_ENC", "PX_KNOWN_TYPE_PER_ENC"]:
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

    def _exc_style(d: pd.DataFrame) -> pd.DataFrame:
        styles = pd.DataFrame("", index=d.index, columns=d.columns)
        for idx in d.index:
            enc       = df.at[idx, "ENC_TYPE"]
            threshold = _DC302_THRESHOLDS.get(enc)
            if threshold is None:
                continue
            try:
                if float(df.at[idx, "PX_KNOWN_TYPE_PER_ENC"]) < threshold:
                    styles.loc[idx, :] = "background-color: #cfe2ff"
            except (ValueError, TypeError):
                pass
        return styles

    fmt = {
        "PROCEDURES records":                                lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "PROCEDURES records with known PX_TYPE":             lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "ENCOUNTER records":                                 lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "Procedures records per encounter":                  lambda x: f"{x:.2f}"    if pd.notna(x) else "—",
        "Procedures records with known PX_TYPE per encounter": lambda x: f"{x:.2f}" if pd.notna(x) else "—",
    }

    st.dataframe(
        display.style.apply(_exc_style, axis=None).format(fmt, na_rep="—"),
        width="stretch",
        hide_index=True,
    )


def _render_table_ive(records: list) -> None:
    dicts = records if isinstance(records[0], dict) else \
            [{col: getattr(r, col) for col in r._fields} for r in records]
    df = pd.DataFrame(dicts)

    for col in ["ENCOUNTERS_WITH_PRINCIPAL_DX", "ENCOUNTERS_WITHOUT_PRINCIPAL_DX", "TOTAL_PRINCIPAL_DX"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["PCT_WITHOUT_PRINCIPAL_DX", "AVG_PRINCIPAL_DX_PER_ENC"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    fmt = {
        "Encounters with a principal diagnosis":    lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "Encounters without a principal diagnosis": lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "% without a principal diagnosis":          lambda x: f"{x:.2f}"    if pd.notna(x) else "—",
        "Principal diagnoses":                      lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "Principal diagnoses per encounter":        lambda x: f"{x:.2f}"    if pd.notna(x) else "—",
    }

    for enc_type, grp in df.groupby("ENCOUNTER_TYPE", sort=False):
        st.markdown(f"**{enc_type}**")
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

        is_inst = str(enc_type)[:2] in _IVE_INST_ENC_TYPES

        def _exc_style(d: pd.DataFrame, _grp=grp, _is_inst=is_inst) -> pd.DataFrame:
            styles = pd.DataFrame("", index=d.index, columns=d.columns)
            if not _is_inst:
                return styles
            for idx in d.index:
                try:
                    pct      = float(_grp.at[idx, "PCT_WITHOUT_PRINCIPAL_DX"])
                    enc_with = float(_grp.at[idx, "ENCOUNTERS_WITH_PRINCIPAL_DX"])
                    avg      = float(_grp.at[idx, "AVG_PRINCIPAL_DX_PER_ENC"])
                    if pct > 10 or enc_with == 0 or avg > 2.0:
                        styles.loc[idx, :] = "background-color: #cfe2ff"
                except (ValueError, TypeError):
                    pass
            return styles

        st.dataframe(
            display.style.apply(_exc_style, axis=None).format(fmt, na_rep="—"),
            width="stretch",
            hide_index=True,
        )


def _render_table_ivh(records: list) -> None:
    dicts = records if isinstance(records[0], dict) else \
            [{col: getattr(r, col) for col in r._fields} for r in records]
    if "STATUS" in dicts[0]:
        st.error(dicts[0].get("DETAIL", "Unknown error running Table IVH."))
        return
    df = pd.DataFrame(dicts)

    df["NUMERATOR"] = pd.to_numeric(df["NUMERATOR"], errors="coerce")
    df["PCT"]       = pd.to_numeric(df["PCT"],       errors="coerce")

    fmt = {
        "Numerator":  lambda x: f"{int(x):,}" if pd.notna(x) else "—",
        "Percentage": lambda x: f"{x:.2f}"    if pd.notna(x) else "—",
    }

    for table_name, grp in df.groupby("TABLE_NAME", sort=False):
        st.markdown(f"**{table_name}**")
        grp = grp.reset_index(drop=True)

        display = grp[["TIER", "TIER_DESCRIPTION", "TERM_TYPES", "NUMERATOR", "PCT"]].copy()
        display.columns = [
            "Term Type Tier", "Term Type Tier Description", "Term Types",
            "Numerator", "Percentage",
        ]

        def _exc_style(d: pd.DataFrame, _grp=grp) -> pd.DataFrame:
            styles = pd.DataFrame("", index=d.index, columns=d.columns)
            for idx in d.index:
                if _grp.at[idx, "TIER"] != "Tier 1":
                    continue
                try:
                    pct = float(_grp.at[idx, "PCT"])
                    num = float(_grp.at[idx, "NUMERATOR"])
                    if pct < 80 or num == 0:
                        styles.loc[idx, :] = "background-color: #cfe2ff"
                except (ValueError, TypeError):
                    pass
            return styles

        st.dataframe(
            display.style.apply(_exc_style, axis=None).format(fmt, na_rep="—"),
            width="stretch",
            hide_index=True,
        )


def render_table(records: list, item_name: str) -> bool:
    """Render special Section IV tables. Returns True if handled."""
    name = item_name.upper()
    cfg  = _LATENCY_CONFIG.get(name)
    if cfg:
        _render_latency_table(records, cfg)
        return True
    if name == "TABLE IVA":
        _render_table_iva(records)
        return True
    if name == "TABLE IVB":
        _render_table_ivb(records)
        return True
    if name == "TABLE IVC":
        _render_table_ivc(records)
        return True
    if name == "TABLE IVD":
        _render_table_ivd(records)
        return True
    if name == "TABLE IVE":
        _render_table_ive(records)
        return True
    if name == "TABLE IVH":
        _render_table_ivh(records)
        return True
    if name == "TABLE IVI":
        _render_table_ivi(records)
        return True
    return False
