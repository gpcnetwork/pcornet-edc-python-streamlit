"""Report metadata block — the EDCRPT cover/ToC fields.

Mirrors the metadata lines the SAS report prints on the ToC page
(edc_report.sas 436-444): Report Run Date, Query Run Date, Maximum Table
Refresh Date, Query Package, Lookback Date, CDM Version (+ DataMart identity).
"""

import datetime

from utils.constants import DQ_RUNS_TABLE, QUERY_PACKAGE
from utils.schema_inspector import SchemaInspector
from utils.sf_utils import sf_fetch
from utils.sql_loader import DEFAULT_LOOKBACK_YEARS, _minus_years


def _harvest_identity(session, schema: str) -> dict:
    try:
        rows = session.sql(
            f"SELECT MAX(DATAMARTID) AS DATAMARTID, MAX(DATAMART_NAME) AS DATAMART_NAME, "
            f"MAX(CDM_VERSION) AS CDM_VERSION FROM {schema}.HARVEST"
        ).collect()
        if rows:
            r = rows[0]
            return {
                "datamart_id": str(r[0] or ""),
                "datamart_name": str(r[1] or ""),
                "cdm_version": str(r[2] or ""),
            }
    except Exception:
        pass
    return {"datamart_id": "", "datamart_name": "", "cdm_version": ""}


def _session_started_at(meta_conn, session_id: str):
    try:
        rows = sf_fetch(
            meta_conn,
            f"SELECT MIN(STARTED_AT) AS STARTED_AT FROM {DQ_RUNS_TABLE} "
            f"WHERE COALESCE(SESSION_ID, RUN_ID) = ?",
            [session_id],
        )
        return rows[0].STARTED_AT if rows and rows[0].STARTED_AT else None
    except Exception:
        return None


def get_report_metadata(session, meta_conn, schema: str, run_env: dict) -> dict:
    """Resolve (and cache in run_env) the EDCRPT metadata block for this session."""
    cached = run_env.get("report_metadata")
    if cached:
        return cached

    refresh_max = run_env.get("cutoff_date")  # analysis_view resolves this to HARVEST refresh_max
    if refresh_max is None:
        refresh_max = SchemaInspector.get_refresh_max(session, schema)
    lookback_years = int(run_env.get("lookback_years", DEFAULT_LOOKBACK_YEARS) or DEFAULT_LOOKBACK_YEARS)
    if isinstance(refresh_max, datetime.datetime):
        refresh_max = refresh_max.date()
    lookback_date = _minus_years(refresh_max, lookback_years) if refresh_max else None

    started_at = None
    if run_env.get("session_id"):
        started_at = _session_started_at(meta_conn, run_env["session_id"])

    meta = {
        "report_run_date": datetime.date.today(),
        "query_run_date": started_at,
        "refresh_max": refresh_max,
        "query_package": QUERY_PACKAGE,
        "lookback_date": lookback_date,
        "lookback_years": lookback_years,
        "schema": schema,
        **_harvest_identity(session, schema),
    }
    run_env["report_metadata"] = meta
    return meta


def metadata_display_rows(meta: dict) -> list:
    """(label, value) pairs in EDCRPT ToC order."""
    def _d(v):
        if v is None or v == "":
            return "---"
        return str(v)[:19] if isinstance(v, datetime.datetime) else str(v)[:10] if isinstance(v, datetime.date) else str(v)

    return [
        ("Report Run Date", _d(meta.get("report_run_date"))),
        ("Query Run Date", _d(meta.get("query_run_date"))),
        ("Maximum Table Refresh Date", _d(meta.get("refresh_max"))),
        ("Query Package", meta.get("query_package", QUERY_PACKAGE)),
        ("Lookback Date", _d(meta.get("lookback_date"))),
        ("CDM Version", meta.get("cdm_version") or "---"),
        ("DataMart", " · ".join(x for x in (meta.get("datamart_id"), meta.get("datamart_name")) if x) or "---"),
    ]
