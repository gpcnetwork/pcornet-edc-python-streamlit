import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from utils.db import get_meta_conn
from utils.run_repository import RunRepository
from utils.sql_loader import load_sql
from utils.table_renderer import TableRenderer


def _to_date(d) -> datetime.date | None:
    if d is None:
        return None
    if isinstance(d, datetime.date):
        return d
    try:
        return datetime.date.fromisoformat(str(d)[:10])
    except (ValueError, TypeError):
        return None


@st.cache_data
def load_sections() -> pd.DataFrame:
    base = Path(__file__).parent.parent
    return pd.read_csv(base / "dq-analysis" / "section.csv", sep="|")


def run_section_item(
    session, sql_file: str, schema: str, db_name: str,
    run_id: str, query_name: str, network_id: str = "", site_id: str = "",
    prev_schema: str | None = None,
    cutoff_date=None,
) -> tuple[bool, list]:
    try:
        cutoff_dt    = _to_date(cutoff_date)
        ref_dt       = cutoff_dt or datetime.date.today()
        start_dt     = ref_dt.replace(year=ref_dt.year - 5)
        year_1_dt    = ref_dt.replace(year=ref_dt.year - 1)
        report_month = ref_dt.replace(day=1)
        last_schema  = prev_schema or ""

        sql = load_sql(
            sql_file,
            current_schema=schema,
            db_name=db_name,
            prev_schema=last_schema,
            last_schema=last_schema,
            start_date=str(start_dt),
            end_date=str(ref_dt),
            cutoff_date=str(cutoff_dt) if cutoff_dt else None,
            filter_date=str(start_dt),
            year_1=str(year_1_dt),
            report_month=str(report_month),
            loinc_ref_fqn="",
            rx_ref_cte="SELECT NULL::VARCHAR AS rxcui_str, NULL::VARCHAR AS tier_norm WHERE 1=0",
        )
        records = RunRepository(get_meta_conn()).run_sql_with_meta(
            session, sql, query_name, run_id, network_id, site_id
        )
        return True, records
    except Exception as exc:
        return False, [{"STATUS": "ERROR", "DETAIL": str(exc)}]


def render_table_item(records: list) -> str:
    if not records:
        return "<p><em>No results returned.</em></p>"
    return TableRenderer.generate_generic_table(records, [], [], [], None)
