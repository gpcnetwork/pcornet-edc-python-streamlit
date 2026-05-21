import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from utils.constants import DQ_RESULTS_TABLE
from utils.db import get_meta_conn
from utils.run_repository import RunRepository
from utils.sql_loader import load_sql
from utils.table_renderer import TableRenderer


@st.cache_data
def load_dq_checks() -> pd.DataFrame:
    base = Path(__file__).parent.parent
    return pd.read_csv(base / "dq-analysis" / "dq_checks.csv", sep="|")


def _fmt_date(d) -> str | None:
    if d is None:
        return None
    if isinstance(d, datetime.date):
        return str(d)
    try:
        return str(datetime.date.fromisoformat(str(d)[:10]))
    except (ValueError, TypeError):
        return None


def run_single_check(
    session, check_row: pd.Series, schema: str, db_name: str,
    run_id: str, network_id: str = "", site_id: str = "",
    cutoff_date=None, prev_schema: str | None = None,
) -> tuple[bool, list | str]:
    sql_file = str(check_row.get("SQL_File_Ref", "")).strip()
    check_id = f"{check_row['Data Check']}|{check_row['EDC Table']}"
    try:
        cutoff_str = _fmt_date(cutoff_date)
        report_month = (
            str(datetime.date.fromisoformat(cutoff_str).replace(day=1))
            if cutoff_str else
            str(datetime.date.today().replace(day=1))
        )
        sql = load_sql(
            sql_file,
            current_schema=schema,
            db_name=db_name,
            cutoff_date=cutoff_str,
            last_schema=prev_schema or "",
            report_month=report_month,
        )
        records = RunRepository(get_meta_conn()).run_sql_with_meta(
            session, sql, check_id, run_id, network_id, site_id
        )
        return True, records
    except Exception as exc:
        return False, [{"STATUS": "ERROR", "DETAIL": str(exc)}]


def load_previous_run_results(session, run_id: str) -> dict:
    try:
        rows = session.sql(f"""
            SELECT CHECK_ID, STATUS, TOTAL_COUNT, ERROR_MESSAGE
            FROM {DQ_RESULTS_TABLE}
            WHERE RUN_ID = '{run_id}'
        """).collect()
        return {
            row.CHECK_ID: {
                "status": row.STATUS,
                "count": row.TOTAL_COUNT,
                "error": row.ERROR_MESSAGE,
            }
            for row in rows
        }
    except Exception:
        return {}


def render_check_result(records: list) -> str:
    if not records:
        return "<p><em>No results returned.</em></p>"
    return TableRenderer.generate_generic_table(records, [], [], [], None)
