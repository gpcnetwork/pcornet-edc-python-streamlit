from pathlib import Path

import pandas as pd
import streamlit as st

from utils.constants import DQ_RESULTS_TABLE
from utils.db import get_meta_conn
from utils.run_repository import RunRepository
from utils.sql_loader import (
    load_sql,
    SqlLoader,
    DEFAULT_LOOKBACK_YEARS,
)
from utils.table_renderer import TableRenderer


@st.cache_data
def load_dq_checks() -> pd.DataFrame:
    base = Path(__file__).parent.parent
    return pd.read_csv(base / "resources" / "dq-analysis" / "dq_checks.csv", sep="|")


def run_single_check(
    session, check_row: pd.Series, schema: str, db_name: str,
    run_id: str, network_id: str = "", site_id: str = "",
    cutoff_date=None, prev_schema: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
) -> tuple[bool, list | str]:
    sql_file = str(check_row.get("SQL_File_Ref", "")).strip()
    check_id = f"{check_row['Data Check']}|{check_row['EDC Table']}"
    try:
        params = SqlLoader.build_display_params(
            schema, db_name, cutoff_date, prev_schema or "", lookback_years,
        )
        sql = load_sql(sql_file, **params)
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
