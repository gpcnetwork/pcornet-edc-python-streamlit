from pathlib import Path

import pandas as pd
import streamlit as st

from utils.db import get_meta_conn
from utils.run_repository import RunRepository
from utils.sql_loader import load_sql, SqlLoader, DEFAULT_LOOKBACK_YEARS
from utils.table_renderer import TableRenderer


@st.cache_data
def load_sections() -> pd.DataFrame:
    base = Path(__file__).parent.parent
    return pd.read_csv(base / "resources" / "dq-analysis" / "section.csv", sep="|")


def run_section_item(
    session, sql_file: str, schema: str, db_name: str,
    run_id: str, query_name: str, network_id: str = "", site_id: str = "",
    prev_schema: str | None = None,
    cutoff_date=None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
) -> tuple[bool, list]:
    try:
        params = SqlLoader.build_display_params(
            schema, db_name, cutoff_date, prev_schema or "", lookback_years,
        )
        sql = load_sql(sql_file, **params)
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
