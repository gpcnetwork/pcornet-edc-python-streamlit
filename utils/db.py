import os
import threading
from pathlib import Path

import streamlit as st

_MODE = os.environ.get("DQ_APP_MODE", "snowflake")


# ── Snowflake session ──────────────────────────────────────────────────────────

@st.cache_resource
def get_session():
    """Return the Snowflake session, cached for the lifetime of the app process.

    Local    → Session built from st.secrets["connections"]["snowflake"]
    Snowflake → get_active_session() (Streamlit in Snowflake)
    """
    if _MODE == "local":
        from snowflake.snowpark import Session
        return Session.builder.configs(st.secrets["connections"]["snowflake"]).create()
    else:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()


# ── Metadata tables ────────────────────────────────────────────────────────────

_sf_tables_ready = False
_sf_init_lock    = threading.Lock()


def _ensure_sf_tables(session) -> None:
    """Create DQ metadata tables in Snowflake if they don't exist (once per process)."""
    global _sf_tables_ready
    if _sf_tables_ready:
        return
    with _sf_init_lock:
        if not _sf_tables_ready:
            ddl = (Path(__file__).parent.parent / "sql" / "init_snowflake.sql").read_text()
            for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
                session.sql(stmt).collect()
            _sf_tables_ready = True


def get_meta_conn():
    """Return the Snowflake session, ensuring metadata tables exist."""
    session = get_session()
    if session is None:
        raise RuntimeError("No active Snowflake session available.")
    _ensure_sf_tables(session)
    return session
