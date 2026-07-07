import datetime
import streamlit as st

from utils.network_repository import NetworkRepository
from utils.db import get_meta_conn, get_session

from views.sidebar_nav import render_sidebar_nav
from utils.schema_inspector import SchemaInspector
from views.home_view import render_home_view
from views.run_env_view import render_run_env_view
from views.analysis_view import render_analysis_view


st.set_page_config(layout="wide")


def _restore_from_url():
    """On browser refresh, repopulate active_site and run_env from URL query params."""
    if st.session_state.get("active_site"):
        return

    site_id = st.query_params.get("site_id")
    if not site_id:
        return

    site = NetworkRepository(get_meta_conn()).get_site_by_id(site_id)
    if not site:
        return

    st.session_state["active_site"] = site
    st.session_state["nav_network"] = {
        "NETWORK_ID":   site["network_id"],
        "NETWORK_NAME": site["network_name"],
    }

    schema     = st.query_params.get("schema")
    session_id = st.query_params.get("session_id")
    if not schema or not session_id:
        return

    cutoff_str = st.query_params.get("cutoff", "")
    try:
        cutoff = datetime.date.fromisoformat(cutoff_str)
    except (ValueError, TypeError):
        cutoff = None

    try:
        lookback_years = int(st.query_params.get("lookback", "10"))
    except (ValueError, TypeError):
        lookback_years = 10

    prev_schema = st.query_params.get("prev_schema") or None

    st.session_state["run_env"] = {
        "current_schema":     schema,
        "prev_schema":        prev_schema,
        "cutoff_date":        cutoff,
        "lookback_years":     lookback_years,
        "loaded_run_id":      None,
        "session_id":         session_id,
        "restore_session_id": session_id,
    }


_restore_from_url()  # must run before sidebar so active_site/nav_network are set


render_sidebar_nav()

active  = st.session_state.get("active_site")
run_env = st.session_state.get("run_env")

if not active:
    render_home_view()
elif not run_env:
    session = get_session()
    schemas = SchemaInspector.get_schema(session)
    render_run_env_view(session, schemas)
else:
    session = get_session()
    render_analysis_view(session)
