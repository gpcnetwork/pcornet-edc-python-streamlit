import streamlit as st


def render_home_view():
    st.title("PCORNet Empirical Data Curation Report")
    st.write("")
    st.info(
        "Select a network from the sidebar, then click a site to activate it. "
        "Once active, the full DQ analysis suite will appear here."
    )
