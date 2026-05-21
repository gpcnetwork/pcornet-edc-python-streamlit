import streamlit as st

from utils.db import get_meta_conn
from utils.network_repository import NetworkRepository


def _activate(net, site):
    st.session_state["active_site"] = {
        "site_id":       site["SITE_ID"],
        "site_name":     site["SITE_NAME"],
        "network_id":    net["NETWORK_ID"],
        "network_name":  net["NETWORK_NAME"],
        "database_name": site["DATABASE_NAME"],
        "cdm_schema":    site["CDM_SCHEMA"],
    }
    st.query_params["site_id"] = site["SITE_ID"]
    for k in ("schema", "prev_schema", "cutoff", "session_id", "tab"):
        st.query_params.pop(k, None)


def render_sidebar_nav():
    with st.sidebar:
        active = st.session_state.get("active_site")
        if active:
            st.success(
                f"**{active['site_name']}**  \n"
                f"`{active['database_name']}`"
            )
            run_env = st.session_state.get("run_env")
            if run_env:
                parts = [f"Schema: **{run_env['current_schema']}**"]
                if run_env.get("prev_schema"):
                    parts.append(f"Prev: **{run_env['prev_schema']}**")
                if run_env.get("cutoff_date"):
                    parts.append(f"Cutoff: **{run_env['cutoff_date']}**")
                st.caption("  ·  ".join(parts))
                if st.button("⌂ Home", key="sb_reconfig", width="stretch"):
                    st.session_state.pop("run_env", None)
                    for k in ("schema", "prev_schema", "cutoff", "session_id", "tab"):
                        st.query_params.pop(k, None)
                    st.rerun()
            if st.button("⊘ Clear Active Site", key="sb_clear", width="stretch"):
                st.session_state.pop("active_site",    None)
                st.session_state.pop("run_env",        None)
                st.session_state.pop("nav_network",    None)
                st.session_state.pop("sb_site_select", None)
                st.query_params.clear()
                st.rerun()
            st.markdown("---")

        nav_network = st.session_state.get("nav_network")
        if nav_network:
            _render_sites(nav_network, active)
        else:
            _render_networks()


def _render_networks():
    with st.sidebar:
        st.markdown("### Networks")
        net_repo = NetworkRepository(get_meta_conn())
        networks = net_repo.list_networks()

        if not networks:
            st.caption("No networks yet.")

        for net in networks:
            if st.button(
                f"{net['NETWORK_NAME']}  ({net['SITE_COUNT']})",
                key=f"sb_net_{net['NETWORK_ID']}",
                width="stretch",
            ):
                st.session_state["nav_network"] = net.asDict()
                st.rerun()

        with st.expander("＋ New Network"):
            with st.form("sb_create_network_form"):
                name = st.text_input("Name *", placeholder="e.g. PCORNET")
                desc = st.text_area("Description", placeholder="Optional")
                if st.form_submit_button("Save"):
                    if name.strip():
                        new_id = net_repo.create_network(name.strip(), desc.strip())
                        st.session_state["nav_network"] = {
                            "NETWORK_ID":   new_id,
                            "NETWORK_NAME": name.strip(),
                            "DESCRIPTION":  desc.strip(),
                            "SITE_COUNT":   0,
                        }
                        st.rerun()
                    else:
                        st.error("Name required.")


def _render_sites(net, active):
    with st.sidebar:
        if st.button("← Networks", key="sb_back_networks", width="stretch"):
            st.session_state.pop("nav_network", None)
            st.rerun()

        st.markdown(f"### {net['NETWORK_NAME']}")
        if net.get("DESCRIPTION"):
            st.caption(net["DESCRIPTION"])
        st.markdown("---")

        net_repo = NetworkRepository(get_meta_conn())
        sites = net_repo.list_sites(net["NETWORK_ID"])

        if sites:
            site_options = [s.asDict() for s in sites]
            default_idx = 0
            if active and active.get("network_id") == net["NETWORK_ID"]:
                names = [s["SITE_NAME"] for s in site_options]
                try:
                    default_idx = names.index(active["site_name"]) + 1
                except ValueError:
                    default_idx = 0

            labels = ["— Select a site —"] + [s["SITE_NAME"] for s in site_options]
            chosen = st.selectbox("Site", labels, index=default_idx, key="sb_site_select")
            if chosen != "— Select a site —":
                idx = labels.index(chosen) - 1
                site = site_options[idx]
                if not active or active.get("site_id") != site["SITE_ID"]:
                    _activate(net, site)
                    st.session_state.pop("run_env", None)
                    st.rerun()
        else:
            st.caption("No sites yet.")

        st.markdown("---")

        with st.expander("＋ Register Site"):
            with st.form("sb_register_site_form"):
                site_name = st.text_input("Site Name *", placeholder="e.g. MU")
                db_name   = st.text_input("Database *", placeholder="e.g. PCORNET_CDM")
                if st.form_submit_button("Register"):
                    if all([site_name.strip(), db_name.strip()]):
                        site_id = net_repo.create_site(
                            net["NETWORK_ID"], site_name.strip(), db_name.strip()
                        )
                        st.session_state["active_site"] = {
                            "site_id":       site_id,
                            "site_name":     site_name.strip(),
                            "network_id":    net["NETWORK_ID"],
                            "network_name":  net["NETWORK_NAME"],
                            "database_name": db_name.strip(),
                            "cdm_schema":    "",
                        }
                        st.session_state.pop("run_env", None)
                        st.rerun()
                    else:
                        st.error("All fields required.")

        if active and active.get("network_id") == net["NETWORK_ID"]:
            with st.expander("⚠ Delete Site"):
                st.warning(f"Permanently deletes **{active['site_name']}**.")
                if st.button("Confirm Delete Site", key="sb_del_site"):
                    net_repo.delete_site(active["site_id"])
                    st.session_state.pop("active_site", None)
                    st.session_state.pop("run_env", None)
                    st.session_state.pop("sb_site_select", None)
                    st.query_params.clear()
                    st.rerun()

        with st.expander("⚠ Delete Network"):
            st.warning(f"Deletes **{net['NETWORK_NAME']}** and all its sites.")
            if st.button("Confirm Delete", key="sb_del_net"):
                net_repo.delete_network(net["NETWORK_ID"])
                st.session_state.pop("nav_network", None)
                if active and active.get("network_id") == net["NETWORK_ID"]:
                    st.session_state.pop("active_site", None)
                    st.session_state.pop("run_env", None)
                    st.query_params.clear()
                st.rerun()
