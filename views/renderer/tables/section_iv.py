"""Streamlit renderers for the custom Section IV tables.

All dataframe/mask/format logic lives in views/renderer/tables/section_iv_frames.py
(pure pandas, shared with the PDF exporter); this module only wraps the built
frames in `st.dataframe` Stylers.
"""

import pandas as pd
import streamlit as st

from views.renderer.tables.section_iv_frames import (
    LATENCY_BUILDERS,
    build_table_iva_frame,
    build_table_ivb_frame,
    build_table_ivc_frame,
    build_table_ivd_frame,
    build_table_ive_frame,
    build_table_ivh_frame,
    build_table_ivi_frame,
)

_EXC_CSS = "background-color: #cfe2ff"


def _mask_styles(mask: pd.DataFrame) -> pd.DataFrame:
    """Boolean mask -> Styler style frame (blue background where True)."""
    styles = pd.DataFrame("", index=mask.index, columns=mask.columns)
    return styles.mask(mask, _EXC_CSS)


def _show_frame(frame: dict, title=None) -> None:
    """Render one built frame ({"df", "fmt", "masks"}) as a styled st.dataframe."""
    if title is not None:
        st.markdown(f"**{title}**")
    blue = frame["masks"]["blue"]
    styler = frame["df"].style.apply(lambda _df, _m=blue: _mask_styles(_m), axis=None)
    if frame.get("fmt"):
        styler = styler.format(frame["fmt"], na_rep="—")
    st.dataframe(styler, width="stretch", hide_index=True)


def _show_groups(result: dict) -> None:
    for group in result["groups"]:
        _show_frame(group, title=group["title"])


def _render_latency_table(records: list, builder) -> None:
    result = builder(records)
    _show_frame(result["current"],   title=result["current"]["title"])
    _show_frame(result["benchmark"], title=result["benchmark"]["title"])


def _render_table_iva(records: list) -> None:
    _show_frame(build_table_iva_frame(records))


def _render_table_ivb(records: list) -> None:
    _show_frame(build_table_ivb_frame(records))


def _render_table_ivc(records: list) -> None:
    _show_frame(build_table_ivc_frame(records))


def _render_table_ivd(records: list) -> None:
    _show_frame(build_table_ivd_frame(records))


def _render_table_ive(records: list) -> None:
    _show_groups(build_table_ive_frame(records))


def _render_table_ivh(records: list) -> None:
    result = build_table_ivh_frame(records)
    if "error" in result:
        st.error(result["error"])
        return
    _show_groups(result)


def _render_table_ivi(records: list) -> None:
    _show_groups(build_table_ivi_frame(records))


def render_table(records: list, item_name: str) -> bool:
    """Render special Section IV tables. Returns True if handled."""
    name = item_name.upper()
    builder = LATENCY_BUILDERS.get(name)
    if builder:
        _render_latency_table(records, builder)
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
