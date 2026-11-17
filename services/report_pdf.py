"""EDC report PDF export — a faithful replica of the SAS-generated EDCRPT.

Page order mirrors the SAS report (edc_report.sas print order, 11445-11545):
cover/ToC with the DataMart metadata block -> Data Check Exception Summary
(red/blue severity, SAS rule) -> Sections I-V, one artifact per page with
title, descriptive paragraph, table/chart, and footnote. Built fully in
memory (io.BytesIO) from stored session results — no CDM queries.

reportlab/matplotlib imports happen inside functions so environments without
them can still boot the app; the export button surfaces the ImportError.
"""

import io

import pandas as pd

from services.dc_summary_service import (
    SEVERITY_BLUE,
    SEVERITY_RED,
    SUMMARY_COLUMNS,
)
from services.report_metadata import metadata_display_rows
from services.report_text import get_report_text
from services.static_tables import load_static_table
from views.renderer.frames import (
    exception_row_mask,
    pct_change_mask,
    records_to_df,
    to_multiindex,
)

_MAX_PDF_ROWS = 1000  # stored results are capped at 1000 rows anyway

_RED = (0.70, 0.00, 0.00)
_BLUE = (0.04, 0.37, 0.84)
_HEADER_BG = (0.80, 0.80, 0.80)
_RED_BG = (0.99, 0.91, 0.91)
_BLUE_BG = (0.81, 0.89, 1.00)


def _styles():
    from reportlab.lib.styles import ParagraphStyle
    base = dict(fontName="Times-Roman")
    return {
        "doc_title": ParagraphStyle("doc_title", fontName="Times-Bold", fontSize=16, leading=20),
        "title": ParagraphStyle("title", fontName="Times-Bold", fontSize=11, leading=14, spaceAfter=4),
        "desc": ParagraphStyle("desc", fontSize=9, leading=11, spaceAfter=6, **base),
        "cell": ParagraphStyle("cell", fontSize=7, leading=8.5, **base),
        "cell_hdr": ParagraphStyle("cell_hdr", fontName="Times-Bold", fontSize=7.5, leading=9),
        "footnote": ParagraphStyle("footnote", fontSize=7, leading=8.5, textColor=(0.25, 0.25, 0.25), **base),
        "placeholder": ParagraphStyle("placeholder", fontSize=9, leading=11, textColor=(0.45, 0.45, 0.45), **base),
    }


_SUPERSCRIPTS = {"ᵃ": "a", "ᵇ": "b", "ᶜ": "c"}


def _p(text, style):
    from reportlab.platypus import Paragraph
    safe = (str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
    # Times-Roman has no Unicode superscript glyphs — use reportlab markup instead
    for uni, plain in _SUPERSCRIPTS.items():
        safe = safe.replace(uni, f"<super>{plain}</super>")
    return Paragraph(safe, style)


def _fmt_value(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "---"
    return str(v)


def _header_rows(df, st):
    """One or two header rows (+ SPAN commands) from flat or MultiIndex columns."""
    spans = []
    if isinstance(df.columns, pd.MultiIndex):
        top = [str(c[0]) for c in df.columns]
        sub = [str(c[1]) for c in df.columns]
        row0 = []
        i = 0
        while i < len(top):
            j = i
            while j + 1 < len(top) and top[j + 1] == top[i] and top[i] != "":
                j += 1
            row0.extend([_p(top[i], st["cell_hdr"])] + [""] * (j - i))
            if j > i:
                spans.append(("SPAN", (i, 0), (j, 0)))
            i = j + 1
        return [row0, [_p(c, st["cell_hdr"]) for c in sub]], spans, 2
    return [[_p(str(c), st["cell_hdr"]) for c in df.columns]], spans, 1


def _df_table(df, st, red_rows=None, blue_cells=None, col_widths=None):
    """DataFrame -> styled LongTable with repeated header and SAS-like coloring."""
    from reportlab.platypus import LongTable
    from reportlab.lib import colors

    display = df.head(_MAX_PDF_ROWS)
    header, spans, n_hdr = _header_rows(display, st)
    body = [[_p(_fmt_value(v), st["cell"]) for v in row] for row in display.itertuples(index=False)]
    data = header + body

    cmds = [
        ("BACKGROUND", (0, 0), (-1, n_hdr - 1), colors.Color(*_HEADER_BG)),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.Color(0.55, 0.55, 0.55)),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
    ] + spans

    if red_rows is not None:
        for pos, flagged in enumerate(red_rows.head(_MAX_PDF_ROWS)):
            if flagged:
                cmds.append(("BACKGROUND", (0, n_hdr + pos), (-1, n_hdr + pos), colors.Color(*_RED_BG)))
    if blue_cells is not None:
        bc = blue_cells.head(_MAX_PDF_ROWS)
        for r_pos in range(len(bc)):
            for c_pos in range(len(bc.columns)):
                if bool(bc.iat[r_pos, c_pos]):
                    cmds.append(("BACKGROUND", (c_pos, n_hdr + r_pos), (c_pos, n_hdr + r_pos),
                                 colors.Color(*_BLUE_BG)))

    table = LongTable(data, repeatRows=n_hdr, colWidths=col_widths)
    table.setStyle(cmds)
    return table


def _summary_table(summary_df, st):
    from reportlab.platypus import LongTable
    from reportlab.lib import colors

    display = summary_df[SUMMARY_COLUMNS]
    header = [[_p(c, st["cell_hdr"]) for c in display.columns]]
    body = []
    cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), colors.Color(*_HEADER_BG)),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.Color(0.55, 0.55, 0.55)),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]
    exc_col = list(display.columns).index("Exception?")
    for pos, (_, row) in enumerate(summary_df.iterrows()):
        cells = [_p(_fmt_value(row[c]), st["cell"]) for c in display.columns]
        body.append(cells)
        sev = row["_severity"]
        if sev in (SEVERITY_RED, SEVERITY_BLUE):
            color = colors.Color(*(_RED if sev == SEVERITY_RED else _BLUE))
            cmds.append(("TEXTCOLOR", (exc_col, pos + 1), (exc_col, pos + 1), color))
            cmds.append(("BACKGROUND", (exc_col, pos + 1), (exc_col, pos + 1),
                         colors.Color(*(_RED_BG if sev == SEVERITY_RED else _BLUE_BG))))

    from reportlab.lib.pagesizes import landscape, letter
    page_w = landscape(letter)[0] - 72
    widths = [w * page_w for w in (0.07, 0.07, 0.42, 0.14, 0.09, 0.08, 0.13)]
    table = LongTable(header + body, repeatRows=1, colWidths=widths)
    table.setStyle(cmds)
    return table


def _section_iv_builder(item_key):
    try:
        from views.renderer.tables import section_iv_frames
    except ImportError:
        return None
    name = "build_" + item_key.strip().lower().replace(" ", "_") + "_frame"
    return getattr(section_iv_frames, name, None)


def _built_frame_flowables(built, st):
    """section_iv_frames builder result -> list of flowables (3 shapes: simple,
    grouped {"groups": [...]}, latency {"current"/"benchmark"/"benchmark_row"})."""
    from reportlab.platypus import Spacer

    def one(part):
        masks = part.get("masks", {}) or {}
        red = masks.get("red")
        blue = masks.get("blue")
        return _df_table(
            part["df"], st,
            red_rows=red.any(axis=1) if isinstance(red, pd.DataFrame) else red,
            blue_cells=blue,
        )

    if "groups" in built:
        out = []
        for grp in built["groups"]:
            if grp.get("title"):
                out.append(_p(grp["title"], st["cell_hdr"]))
            out.append(one(grp))
            out.append(Spacer(1, 6))
        return out
    if "current" in built and "benchmark" in built:
        out = []
        for key in ("current", "benchmark"):
            part = built[key]
            if part.get("title"):
                out.append(_p(part["title"], st["cell_hdr"]))
            out.append(one(part))
            out.append(Spacer(1, 6))
        return out
    if "error" in built:
        return [_p(str(built["error"]), st["placeholder"])]
    return [one(built)]


def _item_table(item_key, records, st):
    builder = _section_iv_builder(item_key)
    if builder is not None:
        try:
            return _built_frame_flowables(builder(records), st)
        except Exception:
            pass  # fall back to the generic path

    df = records_to_df(records)
    if df.empty:
        return None
    drop = [c for c in df.columns if str(c).startswith("__")]
    df = df.drop(columns=drop, errors="ignore")
    display = to_multiindex(df)
    return [_df_table(display, st,
                      red_rows=exception_row_mask(df),
                      blue_cells=pct_change_mask(display))]


def build_report_pdf(metadata: dict, summary_df, sections_df, section_results: dict,
                     report_texts: dict, rowcounts: dict | None = None,
                     dq_statuses: dict | None = None, site_label: str = "") -> bytes:
    """Assemble the full EDCRPT-replica PDF and return its bytes.

    section_results: {section slug: {item_key: records list}}.
    rowcounts: {item_key: total row count at run time} for 1000-row-cap footnotes.
    """
    from reportlab.lib.pagesizes import landscape, letter
    from reportlab.platypus import PageBreak, SimpleDocTemplate, Spacer, Table

    st = _styles()
    rowcounts = rowcounts or {}
    dq_statuses = dq_statuses or {}
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=landscape(letter),
        leftMargin=36, rightMargin=36, topMargin=36, bottomMargin=36,
        title="PCORnet Empirical Data Curation Report",
    )
    story = []

    # ── Cover / ToC ────────────────────────────────────────────────────────────
    toc_text = get_report_text(report_texts, "TOC")
    story.append(_p("PCORnet Empirical Data Curation Report", st["doc_title"]))
    if site_label:
        story.append(_p(site_label, st["desc"]))
    story.append(Spacer(1, 10))

    meta_rows = [[_p(label, st["cell_hdr"]), _p(value, st["cell"])]
                 for label, value in metadata_display_rows(metadata)]
    meta_tbl = Table(meta_rows, colWidths=[160, 220], hAlign="LEFT")
    from reportlab.lib import colors
    meta_tbl.setStyle([
        ("GRID", (0, 0), (-1, -1), 0.4, colors.Color(0.7, 0.7, 0.7)),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ])
    story.append(meta_tbl)
    story.append(Spacer(1, 12))

    toc_df = pd.DataFrame(
        [["Data Check Summary", "DC Summary", "Data Check Exception Summary", "n/a"]] + [
            [r["Section"], r["Table"], r["Table Description"],
             "n/a" if str(r.get("Data Check(s)", "")).lower() in ("nan", "n/a", "")
             else str(r["Data Check(s)"])]
            for _, r in sections_df.iterrows()
        ],
        columns=["Section", "Table", "Table Description", "Data Check(s)"],
    )
    page_w = landscape(letter)[0] - 72
    story.append(_df_table(toc_df, st, col_widths=[0.3 * page_w, 0.1 * page_w, 0.45 * page_w, 0.15 * page_w]))
    if toc_text["Footnote"]:
        story.append(Spacer(1, 6))
        story.append(_p(toc_text["Footnote"], st["footnote"]))
    story.append(PageBreak())

    # ── Data Check Exception Summary ───────────────────────────────────────────
    dc_text = get_report_text(report_texts, "DC Summary")
    story.append(_p(dc_text["Title"] or "Data Check Exception Summary", st["title"]))
    if dc_text["Description_Paragraph"]:
        story.append(_p(dc_text["Description_Paragraph"], st["desc"]))
    story.append(_summary_table(summary_df, st))
    if dc_text["Footnote"]:
        story.append(Spacer(1, 6))
        story.append(_p(dc_text["Footnote"], st["footnote"]))
    story.append(PageBreak())

    # ── Sections I-V ───────────────────────────────────────────────────────────
    charts_iii_on = dq_statuses.get("DC 2.08|Table IIIG") == "fail"

    for section_name in sections_df["Section"].unique():
        slug = section_name.split(":")[0].strip().lower().replace(" ", "_")
        results = section_results.get(slug, {})
        items = sections_df[sections_df["Section"] == section_name]

        for _, row in items.iterrows():
            item_key = str(row["Table"])
            text = get_report_text(report_texts, item_key)
            title = text["Title"] or f"{item_key}. {row['Table Description']}"
            is_chart = item_key.lower().startswith("chart")
            sql_ref = str(row.get("SQL_File_Ref", "n/a")).strip().lower()

            story.append(_p(title, st["title"]))
            if text["Description_Paragraph"]:
                story.append(_p(text["Description_Paragraph"], st["desc"]))

            # Conditional Section III charts, like SAS (only when DC 2.08 fails)
            if is_chart and item_key.upper().startswith("CHART III") and not charts_iii_on:
                story.append(_p("No DC 2.08 monthly-volume exceptions — chart omitted "
                                "(matches the SAS report, which only renders Charts IIIA-IIIG "
                                "when exceptions exist).", st["placeholder"]))
                story.append(PageBreak())
                continue

            if sql_ref in ("n/a", "nan", ""):
                static_df = load_static_table(item_key)
                if static_df is not None:
                    story.append(_df_table(static_df, st))
                else:
                    story.append(_p("Reference item — no data.", st["placeholder"]))
            elif item_key not in results or results.get(item_key) is None:
                story.append(_p("Not run in this session.", st["placeholder"]))
            else:
                records = results[item_key]
                flowables = None
                if is_chart:
                    try:
                        from services.report_charts import chart_png
                        png = chart_png(item_key, records)
                        if png:
                            from reportlab.platypus import Image
                            flowables = [Image(io.BytesIO(png), width=680, height=None,
                                               kind="proportional")]
                    except Exception:
                        flowables = None
                if flowables is None:
                    flowables = _item_table(item_key, records, st)
                if not flowables:
                    story.append(_p("No records returned.", st["placeholder"]))
                else:
                    story.extend(flowables)
                total = rowcounts.get(item_key)
                if total and total > min(len(records or []), _MAX_PDF_ROWS):
                    story.append(Spacer(1, 4))
                    story.append(_p(f"Showing first {min(len(records), _MAX_PDF_ROWS):,} "
                                    f"of {total:,} rows (stored-result cap).", st["footnote"]))

            if text["Footnote"]:
                story.append(Spacer(1, 6))
                story.append(_p(text["Footnote"], st["footnote"]))
            story.append(PageBreak())

    doc.build(story)
    return buf.getvalue()
