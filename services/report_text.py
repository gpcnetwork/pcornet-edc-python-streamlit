"""EDCRPT narrative text (titles, descriptive paragraphs, footnotes).

Loaded from resources/dq-analysis/report_text.csv, which is transcribed from
the SAS-generated gold report (the SAS `headers`/`footers` datasets). Items are
keyed by section.csv `Table` values plus the specials "TOC" and "DC Summary".
"""

from pathlib import Path

import pandas as pd

_CSV = Path(__file__).parent.parent / "resources" / "dq-analysis" / "report_text.csv"

_EMPTY = {"Title": "", "Description_Paragraph": "", "Footnote": ""}


def load_report_text() -> dict:
    """{item: {"Title": ..., "Description_Paragraph": ..., "Footnote": ...}}

    Returns {} when the resource is missing so callers can fall back gracefully.
    """
    if not _CSV.exists():
        return {}
    df = pd.read_csv(_CSV, sep="|").fillna("")
    return {
        str(r["Item"]).strip(): {
            "Title": str(r["Title"]),
            "Description_Paragraph": str(r["Description_Paragraph"]),
            "Footnote": str(r["Footnote"]),
        }
        for _, r in df.iterrows()
    }


def get_report_text(texts: dict, item: str) -> dict:
    return texts.get(item, _EMPTY)
