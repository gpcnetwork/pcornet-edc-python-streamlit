#!/usr/bin/env python3
"""Extract the EDCRPT "Data Check Exception Summary" into a fixture CSV.

The SAS-generated EDC report (drnoc/*_EDCRPT_all.pdf) contains a per-check
exception summary whose ``Exception?`` column is the ground-truth Pass/Fail for
the DataMart it was run on. We parse that column so the Streamlit/Snowflake
reimplementation can be verified numerically against it (see the alignment plan).

Usage:
    python tools/extract_expected_dc.py <EDCRPT.pdf> [out.csv]

Requires the ``pdftotext`` binary (poppler) on PATH. Output is pipe-delimited to
match the other files in resources/dq-analysis/.

Columns:
    check_id      normalized DC id, e.g. "1.01" (footnote suffix stripped)
    raw_id        id as printed, e.g. "DC 1.01a"
    edc_table     e.g. "Table IID"
    category      Data Model Conformance | Data Plausibility | ...
    type          Required | Investigative
    exception     Yes | No        (ground-truth: Yes == the app should Fail)
    network_pct   % of DataMarts with an exception, or "---" when not comparable
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

# One summary row: "DC <id>  Table <X>  <desc...>  <Category>  <Type>  <Yes|No|--->  <pct|--->"
ROW_RE = re.compile(
    r"^\s*(DC\s+(\d+\.\d+)[a-z]?)\s+"          # 1: raw id, 2: numeric id
    r"(Table\s+\S+)\s+"                          # 3: edc table
    r".*?"                                        # description (non-greedy)
    r"(Data Model Conformance|Data Plausibility|Data Completeness|Data Persistence)\s+"  # 4: category
    r"(Required|Investigative)\s+"               # 5: type
    r"(Yes|No|---)\s+"                            # 6: exception
    r"(\S+)\s*$"                                  # 7: network pct
)


def extract(pdf_path: Path) -> list[dict]:
    txt = subprocess.run(
        ["pdftotext", "-layout", str(pdf_path), "-"],
        capture_output=True, text=True, check=True,
    ).stdout
    rows = []
    for line in txt.splitlines():
        m = ROW_RE.match(line)
        if not m:
            continue
        rows.append({
            "check_id": m.group(2),
            "raw_id": m.group(1).replace("  ", " "),
            "edc_table": m.group(3),
            "category": m.group(4),
            "type": m.group(5),
            "exception": m.group(6),
            "network_pct": m.group(7),
        })
    return rows


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    pdf = Path(sys.argv[1])
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("resources/dq-analysis/expected_dc.csv")
    rows = extract(pdf)
    if not rows:
        print("ERROR: no exception-summary rows parsed — check the PDF/layout.", file=sys.stderr)
        return 1
    cols = ["check_id", "raw_id", "edc_table", "category", "type", "exception", "network_pct"]
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as f:
        f.write("|".join(cols) + "\n")
        for r in rows:
            f.write("|".join(r[c] for c in cols) + "\n")
    n_excep = sum(1 for r in rows if r["exception"] == "Yes")
    print(f"Wrote {len(rows)} rows to {out} ({n_excep} exception(s): "
          + ", ".join(f"{r['check_id']}/{r['edc_table']}" for r in rows if r["exception"] == "Yes")
          + ")")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
