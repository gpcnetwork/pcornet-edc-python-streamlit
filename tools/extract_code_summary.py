#!/usr/bin/env python3
"""Extract the "Code Summary" table from the SAS Potential Code Errors PDF.

This is the ground truth for DC 1.13 (bad-record %) and DC 1.16 (misplaced %),
broken down per CDM table + code type. The Streamlit/Snowflake reimplementation
of dc_1_13.sql / dc_1_16.sql (and Table IIF) is verified against these counts.

Usage:
    python tools/extract_code_summary.py <Potential_Code_Errors.pdf> [out.csv]

Requires ``pdftotext`` (poppler). Output is pipe-delimited.

Columns: table, code_type, total_records, bad_records, bad_record_pct,
         misplaced_records, misplaced_record_pct
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

# TABLE  CODE  Total  Bad  BadPct  Misplaced  MisplacedPct
# Table names contain no spaces (underscores); numbers may contain commas.
ROW_RE = re.compile(
    r"^\s*([A-Z_]+)\s+(\S+)\s+"
    r"([\d,]+)\s+([\d,]+)\s+([\d.]+)\s+([\d,]+)\s+([\d.]+)\s*$"
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
        table = m.group(1)
        # Skip header/summary artifacts that happen to match (none expected).
        rows.append({
            "table": table,
            "code_type": m.group(2),
            "total_records": m.group(3).replace(",", ""),
            "bad_records": m.group(4).replace(",", ""),
            "bad_record_pct": m.group(5),
            "misplaced_records": m.group(6).replace(",", ""),
            "misplaced_record_pct": m.group(7),
        })
    return rows


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    pdf = Path(sys.argv[1])
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("resources/dq-analysis/expected_code_summary.csv")
    rows = extract(pdf)
    if not rows:
        print("ERROR: no Code Summary rows parsed — check the PDF/layout.", file=sys.stderr)
        return 1
    cols = ["table", "code_type", "total_records", "bad_records", "bad_record_pct",
            "misplaced_records", "misplaced_record_pct"]
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as f:
        f.write("|".join(cols) + "\n")
        for r in rows:
            f.write("|".join(r[c] for c in cols) + "\n")
    worst = max(rows, key=lambda r: float(r["bad_record_pct"]))
    print(f"Wrote {len(rows)} rows to {out}. "
          f"Max bad_record_pct = {worst['bad_record_pct']}% "
          f"({worst['table']}/{worst['code_type']}) -> DC 1.13 expects Pass (<5%).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
