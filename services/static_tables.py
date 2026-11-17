"""Static (non-SQL) report tables, e.g. Table IVI_Ref.

These are fixed reference tables printed in the EDC report that have no
query behind them; content is transcribed from the SAS package resources.
"""

from pathlib import Path

import pandas as pd

_BASE = Path(__file__).parent.parent / "resources" / "dq-analysis"

STATIC_TABLES = {
    "Table IVI_Ref": _BASE / "table_ivi_ref.csv",
}


def load_static_table(item_key: str):
    """DataFrame for a static report table, or None when the item isn't static."""
    path = STATIC_TABLES.get(str(item_key).strip())
    if path is None or not path.exists():
        return None
    return pd.read_csv(path, sep="|").fillna("")
