import base64
from datetime import date, datetime


class Row:
    """Attribute-access row compatible with the Snowflake Row interface."""

    def __init__(self, columns: tuple, values: tuple):
        self._fields = columns
        for col, val in zip(columns, values):
            setattr(self, col, val)

    def __getitem__(self, key):
        if isinstance(key, str):
            return getattr(self, key.upper())
        return getattr(self, self._fields[key])

    def asDict(self) -> dict:
        return {col: getattr(self, col) for col in self._fields}

    def __repr__(self):
        return f"Row({self.asDict()})"


def _bind(sql: str, params) -> str:
    """Inline params as SQL literals for Snowflake execution.

    Snowpark's session.sql(sql, params=[...]) is unreliable for DML in SiS.
    String substitution is safer for our controlled insert/update patterns.

    ?::JSON  → TRY_PARSE_JSON(BASE64_DECODE_STRING('..b64..'))
    ?        → NULL / TRUE/FALSE / number / 'single-quote-escaped string'
    """
    def _esc(v):
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, (int, float)):
            return str(v)
        if isinstance(v, (date, datetime)):
            return f"'{v.isoformat()}'"
        return "'" + str(v).replace("'", "''") + "'"

    result, p_iter = [], iter(params)
    i = 0
    while i < len(sql):
        if sql[i:i+7] == "?::JSON":
            v = next(p_iter)
            b64 = base64.b64encode(str(v).encode("utf-8")).decode("ascii")
            result.append(f"TRY_PARSE_JSON(BASE64_DECODE_STRING('{b64}'))")
            i += 7
        elif sql[i] == "?":
            result.append(_esc(next(p_iter)))
            i += 1
        else:
            result.append(sql[i])
            i += 1
    return "".join(result)


def sf_fetch(session, sql: str, params=None) -> list:
    """Execute a SELECT via Snowflake session and return a list of Row objects."""
    sf_sql = _bind(sql, params) if params else sql
    sf_rows = session.sql(sf_sql).collect()
    if not sf_rows:
        return []
    cols = tuple(f.upper() for f in sf_rows[0]._fields)
    return [Row(cols, tuple(r[i] for i in range(len(cols)))) for r in sf_rows]
