import datetime as _dt
from pathlib import Path

from jinja2 import Environment

_env = Environment()
_base = Path(__file__).parent.parent


def load_sql(rel_path: str, **params) -> str:
    """Render a SQL template file with Jinja2."""
    return _env.from_string((_base / rel_path).read_text()).render(**params)


def load_raw(rel_path: str) -> str:
    """Return the SQL template verbatim with no rendering."""
    return (_base / rel_path).read_text()


class SqlLoader:
    load_raw = staticmethod(load_raw)
    load_sql = staticmethod(load_sql)

    @staticmethod
    def build_display_params(
        schema: str, db_name: str,
        cutoff_date=None, prev_schema: str = "",
    ) -> dict:
        """Build Jinja2 template params for SQL preview rendering."""
        cutoff_str = str(cutoff_date)[:10] if cutoff_date else None
        ref_dt = _dt.date.fromisoformat(cutoff_str) if cutoff_str else _dt.date.today()
        start_date = str(ref_dt.replace(year=ref_dt.year - 5))
        return dict(
            current_schema=schema,
            db_name=db_name,
            cutoff_date=cutoff_str,
            last_schema=prev_schema,
            prev_schema=prev_schema,
            report_month=str(ref_dt.replace(day=1)),
            start_date=start_date,
            end_date=str(ref_dt),
            filter_date=start_date,
            year_1=str(ref_dt.replace(year=ref_dt.year - 1)),
            zip_table="ENCOUNTER",
            zip_column="ZIP",
        )
