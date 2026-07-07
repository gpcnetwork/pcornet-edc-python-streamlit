import datetime as _dt
import os as _os
from pathlib import Path

from jinja2 import Environment

_REF_SCHEMA = _os.environ.get("DQ_REF_SCHEMA", "PCORNET_DC_REF")
_REQUIRED_STRUCTURE_FQN = _os.environ.get(
    "DQ_REQUIRED_STRUCTURE_FQN",
    "CHARACTERIZATION.EDC_REF.REQUIRED_STRUCTURE_RAW",
)

_env = Environment()
_base = Path(__file__).parent.parent

DEFAULT_LOOKBACK_YEARS = 10


def _minus_years(d: _dt.date, n: int) -> _dt.date:
    """Subtract n years, clamping Feb 29 to Feb 28 in non-leap target years."""
    try:
        return d.replace(year=d.year - n)
    except ValueError:  # Feb 29 -> Feb 28
        return d.replace(month=2, day=28, year=d.year - n)


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
        lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    ) -> dict:
        """Build Jinja2 template params for SQL preview rendering.

        All reports run over one unified window: [ref - lookback_years, ref],
        where ref = cutoff_date if given, else today().
        """
        cutoff_str = str(cutoff_date)[:10] if cutoff_date else None
        ref_dt = _dt.date.fromisoformat(cutoff_str) if cutoff_str else _dt.date.today()
        start_date = str(_minus_years(ref_dt, lookback_years))
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
            year_1=start_date,
            zip_table="ENCOUNTER",
            zip_column="ZIP",
            loinc_ref_fqn=f"{_REF_SCHEMA}.LOINC",
            rxnorm_ref_fqn=f"{_REF_SCHEMA}.RXNORM_CUI_REF",
            required_structure_fqn=_REQUIRED_STRUCTURE_FQN,
        )
