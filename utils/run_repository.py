import json
import uuid
from datetime import date, datetime

from utils.constants import DQ_RESULTS_TABLE, DQ_RUNS_TABLE
from utils.sf_utils import Row, _bind, sf_fetch


def _json_default(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return str(v)


class RunRepository:
    """Manages the DQ run lifecycle and per-check result logging."""

    def __init__(self, conn):
        self._conn = conn

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _exec(self, sql: str, params: list):
        bound = _bind(sql, params)
        self._conn.sql(bound).collect()
        

    def _fetch(self, sql: str, params: list | None = None) -> list:
        return sf_fetch(self._conn, sql, params)

    # ── Run lifecycle ──────────────────────────────────────────────────────────

    def begin_run(self, network_id: str, site_id: str, cdm_schema: str,
                  cutoff_date=None, triggered_by: str = "",
                  session_id: str | None = None,
                  prev_schema: str | None = None) -> str:
        run_id = str(uuid.uuid4())
        self._exec(
            f"INSERT INTO {DQ_RUNS_TABLE} "
            "(RUN_ID, NETWORK_ID, SITE_ID, CDM_SCHEMA, CUTOFF_DATE, "
            "TRIGGERED_BY, STATUS, STARTED_AT, SESSION_ID, PREV_SCHEMA) "
            "VALUES (?, ?, ?, ?, ?, ?, 'RUNNING', CURRENT_TIMESTAMP, ?, ?)",
            [run_id, network_id, site_id, str(cdm_schema),
             str(cutoff_date)[:10] if cutoff_date else None,
             str(triggered_by), session_id, prev_schema],
        )
        return run_id

    def complete_run(self, run_id: str, status: str):
        self._exec(
            f"UPDATE {DQ_RUNS_TABLE} "
            "SET STATUS = ?, COMPLETED_AT = CURRENT_TIMESTAMP WHERE RUN_ID = ?",
            [status, run_id],
        )

    # ── Session queries ────────────────────────────────────────────────────────

    def load_sessions_for_site(self, site_id: str) -> list:
        return self._fetch(f"""
            SELECT
                COALESCE(r.SESSION_ID, r.RUN_ID)                                    AS SESSION_ID,
                r.CDM_SCHEMA, r.CUTOFF_DATE,
                MAX(r.PREV_SCHEMA)                                                   AS PREV_SCHEMA,
                MIN(r.STARTED_AT)                                                    AS STARTED_AT,
                CASE WHEN SUM(CASE WHEN r.STATUS != 'COMPLETE' THEN 1 ELSE 0 END) > 0
                     THEN 'PARTIAL' ELSE 'COMPLETE' END                              AS SESSION_STATUS,
                COALESCE(SUM(c.TOTAL), 0)                                            AS TOTAL_CHECKS,
                COALESCE(SUM(CASE WHEN r.TRIGGERED_BY = 'dq_checks'
                               THEN c.DQ_PASSED ELSE c.PASSED END), 0)              AS PASS_COUNT,
                COALESCE(SUM(CASE WHEN r.TRIGGERED_BY = 'dq_checks'
                               THEN c.DQ_FAILED ELSE c.FAILED END), 0)              AS FAIL_COUNT,
                COALESCE(SUM(CASE WHEN r.TRIGGERED_BY = 'dq_checks'
                               THEN c.TOTAL     ELSE 0 END), 0)                      AS DQ_COUNT,
                COALESCE(SUM(CASE WHEN r.TRIGGERED_BY != 'dq_checks'
                               THEN c.TABLE_CNT ELSE 0 END), 0)                      AS TABLE_COUNT,
                COALESCE(SUM(CASE WHEN r.TRIGGERED_BY != 'dq_checks'
                               THEN c.CHART_CNT ELSE 0 END), 0)                      AS CHART_COUNT,
                COALESCE(SUM(c.DURATION_MS), 0)                                      AS TOTAL_DURATION_MS
            FROM {DQ_RUNS_TABLE} r
            LEFT JOIN (
                SELECT RUN_ID,
                       COUNT(*)                                                       AS TOTAL,
                       SUM(CASE WHEN STATUS = 'SUCCESS' THEN 1 ELSE 0 END)           AS PASSED,
                       SUM(CASE WHEN STATUS = 'ERROR'   THEN 1 ELSE 0 END)           AS FAILED,
                       SUM(CASE WHEN STATUS = 'SUCCESS'
                                AND RESULT_DATA[0]:STATUS::VARCHAR = 'Pass'
                           THEN 1 ELSE 0 END)                                         AS DQ_PASSED,
                       SUM(CASE WHEN STATUS = 'ERROR'
                                OR (STATUS = 'SUCCESS'
                                    AND RESULT_DATA[0]:STATUS::VARCHAR = 'Fail')
                           THEN 1 ELSE 0 END)                                         AS DQ_FAILED,
                       SUM(CASE WHEN UPPER(CHECK_ID) NOT LIKE 'CHART%' THEN 1 ELSE 0 END) AS TABLE_CNT,
                       SUM(CASE WHEN UPPER(CHECK_ID) LIKE 'CHART%' THEN 1 ELSE 0 END)     AS CHART_CNT,
                       SUM(DURATION_MS)                                               AS DURATION_MS
                FROM {DQ_RESULTS_TABLE}
                GROUP BY RUN_ID
            ) c ON c.RUN_ID = r.RUN_ID
            WHERE r.SITE_ID = ?
            GROUP BY COALESCE(r.SESSION_ID, r.RUN_ID), r.CDM_SCHEMA, r.CUTOFF_DATE
            ORDER BY MIN(r.STARTED_AT) DESC
            LIMIT 50
        """, [site_id])

    def load_session_results(self, session_id: str) -> list:
        return self._fetch(f"""
            SELECT r.TRIGGERED_BY, c.CHECK_ID, c.STATUS,
                   c.ERROR_MESSAGE, c.RESULT_DATA, c.DURATION_MS
            FROM {DQ_RESULTS_TABLE} c
            JOIN {DQ_RUNS_TABLE} r ON r.RUN_ID = c.RUN_ID
            WHERE COALESCE(r.SESSION_ID, r.RUN_ID) = ?
            ORDER BY r.TRIGGERED_BY, c.EXECUTED_AT
        """, [session_id])

    def delete_session(self, session_id: str):
        self._exec(
            f"DELETE FROM {DQ_RESULTS_TABLE} WHERE RUN_ID IN "
            f"(SELECT RUN_ID FROM {DQ_RUNS_TABLE} WHERE COALESCE(SESSION_ID, RUN_ID) = ?)",
            [session_id],
        )
        self._exec(
            f"DELETE FROM {DQ_RUNS_TABLE} WHERE COALESCE(SESSION_ID, RUN_ID) = ?",
            [session_id],
        )

    # ── Check execution ────────────────────────────────────────────────────────

    def run_sql_with_meta(self, session, sql: str, query_name: str,
                          run_id=None, network_id=None, site_id=None) -> list:
        """Execute CDM SQL via Snowflake session and log the result."""
        if run_id is None:
            return session.sql(sql).collect()
        started = datetime.utcnow()
        try:
            rows = session.sql(sql).collect()
            duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
            self._log_check(run_id, query_name, len(rows),
                            duration_ms, network_id, site_id, "SUCCESS", rows=rows)
            return rows
        except Exception as exc:
            duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
            self._log_check(run_id, query_name, 0,
                            duration_ms, network_id, site_id, "ERROR", str(exc))
            raise

    # ── Internal ───────────────────────────────────────────────────────────────

    def _log_check(self, run_id, query_name,
                   row_count, duration_ms, network_id, site_id, status,
                   error_msg="", rows=None):
        result_id = str(uuid.uuid4())
        if status == "SUCCESS":
            capped = (rows or [])[:1000]
            try:
                data_rows = [{col: v for col, v in zip(r._fields, r)} for r in capped]
                col_meta  = [{"__columns__": list(capped[0]._fields)}] if capped else []
                serialized = json.dumps(data_rows + col_meta, default=_json_default)
                
            except Exception as ser_exc:
                serialized = json.dumps([{"STATUS": "ERROR", "DETAIL": str(ser_exc)}])
        else:
            serialized = json.dumps([{"STATUS": "ERROR", "DETAIL": error_msg}], default=_json_default)
    
        self._exec(
            f"INSERT INTO {DQ_RESULTS_TABLE} "
            "(RESULT_ID, RUN_ID, CHECK_ID, CHECK_NAME, STATUS, "
            "TOTAL_COUNT, ERROR_MESSAGE, NETWORK_ID, SITE_ID, DURATION_MS, RESULT_DATA) "
            "SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::JSON",
            [result_id, run_id, query_name, query_name,
                status, row_count,
                error_msg[:1999] if error_msg else "",
                network_id or "", site_id or "", duration_ms, serialized],
        )

