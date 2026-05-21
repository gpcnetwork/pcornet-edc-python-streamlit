import uuid

from utils.constants import DQ_NETWORKS_TABLE, DQ_SITES_TABLE
from utils.sf_utils import Row, _bind, sf_fetch


class NetworkRepository:
    """Manages network and site registry CRUD."""

    def __init__(self, conn):
        self._conn = conn

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _exec(self, sql: str, params: list):
        self._conn.sql(_bind(sql, params)).collect()

    def _fetch(self, sql: str, params: list | None = None) -> list:
        return sf_fetch(self._conn, sql, params)

    # ── Networks ───────────────────────────────────────────────────────────────

    def list_networks(self) -> list:
        return self._fetch(f"""
            SELECT n.NETWORK_ID, n.NETWORK_NAME, n.DESCRIPTION, n.CREATED_AT,
                   COUNT(s.SITE_ID) AS SITE_COUNT
            FROM {DQ_NETWORKS_TABLE} n
            LEFT JOIN {DQ_SITES_TABLE} s ON s.NETWORK_ID = n.NETWORK_ID
            GROUP BY n.NETWORK_ID, n.NETWORK_NAME, n.DESCRIPTION, n.CREATED_AT
            ORDER BY n.NETWORK_NAME
        """)

    def create_network(self, name: str, description: str = "") -> str:
        network_id = str(uuid.uuid4())
        self._exec(
            f"INSERT INTO {DQ_NETWORKS_TABLE} (NETWORK_ID, NETWORK_NAME, DESCRIPTION) VALUES (?, ?, ?)",
            [network_id, name, description],
        )
        return network_id

    def delete_network(self, network_id: str):
        self._exec(f"DELETE FROM {DQ_SITES_TABLE} WHERE NETWORK_ID = ?", [network_id])
        self._exec(f"DELETE FROM {DQ_NETWORKS_TABLE} WHERE NETWORK_ID = ?", [network_id])

    # ── Sites ──────────────────────────────────────────────────────────────────

    def list_sites(self, network_id: str) -> list:
        return self._fetch(f"""
            SELECT SITE_ID, NETWORK_ID, SITE_NAME, DATABASE_NAME,
                   CDM_SCHEMA, DESCRIPTION, CREATED_AT
            FROM {DQ_SITES_TABLE}
            WHERE NETWORK_ID = ?
            ORDER BY SITE_NAME
        """, [network_id])

    def create_site(self, network_id: str, site_name: str,
                    database_name: str, cdm_schema: str = "",
                    description: str = "") -> str:
        site_id = str(uuid.uuid4())
        self._exec(
            f"INSERT INTO {DQ_SITES_TABLE} "
            "(SITE_ID, NETWORK_ID, SITE_NAME, DATABASE_NAME, CDM_SCHEMA, DESCRIPTION) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [site_id, network_id, site_name, database_name, cdm_schema, description],
        )
        return site_id

    def delete_site(self, site_id: str):
        self._exec(f"DELETE FROM {DQ_SITES_TABLE} WHERE SITE_ID = ?", [site_id])

    def get_site_by_id(self, site_id: str) -> dict | None:
        rows = self._fetch(f"""
            SELECT s.SITE_ID, s.SITE_NAME, s.DATABASE_NAME, s.CDM_SCHEMA,
                   n.NETWORK_ID, n.NETWORK_NAME
            FROM {DQ_SITES_TABLE} s
            JOIN {DQ_NETWORKS_TABLE} n ON s.NETWORK_ID = n.NETWORK_ID
            WHERE s.SITE_ID = ?
            LIMIT 1
        """, [site_id])
        if not rows:
            return None
        r = rows[0]
        return {
            "site_id":       r["SITE_ID"],
            "site_name":     r["SITE_NAME"],
            "database_name": r["DATABASE_NAME"],
            "cdm_schema":    r["CDM_SCHEMA"] or "",
            "network_id":    r["NETWORK_ID"],
            "network_name":  r["NETWORK_NAME"],
        }


def get_network_site_from_harvest(session, current_schema: str) -> tuple:
    """Query CDM HARVEST table for network and datamart IDs."""
    try:
        rows = session.sql(
            f"SELECT NETWORKID, DATAMARTID FROM {current_schema}.HARVEST LIMIT 1"
        ).collect()
        if rows:
            return str(rows[0]["NETWORKID"] or ""), str(rows[0]["DATAMARTID"] or "")
    except Exception:
        pass
    return ("", "")
