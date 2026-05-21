class SchemaInspector:
    """Queries Snowflake information schema for schema, table, and column metadata."""

    @staticmethod
    def get_schema(session) -> list:
        rows = session.sql("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA").collect()
        return [row.SCHEMA_NAME for row in rows]

    @staticmethod
    def table_exists(session, schema: str, table: str) -> bool:
        try:
            rows = session.sql(f"""
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                WHERE UPPER(TABLE_SCHEMA) = UPPER('{schema}')
                  AND UPPER(TABLE_NAME)   = UPPER('{table}')
                LIMIT 1
            """).collect()
            return len(rows) > 0
        except Exception:
            return False

    @staticmethod
    def get_columns(session, schema: str, table: str) -> set:
        try:
            rows = session.sql(f"""
                SELECT UPPER(COLUMN_NAME) AS COL
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE UPPER(TABLE_SCHEMA) = UPPER('{schema}')
                  AND UPPER(TABLE_NAME)   = UPPER('{table}')
            """).collect()
            return {r["COL"] for r in rows}
        except Exception:
            return set()

    @staticmethod
    def get_current_database(session) -> str:
        return session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]

    @staticmethod
    def get_current_schema(session) -> str:
        return session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
