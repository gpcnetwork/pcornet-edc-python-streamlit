-- DC 1.03 | Table IID | Data Model Conformance | Required
-- Required fields are not present. Driven by the external required-structure reference table
-- ({{ required_structure_fqn }}) so the canonical spec is single-sourced.
-- Parameters: {{ db_name }}, {{ current_schema }}, {{ required_structure_fqn }}
WITH missing AS (
    SELECT
        UPPER(rs.MEMNAME) AS TABLE_NAME,
        UPPER(rs.NAME)    AS COLUMN_NAME
    FROM {{ required_structure_fqn }} rs
    LEFT JOIN {{ db_name }}.INFORMATION_SCHEMA.COLUMNS isc
        ON  UPPER(rs.MEMNAME)        = UPPER(isc.TABLE_NAME)
        AND UPPER(rs.NAME)           = UPPER(isc.COLUMN_NAME)
        AND UPPER(isc.TABLE_CATALOG) = UPPER('{{ db_name }}')
        AND UPPER(isc.TABLE_SCHEMA)  = UPPER('{{ current_schema }}')
    WHERE isc.COLUMN_NAME IS NULL
),
summary AS (
    SELECT
        '1.03'                                                                AS CHECK_NUM,
        'Required fields are not present'                                     AS DESCRIPTION,
        CASE WHEN (SELECT COUNT(*) FROM missing) = 0 THEN 'Pass' ELSE 'Fail' END AS STATUS,
        'SUMMARY'                                                             AS ROW_TYPE,
        CAST(NULL AS VARCHAR)                                                 AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                 AS EXC_FIELD,
        CAST(NULL AS VARCHAR)                                                 AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                  AS EXC_COUNT,
        0                                                                     AS ROW_ORDER
),
details AS (
    SELECT
        '1.03'                                AS CHECK_NUM,
        'Required fields are not present'     AS DESCRIPTION,
        'Fail'                                AS STATUS,
        'DETAIL'                              AS ROW_TYPE,
        m.TABLE_NAME                          AS EXC_TABLE,
        m.COLUMN_NAME                         AS EXC_FIELD,
        'Missing field'                       AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                  AS EXC_COUNT,
        ROW_NUMBER() OVER (ORDER BY m.TABLE_NAME, m.COLUMN_NAME) AS ROW_ORDER
    FROM missing m
)
SELECT * FROM summary
UNION ALL
SELECT * FROM details
ORDER BY ROW_ORDER
