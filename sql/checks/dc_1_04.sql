-- DC 1.04 | Table IID | Data Model Conformance | Required
-- Required fields do not conform to data model specifications for data type, length, or name
-- Parameters: {{ db_name }}, {{ current_schema }}, {{ required_structure_fqn }}
WITH mismatches AS (
    SELECT
        UPPER(rs.MEMNAME) AS TABLE_NAME,
        UPPER(rs.NAME)    AS COLUMN_NAME,
        CASE
            WHEN rs.R_TYPE = '2'
                 AND NOT (
                     UPPER(isc.DATA_TYPE) LIKE '%CHAR%'
                     OR UPPER(isc.DATA_TYPE) LIKE '%TEXT%'
                     OR UPPER(isc.DATA_TYPE) LIKE '%STRING%'
                 )
                THEN 'Type mismatch (expected STRING)'
            WHEN rs.R_TYPE = '1'
                 AND (
                     UPPER(isc.DATA_TYPE) LIKE '%CHAR%'
                     OR UPPER(isc.DATA_TYPE) LIKE '%TEXT%'
                     OR UPPER(isc.DATA_TYPE) LIKE '%STRING%'
                 )
                 AND NOT (
                     UPPER(rs.NAME) LIKE '%_TIME'
                     AND (
                         UPPER(isc.DATA_TYPE) LIKE '%TIME%'
                         OR (isc.CHARACTER_MAXIMUM_LENGTH IS NOT NULL AND isc.CHARACTER_MAXIMUM_LENGTH <= 8)
                     )
                 )
                THEN 'Type mismatch (expected non-STRING)'
            WHEN rs.R_TYPE = '2'
                 AND rs.R_LENGTH IS NOT NULL
                 AND (isc.CHARACTER_MAXIMUM_LENGTH IS NULL OR isc.CHARACTER_MAXIMUM_LENGTH < rs.R_LENGTH)
                THEN 'Length too short'
            ELSE NULL
        END AS REASON
    FROM {{ required_structure_fqn }} rs
    LEFT JOIN {{ db_name }}.INFORMATION_SCHEMA.COLUMNS isc
        ON  UPPER(rs.MEMNAME)        = UPPER(isc.TABLE_NAME)
        AND UPPER(rs.NAME)           = UPPER(isc.COLUMN_NAME)
        AND UPPER(isc.TABLE_CATALOG) = UPPER('{{ db_name }}')
        AND UPPER(isc.TABLE_SCHEMA)  = UPPER('{{ current_schema }}')
    WHERE isc.COLUMN_NAME IS NOT NULL  -- ignore missing columns (handled in 1.03)
),
flagged AS (
    SELECT TABLE_NAME, COLUMN_NAME, REASON
    FROM mismatches
    WHERE REASON IS NOT NULL
),
summary AS (
    SELECT
        '1.04'                                                                AS CHECK_NUM,
        'Required fields do not conform to data model specifications'         AS DESCRIPTION,
        CASE WHEN (SELECT COUNT(*) FROM flagged) = 0 THEN 'Pass' ELSE 'Fail' END AS STATUS,
        'SUMMARY'                                                             AS ROW_TYPE,
        CAST(NULL AS VARCHAR)                                                 AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                 AS EXC_FIELD,
        CAST(NULL AS VARCHAR)                                                 AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                  AS EXC_COUNT,
        0                                                                     AS ROW_ORDER
),
details AS (
    SELECT
        '1.04'                                                                AS CHECK_NUM,
        'Required fields do not conform to data model specifications'         AS DESCRIPTION,
        'Fail'                                                                AS STATUS,
        'DETAIL'                                                              AS ROW_TYPE,
        f.TABLE_NAME                                                          AS EXC_TABLE,
        f.COLUMN_NAME                                                         AS EXC_FIELD,
        f.REASON                                                              AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                  AS EXC_COUNT,
        ROW_NUMBER() OVER (ORDER BY f.TABLE_NAME, f.COLUMN_NAME)              AS ROW_ORDER
    FROM flagged f
)
SELECT * FROM summary
UNION ALL
SELECT * FROM details
ORDER BY ROW_ORDER
