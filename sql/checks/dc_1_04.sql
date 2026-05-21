-- DC 1.04: Required fields conform to expected type/length
-- Parameters: {{ db_name }}, {{ current_schema }}
WITH mismatches AS (
    SELECT COUNT(*) AS MISMATCH_COUNT
    FROM CHARACTERIZATION.EDC_REF.REQUIRED_STRUCTURE_RAW rs
    LEFT JOIN {{ db_name }}.INFORMATION_SCHEMA.COLUMNS isc
        ON  rs.MEMNAME       = isc.TABLE_NAME
        AND rs.NAME          = isc.COLUMN_NAME
        AND isc.TABLE_CATALOG = '{{ db_name }}'
        AND isc.TABLE_SCHEMA  = '{{ current_schema }}'
    WHERE
        isc.COLUMN_NAME IS NOT NULL  -- ignore missing columns (handled in 1.03)
        AND (
            -- Expected STRING but actual is not STRING
            (
                rs.R_TYPE = '2'
                AND NOT (
                    UPPER(isc.DATA_TYPE) LIKE '%CHAR%'
                    OR UPPER(isc.DATA_TYPE) LIKE '%TEXT%'
                    OR UPPER(isc.DATA_TYPE) LIKE '%STRING%'
                )
            )
            OR
            -- Expected NON-STRING but actual is STRING (with time-field exception)
            (
                rs.R_TYPE = '1'
                AND (
                    UPPER(isc.DATA_TYPE) LIKE '%CHAR%'
                    OR UPPER(isc.DATA_TYPE) LIKE '%TEXT%'
                    OR UPPER(isc.DATA_TYPE) LIKE '%STRING%'
                )
                AND NOT (
                    UPPER(rs.NAME) LIKE '%_TIME'
                    AND (
                        UPPER(isc.DATA_TYPE) LIKE '%TIME%'
                        OR (
                            isc.CHARACTER_MAXIMUM_LENGTH IS NOT NULL
                            AND isc.CHARACTER_MAXIMUM_LENGTH <= 8
                        )
                    )
                )
            )
            OR
            -- Length mismatch for STRING fields
            (
                rs.R_TYPE = '2'
                AND rs.R_LENGTH IS NOT NULL
                AND (
                    isc.CHARACTER_MAXIMUM_LENGTH IS NULL
                    OR isc.CHARACTER_MAXIMUM_LENGTH < rs.R_LENGTH
                )
            )
        )
)
SELECT
    '1.04'                                                            AS CHECK_NUM,
    'Required fields conform to expected type/length'                    AS DESCRIPTION,
    CASE WHEN MISMATCH_COUNT = 0 THEN 'Pass' ELSE 'Fail' END             AS STATUS
FROM mismatches
