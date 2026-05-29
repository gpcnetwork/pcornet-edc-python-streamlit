-- DC 1.17 | Table IID | Data Model Conformance | Required
-- Zip codes in the ENCOUNTER or LDS_ADDRESS_HISTORY table do not conform to expected values.
-- Sub-checks:
--   * LDS_ADDRESS_HISTORY.ADDRESS_ZIP5 must be exactly 5 digits
--   * LDS_ADDRESS_HISTORY.ADDRESS_ZIP9 must be exactly 9 digits
--   * ENCOUNTER.FACILITY_LOCATION    must be exactly 5 digits
-- Excludes NULL, blank, and the placeholder tokens NULL/N/A/NA/UNKNOWN/NI/UN/OT.
-- Parameters: {{ current_schema }}
WITH violations AS (
    SELECT 'LDS_ADDRESS_HISTORY' AS EXC_TABLE, 'ADDRESS_ZIP5' AS EXC_FIELD,
           SUM(CASE WHEN NULLIF(TRIM(ADDRESS_ZIP5), '') IS NOT NULL
                     AND UPPER(TRIM(ADDRESS_ZIP5)) NOT IN ('NULL','N/A','NA','UNKNOWN','NI','UN','OT')
                     AND NOT REGEXP_LIKE(TRIM(ADDRESS_ZIP5), '^[0-9]{5}$') THEN 1 ELSE 0 END) AS EXC_COUNT,
           1 AS ROW_ORDER
    FROM {{ current_schema }}.LDS_ADDRESS_HISTORY
    UNION ALL
    SELECT 'LDS_ADDRESS_HISTORY', 'ADDRESS_ZIP9',
           SUM(CASE WHEN NULLIF(TRIM(ADDRESS_ZIP9), '') IS NOT NULL
                     AND UPPER(TRIM(ADDRESS_ZIP9)) NOT IN ('NULL','N/A','NA','UNKNOWN','NI','UN','OT')
                     AND NOT REGEXP_LIKE(TRIM(ADDRESS_ZIP9), '^[0-9]{9}$') THEN 1 ELSE 0 END),
           2
    FROM {{ current_schema }}.LDS_ADDRESS_HISTORY
    UNION ALL
    SELECT 'ENCOUNTER', 'FACILITY_LOCATION',
           SUM(CASE WHEN NULLIF(TRIM(FACILITY_LOCATION), '') IS NOT NULL
                     AND UPPER(TRIM(FACILITY_LOCATION)) NOT IN ('NULL','N/A','NA','UNKNOWN','NI','UN','OT')
                     AND NOT REGEXP_LIKE(TRIM(FACILITY_LOCATION), '^[0-9]{5}$') THEN 1 ELSE 0 END),
           3
    FROM {{ current_schema }}.ENCOUNTER
),
flagged AS (
    SELECT EXC_TABLE, EXC_FIELD, EXC_COUNT, ROW_ORDER
    FROM violations
    WHERE COALESCE(EXC_COUNT, 0) > 0
),
summary AS (
    SELECT
        '1.17'                                                                                          AS CHECK_NUM,
        'Zip codes in ENCOUNTER.FACILITY_LOCATION or LDS_ADDRESS_HISTORY.ADDRESS_ZIP5/9 do not conform' AS DESCRIPTION,
        CASE WHEN (SELECT COUNT(*) FROM flagged) = 0 THEN 'Pass' ELSE 'Fail' END                       AS STATUS,
        'SUMMARY'                                                                                       AS ROW_TYPE,
        CAST(NULL AS VARCHAR)                                                                           AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                                           AS EXC_FIELD,
        CAST(NULL AS VARCHAR)                                                                           AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                                            AS EXC_COUNT,
        0                                                                                               AS ROW_ORDER
),
details AS (
    SELECT
        '1.17'                                                                                          AS CHECK_NUM,
        'Zip codes in ENCOUNTER.FACILITY_LOCATION or LDS_ADDRESS_HISTORY.ADDRESS_ZIP5/9 do not conform' AS DESCRIPTION,
        'Fail'                                                                                          AS STATUS,
        'DETAIL'                                                                                        AS ROW_TYPE,
        f.EXC_TABLE                                                                                     AS EXC_TABLE,
        f.EXC_FIELD                                                                                     AS EXC_FIELD,
        'Non-conforming ZIP'                                                                            AS EXC_DETAIL,
        f.EXC_COUNT                                                                                     AS EXC_COUNT,
        f.ROW_ORDER                                                                                     AS ROW_ORDER
    FROM flagged f
)
SELECT * FROM summary
UNION ALL
SELECT * FROM details
ORDER BY ROW_ORDER
