-- DC 1.02 | Table IID | Data Model Conformance | Required
-- Required tables are not populated. DEMOGRAPHIC, ENROLLMENT, ENCOUNTER, DIAGNOSIS, PROCEDURES, and HARVEST are required
-- for all network partners; LAB_RESULT_CM, PRESCRIBING, and VITAL are required for network partners with EHR data.
-- Parameters: {{ db_name }}, {{ current_schema }}
--
-- "EHR data is included" is detected by the presence of LAB_RESULT_CM in the schema. When EHR is not
-- included, LAB_RESULT_CM / PRESCRIBING / VITAL are silently skipped (they are not required for
-- claims-only DataMarts). NULL ROW_COUNT (view) is treated as populated.
WITH required AS (
    SELECT v.TABLE_NAME, v.IS_EHR FROM (VALUES
        ('DEMOGRAPHIC',   FALSE),
        ('ENROLLMENT',    FALSE),
        ('ENCOUNTER',     FALSE),
        ('DIAGNOSIS',     FALSE),
        ('PROCEDURES',    FALSE),
        ('HARVEST',       FALSE),
        ('LAB_RESULT_CM', TRUE),
        ('PRESCRIBING',   TRUE),
        ('VITAL',         TRUE)
    ) v(TABLE_NAME, IS_EHR)
),
existing AS (
    SELECT UPPER(TABLE_NAME) AS TABLE_NAME, ROW_COUNT
    FROM {{ db_name }}.INFORMATION_SCHEMA.TABLES
    WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}')
),
ehr_present AS (
    SELECT CASE WHEN EXISTS (SELECT 1 FROM existing WHERE TABLE_NAME = 'LAB_RESULT_CM') THEN TRUE ELSE FALSE END AS HAS_EHR
),
problems AS (
    SELECT
        r.TABLE_NAME,
        r.IS_EHR,
        CASE
            WHEN e.TABLE_NAME IS NULL THEN 'Not present'
            WHEN e.ROW_COUNT = 0      THEN 'Empty'
            ELSE NULL
        END AS PROBLEM
    FROM required r
    LEFT JOIN existing e ON UPPER(r.TABLE_NAME) = e.TABLE_NAME
    CROSS JOIN ehr_present p
    WHERE (NOT r.IS_EHR OR p.HAS_EHR)  -- skip EHR tables entirely when EHR not included
),
flagged AS (
    SELECT TABLE_NAME, PROBLEM
    FROM problems
    WHERE PROBLEM IS NOT NULL
),
summary AS (
    SELECT
        '1.02'                                                                              AS CHECK_NUM,
        'Expected tables are not populated (DEMOGRAPHIC, ENROLLMENT, ENCOUNTER, DIAGNOSIS, PROCEDURES, HARVEST; plus LAB_RESULT_CM/PRESCRIBING/VITAL for EHR DataMarts)' AS DESCRIPTION,
        CASE WHEN (SELECT COUNT(*) FROM flagged) = 0 THEN 'Pass' ELSE 'Fail' END           AS STATUS,
        'SUMMARY'                                                                           AS ROW_TYPE,
        CAST(NULL AS VARCHAR)                                                               AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                               AS EXC_FIELD,
        CAST(NULL AS VARCHAR)                                                               AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                                AS EXC_COUNT,
        0                                                                                   AS ROW_ORDER
),
details AS (
    SELECT
        '1.02'                                                                              AS CHECK_NUM,
        'Expected tables are not populated (DEMOGRAPHIC, ENROLLMENT, ENCOUNTER, DIAGNOSIS, PROCEDURES, HARVEST; plus LAB_RESULT_CM/PRESCRIBING/VITAL for EHR DataMarts)' AS DESCRIPTION,
        'Fail'                                                                              AS STATUS,
        'DETAIL'                                                                            AS ROW_TYPE,
        f.TABLE_NAME                                                                        AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                               AS EXC_FIELD,
        f.PROBLEM                                                                           AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                                AS EXC_COUNT,
        ROW_NUMBER() OVER (ORDER BY f.TABLE_NAME)                                           AS ROW_ORDER
    FROM flagged f
)
SELECT * FROM summary
UNION ALL
SELECT * FROM details
ORDER BY ROW_ORDER
