-- DC 1.01 | Table IID | Data Model Conformance | Required
-- Required tables are not present
-- Parameters: {{ db_name }}, {{ current_schema }}
WITH required AS (
    SELECT v.TABLE_NAME FROM (VALUES
        ('DEMOGRAPHIC'),('ENROLLMENT'),('ENCOUNTER'),('DIAGNOSIS'),('PROCEDURES'),
        ('VITAL'),('DISPENSING'),('LAB_RESULT_CM'),('CONDITION'),('PRO_CM'),
        ('PRESCRIBING'),('PCORNET_TRIAL'),('DEATH'),('DEATH_CAUSE'),('MED_ADMIN'),
        ('PROVIDER'),('OBS_GEN'),('OBS_CLIN'),('HASH_TOKEN'),('LDS_ADDRESS_HISTORY'),
        ('IMMUNIZATION'),('HARVEST'),('LAB_HISTORY'),('EXTERNAL_MEDS'),('PAT_RELATIONSHIP')
    ) v(TABLE_NAME)
),
existing AS (
    SELECT UPPER(TABLE_NAME) AS TABLE_NAME
    FROM {{ db_name }}.INFORMATION_SCHEMA.TABLES
    WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}')
),
missing AS (
    SELECT r.TABLE_NAME
    FROM required r
    LEFT JOIN existing e ON UPPER(r.TABLE_NAME) = e.TABLE_NAME
    WHERE e.TABLE_NAME IS NULL
),
summary AS (
    SELECT
        '1.01'                                                                AS CHECK_NUM,
        'Required tables are not present'                                     AS DESCRIPTION,
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
        '1.01'                              AS CHECK_NUM,
        'Required tables are not present'   AS DESCRIPTION,
        'Fail'                              AS STATUS,
        'DETAIL'                            AS ROW_TYPE,
        m.TABLE_NAME                        AS EXC_TABLE,
        CAST(NULL AS VARCHAR)               AS EXC_FIELD,
        'Required table does not exist'     AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                AS EXC_COUNT,
        ROW_NUMBER() OVER (ORDER BY m.TABLE_NAME) AS ROW_ORDER
    FROM missing m
)
SELECT * FROM summary
UNION ALL
SELECT * FROM details
ORDER BY ROW_ORDER
