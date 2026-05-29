-- DC 1.05 | Table IIA | Data Model Conformance | Required
-- Tables have primary key definition errors. PK expressions and table list mirror
-- sql/section_ii/table_iia_primary_key_errors.sql exactly.
-- Parameters: {{ current_schema }}
WITH counts AS (
    SELECT 'DEMOGRAPHIC' AS TABLE_NAME,
           'PATID' AS PK_SPEC,
           COUNT(*) AS ALL_N,
           COUNT(DISTINCT PATID) AS DISTINCT_N,
           1 AS ROW_ORDER
    FROM {{ current_schema }}.DEMOGRAPHIC
    UNION ALL
    SELECT 'ENROLLMENT',
           'PATID + ENR_START_DATE + ENR_BASIS',
           COUNT(*),
           COUNT(DISTINCT PATID || TO_VARCHAR(ENR_START_DATE) || ENR_BASIS),
           2
    FROM {{ current_schema }}.ENROLLMENT
    UNION ALL
    SELECT 'ENCOUNTER',
           'ENCOUNTERID',
           COUNT(*),
           COUNT(DISTINCT ENCOUNTERID),
           3
    FROM {{ current_schema }}.ENCOUNTER
    UNION ALL
    SELECT 'DIAGNOSIS',
           'DIAGNOSISID',
           COUNT(*),
           COUNT(DISTINCT DIAGNOSISID),
           4
    FROM {{ current_schema }}.DIAGNOSIS
    UNION ALL
    SELECT 'PROCEDURES',
           'PROCEDURESID',
           COUNT(*),
           COUNT(DISTINCT PROCEDURESID),
           5
    FROM {{ current_schema }}.PROCEDURES
    UNION ALL
    SELECT 'VITAL',
           'VITALID',
           COUNT(*),
           COUNT(DISTINCT VITALID),
           6
    FROM {{ current_schema }}.VITAL
    UNION ALL
    SELECT 'LAB_RESULT_CM',
           'LAB_RESULT_CM_ID',
           COUNT(*),
           COUNT(DISTINCT LAB_RESULT_CM_ID),
           7
    FROM {{ current_schema }}.LAB_RESULT_CM
    UNION ALL
    SELECT 'PRESCRIBING',
           'PRESCRIBINGID',
           COUNT(*),
           COUNT(DISTINCT PRESCRIBINGID),
           8
    FROM {{ current_schema }}.PRESCRIBING
    UNION ALL
    SELECT 'DISPENSING',
           'DISPENSINGID',
           COUNT(*),
           COUNT(DISTINCT DISPENSINGID),
           9
    FROM {{ current_schema }}.DISPENSING
    UNION ALL
    SELECT 'DEATH',
           'PATID + DEATH_SOURCE',
           COUNT(*),
           COUNT(DISTINCT PATID || COALESCE(DEATH_SOURCE, '')),
           10
    FROM {{ current_schema }}.DEATH
    UNION ALL
    SELECT 'DEATH_CAUSE',
           'PATID + DEATH_CAUSE + DEATH_CAUSE_CODE + DEATH_CAUSE_TYPE + DEATH_CAUSE_SOURCE',
           COUNT(*),
           COUNT(DISTINCT PATID
                       || COALESCE(DEATH_CAUSE, '')
                       || COALESCE(DEATH_CAUSE_CODE, '')
                       || COALESCE(DEATH_CAUSE_TYPE, '')
                       || COALESCE(DEATH_CAUSE_SOURCE, '')),
           11
    FROM {{ current_schema }}.DEATH_CAUSE
    UNION ALL
    SELECT 'CONDITION',
           'CONDITIONID',
           COUNT(*),
           COUNT(DISTINCT CONDITIONID),
           12
    FROM {{ current_schema }}.CONDITION
    UNION ALL
    SELECT 'IMMUNIZATION',
           'IMMUNIZATIONID',
           COUNT(*),
           COUNT(DISTINCT IMMUNIZATIONID),
           13
    FROM {{ current_schema }}.IMMUNIZATION
    UNION ALL
    SELECT 'MED_ADMIN',
           'MEDADMINID',
           COUNT(*),
           COUNT(DISTINCT MEDADMINID),
           14
    FROM {{ current_schema }}.MED_ADMIN
    UNION ALL
    SELECT 'OBS_CLIN',
           'OBSCLINID',
           COUNT(*),
           COUNT(DISTINCT OBSCLINID),
           15
    FROM {{ current_schema }}.OBS_CLIN
    UNION ALL
    SELECT 'OBS_GEN',
           'OBSGENID',
           COUNT(*),
           COUNT(DISTINCT OBSGENID),
           16
    FROM {{ current_schema }}.OBS_GEN
    UNION ALL
    SELECT 'HASH_TOKEN',
           'PATID + TOKEN_ENCRYPTION_KEY',
           COUNT(*),
           COUNT(DISTINCT PATID || COALESCE(TOKEN_ENCRYPTION_KEY, '')),
           17
    FROM {{ current_schema }}.HASH_TOKEN
    UNION ALL
    SELECT 'LDS_ADDRESS_HISTORY',
           'ADDRESSID',
           COUNT(*),
           COUNT(DISTINCT ADDRESSID),
           18
    FROM {{ current_schema }}.LDS_ADDRESS_HISTORY
    UNION ALL
    SELECT 'PRO_CM',
           'PRO_CM_ID',
           COUNT(*),
           COUNT(DISTINCT PRO_CM_ID),
           19
    FROM {{ current_schema }}.PRO_CM
    UNION ALL
    SELECT 'PROVIDER',
           'PROVIDERID',
           COUNT(*),
           COUNT(DISTINCT PROVIDERID),
           20
    FROM {{ current_schema }}.PROVIDER
    UNION ALL
    SELECT 'PCORNET_TRIAL',
           'PATID + TRIALID + PARTICIPANTID',
           COUNT(*),
           COUNT(DISTINCT PATID || COALESCE(TRIALID, '') || COALESCE(PARTICIPANTID, '')),
           21
    FROM {{ current_schema }}.PCORNET_TRIAL
    UNION ALL
    SELECT 'EXTERNAL_MEDS',
           'PATID + EXTMEDID',
           COUNT(*),
           COUNT(DISTINCT PATID || COALESCE(EXTMEDID, '')),
           22
    FROM {{ current_schema }}.EXTERNAL_MEDS
    UNION ALL
    SELECT 'PAT_RELATIONSHIP',
           'PATID_1 + PATID_2 + RELATIONSHIP_TYPE',
           COUNT(*),
           COUNT(DISTINCT PATID_1 || COALESCE(PATID_2, '') || COALESCE(RELATIONSHIP_TYPE, '')),
           23
    FROM {{ current_schema }}.PAT_RELATIONSHIP
    UNION ALL
    SELECT 'LAB_HISTORY',
           'LABHISTORYID',
           COUNT(*),
           COUNT(DISTINCT LABHISTORYID),
           24
    FROM {{ current_schema }}.LAB_HISTORY
    UNION ALL
    SELECT 'HARVEST',
           'NETWORKID + DATAMARTID',
           COUNT(*),
           COUNT(DISTINCT NETWORKID || COALESCE(DATAMARTID, '')),
           25
    FROM {{ current_schema }}.HARVEST
),
flagged AS (
    SELECT TABLE_NAME, PK_SPEC, ALL_N, DISTINCT_N, ROW_ORDER
    FROM counts
    WHERE ALL_N <> DISTINCT_N
),
summary AS (
    SELECT
        '1.05'                                                                AS CHECK_NUM,
        'Tables have primary key definition errors'                           AS DESCRIPTION,
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
        '1.05'                                        AS CHECK_NUM,
        'Tables have primary key definition errors'   AS DESCRIPTION,
        'Fail'                                        AS STATUS,
        'DETAIL'                                      AS ROW_TYPE,
        f.TABLE_NAME                                  AS EXC_TABLE,
        f.PK_SPEC                                     AS EXC_FIELD,
        'Duplicate PK'                                AS EXC_DETAIL,
        (f.ALL_N - f.DISTINCT_N)                      AS EXC_COUNT,
        f.ROW_ORDER                                   AS ROW_ORDER
    FROM flagged f
)
SELECT * FROM summary
UNION ALL
SELECT * FROM details
ORDER BY ROW_ORDER
