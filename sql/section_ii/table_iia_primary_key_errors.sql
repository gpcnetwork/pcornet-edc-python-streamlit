-- Table IIA. Primary Key Errors
-- Required primary key definitions for all 25 CDM tables. Supports DC 1.05.
-- Exceptions (HAS_PK_ERROR = Yes) are highlighted in red and must be corrected.
-- ALL_N = total records; DISTINCT_N = distinct primary key records.
-- Parameters: {{ current_schema }}

WITH counts AS (
    SELECT 'DEMOGRAPHIC' AS TABLE_NAME,
           COUNT(*) AS ALL_N,
           COUNT(DISTINCT PATID) AS DISTINCT_N,
           1 AS ROW_ORDER
    FROM {{ current_schema }}.DEMOGRAPHIC
    UNION ALL
    SELECT 'ENROLLMENT',
           COUNT(*),
           COUNT(DISTINCT PATID || TO_VARCHAR(ENR_START_DATE) || ENR_BASIS),
           2
    FROM {{ current_schema }}.ENROLLMENT
    UNION ALL
    SELECT 'ENCOUNTER',
           COUNT(*),
           COUNT(DISTINCT ENCOUNTERID),
           3
    FROM {{ current_schema }}.ENCOUNTER
    UNION ALL
    SELECT 'DIAGNOSIS',
           COUNT(*),
           COUNT(DISTINCT DIAGNOSISID),
           4
    FROM {{ current_schema }}.DIAGNOSIS
    UNION ALL
    SELECT 'PROCEDURES',
           COUNT(*),
           COUNT(DISTINCT PROCEDURESID),
           5
    FROM {{ current_schema }}.PROCEDURES
    UNION ALL
    SELECT 'VITAL',
           COUNT(*),
           COUNT(DISTINCT VITALID),
           6
    FROM {{ current_schema }}.VITAL
    UNION ALL
    SELECT 'LAB_RESULT_CM',
           COUNT(*),
           COUNT(DISTINCT LAB_RESULT_CM_ID),
           7
    FROM {{ current_schema }}.LAB_RESULT_CM
    UNION ALL
    SELECT 'PRESCRIBING',
           COUNT(*),
           COUNT(DISTINCT PRESCRIBINGID),
           8
    FROM {{ current_schema }}.PRESCRIBING
    UNION ALL
    SELECT 'DISPENSING',
           COUNT(*),
           COUNT(DISTINCT DISPENSINGID),
           9
    FROM {{ current_schema }}.DISPENSING
    UNION ALL
    SELECT 'DEATH',
           COUNT(*),
           COUNT(DISTINCT PATID || COALESCE(DEATH_SOURCE, '')),
           10
    FROM {{ current_schema }}.DEATH
    UNION ALL
    SELECT 'DEATH_CAUSE',
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
           COUNT(*),
           COUNT(DISTINCT CONDITIONID),
           12
    FROM {{ current_schema }}.CONDITION
    UNION ALL
    SELECT 'IMMUNIZATION',
           COUNT(*),
           COUNT(DISTINCT IMMUNIZATIONID),
           13
    FROM {{ current_schema }}.IMMUNIZATION
    UNION ALL
    SELECT 'MED_ADMIN',
           COUNT(*),
           COUNT(DISTINCT MEDADMINID),
           14
    FROM {{ current_schema }}.MED_ADMIN
    UNION ALL
    SELECT 'OBS_CLIN',
           COUNT(*),
           COUNT(DISTINCT OBSCLINID),
           15
    FROM {{ current_schema }}.OBS_CLIN
    UNION ALL
    SELECT 'OBS_GEN',
           COUNT(*),
           COUNT(DISTINCT OBSGENID),
           16
    FROM {{ current_schema }}.OBS_GEN
    UNION ALL
    SELECT 'HASH_TOKEN',
           COUNT(*),
           COUNT(DISTINCT PATID || COALESCE(TOKEN_ENCRYPTION_KEY, '')),
           17
    FROM {{ current_schema }}.HASH_TOKEN
    UNION ALL
    SELECT 'LDS_ADDRESS_HISTORY',
           COUNT(*),
           COUNT(DISTINCT ADDRESSID),
           18
    FROM {{ current_schema }}.LDS_ADDRESS_HISTORY
    UNION ALL
    SELECT 'PRO_CM',
           COUNT(*),
           COUNT(DISTINCT PRO_CM_ID),
           19
    FROM {{ current_schema }}.PRO_CM
    UNION ALL
    SELECT 'PROVIDER',
           COUNT(*),
           COUNT(DISTINCT PROVIDERID),
           20
    FROM {{ current_schema }}.PROVIDER
    UNION ALL
    SELECT 'PCORNET_TRIAL',
           COUNT(*),
           COUNT(DISTINCT PATID || COALESCE(TRIALID, '') || COALESCE(PARTICIPANTID, '')),
           21
    FROM {{ current_schema }}.PCORNET_TRIAL
    UNION ALL
    SELECT 'EXTERNAL_MEDS',
           COUNT(*),
           COUNT(DISTINCT PATID || COALESCE(EXTMEDID, '')),
           22
    FROM {{ current_schema }}.EXTERNAL_MEDS
    UNION ALL
    SELECT 'PAT_RELATIONSHIP',
           COUNT(*),
           COUNT(DISTINCT PATID_1 || COALESCE(PATID_2, '') || COALESCE(RELATIONSHIP_TYPE, '')),
           23
    FROM {{ current_schema }}.PAT_RELATIONSHIP
    UNION ALL
    SELECT 'LAB_HISTORY',
           COUNT(*),
           COUNT(DISTINCT LABHISTORYID),
           24
    FROM {{ current_schema }}.LAB_HISTORY
    UNION ALL
    SELECT 'HARVEST',
           COUNT(*),
           COUNT(DISTINCT NETWORKID || COALESCE(DATAMARTID, '')),
           25
    FROM {{ current_schema }}.HARVEST
),
specs AS (
    SELECT 'DEMOGRAPHIC'         AS TABLE_NAME, 'PATID is unique'                                                                             AS CDM_PK_SPEC UNION ALL
    SELECT 'ENROLLMENT',                         'PATID + ENR_START_DATE + ENR_BASIS are unique'                                                             UNION ALL
    SELECT 'ENCOUNTER',                          'ENCOUNTERID is unique'                                                                                     UNION ALL
    SELECT 'DIAGNOSIS',                          'DIAGNOSISID is unique'                                                                                     UNION ALL
    SELECT 'PROCEDURES',                         'PROCEDURESID is unique'                                                                                    UNION ALL
    SELECT 'VITAL',                              'VITALID is unique'                                                                                         UNION ALL
    SELECT 'LAB_RESULT_CM',                      'LAB_RESULT_CM_ID is unique'                                                                               UNION ALL
    SELECT 'PRESCRIBING',                        'PRESCRIBINGID is unique'                                                                                   UNION ALL
    SELECT 'DISPENSING',                         'DISPENSINGID is unique'                                                                                    UNION ALL
    SELECT 'DEATH',                              'PATID + DEATH_SOURCE are unique'                                                                           UNION ALL
    SELECT 'DEATH_CAUSE',                        'PATID + DEATH_CAUSE + DEATH_CAUSE_CODE + DEATH_CAUSE_TYPE + DEATH_CAUSE_SOURCE are unique'                 UNION ALL
    SELECT 'CONDITION',                          'CONDITIONID is unique'                                                                                     UNION ALL
    SELECT 'IMMUNIZATION',                       'IMMUNIZATIONID is unique'                                                                                  UNION ALL
    SELECT 'MED_ADMIN',                          'MEDADMINID is unique'                                                                                      UNION ALL
    SELECT 'OBS_CLIN',                           'OBSCLINID is unique'                                                                                       UNION ALL
    SELECT 'OBS_GEN',                            'OBSGENID is unique'                                                                                        UNION ALL
    SELECT 'HASH_TOKEN',                         'PATID + TOKEN_ENCRYPTION_KEY are unique'                                                                   UNION ALL
    SELECT 'LDS_ADDRESS_HISTORY',                'ADDRESSID is unique'                                                                                       UNION ALL
    SELECT 'PRO_CM',                             'PRO_CM_ID is unique'                                                                                       UNION ALL
    SELECT 'PROVIDER',                           'PROVIDERID is unique'                                                                                      UNION ALL
    SELECT 'PCORNET_TRIAL',                      'PATID + TRIALID + PARTICIPANTID are unique'                                                                UNION ALL
    SELECT 'EXTERNAL_MEDS',                      'PATID + EXTMEDID are unique'                                                                               UNION ALL
    SELECT 'PAT_RELATIONSHIP',                   'PATID_1 + PATID_2 + RELATIONSHIP_TYPE are unique'                                                          UNION ALL
    SELECT 'LAB_HISTORY',                        'LABHISTORYID is unique'                                                                                    UNION ALL
    SELECT 'HARVEST',                            'NETWORKID + DATAMARTID are unique'
)
SELECT
    c.TABLE_NAME                                                       AS "Table",
    s.CDM_PK_SPEC                                                      AS "CDM specifications for primary keys",
    c.ALL_N,
    c.DISTINCT_N,
    CASE WHEN c.ALL_N = c.DISTINCT_N THEN 'No' ELSE 'Yes' END         AS HAS_PK_ERROR
FROM counts c
JOIN specs s ON s.TABLE_NAME = c.TABLE_NAME
ORDER BY c.ROW_ORDER
