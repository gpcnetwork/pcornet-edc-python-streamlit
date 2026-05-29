-- DC 4.01 | Table VA | Data Persistence | Investigative
-- More than a 5% decrease in the number of patients or records in a CDM table between the previous
-- and current DataMart refresh
-- Parameters: {{ current_schema }}, {{ last_schema }}
WITH crt AS (
    SELECT 'DEMOGRAPHIC'        AS T, COUNT(*) AS R, COUNT(DISTINCT PATID) AS P FROM {{ current_schema }}.DEMOGRAPHIC        UNION ALL
    SELECT 'ENROLLMENT',               COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.ENROLLMENT         UNION ALL
    SELECT 'ENCOUNTER',                COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.ENCOUNTER          UNION ALL
    SELECT 'DIAGNOSIS',                COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.DIAGNOSIS          UNION ALL
    SELECT 'PROCEDURES',               COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.PROCEDURES         UNION ALL
    SELECT 'VITAL',                    COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.VITAL              UNION ALL
    SELECT 'DEATH',                    COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.DEATH              UNION ALL
    SELECT 'PRESCRIBING',              COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.PRESCRIBING        UNION ALL
    SELECT 'DISPENSING',               COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.DISPENSING         UNION ALL
    SELECT 'LAB_RESULT_CM',            COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.LAB_RESULT_CM     UNION ALL
    SELECT 'CONDITION',                COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.CONDITION          UNION ALL
    SELECT 'DEATH_CAUSE',              COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.DEATH_CAUSE        UNION ALL
    SELECT 'PRO_CM',                   COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.PRO_CM             UNION ALL
    SELECT 'PROVIDER',                 COUNT(*),      NULL                        FROM {{ current_schema }}.PROVIDER           UNION ALL
    SELECT 'MED_ADMIN',                COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.MED_ADMIN          UNION ALL
    SELECT 'OBS_CLIN',                 COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.OBS_CLIN           UNION ALL
    SELECT 'OBS_GEN',                  COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.OBS_GEN            UNION ALL
    SELECT 'HASH_TOKEN',               COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.HASH_TOKEN         UNION ALL
    SELECT 'IMMUNIZATION',             COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.IMMUNIZATION       UNION ALL
    SELECT 'LDS_ADDRESS_HISTORY',      COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.LDS_ADDRESS_HISTORY UNION ALL
    SELECT 'LAB_HISTORY',              COUNT(*),      NULL                        FROM {{ current_schema }}.LAB_HISTORY        UNION ALL
    SELECT 'EXTERNAL_MEDS',            COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.EXTERNAL_MEDS     UNION ALL
    SELECT 'PAT_RELATIONSHIP',         COUNT(*),      NULL                        FROM {{ current_schema }}.PAT_RELATIONSHIP
),
prv AS (
    SELECT 'DEMOGRAPHIC'        AS T, COUNT(*) AS R, COUNT(DISTINCT PATID) AS P FROM {{ last_schema }}.DEMOGRAPHIC        UNION ALL
    SELECT 'ENROLLMENT',               COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.ENROLLMENT         UNION ALL
    SELECT 'ENCOUNTER',                COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.ENCOUNTER          UNION ALL
    SELECT 'DIAGNOSIS',                COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.DIAGNOSIS          UNION ALL
    SELECT 'PROCEDURES',               COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.PROCEDURES         UNION ALL
    SELECT 'VITAL',                    COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.VITAL              UNION ALL
    SELECT 'DEATH',                    COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.DEATH              UNION ALL
    SELECT 'PRESCRIBING',              COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.PRESCRIBING        UNION ALL
    SELECT 'DISPENSING',               COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.DISPENSING         UNION ALL
    SELECT 'LAB_RESULT_CM',            COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.LAB_RESULT_CM     UNION ALL
    SELECT 'CONDITION',                COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.CONDITION          UNION ALL
    SELECT 'DEATH_CAUSE',              COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.DEATH_CAUSE        UNION ALL
    SELECT 'PRO_CM',                   COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.PRO_CM             UNION ALL
    SELECT 'PROVIDER',                 COUNT(*),      NULL                        FROM {{ last_schema }}.PROVIDER           UNION ALL
    SELECT 'MED_ADMIN',                COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.MED_ADMIN          UNION ALL
    SELECT 'OBS_CLIN',                 COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.OBS_CLIN           UNION ALL
    SELECT 'OBS_GEN',                  COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.OBS_GEN            UNION ALL
    SELECT 'HASH_TOKEN',               COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.HASH_TOKEN         UNION ALL
    SELECT 'IMMUNIZATION',             COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.IMMUNIZATION       UNION ALL
    SELECT 'LDS_ADDRESS_HISTORY',      COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.LDS_ADDRESS_HISTORY UNION ALL
    SELECT 'LAB_HISTORY',              COUNT(*),      NULL                        FROM {{ last_schema }}.LAB_HISTORY        UNION ALL
    SELECT 'EXTERNAL_MEDS',            COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.EXTERNAL_MEDS     UNION ALL
    SELECT 'PAT_RELATIONSHIP',         COUNT(*),      NULL                        FROM {{ last_schema }}.PAT_RELATIONSHIP
),
exceptions AS (
    SELECT COUNT(*) AS EXCEPTION_COUNT
    FROM crt JOIN prv ON crt.T = prv.T
    WHERE (prv.R > 0 AND ((crt.R - prv.R) / prv.R::FLOAT) * 100 < -5)
       OR (prv.P > 0 AND ((crt.P - prv.P) / prv.P::FLOAT) * 100 < -5)
)
SELECT
    '4.01'                                                                  AS CHECK_NUM,
    'More than 5% decrease in patients or records in any CDM table'        AS DESCRIPTION,
    CASE WHEN EXCEPTION_COUNT > 0 THEN 'Fail' ELSE 'Pass' END              AS STATUS
FROM exceptions
