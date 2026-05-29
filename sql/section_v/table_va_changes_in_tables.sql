-- Table VA. Changes in Tables
-- This table shows changes in key DataMart attributes between the most recent approved DataMart refresh and the current DataMart refresh and supports Data Check
-- 4.01 (more than a 5% decrease in the number of patients or records in a CDM table). Data check exceptions are highlighted in blue and should be investigated and
-- explained in the ETL ADD.

WITH all_tables AS (
    SELECT * FROM VALUES
        ('DEMOGRAPHIC',           1,  'DEM_L3_N'),
        ('ENROLLMENT',            2,  'ENR_L3_N'),
        ('ENCOUNTER',             3,  'ENC_L3_N'),
        ('DIAGNOSIS',             4,  'DIA_L3_N'),
        ('PROCEDURES',            5,  'PRO_L3_N'),
        ('VITAL',                 6,  'VIT_L3_N'),
        ('DEATH',                 7,  'DEATH_L3_N'),
        ('PRESCRIBING',           8,  'PRES_L3_N'),
        ('DISPENSING',            9,  'DISP_L3_N'),
        ('LAB_RESULT_CM',        10,  'LAB_L3_N'),
        ('CONDITION',            11,  'COND_L3_N'),
        ('DEATH_CAUSE',          12,  'DEATHC_L3_N'),
        ('PRO_CM',               13,  'PROCM_L3_N'),
        ('PROVIDER',             14,  'PROV_L3_N'),
        ('MED_ADMIN',            15,  'MEDA_L3_N'),
        ('OBS_CLIN',             16,  'OBSCLIN_L3_N'),
        ('OBS_GEN',              17,  'OBSGEN_L3_N'),
        ('HASH_TOKEN',           18,  'HASH_L3_N'),
        ('IMMUNIZATION',         19,  'IMMUNE_L3_N'),
        ('LDS_ADDRESS_HISTORY',  20,  'LDSADRS_L3_N'),
        ('LAB_HISTORY',          21,  'LABHIST_L3_N'),
        ('EXTERNAL_MEDS',        22,  'EXTMED_L3_N'),
        ('PAT_RELATIONSHIP',     23,  'PATREL_L3_N')
    AS v(TABLE_NAME, ROW_ORDER)
),
crt AS (
    SELECT 'DEMOGRAPHIC'         AS T, COUNT(*) AS R, COUNT(DISTINCT PATID) AS P FROM {{ current_schema }}.DEMOGRAPHIC       UNION ALL
    SELECT 'ENROLLMENT',                COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.ENROLLMENT        UNION ALL
    SELECT 'ENCOUNTER',                 COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.ENCOUNTER         UNION ALL
    SELECT 'DIAGNOSIS',                 COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.DIAGNOSIS         UNION ALL
    SELECT 'PROCEDURES',                COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.PROCEDURES        UNION ALL
    SELECT 'VITAL',                     COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.VITAL             UNION ALL
    SELECT 'DEATH',                     COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.DEATH             UNION ALL
    SELECT 'PRESCRIBING',               COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.PRESCRIBING       UNION ALL
    SELECT 'DISPENSING',                COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.DISPENSING        UNION ALL
    SELECT 'LAB_RESULT_CM',             COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.LAB_RESULT_CM    UNION ALL
    SELECT 'CONDITION',                 COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.CONDITION         UNION ALL
    SELECT 'DEATH_CAUSE',               COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.DEATH_CAUSE       UNION ALL
    SELECT 'PRO_CM',                    COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.PRO_CM            UNION ALL
    SELECT 'PROVIDER',                  COUNT(*),      NULL                        FROM {{ current_schema }}.PROVIDER          UNION ALL
    SELECT 'MED_ADMIN',                 COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.MED_ADMIN         UNION ALL
    SELECT 'OBS_CLIN',                  COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.OBS_CLIN          UNION ALL
    SELECT 'OBS_GEN',                   COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.OBS_GEN           UNION ALL
    SELECT 'HASH_TOKEN',                COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.HASH_TOKEN        UNION ALL
    SELECT 'IMMUNIZATION',              COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.IMMUNIZATION      UNION ALL
    SELECT 'LDS_ADDRESS_HISTORY',       COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.LDS_ADDRESS_HISTORY UNION ALL
    SELECT 'LAB_HISTORY',               COUNT(*),      NULL                        FROM {{ current_schema }}.LAB_HISTORY       UNION ALL
    SELECT 'EXTERNAL_MEDS',             COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ current_schema }}.EXTERNAL_MEDS    UNION ALL
    SELECT 'PAT_RELATIONSHIP',          COUNT(*),      NULL                        FROM {{ current_schema }}.PAT_RELATIONSHIP
),
prv AS (
    SELECT 'DEMOGRAPHIC'         AS T, COUNT(*) AS R, COUNT(DISTINCT PATID) AS P FROM {{ last_schema }}.DEMOGRAPHIC       UNION ALL
    SELECT 'ENROLLMENT',                COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.ENROLLMENT        UNION ALL
    SELECT 'ENCOUNTER',                 COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.ENCOUNTER         UNION ALL
    SELECT 'DIAGNOSIS',                 COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.DIAGNOSIS         UNION ALL
    SELECT 'PROCEDURES',                COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.PROCEDURES        UNION ALL
    SELECT 'VITAL',                     COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.VITAL             UNION ALL
    SELECT 'DEATH',                     COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.DEATH             UNION ALL
    SELECT 'PRESCRIBING',               COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.PRESCRIBING       UNION ALL
    SELECT 'DISPENSING',                COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.DISPENSING        UNION ALL
    SELECT 'LAB_RESULT_CM',             COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.LAB_RESULT_CM    UNION ALL
    SELECT 'CONDITION',                 COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.CONDITION         UNION ALL
    SELECT 'DEATH_CAUSE',               COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.DEATH_CAUSE       UNION ALL
    SELECT 'PRO_CM',                    COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.PRO_CM            UNION ALL
    SELECT 'PROVIDER',                  COUNT(*),      NULL                        FROM {{ last_schema }}.PROVIDER          UNION ALL
    SELECT 'MED_ADMIN',                 COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.MED_ADMIN         UNION ALL
    SELECT 'OBS_CLIN',                  COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.OBS_CLIN          UNION ALL
    SELECT 'OBS_GEN',                   COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.OBS_GEN           UNION ALL
    SELECT 'HASH_TOKEN',                COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.HASH_TOKEN        UNION ALL
    SELECT 'IMMUNIZATION',              COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.IMMUNIZATION      UNION ALL
    SELECT 'LDS_ADDRESS_HISTORY',       COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.LDS_ADDRESS_HISTORY UNION ALL
    SELECT 'LAB_HISTORY',               COUNT(*),      NULL                        FROM {{ last_schema }}.LAB_HISTORY       UNION ALL
    SELECT 'EXTERNAL_MEDS',             COUNT(*),      COUNT(DISTINCT PATID)       FROM {{ last_schema }}.EXTERNAL_MEDS    UNION ALL
    SELECT 'PAT_RELATIONSHIP',          COUNT(*),      NULL                        FROM {{ last_schema }}.PAT_RELATIONSHIP
)

SELECT a.TABLE_NAME                                                  AS "Table",
       COALESCE(TO_VARCHAR(p.R), '')                                  AS "RECORDS__Previous Refresh",
       COALESCE(TO_VARCHAR(c.R), '')                                  AS "RECORDS__Current Refresh",
       CASE WHEN COALESCE(p.R, 0) = 0 THEN NULL
            ELSE ROUND((COALESCE(c.R, 0) - p.R) * 100.0 / p.R, 1)
       END                                                            AS "RECORDS__PCT_CHANGE",
       COALESCE(TO_VARCHAR(p.P), '')                                  AS "PATIENTS__Previous Refresh",
       COALESCE(TO_VARCHAR(c.P), '')                                  AS "PATIENTS__Current Refresh",
       CASE WHEN COALESCE(p.P, 0) = 0 THEN NULL
            ELSE ROUND((COALESCE(c.P, 0) - p.P) * 100.0 / p.P, 1)
       END                                                            AS "PATIENTS__PCT_CHANGE"
FROM all_tables a
LEFT JOIN crt c ON c.T = a.TABLE_NAME
LEFT JOIN prv p ON p.T = a.TABLE_NAME
ORDER BY a.ROW_ORDER
