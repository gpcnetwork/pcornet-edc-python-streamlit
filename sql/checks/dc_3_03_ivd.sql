-- DC 3.03 | Table IVD | Data Completeness | Investigative
-- More than 10% of records have missing or unknown values for optional-table fields.
--
-- SAS source of truth: edc_report.sas 8039-8692 (Table IVD, row=3.03):
--   * Numerators  (8451-8529): records whose field value is "NULL or missing", NI, UN or OT
--       - provenance/source fields from the *_L3_SOURCE / *_L3_RSOURCE distributions
--         (category in NULL/NI/UN/OT, RECORD_N)
--       - code fields MEDADMIN_CODE / PRO_CODE from MEDADM_L3_CODE_TYPE / PROCM_L3_CODE_TYPE
--         (category = "NULL or missing" ONLY -> NULL counts, no NI/UN/OT; 8476-8480)
--       - ENCOUNTERID from VIT_L3_N / LAB_L3_N / PRES_L3_N / MEDADM_L3_N
--         (statistic NULL_N -> NULL only; 8482, 8486)
--       - RX_ORDER_DATE from XTBL_L3_DATES (statistic NMISS -> NULL only; 8493-8497)
--   * Denominators (8418-8445): total table record count (*_L3_N ALL_N + NULL_N of PATID).
--   * Exception fields (8596-8599): thresholdflag=1 only for DEATH_SOURCE,
--       LAB_RESULT_CM.ENCOUNTERID, LAB_RESULT_SOURCE, MED_ADMIN.ENCOUNTERID, MEDADMIN_CODE,
--       MEDADMIN_SOURCE, PRESCRIBING.ENCOUNTERID, RX_ORDER_DATE, RX_SOURCE, PRO_CM.PRO_CODE,
--       PRO_CM.PRO_SOURCE, VITAL.ENCOUNTERID, VITAL_SOURCE when num>0, denom>0 and perc >= 10.00.
--       (Other displayed ENCOUNTERIDs - CONDITION, IMMUNIZATION, OBS_CLIN, OBS_GEN, PRO_CM -
--       are NOT exception fields.)
--   * Exception row (8625-8644): row=3.03, edc_table_s_="Table IVD".
-- The underlying DC L3 aggregates are FULL-TABLE (no date window), so no
-- {{ start_date }} / {{ end_date }} filtering is applied here.
-- Parameters: {{ current_schema }}
WITH death AS (
    SELECT COUNT(*) AS DENOM,
           COUNT_IF(DEATH_SOURCE IS NULL OR TRIM(DEATH_SOURCE) = ''
                    OR DEATH_SOURCE IN ('NI','UN','OT'))                       AS BAD_DEATH_SOURCE
    FROM {{ current_schema }}.DEATH
),
lab AS (
    SELECT COUNT(*) AS DENOM,
           COUNT_IF(ENCOUNTERID IS NULL OR TRIM(ENCOUNTERID) = '')             AS BAD_ENCOUNTERID,
           COUNT_IF(LAB_RESULT_SOURCE IS NULL OR TRIM(LAB_RESULT_SOURCE) = ''
                    OR LAB_RESULT_SOURCE IN ('NI','UN','OT'))                  AS BAD_LAB_RESULT_SOURCE
    FROM {{ current_schema }}.LAB_RESULT_CM
),
med_admin AS (
    SELECT COUNT(*) AS DENOM,
           COUNT_IF(ENCOUNTERID IS NULL OR TRIM(ENCOUNTERID) = '')             AS BAD_ENCOUNTERID,
           COUNT_IF(MEDADMIN_CODE IS NULL OR TRIM(MEDADMIN_CODE) = '')         AS BAD_MEDADMIN_CODE,
           COUNT_IF(MEDADMIN_SOURCE IS NULL OR TRIM(MEDADMIN_SOURCE) = ''
                    OR MEDADMIN_SOURCE IN ('NI','UN','OT'))                    AS BAD_MEDADMIN_SOURCE
    FROM {{ current_schema }}.MED_ADMIN
),
prescribing AS (
    SELECT COUNT(*) AS DENOM,
           COUNT_IF(ENCOUNTERID IS NULL OR TRIM(ENCOUNTERID) = '')             AS BAD_ENCOUNTERID,
           COUNT_IF(RX_ORDER_DATE IS NULL)                                     AS BAD_RX_ORDER_DATE,
           COUNT_IF(RX_SOURCE IS NULL OR TRIM(RX_SOURCE) = ''
                    OR RX_SOURCE IN ('NI','UN','OT'))                          AS BAD_RX_SOURCE
    FROM {{ current_schema }}.PRESCRIBING
),
pro_cm AS (
    SELECT COUNT(*) AS DENOM,
           COUNT_IF(PRO_CODE IS NULL OR TRIM(PRO_CODE) = '')                   AS BAD_PRO_CODE,
           COUNT_IF(PRO_SOURCE IS NULL OR TRIM(PRO_SOURCE) = ''
                    OR PRO_SOURCE IN ('NI','UN','OT'))                         AS BAD_PRO_SOURCE
    FROM {{ current_schema }}.PRO_CM
),
vital AS (
    SELECT COUNT(*) AS DENOM,
           COUNT_IF(ENCOUNTERID IS NULL OR TRIM(ENCOUNTERID) = '')             AS BAD_ENCOUNTERID,
           COUNT_IF(VITAL_SOURCE IS NULL OR TRIM(VITAL_SOURCE) = ''
                    OR VITAL_SOURCE IN ('NI','UN','OT'))                       AS BAD_VITAL_SOURCE
    FROM {{ current_schema }}.VITAL
),
fields AS (
    SELECT 'DEATH' AS EXC_TABLE, 'DEATH_SOURCE' AS EXC_FIELD, BAD_DEATH_SOURCE AS NUM, DENOM FROM death
    UNION ALL SELECT 'LAB_RESULT_CM', 'ENCOUNTERID',       BAD_ENCOUNTERID,       DENOM FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'LAB_RESULT_SOURCE', BAD_LAB_RESULT_SOURCE, DENOM FROM lab
    UNION ALL SELECT 'MED_ADMIN',     'ENCOUNTERID',       BAD_ENCOUNTERID,       DENOM FROM med_admin
    UNION ALL SELECT 'MED_ADMIN',     'MEDADMIN_CODE',     BAD_MEDADMIN_CODE,     DENOM FROM med_admin
    UNION ALL SELECT 'MED_ADMIN',     'MEDADMIN_SOURCE',   BAD_MEDADMIN_SOURCE,   DENOM FROM med_admin
    UNION ALL SELECT 'PRESCRIBING',   'ENCOUNTERID',       BAD_ENCOUNTERID,       DENOM FROM prescribing
    UNION ALL SELECT 'PRESCRIBING',   'RX_ORDER_DATE',     BAD_RX_ORDER_DATE,     DENOM FROM prescribing
    UNION ALL SELECT 'PRESCRIBING',   'RX_SOURCE',         BAD_RX_SOURCE,         DENOM FROM prescribing
    UNION ALL SELECT 'PRO_CM',        'PRO_CODE',          BAD_PRO_CODE,          DENOM FROM pro_cm
    UNION ALL SELECT 'PRO_CM',        'PRO_SOURCE',        BAD_PRO_SOURCE,        DENOM FROM pro_cm
    UNION ALL SELECT 'VITAL',         'ENCOUNTERID',       BAD_ENCOUNTERID,       DENOM FROM vital
    UNION ALL SELECT 'VITAL',         'VITAL_SOURCE',      BAD_VITAL_SOURCE,      DENOM FROM vital
),
evaluated AS (
    SELECT EXC_TABLE, EXC_FIELD, NUM, DENOM,
           CASE WHEN NUM > 0 AND DENOM > 0 THEN 100.0 * NUM / DENOM END AS PCT,
           -- SAS edc_report.sas 8594-8599: perc only computed when num>0 and denom>0;
           -- thresholdflag=1 when perc >= 10.00
           CASE WHEN NUM > 0 AND DENOM > 0 AND 100.0 * NUM / DENOM >= 10.0 THEN 1 ELSE 0 END AS FLAG
    FROM fields
),
summary AS (
    SELECT
        '3.03'                                                                 AS CHECK_NUM,
        'Table IVD: more than 10% of records have missing or unknown values for optional-table fields' AS DESCRIPTION,
        CASE WHEN (SELECT MAX(FLAG) FROM evaluated) = 1 THEN 'Fail' ELSE 'Pass' END AS STATUS,
        'SUMMARY'                                                              AS ROW_TYPE,
        CAST(NULL AS VARCHAR)                                                  AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                  AS EXC_FIELD,
        CAST(NULL AS VARCHAR)                                                  AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                   AS EXC_COUNT,
        0                                                                      AS ROW_ORDER
),
details AS (
    SELECT
        '3.03'                                                                 AS CHECK_NUM,
        'Table IVD: more than 10% of records have missing or unknown values for optional-table fields' AS DESCRIPTION,
        CASE WHEN FLAG = 1 THEN 'Fail' ELSE 'Pass' END                         AS STATUS,
        'DETAIL'                                                               AS ROW_TYPE,
        e.EXC_TABLE                                                            AS EXC_TABLE,
        e.EXC_FIELD                                                            AS EXC_FIELD,
        'missing/unknown ' || COALESCE(TO_VARCHAR(ROUND(e.PCT, 1)::DECIMAL(10,1)), '0.0') || '% ('
            || TO_VARCHAR(e.NUM) || '/' || TO_VARCHAR(e.DENOM) || ')'          AS EXC_DETAIL,
        e.NUM                                                                  AS EXC_COUNT,
        ROW_NUMBER() OVER (ORDER BY e.EXC_TABLE, e.EXC_FIELD)                  AS ROW_ORDER
    FROM evaluated e
)
SELECT * FROM summary
UNION ALL
SELECT * FROM details
ORDER BY ROW_ORDER
