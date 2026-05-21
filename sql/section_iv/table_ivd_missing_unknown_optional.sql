-- Table IVD. Missing or Unknown Values, Optional Tables
-- Fields in optional CDM tables with missing/unknown values. Supports DC 3.03.
-- Exceptions highlighted in blue and should be investigated and explained in the ETL ADD.

SELECT TABLE_NAME, FIELD_NAME,
       TO_VARCHAR(NUMERATOR) AS NUMERATOR,
       TO_VARCHAR(DENOMINATOR) AS DENOMINATOR,
       TO_VARCHAR(ROUND(NUMERATOR * 100.0 / NULLIF(DENOMINATOR, 0), 1)) || '%' AS PCT,
       SOURCE_TABLE
FROM (
    -- DEATH
    SELECT 'DEATH' AS TABLE_NAME, 'DEATH_DATE' AS FIELD_NAME,
           SUM(CASE WHEN DEATH_DATE IS NULL THEN 1 ELSE 0 END) AS NUMERATOR,
           COUNT(*) AS DENOMINATOR, 'DEATH_L3_N' AS SOURCE_TABLE, 1 AS ROW_ORDER
    FROM {{ current_schema }}.DEATH

    UNION ALL
    SELECT 'DEATH', 'DEATH_SOURCE',
           SUM(CASE WHEN DEATH_SOURCE IN ('NI','UN','OT') OR DEATH_SOURCE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'DEATH_L3_N', 2
    FROM {{ current_schema }}.DEATH

    UNION ALL
    -- CONDITION
    SELECT 'CONDITION', 'CONDITION_TYPE',
           SUM(CASE WHEN CONDITION_TYPE IN ('NI','UN','OT') OR CONDITION_TYPE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'COND_L3_TYPE', 3
    FROM {{ current_schema }}.CONDITION
    WHERE REPORT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'CONDITION', 'CONDITION_SOURCE',
           SUM(CASE WHEN CONDITION_SOURCE IN ('NI','UN','OT') OR CONDITION_SOURCE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'COND_L3_N', 4
    FROM {{ current_schema }}.CONDITION
    WHERE REPORT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'CONDITION', 'CONDITION_STATUS',
           SUM(CASE WHEN CONDITION_STATUS IN ('NI','UN','OT') OR CONDITION_STATUS IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'COND_L3_N', 5
    FROM {{ current_schema }}.CONDITION
    WHERE REPORT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- DISPENSING
    SELECT 'DISPENSING', 'DISPENSE_SOURCE',
           SUM(CASE WHEN DISPENSE_SOURCE IN ('NI','UN','OT') OR DISPENSE_SOURCE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'DISP_L3_N', 6
    FROM {{ current_schema }}.DISPENSING
    WHERE DISPENSE_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'DISPENSING', 'NDC',
           SUM(CASE WHEN NDC IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'DISP_L3_NDC', 7
    FROM {{ current_schema }}.DISPENSING
    WHERE DISPENSE_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'DISPENSING', 'DISPENSE_AMT',
           SUM(CASE WHEN DISPENSE_AMT IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'DISP_L3_N', 8
    FROM {{ current_schema }}.DISPENSING
    WHERE DISPENSE_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- MED_ADMIN
    SELECT 'MED_ADMIN', 'MEDADMIN_CODE',
           SUM(CASE WHEN MEDADMIN_CODE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'MEDA_L3_N', 9
    FROM {{ current_schema }}.MED_ADMIN
    WHERE MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'MED_ADMIN', 'MEDADMIN_TYPE',
           SUM(CASE WHEN MEDADMIN_TYPE IN ('NI','UN','OT') OR MEDADMIN_TYPE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'MEDA_L3_TYPE', 10
    FROM {{ current_schema }}.MED_ADMIN
    WHERE MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'MED_ADMIN', 'MEDADMIN_SOURCE',
           SUM(CASE WHEN MEDADMIN_SOURCE IN ('NI','UN','OT') OR MEDADMIN_SOURCE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'MEDA_L3_N', 11
    FROM {{ current_schema }}.MED_ADMIN
    WHERE MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'MED_ADMIN', 'ENCOUNTERID',
           SUM(CASE WHEN ENCOUNTERID IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'MEDA_L3_N', 12
    FROM {{ current_schema }}.MED_ADMIN
    WHERE MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- IMMUNIZATION
    SELECT 'IMMUNIZATION', 'VX_RECORD_DATE',
           SUM(CASE WHEN VX_RECORD_DATE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'IMMUNE_L3_N', 13
    FROM {{ current_schema }}.IMMUNIZATION
    WHERE VX_RECORD_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'IMMUNIZATION', 'VX_CODE_TYPE',
           SUM(CASE WHEN VX_CODE_TYPE IN ('NI','UN','OT') OR VX_CODE_TYPE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'IMMUNE_L3_N', 14
    FROM {{ current_schema }}.IMMUNIZATION
    WHERE VX_RECORD_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'IMMUNIZATION', 'VX_SOURCE',
           SUM(CASE WHEN VX_SOURCE IN ('NI','UN','OT') OR VX_SOURCE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'IMMUNE_L3_N', 15
    FROM {{ current_schema }}.IMMUNIZATION
    WHERE VX_RECORD_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- LAB_RESULT_CM
    SELECT 'LAB_RESULT_CM', 'LAB_LOINC',
           SUM(CASE WHEN LAB_LOINC IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'LAB_L3_LOINC', 16
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'LAB_RESULT_CM', 'RESULT_LOC',
           SUM(CASE WHEN RESULT_LOC IN ('NI','UN','OT') OR RESULT_LOC IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'LAB_L3_LOC', 17
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'LAB_RESULT_CM', 'LAB_RESULT_SOURCE',
           SUM(CASE WHEN LAB_RESULT_SOURCE IN ('NI','UN','OT') OR LAB_RESULT_SOURCE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'LAB_L3_N', 18
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- OBS_CLIN
    SELECT 'OBS_CLIN', 'OBSCLIN_CODE',
           SUM(CASE WHEN OBSCLIN_CODE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'OBSCLIN_L3_CODE', 19
    FROM {{ current_schema }}.OBS_CLIN
    WHERE OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'OBS_CLIN', 'OBSCLIN_TYPE',
           SUM(CASE WHEN OBSCLIN_TYPE IN ('NI','UN','OT') OR OBSCLIN_TYPE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'OBSCLIN_L3_CODE', 20
    FROM {{ current_schema }}.OBS_CLIN
    WHERE OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- OBS_GEN
    SELECT 'OBS_GEN', 'OBSGEN_CODE',
           SUM(CASE WHEN OBSGEN_CODE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'OBSGEN_L3_CODE', 21
    FROM {{ current_schema }}.OBS_GEN
    WHERE OBSGEN_START_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'OBS_GEN', 'OBSGEN_TYPE',
           SUM(CASE WHEN OBSGEN_TYPE IN ('NI','UN','OT') OR OBSGEN_TYPE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'OBSGEN_L3_CODE', 22
    FROM {{ current_schema }}.OBS_GEN
    WHERE OBSGEN_START_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- PRO_CM
    SELECT 'PRO_CM', 'PRO_CODE',
           SUM(CASE WHEN PRO_CODE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'PROM_L3_N', 23
    FROM {{ current_schema }}.PRO_CM
    WHERE PRO_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'PRO_CM', 'PRO_TYPE',
           SUM(CASE WHEN PRO_TYPE IN ('NI','UN','OT') OR PRO_TYPE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'PROM_L3_N', 24
    FROM {{ current_schema }}.PRO_CM
    WHERE PRO_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- EXTERNAL_MEDS
    SELECT 'EXTERNAL_MEDS', 'EXTMED_SOURCE',
           SUM(CASE WHEN EXTMED_SOURCE IN ('NI','UN','OT') OR EXTMED_SOURCE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'EXTMED_L3_N', 25
    FROM {{ current_schema }}.EXTERNAL_MEDS
    WHERE EM_START_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- VITAL optional fields
    SELECT 'VITAL', 'SMOKING',
           SUM(CASE WHEN SMOKING IN ('NI','UN','OT') OR SMOKING IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'VIT_L3_SMOKING', 26
    FROM {{ current_schema }}.VITAL
    WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'VITAL', 'TOBACCO',
           SUM(CASE WHEN TOBACCO IN ('NI','UN','OT') OR TOBACCO IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'VIT_L3_TOBACCO', 27
    FROM {{ current_schema }}.VITAL
    WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- PRESCRIBING
    SELECT 'PRESCRIBING', 'RX_ORDER_DATE',
           SUM(CASE WHEN RX_ORDER_DATE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'PRES_L3_N', 28
    FROM {{ current_schema }}.PRESCRIBING
    WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'PRESCRIBING', 'RX_DAYS_SUPPLY',
           SUM(CASE WHEN RX_DAYS_SUPPLY IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'PRES_L3_N', 29
    FROM {{ current_schema }}.PRESCRIBING
    WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}')

) sub
ORDER BY ROW_ORDER
