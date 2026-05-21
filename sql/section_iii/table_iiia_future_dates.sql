-- Table IIIA. Future Dates
-- Date fields with values after the maximum HARVEST refresh date. Supports DC 2.01.
-- Exceptions (> 5%) highlighted in blue and should be investigated in ETL ADD.

WITH harvest_anchor AS (
    SELECT GREATEST(
        COALESCE(REFRESH_DEMOGRAPHIC_DATE,   '1900-01-01'),
        COALESCE(REFRESH_ENROLLMENT_DATE,    '1900-01-01'),
        COALESCE(REFRESH_ENCOUNTER_DATE,     '1900-01-01'),
        COALESCE(REFRESH_DIAGNOSIS_DATE,     '1900-01-01'),
        COALESCE(REFRESH_PROCEDURES_DATE,    '1900-01-01'),
        COALESCE(REFRESH_VITAL_DATE,         '1900-01-01'),
        COALESCE(REFRESH_LAB_RESULT_CM_DATE, '1900-01-01'),
        COALESCE(REFRESH_PRESCRIBING_DATE,   '1900-01-01'),
        COALESCE(REFRESH_DISPENSING_DATE,    '1900-01-01'),
        COALESCE(REFRESH_DEATH_DATE,         '1900-01-01'),
        COALESCE(REFRESH_CONDITION_DATE,     '1900-01-01'),
        COALESCE(REFRESH_MED_ADMIN_DATE,     '1900-01-01'),
        COALESCE(REFRESH_IMMUNIZATION_DATE,  '1900-01-01'),
        COALESCE(REFRESH_OBS_CLIN_DATE,      '1900-01-01'),
        COALESCE(REFRESH_OBS_GEN_DATE,       '1900-01-01')
    ) AS ANCHOR_DATE
    FROM {{ current_schema }}.HARVEST LIMIT 1
)
SELECT TABLE_NAME, FIELD_NAME,
       TO_VARCHAR(NUMERATOR) AS NUMERATOR,
       TO_VARCHAR(DENOMINATOR) AS DENOMINATOR,
       TO_VARCHAR(ROUND(NUMERATOR * 100.0 / NULLIF(DENOMINATOR, 0), 1)) || '%' AS PCT,
       SOURCE_TABLE
FROM (
    SELECT 'DEMOGRAPHIC' AS TABLE_NAME, 'BIRTH_DATE' AS FIELD_NAME,
           SUM(CASE WHEN BIRTH_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END) AS NUMERATOR,
           COUNT(*) AS DENOMINATOR,
           'DEM_L3_AGEYRSDIST1' AS SOURCE_TABLE, 1 AS ROW_ORDER
    FROM {{ current_schema }}.DEMOGRAPHIC, harvest_anchor h
    WHERE BIRTH_DATE IS NOT NULL

    UNION ALL
    SELECT 'ENROLLMENT', 'ENR_START_DATE',
           SUM(CASE WHEN ENR_START_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'ENR_L3_N', 2
    FROM {{ current_schema }}.ENROLLMENT, harvest_anchor h
    WHERE ENR_START_DATE IS NOT NULL

    UNION ALL
    SELECT 'ENROLLMENT', 'ENR_END_DATE',
           SUM(CASE WHEN ENR_END_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'ENR_L3_N', 3
    FROM {{ current_schema }}.ENROLLMENT, harvest_anchor h
    WHERE ENR_END_DATE IS NOT NULL

    UNION ALL
    SELECT 'DEATH', 'DEATH_DATE',
           SUM(CASE WHEN DEATH_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'DEATH_L3_N', 4
    FROM {{ current_schema }}.DEATH, harvest_anchor h
    WHERE DEATH_DATE IS NOT NULL

    UNION ALL
    SELECT 'ENCOUNTER', 'ADMIT_DATE',
           SUM(CASE WHEN ADMIT_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'ENC_L3_N', 5
    FROM {{ current_schema }}.ENCOUNTER, harvest_anchor h
    WHERE ADMIT_DATE IS NOT NULL

    UNION ALL
    SELECT 'ENCOUNTER', 'DISCHARGE_DATE',
           SUM(CASE WHEN DISCHARGE_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'ENC_L3_N', 6
    FROM {{ current_schema }}.ENCOUNTER, harvest_anchor h
    WHERE DISCHARGE_DATE IS NOT NULL

    UNION ALL
    SELECT 'DIAGNOSIS', 'ADMIT_DATE',
           SUM(CASE WHEN ADMIT_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'DIA_L3_N', 7
    FROM {{ current_schema }}.DIAGNOSIS, harvest_anchor h
    WHERE ADMIT_DATE IS NOT NULL

    UNION ALL
    SELECT 'PROCEDURES', 'PX_DATE',
           SUM(CASE WHEN PX_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'PRO_L3_N', 8
    FROM {{ current_schema }}.PROCEDURES, harvest_anchor h
    WHERE PX_DATE IS NOT NULL

    UNION ALL
    SELECT 'VITAL', 'MEASURE_DATE',
           SUM(CASE WHEN MEASURE_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'VIT_L3_N', 9
    FROM {{ current_schema }}.VITAL, harvest_anchor h
    WHERE MEASURE_DATE IS NOT NULL

    UNION ALL
    SELECT 'LAB_RESULT_CM', 'LAB_ORDER_DATE',
           SUM(CASE WHEN LAB_ORDER_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'LAB_L3_N', 10
    FROM {{ current_schema }}.LAB_RESULT_CM, harvest_anchor h
    WHERE LAB_ORDER_DATE IS NOT NULL

    UNION ALL
    SELECT 'LAB_RESULT_CM', 'RESULT_DATE',
           SUM(CASE WHEN RESULT_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'LAB_L3_N', 11
    FROM {{ current_schema }}.LAB_RESULT_CM, harvest_anchor h
    WHERE RESULT_DATE IS NOT NULL

    UNION ALL
    SELECT 'PRESCRIBING', 'RX_ORDER_DATE',
           SUM(CASE WHEN RX_ORDER_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'PRES_L3_N', 12
    FROM {{ current_schema }}.PRESCRIBING, harvest_anchor h
    WHERE RX_ORDER_DATE IS NOT NULL

    UNION ALL
    SELECT 'PRESCRIBING', 'RX_START_DATE',
           SUM(CASE WHEN RX_START_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'PRES_L3_N', 13
    FROM {{ current_schema }}.PRESCRIBING, harvest_anchor h
    WHERE RX_START_DATE IS NOT NULL

    UNION ALL
    SELECT 'DISPENSING', 'DISPENSE_DATE',
           SUM(CASE WHEN DISPENSE_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'DISP_L3_N', 14
    FROM {{ current_schema }}.DISPENSING, harvest_anchor h
    WHERE DISPENSE_DATE IS NOT NULL

    UNION ALL
    SELECT 'CONDITION', 'REPORT_DATE',
           SUM(CASE WHEN REPORT_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'COND_L3_N', 15
    FROM {{ current_schema }}.CONDITION, harvest_anchor h
    WHERE REPORT_DATE IS NOT NULL

    UNION ALL
    SELECT 'MED_ADMIN', 'MEDADMIN_START_DATE',
           SUM(CASE WHEN MEDADMIN_START_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'MEDA_L3_N', 16
    FROM {{ current_schema }}.MED_ADMIN, harvest_anchor h
    WHERE MEDADMIN_START_DATE IS NOT NULL

    UNION ALL
    SELECT 'MED_ADMIN', 'MEDADMIN_STOP_DATE',
           SUM(CASE WHEN MEDADMIN_STOP_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'MEDA_L3_N', 17
    FROM {{ current_schema }}.MED_ADMIN, harvest_anchor h
    WHERE MEDADMIN_STOP_DATE IS NOT NULL

    UNION ALL
    SELECT 'OBS_CLIN', 'OBSCLIN_START_DATE',
           SUM(CASE WHEN OBSCLIN_START_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'OBSCLIN_L3_N', 18
    FROM {{ current_schema }}.OBS_CLIN, harvest_anchor h
    WHERE OBSCLIN_START_DATE IS NOT NULL

    UNION ALL
    SELECT 'OBS_CLIN', 'OBSCLIN_STOP_DATE',
           SUM(CASE WHEN OBSCLIN_STOP_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'OBSCLIN_L3_N', 19
    FROM {{ current_schema }}.OBS_CLIN, harvest_anchor h
    WHERE OBSCLIN_STOP_DATE IS NOT NULL

    UNION ALL
    SELECT 'OBS_GEN', 'OBSGEN_START_DATE',
           SUM(CASE WHEN OBSGEN_START_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'OBSGEN_L3_N', 20
    FROM {{ current_schema }}.OBS_GEN, harvest_anchor h
    WHERE OBSGEN_START_DATE IS NOT NULL

    UNION ALL
    SELECT 'OBS_GEN', 'OBSGEN_STOP_DATE',
           SUM(CASE WHEN OBSGEN_STOP_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'OBSGEN_L3_N', 21
    FROM {{ current_schema }}.OBS_GEN, harvest_anchor h
    WHERE OBSGEN_STOP_DATE IS NOT NULL

    UNION ALL
    SELECT 'IMMUNIZATION', 'VX_RECORD_DATE',
           SUM(CASE WHEN VX_RECORD_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'IMMUNE_L3_N', 22
    FROM {{ current_schema }}.IMMUNIZATION, harvest_anchor h
    WHERE VX_RECORD_DATE IS NOT NULL

    UNION ALL
    SELECT 'PRO_CM', 'PRO_DATE',
           SUM(CASE WHEN PRO_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'PROM_L3_N', 23
    FROM {{ current_schema }}.PRO_CM, harvest_anchor h
    WHERE PRO_DATE IS NOT NULL

    UNION ALL
    SELECT 'LDS_ADDRESS_HISTORY', 'ADDRESS_PERIOD_START',
           SUM(CASE WHEN ADDRESS_PERIOD_START > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'LDSADD_L3_N', 24
    FROM {{ current_schema }}.LDS_ADDRESS_HISTORY, harvest_anchor h
    WHERE ADDRESS_PERIOD_START IS NOT NULL

    UNION ALL
    SELECT 'EXTERNAL_MEDS', 'EM_START_DATE',
           SUM(CASE WHEN EM_START_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*),
           'EXTMED_L3_N', 25
    FROM {{ current_schema }}.EXTERNAL_MEDS, harvest_anchor h
    WHERE EM_START_DATE IS NOT NULL

) subq
ORDER BY ROW_ORDER
