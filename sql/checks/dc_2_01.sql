-- DC 2.01 | Table IIIA | Data Plausibility | Investigative
-- More than 5% of records have future dates. Future dates are defined as those with dates occurring
-- after the maximum refresh date in the HARVEST table
-- Parameters: {{ current_schema }}, {{ start_date }}
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
),
date_checks AS (
    SELECT 'ENCOUNTER.ADMIT_DATE' AS SRC,
           SUM(CASE WHEN ADMIT_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END) AS FUTURE_CNT,
           COUNT(*) AS TOTAL
    FROM {{ current_schema }}.ENCOUNTER, harvest_anchor h
    WHERE ADMIT_DATE IS NOT NULL AND ADMIT_DATE >= TO_DATE('{{ start_date }}')
    UNION ALL
    SELECT 'DIAGNOSIS.ADMIT_DATE',
           SUM(CASE WHEN ADMIT_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*)
    FROM {{ current_schema }}.DIAGNOSIS, harvest_anchor h
    WHERE ADMIT_DATE IS NOT NULL AND ADMIT_DATE >= TO_DATE('{{ start_date }}')
    UNION ALL
    SELECT 'PROCEDURES.PX_DATE',
           SUM(CASE WHEN PX_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*)
    FROM {{ current_schema }}.PROCEDURES, harvest_anchor h
    WHERE PX_DATE IS NOT NULL AND PX_DATE >= TO_DATE('{{ start_date }}')
    UNION ALL
    SELECT 'VITAL.MEASURE_DATE',
           SUM(CASE WHEN MEASURE_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*)
    FROM {{ current_schema }}.VITAL, harvest_anchor h
    WHERE MEASURE_DATE IS NOT NULL AND MEASURE_DATE >= TO_DATE('{{ start_date }}')
    UNION ALL
    SELECT 'LAB_RESULT_CM.RESULT_DATE',
           SUM(CASE WHEN RESULT_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*)
    FROM {{ current_schema }}.LAB_RESULT_CM, harvest_anchor h
    WHERE RESULT_DATE IS NOT NULL AND RESULT_DATE >= TO_DATE('{{ start_date }}')
    UNION ALL
    SELECT 'PRESCRIBING.RX_ORDER_DATE',
           SUM(CASE WHEN RX_ORDER_DATE > h.ANCHOR_DATE THEN 1 ELSE 0 END),
           COUNT(*)
    FROM {{ current_schema }}.PRESCRIBING, harvest_anchor h
    WHERE RX_ORDER_DATE IS NOT NULL AND RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
),
summary AS (
    SELECT MAX(ROUND(100.0 * FUTURE_CNT / NULLIF(TOTAL, 0), 2)) AS MAX_PCT_FUTURE
    FROM date_checks
)
SELECT
    '2.01'                                                           AS CHECK_NUM,
    'More than 5% of records have future dates after the max HARVEST refresh date' AS DESCRIPTION,
    CASE WHEN MAX_PCT_FUTURE > 5 THEN 'Fail' ELSE 'Pass' END         AS STATUS
FROM summary
