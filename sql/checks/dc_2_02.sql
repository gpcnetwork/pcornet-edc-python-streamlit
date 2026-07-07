-- DC 2.02 | Table IIIB | Data Plausibility | Investigative
-- More than 10% of records fall into the lowest or highest categories of age (<0 or >89),
-- height (<21 or >76 inches), weight (<0 or >350 lbs), diastolic BP (<40 or >120 mmHg),
-- systolic BP (<40 or >210 mmHg), or dispensed days supply (<1 or >90 days)
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH height AS (
    SELECT COUNT(*) AS TOTAL, COUNT_IF(HT < 21 OR HT > 76) AS EXTREME
    FROM {{ current_schema }}.VITAL
    WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}') AND MEASURE_DATE <= TO_DATE('{{ end_date }}') AND HT IS NOT NULL
),
weight AS (
    SELECT COUNT(*) AS TOTAL, COUNT_IF(WT < 0 OR WT > 350) AS EXTREME
    FROM {{ current_schema }}.VITAL
    WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}') AND MEASURE_DATE <= TO_DATE('{{ end_date }}') AND WT IS NOT NULL
),
bp AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(DIASTOLIC < 40 OR DIASTOLIC > 120 OR SYSTOLIC < 40 OR SYSTOLIC > 210) AS EXTREME
    FROM {{ current_schema }}.VITAL
    WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}') AND MEASURE_DATE <= TO_DATE('{{ end_date }}')
      AND (DIASTOLIC IS NOT NULL OR SYSTOLIC IS NOT NULL)
),
supply AS (
    SELECT COUNT(*) AS TOTAL, COUNT_IF(DISPENSE_SUP < 1 OR DISPENSE_SUP > 90) AS EXTREME
    FROM {{ current_schema }}.DISPENSING
    WHERE DISPENSE_DATE >= TO_DATE('{{ start_date }}') AND DISPENSE_DATE <= TO_DATE('{{ end_date }}') AND DISPENSE_SUP IS NOT NULL
),
all_checks AS (
    SELECT TOTAL, EXTREME FROM height UNION ALL
    SELECT TOTAL, EXTREME FROM weight UNION ALL
    SELECT TOTAL, EXTREME FROM bp UNION ALL
    SELECT TOTAL, EXTREME FROM supply
),
summary AS (SELECT MAX(ROUND(100.0 * EXTREME / NULLIF(TOTAL, 0), 2)) AS MAX_PCT FROM all_checks)
SELECT
    '2.02'                                                                AS CHECK_NUM,
    'More than 10% of records fall into extreme value categories'         AS DESCRIPTION,
    CASE WHEN MAX_PCT > 10 THEN 'Fail' ELSE 'Pass' END                    AS STATUS
FROM summary
