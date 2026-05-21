-- DC 2.02: More than 10% of records fall into the lowest or highest categories of age
-- (<0 or >89), height (<21 inches or >76 inches), weight (<0 lbs or >350 lbs),
-- diastolic blood pressure (<40 mmHg or >120 mmHg), systolic blood
-- pressure (<40 mmHg or >210 mmHg), or dispensed days supply (<1 day or
-- >90 days)
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH height AS (
    SELECT COUNT(*) AS TOTAL, COUNT_IF(HT < 21 OR HT > 76) AS EXTREME
    FROM {{ current_schema }}.VITAL WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND MEASURE_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND HT IS NOT NULL
),
weight AS (
    SELECT COUNT(*) AS TOTAL, COUNT_IF(WT < 0 OR WT > 350) AS EXTREME
    FROM {{ current_schema }}.VITAL WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND MEASURE_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND WT IS NOT NULL
),
bp AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(DIASTOLIC < 40 OR DIASTOLIC > 120 OR SYSTOLIC < 40 OR SYSTOLIC > 210) AS EXTREME
    FROM {{ current_schema }}.VITAL
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND MEASURE_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND (DIASTOLIC IS NOT NULL OR SYSTOLIC IS NOT NULL)
),
supply AS (
    SELECT COUNT(*) AS TOTAL, COUNT_IF(DISPENSE_SUP < 1 OR DISPENSE_SUP > 90) AS EXTREME
    FROM {{ current_schema }}.DISPENSING WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND DISPENSE_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND DISPENSE_SUP IS NOT NULL
),
all_checks AS (
    SELECT TOTAL, EXTREME FROM height UNION ALL
    SELECT TOTAL, EXTREME FROM weight UNION ALL
    SELECT TOTAL, EXTREME FROM bp UNION ALL
    SELECT TOTAL, EXTREME FROM supply
),
summary AS (SELECT MAX(ROUND(100.0 * EXTREME / NULLIF(TOTAL, 0), 2)) AS MAX_PCT FROM all_checks)
SELECT
    '2.02'                                          AS CHECK_NUM,
    'More than 10% records in extreme value category' AS DESCRIPTION,
    CASE WHEN MAX_PCT > 10 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM summary
