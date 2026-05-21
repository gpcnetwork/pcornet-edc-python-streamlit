-- DC 3.11: Vital, prescribing, or laboratory records are less than 75% complete three
--months prior to the current month. Data completeness is calculated by
-- comparing actual volume to the average volume during the previous year
-- Parameters: {{ current_schema }}, {{ cutoff_date }}, {{ report_month }}
WITH months AS (
    SELECT DATE_TRUNC('month', DATEADD(month, -seq, TO_DATE('{{ report_month }}'))) AS MONTH_START
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS seq FROM TABLE(GENERATOR(ROWCOUNT => 24)))
),
vital_monthly AS (
    SELECT DATE_TRUNC('month', MEASURE_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.VITAL WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND MEASURE_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
    GROUP BY DATE_TRUNC('month', MEASURE_DATE)
),
combined AS (
    SELECT m.MONTH_START, COALESCE(v.CNT, 0) AS RECORD_COUNT
    FROM months m LEFT JOIN vital_monthly v ON v.MONTH_START = m.MONTH_START
),
with_avg AS (
    SELECT MONTH_START, RECORD_COUNT,
           AVG(RECORD_COUNT) OVER (ORDER BY MONTH_START ROWS BETWEEN 14 PRECEDING AND 3 PRECEDING) AS PRIOR_YEAR_AVG
    FROM combined
),
check_month AS (
    SELECT CASE WHEN PRIOR_YEAR_AVG > 0 THEN ROUND(100.0 * RECORD_COUNT / PRIOR_YEAR_AVG, 1) ELSE NULL END AS COMPLETENESS_PCT
    FROM with_avg
    WHERE MONTH_START = DATEADD(month, -3, TO_DATE('{{ report_month }}'))
)
SELECT
    '3.11'                                                              AS CHECK_NUM,
    '< 75% completeness 3 months prior vs prior-year average'           AS DESCRIPTION,
    CASE WHEN COMPLETENESS_PCT IS NOT NULL AND COMPLETENESS_PCT < 75 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM check_month
