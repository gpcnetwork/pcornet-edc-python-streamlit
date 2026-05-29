-- DC 3.14 | Table IVJ | Data Completeness | Investigative
-- Medication administration, dispensing, or clinical observation records are less than 75% complete
-- three months prior to the current month. Data completeness is calculated by comparing actual volume
-- to the average volume during the previous year
-- Parameters: {{ current_schema }}, {{ report_month }}
WITH months AS (
    SELECT DATE_TRUNC('month', DATEADD(month, -seq, TO_DATE('{{ report_month }}'))) AS MONTH_START
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS seq FROM TABLE(GENERATOR(ROWCOUNT => 24)))
),
medadmin_monthly AS (
    SELECT DATE_TRUNC('month', MEDADMIN_START_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.MED_ADMIN
    GROUP BY DATE_TRUNC('month', MEDADMIN_START_DATE)
),
dispensing_monthly AS (
    SELECT DATE_TRUNC('month', DISPENSE_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.DISPENSING
    GROUP BY DATE_TRUNC('month', DISPENSE_DATE)
),
obs_clin_monthly AS (
    SELECT DATE_TRUNC('month', OBSCLIN_START_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.OBS_CLIN
    GROUP BY DATE_TRUNC('month', OBSCLIN_START_DATE)
),
combined AS (
    SELECT 'MED_ADMIN' AS DOMAIN, m.MONTH_START, COALESCE(d.CNT, 0) AS RECORD_COUNT
    FROM months m LEFT JOIN medadmin_monthly d ON d.MONTH_START = m.MONTH_START
    UNION ALL
    SELECT 'DISPENSING', m.MONTH_START, COALESCE(d.CNT, 0)
    FROM months m LEFT JOIN dispensing_monthly d ON d.MONTH_START = m.MONTH_START
    UNION ALL
    SELECT 'OBS_CLIN', m.MONTH_START, COALESCE(d.CNT, 0)
    FROM months m LEFT JOIN obs_clin_monthly d ON d.MONTH_START = m.MONTH_START
),
with_avg AS (
    SELECT DOMAIN, MONTH_START, RECORD_COUNT,
           AVG(RECORD_COUNT) OVER (
               PARTITION BY DOMAIN
               ORDER BY MONTH_START
               ROWS BETWEEN 14 PRECEDING AND 3 PRECEDING
           ) AS PRIOR_YEAR_AVG
    FROM combined
),
check_month AS (
    SELECT MAX(CASE
                   WHEN RECORD_COUNT = 0 THEN 1
                   WHEN PRIOR_YEAR_AVG > 0 AND RECORD_COUNT < 0.75 * PRIOR_YEAR_AVG THEN 1
                   ELSE 0
               END) AS HAS_EXCEPTION
    FROM with_avg
    WHERE MONTH_START = DATEADD(month, -3, TO_DATE('{{ report_month }}'))
)
SELECT
    '3.14'                                                                                                          AS CHECK_NUM,
    'Medication administration, dispensing, or clinical observation records < 75% complete three months prior'     AS DESCRIPTION,
    CASE WHEN HAS_EXCEPTION = 1 THEN 'Fail' ELSE 'Pass' END                                                        AS STATUS
FROM check_month
