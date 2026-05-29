-- DC 3.11 | Table IVG | Data Completeness | Investigative
-- Vital, prescribing, or laboratory records are less than 75% complete three months prior to the
-- current month. Data completeness is calculated by comparing actual volume to the average volume
-- during the previous year
-- Parameters: {{ current_schema }}, {{ report_month }}
WITH months AS (
    SELECT DATE_TRUNC('month', DATEADD(month, -seq, TO_DATE('{{ report_month }}'))) AS MONTH_START
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS seq FROM TABLE(GENERATOR(ROWCOUNT => 24)))
),
vital_monthly AS (
    SELECT DATE_TRUNC('month', MEASURE_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.VITAL
    GROUP BY DATE_TRUNC('month', MEASURE_DATE)
),
rx_monthly AS (
    SELECT DATE_TRUNC('month', RX_ORDER_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.PRESCRIBING
    GROUP BY DATE_TRUNC('month', RX_ORDER_DATE)
),
lab_monthly AS (
    SELECT DATE_TRUNC('month', RESULT_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.LAB_RESULT_CM
    GROUP BY DATE_TRUNC('month', RESULT_DATE)
),
combined AS (
    SELECT 'VITAL' AS DOMAIN, m.MONTH_START, COALESCE(d.CNT, 0) AS RECORD_COUNT
    FROM months m LEFT JOIN vital_monthly d ON d.MONTH_START = m.MONTH_START
    UNION ALL
    SELECT 'PRESCRIBING', m.MONTH_START, COALESCE(d.CNT, 0)
    FROM months m LEFT JOIN rx_monthly d ON d.MONTH_START = m.MONTH_START
    UNION ALL
    SELECT 'LAB_RESULT_CM', m.MONTH_START, COALESCE(d.CNT, 0)
    FROM months m LEFT JOIN lab_monthly d ON d.MONTH_START = m.MONTH_START
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
    '3.11'                                                                                              AS CHECK_NUM,
    'Vital, prescribing, or laboratory records < 75% complete three months prior to the current month' AS DESCRIPTION,
    CASE WHEN HAS_EXCEPTION = 1 THEN 'Fail' ELSE 'Pass' END                                            AS STATUS
FROM check_month
