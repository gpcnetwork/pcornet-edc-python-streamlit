-- Table IVG. Data Latency and Completeness of Vital, Prescribing, and Lab Data, Past 2 Years
-- This table includes VITAL, PRESCRIBING, and LAB_RESULT_CM data from the most recent 24 month period; month -0 is the month the data curation query was
-- run. Data completeness is determined by comparing the actual volume to the expected volume in each month. Expected volume is determined by taking the average
-- volume during the benchmark period of months -12 to month -23. Data completeness is reported as a percentage of the benchmark average. Temporal differences may
-- be affected by data availability, ETL processes, date shifting, secular trends, and/or changes in data provenance.
-- These data support Data Check 3.11 (vital, prescribing, or laboratory records are less than 75% complete three months prior to the current month). Data check
-- exceptions occur if the month -3 result is <75% of the benchmark average or 0 records. Data check exceptions are highlighted in blue. Data check exceptions and
-- unexpected results should be investigated and explained in the ETL ADD.

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
)
SELECT DOMAIN, MONTH_START, RECORD_COUNT, ROUND(PRIOR_YEAR_AVG, 0) AS PRIOR_YEAR_AVG,
    CASE WHEN PRIOR_YEAR_AVG > 0
         THEN ROUND(100.0 * RECORD_COUNT / PRIOR_YEAR_AVG, 1)
         ELSE NULL END AS COMPLETENESS_PCT
FROM with_avg
ORDER BY DOMAIN, MONTH_START
