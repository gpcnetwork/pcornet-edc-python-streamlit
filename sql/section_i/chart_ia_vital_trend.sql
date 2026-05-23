-- Chart IA. Trend In Vital Measures by Measurement Date, Past 5 Years
-- X-axis: 60 months prior to the maximum REFRESH_VITAL_DATE in the HARVEST table.
-- Monthly record counts standardized to z-scores (mean=0, std=1) over that window.
-- Y-axis reflects deviation of each month's count from the mean.
-- Values above 0 = above-average record volume; below 0 = below-average.
-- Significant inflection points and unexpected patterns should be investigated.

WITH harvest AS (
    SELECT MAX(REFRESH_VITAL_DATE) AS MAX_REFRESH_DATE
    FROM {{ current_schema }}.HARVEST
),
date_range AS (
    SELECT
        DATE_TRUNC('month', MAX_REFRESH_DATE)                        AS END_MONTH,
        DATEADD('month', -59, DATE_TRUNC('month', MAX_REFRESH_DATE)) AS START_MONTH
    FROM harvest
),
monthly AS (
    SELECT DATE_TRUNC('month', v.MEASURE_DATE) AS MEASURE_DATE,
           COUNT(*)                             AS RECORD_COUNT
    FROM {{ current_schema }}.VITAL v, date_range d
    WHERE v.MEASURE_DATE >= d.START_MONTH
      AND v.MEASURE_DATE <  DATEADD('month', 1, d.END_MONTH)
    GROUP BY 1
),
stats AS (
    SELECT AVG(RECORD_COUNT)    AS MEAN_CNT,
           STDDEV(RECORD_COUNT) AS STD_CNT
    FROM monthly
)
SELECT m.MEASURE_DATE,
       m.RECORD_COUNT,
       ROUND((m.RECORD_COUNT - s.MEAN_CNT) / NULLIF(s.STD_CNT, 0), 4) AS Z_SCORE
FROM monthly m, stats s
ORDER BY 1
