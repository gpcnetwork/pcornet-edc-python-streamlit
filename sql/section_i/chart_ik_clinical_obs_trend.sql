-- Chart IK. Trend in Clinical Observation Records by Start Date, Past 5 Years
-- Monthly record counts standardized to z-scores (mean=0, std=1) over the 60-month window.

WITH monthly AS (
    SELECT DATE_TRUNC('month', OBSCLIN_START_DATE) AS OBSCLIN_START_DATE,
           COUNT(*) AS RECORD_COUNT
    FROM {{ current_schema }}.OBS_CLIN
    WHERE OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}')
      AND OBSCLIN_START_DATE <= TO_DATE('{{ end_date }}')
    GROUP BY 1
),
stats AS (
    SELECT AVG(RECORD_COUNT) AS MEAN_CNT, STDDEV(RECORD_COUNT) AS STD_CNT FROM monthly
)
SELECT m.OBSCLIN_START_DATE,
       m.RECORD_COUNT,
       ROUND((m.RECORD_COUNT - s.MEAN_CNT) / NULLIF(s.STD_CNT, 0), 4) AS Z_SCORE
FROM monthly m, stats s
ORDER BY 1
