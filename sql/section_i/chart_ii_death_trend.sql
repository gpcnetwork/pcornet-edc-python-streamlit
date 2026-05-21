-- Chart II. Trend in Death Records by Death Date and Source, Past 5 Years
-- Z-scores computed independently per DEATH_SOURCE over the 60-month window.

WITH monthly AS (
    SELECT DATE_TRUNC('month', DEATH_DATE) AS DEATH_DATE,
           DEATH_SOURCE,
           COUNT(*) AS RECORD_COUNT
    FROM {{ current_schema }}.DEATH
    WHERE DEATH_DATE >= TO_DATE('{{ start_date }}')
      AND DEATH_DATE <= TO_DATE('{{ end_date }}')
    GROUP BY 1, 2
),
stats AS (
    SELECT DEATH_SOURCE,
           AVG(RECORD_COUNT) AS MEAN_CNT,
           STDDEV(RECORD_COUNT) AS STD_CNT
    FROM monthly
    GROUP BY DEATH_SOURCE
)
SELECT m.DEATH_DATE,
       m.DEATH_SOURCE,
       m.RECORD_COUNT,
       ROUND((m.RECORD_COUNT - s.MEAN_CNT) / NULLIF(s.STD_CNT, 0), 4) AS Z_SCORE
FROM monthly m
JOIN stats s ON s.DEATH_SOURCE = m.DEATH_SOURCE
ORDER BY 1, 2
