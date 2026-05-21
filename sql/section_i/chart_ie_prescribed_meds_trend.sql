-- Chart IE. Trend in Prescribed Medications by Rx Order Date, Past 5 Years
-- Monthly record counts standardized to z-scores (mean=0, std=1) over the 60-month window.

WITH monthly AS (
    SELECT DATE_TRUNC('month', RX_ORDER_DATE) AS RX_ORDER_DATE,
           COUNT(*) AS RECORD_COUNT
    FROM {{ current_schema }}.PRESCRIBING
    WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
      AND RX_ORDER_DATE <= TO_DATE('{{ end_date }}')
    GROUP BY 1
),
stats AS (
    SELECT AVG(RECORD_COUNT) AS MEAN_CNT, STDDEV(RECORD_COUNT) AS STD_CNT FROM monthly
)
SELECT m.RX_ORDER_DATE,
       m.RECORD_COUNT,
       ROUND((m.RECORD_COUNT - s.MEAN_CNT) / NULLIF(s.STD_CNT, 0), 4) AS Z_SCORE
FROM monthly m, stats s
ORDER BY 1
