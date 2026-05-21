-- Chart IC. Trend in Institutional Encounters by Discharge Date and Encounter Type, Past 5 Years
-- Covers EI (ED to IP Stay), IP (Inpatient Hospital Stay), IS (Non-acute Institutional Stay).
-- Z-scores computed independently per ENC_TYPE over the 60-month window.

WITH monthly AS (
    SELECT DATE_TRUNC('month', DISCHARGE_DATE) AS DISCHARGE_DATE,
           ENC_TYPE,
           COUNT(*) AS RECORD_COUNT
    FROM {{ current_schema }}.ENCOUNTER
    WHERE DISCHARGE_DATE >= TO_DATE('{{ start_date }}')
      AND DISCHARGE_DATE <= TO_DATE('{{ end_date }}')
      AND ENC_TYPE IN ('EI','IP','IS')
    GROUP BY 1, 2
),
stats AS (
    SELECT ENC_TYPE,
           AVG(RECORD_COUNT) AS MEAN_CNT,
           STDDEV(RECORD_COUNT) AS STD_CNT
    FROM monthly
    GROUP BY ENC_TYPE
)
SELECT m.DISCHARGE_DATE,
       m.ENC_TYPE,
       m.RECORD_COUNT,
       ROUND((m.RECORD_COUNT - s.MEAN_CNT) / NULLIF(s.STD_CNT, 0), 4) AS Z_SCORE
FROM monthly m
JOIN stats s ON s.ENC_TYPE = m.ENC_TYPE
ORDER BY 1, 2
