-- Chart IB. Trend in Encounters by Admit Date and Encounter Type, Past 5 Years
-- Three-panel chart: Panel 1 = AV/OA/TH, Panel 2 = ED/EI/IP, Panel 3 = IS/IC/OS.
-- X-axis: 60 months prior to MAX(REFRESH_ENCOUNTER_DATE) from HARVEST.
-- Z-scores computed independently per ENC_TYPE over the 60-month window.
-- Values above 0 = above-average; below 0 = below-average record volume.

WITH harvest AS (
    SELECT MAX(REFRESH_ENCOUNTER_DATE) AS MAX_REFRESH_DATE
    FROM {{ current_schema }}.HARVEST
),
date_range AS (
    SELECT
        DATE_TRUNC('month', MAX_REFRESH_DATE)                        AS END_MONTH,
        DATEADD('month', -59, DATE_TRUNC('month', MAX_REFRESH_DATE)) AS START_MONTH
    FROM harvest
),
monthly AS (
    SELECT DATE_TRUNC('month', e.ADMIT_DATE) AS ADMIT_DATE,
           e.ENC_TYPE,
           COUNT(*)                           AS RECORD_COUNT
    FROM {{ current_schema }}.ENCOUNTER e, date_range d
    WHERE e.ADMIT_DATE >= d.START_MONTH
      AND e.ADMIT_DATE <  DATEADD('month', 1, d.END_MONTH)
      AND e.ENC_TYPE IN ('AV','OA','TH','ED','EI','IP','IS','IC','OS')
    GROUP BY 1, 2
),
stats AS (
    SELECT ENC_TYPE,
           AVG(RECORD_COUNT)    AS MEAN_CNT,
           STDDEV(RECORD_COUNT) AS STD_CNT
    FROM monthly
    GROUP BY ENC_TYPE
)
SELECT m.ADMIT_DATE,
       m.ENC_TYPE,
       m.RECORD_COUNT,
       ROUND((m.RECORD_COUNT - s.MEAN_CNT) / NULLIF(s.STD_CNT, 0), 4) AS Z_SCORE
FROM monthly m
JOIN stats s ON s.ENC_TYPE = m.ENC_TYPE
ORDER BY 1, 2
