-- Chart IVB. Procedure Records Per Encounter by Admit Date and Encounter Type, Past 5 Years
-- This chart complements the information shown in Table IVB. This chart displays changes over time in the number of procedure codes per encounter in the
-- ENCOUNTER table. The X-axis is the 60 months prior to the maximum refresh date. Significant inflection points and other unexpected patterns should be
-- investigated.

WITH enc_monthly AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS MONTH_START, ENC_TYPE,
           COUNT(DISTINCT ENCOUNTERID) AS ENCOUNTER_COUNT
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
      AND ADMIT_DATE <= TO_DATE('{{ end_date }}')
      AND ENC_TYPE IN ('AV','ED','EI','IP')
    GROUP BY DATE_TRUNC('month', ADMIT_DATE), ENC_TYPE
),
px_monthly AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS MONTH_START, ENC_TYPE,
           COUNT(*) AS PX_COUNT
    FROM {{ current_schema }}.PROCEDURES
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
      AND ADMIT_DATE <= TO_DATE('{{ end_date }}')
      AND ENC_TYPE IN ('AV','ED','EI','IP')
      AND PX_TYPE NOT IN ('NI','UN','OT')
    GROUP BY DATE_TRUNC('month', ADMIT_DATE), ENC_TYPE
)
SELECT e.MONTH_START, e.ENC_TYPE,
       e.ENCOUNTER_COUNT, COALESCE(p.PX_COUNT, 0) AS PX_COUNT,
       ROUND(COALESCE(p.PX_COUNT, 0)::FLOAT / NULLIF(e.ENCOUNTER_COUNT, 0), 2) AS AVG_PX_PER_ENC
FROM enc_monthly e
LEFT JOIN px_monthly p ON p.MONTH_START = e.MONTH_START AND p.ENC_TYPE = e.ENC_TYPE
ORDER BY e.MONTH_START, e.ENC_TYPE
