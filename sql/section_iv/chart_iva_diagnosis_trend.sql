-- Chart IVA. Diagnosis Records Per Encounter by Admit Date and Encounter Type, Past 5 Years
-- This chart complements the information shown in Table IVA. This chart displays changes over time in the number of diagnosis codes per encounter in the
-- ENCOUNTER table. The X-axis is the 60 months prior to the maximum refresh date. Significant inflection points and other unexpected patterns should be
-- investigated.

WITH enc_monthly AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS MONTH_START, ENC_TYPE,
           COUNT(DISTINCT ENCOUNTERID) AS ENCOUNTER_COUNT
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
      AND ADMIT_DATE <= TO_DATE('{{ end_date }}')
      AND ENC_TYPE IN ('AV','IP','ED','EI','TH')
    GROUP BY DATE_TRUNC('month', ADMIT_DATE), ENC_TYPE
),
dx_monthly AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS MONTH_START, ENC_TYPE,
           COUNT(*) AS DX_COUNT
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
      AND ADMIT_DATE <= TO_DATE('{{ end_date }}')
      AND ENC_TYPE IN ('AV','IP','ED','EI','TH')
      AND DX_TYPE NOT IN ('NI','UN','OT')
    GROUP BY DATE_TRUNC('month', ADMIT_DATE), ENC_TYPE
)
SELECT e.MONTH_START, e.ENC_TYPE,
       e.ENCOUNTER_COUNT, COALESCE(d.DX_COUNT, 0) AS DX_COUNT,
       ROUND(COALESCE(d.DX_COUNT, 0)::FLOAT / NULLIF(e.ENCOUNTER_COUNT, 0), 2) AS AVG_DX_PER_ENC
FROM enc_monthly e
LEFT JOIN dx_monthly d ON d.MONTH_START = e.MONTH_START AND d.ENC_TYPE = e.ENC_TYPE
ORDER BY e.MONTH_START, e.ENC_TYPE
