-- Chart IIIC: Monthly Record Volume Outliers, Procedures (DC 2.08)
-- Parameters: {{ current_schema }}, {{ start_date }}, {{ end_date }}
SELECT DATE_TRUNC('month', ADMIT_DATE) AS MONTH_START,
       ENC_TYPE,
       COUNT(*) AS RECORD_COUNT,
       COUNT(DISTINCT PATID) AS PATIENT_COUNT
FROM {{ current_schema }}.PROCEDURES
WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
  AND ADMIT_DATE <= TO_DATE('{{ end_date }}')
  AND ENC_TYPE IN ('AV','TH','ED','EI','IP')
GROUP BY DATE_TRUNC('month', ADMIT_DATE), ENC_TYPE
ORDER BY MONTH_START, ENC_TYPE
