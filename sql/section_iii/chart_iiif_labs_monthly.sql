-- Chart IIIF: Monthly Record Volume Outliers, Labs (DC 2.08)
-- Parameters: {{ current_schema }}, {{ start_date }}, {{ end_date }}
SELECT DATE_TRUNC('month', RESULT_DATE) AS MONTH_START,
       COUNT(*) AS RECORD_COUNT,
       COUNT(DISTINCT PATID) AS PATIENT_COUNT
FROM {{ current_schema }}.LAB_RESULT_CM
WHERE RESULT_DATE >= TO_DATE('{{ start_date }}')
  AND RESULT_DATE <= TO_DATE('{{ end_date }}')
GROUP BY DATE_TRUNC('month', RESULT_DATE)
ORDER BY MONTH_START
