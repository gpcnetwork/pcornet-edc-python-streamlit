-- Chart IIID: Monthly Record Volume Outliers, Vitals (DC 2.08)
-- Parameters: {{ current_schema }}, {{ start_date }}, {{ end_date }}
SELECT DATE_TRUNC('month', MEASURE_DATE) AS MONTH_START,
       COUNT(*) AS RECORD_COUNT,
       COUNT(DISTINCT PATID) AS PATIENT_COUNT
FROM {{ current_schema }}.VITAL
WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}')
  AND MEASURE_DATE <= TO_DATE('{{ end_date }}')
GROUP BY DATE_TRUNC('month', MEASURE_DATE)
ORDER BY MONTH_START
