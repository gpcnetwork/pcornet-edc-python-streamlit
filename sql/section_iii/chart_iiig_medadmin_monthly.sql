-- Chart IIIG: Monthly Record Volume Outliers, Med Admin (DC 2.08)
-- Parameters: {{ current_schema }}, {{ start_date }}, {{ end_date }}
SELECT DATE_TRUNC('month', MEDADMIN_START_DATE) AS MONTH_START,
       COUNT(*) AS RECORD_COUNT,
       COUNT(DISTINCT PATID) AS PATIENT_COUNT
FROM {{ current_schema }}.MED_ADMIN
WHERE MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}')
  AND MEDADMIN_START_DATE <= TO_DATE('{{ end_date }}')
GROUP BY DATE_TRUNC('month', MEDADMIN_START_DATE)
ORDER BY MONTH_START
