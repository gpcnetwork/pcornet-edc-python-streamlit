-- Chart IIIE: Monthly Record Volume Outliers, Prescribing (DC 2.08)
-- Parameters: {{ current_schema }}, {{ start_date }}, {{ end_date }}
SELECT DATE_TRUNC('month', RX_ORDER_DATE) AS MONTH_START,
       COUNT(*) AS RECORD_COUNT,
       COUNT(DISTINCT PATID) AS PATIENT_COUNT
FROM {{ current_schema }}.PRESCRIBING
WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
  AND RX_ORDER_DATE <= TO_DATE('{{ end_date }}')
GROUP BY DATE_TRUNC('month', RX_ORDER_DATE)
ORDER BY MONTH_START
