-- DC 3.12 | Table IVI | Data Completeness | Investigative
-- Less than 80% of quantitative results for tests mapped to LAB_LOINC fully specify the RESULT_UNIT
-- (i.e. RESULT_NUM is not null and RESULT_UNIT is not NI, UN, OT, or null)
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH quant AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(RESULT_UNIT NOT IN ('NI','UN','OT') AND RESULT_UNIT IS NOT NULL) AS WITH_UNIT
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE >= TO_DATE('{{ start_date }}')
      AND LAB_LOINC IS NOT NULL
      AND RESULT_NUM IS NOT NULL
      AND RESULT_MODIFIER NOT IN ('NI','UN','OT') AND COALESCE(RESULT_MODIFIER,'') != ''
)
SELECT
    '3.12'                                                                  AS CHECK_NUM,
    'Less than 80% of quantitative LAB_LOINC results fully specify RESULT_UNIT' AS DESCRIPTION,
    CASE WHEN 100.0 * WITH_UNIT / NULLIF(TOTAL, 0) < 80 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM quant
