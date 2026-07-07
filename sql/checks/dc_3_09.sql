-- DC 3.09 | Table IVI | Data Completeness | Investigative
-- Less than 80% of laboratory results are mapped to LAB_LOINC and have either a quantitative result
-- (RESULT_NUM is not null and RESULT_MODIFIER is not NI, UN, OT, or null) or a qualitative result
-- (RESULT_QUAL is not NI, UN, OT, or null)
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH lab AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(
               LAB_LOINC IS NOT NULL AND (
                   (RESULT_NUM IS NOT NULL AND RESULT_MODIFIER NOT IN ('NI','UN','OT') AND RESULT_MODIFIER IS NOT NULL)
                   OR (RESULT_QUAL NOT IN ('NI','UN','OT') AND RESULT_QUAL IS NOT NULL)
               )
           ) AS MAPPED_WITH_RESULT
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE >= TO_DATE('{{ start_date }}') AND RESULT_DATE <= TO_DATE('{{ end_date }}')
)
SELECT
    '3.09'                                                                  AS CHECK_NUM,
    'Less than 80% of lab results mapped to LAB_LOINC with valid result'   AS DESCRIPTION,
    CASE WHEN 100.0 * MAPPED_WITH_RESULT / NULLIF(TOTAL, 0) < 80 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM lab
