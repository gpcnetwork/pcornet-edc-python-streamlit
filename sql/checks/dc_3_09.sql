-- DC 3.09: Less than 80% of laboratory results are mapped to LAB_LOINC and have
-- either a quantitative result (RESULT_NUM is not null and
-- RESULT_MODIFIER is not NI, UN, OT, or null) or a qualitative result
-- (RESULT_QUAL is not NI, UN, OT, or null)
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH lab AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(
               LAB_LOINC IS NOT NULL AND (
                   (RESULT_NUM IS NOT NULL AND RESULT_MODIFIER NOT IN ('NI','UN','OT') AND RESULT_MODIFIER IS NOT NULL)
                   OR (RESULT_QUAL NOT IN ('NI','UN','OT') AND RESULT_QUAL IS NOT NULL)
               )
           ) AS MAPPED_WITH_RESULT
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND RESULT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
)
SELECT
    '3.09'                                                                  AS CHECK_NUM,
    '< 80% lab results mapped to LAB_LOINC with valid result'               AS DESCRIPTION,
    CASE WHEN 100.0 * MAPPED_WITH_RESULT / NULLIF(TOTAL, 0) < 80 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM lab
