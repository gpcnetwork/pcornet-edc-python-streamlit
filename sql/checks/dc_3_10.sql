-- DC 3.10: Less than 80% of quantitative results for tests mapped to LAB_LOINC fully
-- specify the normal range in the range and modifier fields
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH quant AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(
               NORM_RANGE_LOW IS NOT NULL AND NORM_RANGE_HIGH IS NOT NULL
               AND NORM_MODIFIER_LOW NOT IN ('NI','UN','OT') AND COALESCE(NORM_MODIFIER_LOW,'') != ''
               AND NORM_MODIFIER_HIGH NOT IN ('NI','UN','OT') AND COALESCE(NORM_MODIFIER_HIGH,'') != ''
           ) AS WITH_RANGE
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND RESULT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
      AND LAB_LOINC IS NOT NULL
      AND RESULT_NUM IS NOT NULL
      AND RESULT_MODIFIER NOT IN ('NI','UN','OT') AND COALESCE(RESULT_MODIFIER,'') != ''
)
SELECT
    '3.10'                                                                  AS CHECK_NUM,
    '< 80% quantitative LAB_LOINC results with normal range'                AS DESCRIPTION,
    CASE WHEN 100.0 * WITH_RANGE / NULLIF(TOTAL, 0) < 80 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM quant
