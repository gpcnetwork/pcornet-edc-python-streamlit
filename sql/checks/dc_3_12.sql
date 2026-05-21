-- DC 3.12: Less than 80% of quantitative results for tests mapped to LAB_LOINC fully
-- specify the RESULT_UNIT (i.e. RESULT_NUM is not null and
-- RESULT_UNIT is not NI, UN, OT, or null)
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH quant AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(RESULT_UNIT NOT IN ('NI','UN','OT') AND RESULT_UNIT IS NOT NULL) AS WITH_UNIT
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND RESULT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
      AND LAB_LOINC IS NOT NULL
      AND RESULT_NUM IS NOT NULL
      AND RESULT_MODIFIER NOT IN ('NI','UN','OT') AND COALESCE(RESULT_MODIFIER,'') != ''
)
SELECT
    '3.12'                                                                  AS CHECK_NUM,
    '< 80% quantitative LAB_LOINC results with RESULT_UNIT'                 AS DESCRIPTION,
    CASE WHEN 100.0 * WITH_UNIT / NULLIF(TOTAL, 0) < 80 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM quant
