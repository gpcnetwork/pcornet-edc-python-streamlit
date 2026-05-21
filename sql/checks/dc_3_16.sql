-- DC 3.16: Less than 80% of clinical observations are mapped to an OBSCLIN_CODE
--and have a quantitative result (OBSCLIN_RESULT_NUM is not null and
--OBSCLIN_RESULT_MODIFIER is not NI,UN,OT or null), qualitative
--result (OBSCLIN_RESULT_QUAL is not NI,UN,OT or null) or narrative
--result (OBSCLIN_RESULT_TEXT is not null)
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH obs AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(
               OBSCLIN_CODE IS NOT NULL AND (
                   (OBSCLIN_RESULT_NUM IS NOT NULL AND OBSCLIN_RESULT_MODIFIER NOT IN ('NI','UN','OT') AND OBSCLIN_RESULT_MODIFIER IS NOT NULL)
                   OR (OBSCLIN_RESULT_QUAL NOT IN ('NI','UN','OT') AND OBSCLIN_RESULT_QUAL IS NOT NULL)
                   OR OBSCLIN_RESULT_TEXT IS NOT NULL
               )
           ) AS MAPPED_WITH_RESULT
    FROM {{ current_schema }}.OBS_CLIN
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND OBSCLIN_START_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
)
SELECT
    '3.16'                                                                  AS CHECK_NUM,
    '< 80% OBS_CLIN records mapped to OBSCLIN_CODE with valid result'       AS DESCRIPTION,
    CASE WHEN 100.0 * MAPPED_WITH_RESULT / NULLIF(TOTAL, 0) < 80 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM obs
