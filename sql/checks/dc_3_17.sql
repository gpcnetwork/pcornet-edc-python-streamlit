-- DC 3.17: Less than 80% of quantitative results for tests mapped to OBSCLIN_CODE
-- fully specify the RESULT_UNIT (i.e. OBSCLIN_RESULT_NUM is not
-- null and OBSCLIN_RESULT_UNIT is not NI, UN, OT, or null)
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH obs AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(OBSCLIN_RESULT_UNIT NOT IN ('NI','UN','OT') AND OBSCLIN_RESULT_UNIT IS NOT NULL) AS WITH_UNIT
    FROM {{ current_schema }}.OBS_CLIN
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND OBSCLIN_START_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
      AND OBSCLIN_CODE IS NOT NULL
      AND OBSCLIN_RESULT_NUM IS NOT NULL
      AND OBSCLIN_RESULT_MODIFIER NOT IN ('NI','UN','OT') AND COALESCE(OBSCLIN_RESULT_MODIFIER,'') != ''
)
SELECT
    '3.17'                                                                  AS CHECK_NUM,
    '< 80% quantitative OBS_CLIN results with OBSCLIN_RESULT_UNIT'          AS DESCRIPTION,
    CASE WHEN 100.0 * WITH_UNIT / NULLIF(TOTAL, 0) < 80 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM obs
