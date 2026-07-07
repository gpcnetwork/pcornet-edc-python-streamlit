-- DC 3.17 | Table IVI | Data Completeness | Investigative
-- Less than 80% of quantitative results for tests mapped to OBSCLIN_CODE fully specify the RESULT_UNIT
-- (i.e. OBSCLIN_RESULT_NUM is not null and OBSCLIN_RESULT_UNIT is not NI, UN, OT, or null)
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH obs AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(OBSCLIN_RESULT_UNIT NOT IN ('NI','UN','OT') AND OBSCLIN_RESULT_UNIT IS NOT NULL) AS WITH_UNIT
    FROM {{ current_schema }}.OBS_CLIN
    WHERE OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}') AND OBSCLIN_START_DATE <= TO_DATE('{{ end_date }}')
      AND OBSCLIN_CODE IS NOT NULL
      AND OBSCLIN_RESULT_NUM IS NOT NULL
      AND OBSCLIN_RESULT_MODIFIER NOT IN ('NI','UN','OT') AND COALESCE(OBSCLIN_RESULT_MODIFIER,'') != ''
)
SELECT
    '3.17'                                                                          AS CHECK_NUM,
    'Less than 80% of quantitative OBS_CLIN results fully specify OBSCLIN_RESULT_UNIT' AS DESCRIPTION,
    CASE WHEN 100.0 * WITH_UNIT / NULLIF(TOTAL, 0) < 80 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM obs
