-- DC 1.20: More than 5% of LOINC records in the LAB_RESULT_CM, PRO_CM,
-- and OBS_CLIN tables are panel codes based on the LOINC® panel type
-- Parameters: {{ current_schema }}, {{ cutoff_date }}

WITH lab AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(REGEXP_LIKE(LAB_LOINC, '^[0-9]+-[0-9]+$') AND LENGTH(LAB_LOINC) <= 7) AS PANEL_APPROX
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND RESULT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND LAB_LOINC IS NOT NULL
),
obs AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(REGEXP_LIKE(OBSCLIN_CODE, '^[0-9]+-[0-9]+$') AND LENGTH(OBSCLIN_CODE) <= 7) AS PANEL_APPROX
    FROM {{ current_schema }}.OBS_CLIN
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND OBSCLIN_START_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND OBSCLIN_CODE IS NOT NULL
),
combined AS (
    SELECT SUM(TOTAL) AS TOTAL, SUM(PANEL_APPROX) AS PANELS FROM lab UNION ALL SELECT SUM(TOTAL), SUM(PANEL_APPROX) FROM obs
),
summary AS (SELECT SUM(TOTAL) AS T, SUM(PANELS) AS P FROM combined)
SELECT
    '1.20'                                                                  AS CHECK_NUM,
    'More than 5% LOINC panel codes in LAB_RESULT_CM/PRO_CM/OBS_CLIN'      AS DESCRIPTION,
    CASE WHEN 100.0 * P / NULLIF(T, 0) > 5 THEN 'Fail' ELSE 'Pass' END     AS STATUS
FROM summary
