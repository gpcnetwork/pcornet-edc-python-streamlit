-- DC 1.16: Laboratory results or clinical observations are recorded in the wrong table
-- based on the LOINC® classtype
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH lab_panels AS (
    SELECT COUNT(*) AS N
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND RESULT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
      AND LAB_LOINC IS NOT NULL
      AND REGEXP_LIKE(LAB_LOINC, '^[0-9]{1,5}-[0-9]$')
),
obs_non_clinical AS (
    SELECT COUNT(*) AS N
    FROM {{ current_schema }}.OBS_CLIN
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND OBSCLIN_START_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
      AND OBSCLIN_CODE IS NOT NULL
      AND REGEXP_LIKE(OBSCLIN_CODE, '^[0-9]{1,5}-[0-9]$')
),
total AS (SELECT N FROM lab_panels UNION ALL SELECT N FROM obs_non_clinical),
summary AS (SELECT SUM(N) AS TOTAL_MISCLASSIFIED FROM total)
SELECT
    '1.16'                                             AS CHECK_NUM,
    'Laboratory results or clinical observations are recorded in the wrong table based on the LOINC® classtype' AS DESCRIPTION,
    CASE WHEN TOTAL_MISCLASSIFIED > 0 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM summary
