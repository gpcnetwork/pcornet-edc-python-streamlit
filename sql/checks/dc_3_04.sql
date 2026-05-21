-- DC 3.04: < 50% patients with encounters having DIAGNOSIS records (DC 3.04)
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH enc_pats AS (
    SELECT DISTINCT PATID FROM {{ current_schema }}.ENCOUNTER WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
),
dx_pats AS (
    SELECT DISTINCT PATID FROM {{ current_schema }}.DIAGNOSIS WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
),
counts AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(EXISTS (SELECT 1 FROM dx_pats d WHERE d.PATID = e.PATID)) AS WITH_DX
    FROM enc_pats e
)
SELECT
    '3.04'                                                      AS CHECK_NUM,
    '< 50% patients with encounters having DIAGNOSIS records'   AS DESCRIPTION,
    CASE WHEN 100.0 * WITH_DX / NULLIF(TOTAL, 0) < 50 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM counts
