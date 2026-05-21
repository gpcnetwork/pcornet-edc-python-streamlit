-- DC 3.06: More than 10% of IP (inpatient) or ED to inpatient (EI) encounters with any
-- diagnosis from a known DX_ORIGIN don't have a principal diagnosis from
-- that source
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH ip_ei_enc AS (
    SELECT ENCOUNTERID FROM {{ current_schema }}.ENCOUNTER
    WHERE ENC_TYPE IN ('IP','EI') {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
),
with_any_dx AS (
    SELECT DISTINCT ENCOUNTERID FROM {{ current_schema }}.DIAGNOSIS
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
      AND DX_ORIGIN NOT IN ('NI','UN','OT') AND DX_ORIGIN IS NOT NULL
),
with_principal AS (
    SELECT DISTINCT ENCOUNTERID FROM {{ current_schema }}.DIAGNOSIS
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
      AND PDX = 'P'
      AND DX_ORIGIN NOT IN ('NI','UN','OT') AND DX_ORIGIN IS NOT NULL
),
counts AS (
    SELECT
        COUNT(*) AS TOTAL_WITH_ANY_DX,
        COUNT_IF(NOT EXISTS (SELECT 1 FROM with_principal p WHERE p.ENCOUNTERID = e.ENCOUNTERID)) AS WITHOUT_PRINCIPAL
    FROM ip_ei_enc e
    WHERE EXISTS (SELECT 1 FROM with_any_dx a WHERE a.ENCOUNTERID = e.ENCOUNTERID)
)
SELECT
    '3.06'                                                  AS CHECK_NUM,
    '> 10% IP/EI encounters without a principal DX'        AS DESCRIPTION,
    CASE WHEN 100.0 * WITHOUT_PRINCIPAL / NULLIF(TOTAL_WITH_ANY_DX, 0) > 10 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM counts
