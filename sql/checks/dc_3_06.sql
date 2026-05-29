-- DC 3.06 | Table IVE | Data Completeness | Investigative
-- More than 10% of IP (inpatient) or ED to inpatient (EI) encounters with any diagnosis from a known
-- DX_ORIGIN don't have a principal diagnosis from that source
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH ip_ei_enc AS (
    SELECT ENCOUNTERID FROM {{ current_schema }}.ENCOUNTER
    WHERE ENC_TYPE IN ('IP','EI')
      AND ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
with_any_dx AS (
    SELECT DISTINCT ENCOUNTERID FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
      AND DX_ORIGIN NOT IN ('NI','UN','OT') AND DX_ORIGIN IS NOT NULL
),
with_principal AS (
    SELECT DISTINCT ENCOUNTERID FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
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
    'More than 10% of IP/EI encounters without a principal diagnosis from a known DX_ORIGIN' AS DESCRIPTION,
    CASE WHEN 100.0 * WITHOUT_PRINCIPAL / NULLIF(TOTAL_WITH_ANY_DX, 0) > 10 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM counts
