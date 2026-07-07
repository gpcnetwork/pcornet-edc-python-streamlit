-- DC 3.05 | Table IB | Data Completeness | Required
-- Less than 50% of patients with encounters have PROCEDURES records
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH enc_pats AS (
    SELECT DISTINCT PATID FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') AND ADMIT_DATE <= TO_DATE('{{ end_date }}')
),
px_pats AS (
    SELECT DISTINCT PATID FROM {{ current_schema }}.PROCEDURES
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') AND ADMIT_DATE <= TO_DATE('{{ end_date }}')
),
counts AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(EXISTS (SELECT 1 FROM px_pats p WHERE p.PATID = e.PATID)) AS WITH_PX
    FROM enc_pats e
)
SELECT
    '3.05'                                                          AS CHECK_NUM,
    'Less than 50% of patients with encounters have PROCEDURES records' AS DESCRIPTION,
    CASE WHEN 100.0 * WITH_PX / NULLIF(TOTAL, 0) < 50 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM counts
