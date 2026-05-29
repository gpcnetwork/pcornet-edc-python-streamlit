-- DC 3.04 | Table IB | Data Completeness | Required
-- Less than 50% of patients with encounters have DIAGNOSIS records
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH enc_pats AS (
    SELECT DISTINCT PATID FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
dx_pats AS (
    SELECT DISTINCT PATID FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
counts AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(EXISTS (SELECT 1 FROM dx_pats d WHERE d.PATID = e.PATID)) AS WITH_DX
    FROM enc_pats e
)
SELECT
    '3.04'                                                      AS CHECK_NUM,
    'Less than 50% of patients with encounters have DIAGNOSIS records' AS DESCRIPTION,
    CASE WHEN 100.0 * WITH_DX / NULLIF(TOTAL, 0) < 50 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM counts
