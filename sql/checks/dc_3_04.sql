-- DC 3.04 | Table IB | Data Completeness | Required
-- Less than 50% of patients with encounters have DIAGNOSIS records
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH enc_pats AS (
    SELECT DISTINCT PATID FROM {{ current_schema }}.ENCOUNTER
    WHERE (ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL)
),
dx_pats AS (
    SELECT DISTINCT PATID FROM {{ current_schema }}.DIAGNOSIS
    WHERE (ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL)
),
counts AS (
    -- SAS (edc_report.sas Table IB ords 6/7): numerator = DIA_L3_N distinct PATID
    -- (NOT intersected with encounter patients), denominator = ENC_L3_N distinct PATID.
    SELECT (SELECT COUNT(*) FROM enc_pats)       AS TOTAL,
           (SELECT COUNT(*) FROM dx_pats) AS WITH_DX
)
SELECT
    '3.04'                                                      AS CHECK_NUM,
    'Less than 50% of patients with encounters have DIAGNOSIS records' AS DESCRIPTION,
    CASE WHEN 100.0 * WITH_DX / NULLIF(TOTAL, 0) < 50 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM counts
