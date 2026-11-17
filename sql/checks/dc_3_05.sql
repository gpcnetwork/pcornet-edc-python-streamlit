-- DC 3.05 | Table IB | Data Completeness | Required
-- Less than 50% of patients with encounters have PROCEDURES records
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH enc_pats AS (
    SELECT DISTINCT PATID FROM {{ current_schema }}.ENCOUNTER
    WHERE (ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL)
),
px_pats AS (
    SELECT DISTINCT PATID FROM {{ current_schema }}.PROCEDURES
    WHERE (ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL)
),
counts AS (
    -- SAS (edc_report.sas Table IB ord 7): numerator = PRO_L3_N distinct PATID
    -- (NOT intersected with encounter patients), denominator = ENC_L3_N distinct PATID.
    SELECT (SELECT COUNT(*) FROM enc_pats) AS TOTAL,
           (SELECT COUNT(*) FROM px_pats)  AS WITH_PX
)
SELECT
    '3.05'                                                          AS CHECK_NUM,
    'Less than 50% of patients with encounters have PROCEDURES records' AS DESCRIPTION,
    CASE WHEN 100.0 * WITH_PX / NULLIF(TOTAL, 0) < 50 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM counts
