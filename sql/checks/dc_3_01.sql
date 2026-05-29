-- DC 3.01 | Table IVA | Data Completeness | Investigative
-- The average number of diagnoses records with known diagnosis types per encounter is below threshold
-- [1.0 for ambulatory (AV), inpatient (IP), emergency department (ED), ED to inpatient (EI), or telehealth (TH) encounters]
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH dx_per_enc AS (
    SELECT e.ENCOUNTERID,
           COUNT_IF(d.DX_TYPE NOT IN ('NI','UN','OT') AND d.DX_TYPE IS NOT NULL) AS DX_COUNT
    FROM {{ current_schema }}.ENCOUNTER e
    LEFT JOIN {{ current_schema }}.DIAGNOSIS d ON d.ENCOUNTERID = e.ENCOUNTERID
    WHERE e.ENC_TYPE IN ('AV','IP','ED','EI','TH')
      AND e.ADMIT_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY e.ENCOUNTERID
),
avg_dx AS (SELECT ROUND(AVG(DX_COUNT), 2) AS AVG_DX FROM dx_per_enc)
SELECT
    '3.01'                                                                            AS CHECK_NUM,
    'Average number of diagnoses with known DX_TYPE per encounter is < 1.0 for AV/IP/ED/EI/TH' AS DESCRIPTION,
    CASE WHEN AVG_DX < 1.0 THEN 'Fail' ELSE 'Pass' END                               AS STATUS
FROM avg_dx
