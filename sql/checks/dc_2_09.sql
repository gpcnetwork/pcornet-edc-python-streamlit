-- DC 2.09 | Table IB | Data Plausibility | Investigative
-- Less than 80% of patients with a face-to-face encounter during the past 5 years have at least 1
-- face-to-face diagnosis and 1 vital measurement. Face-to-face is defined as an encounter type of
-- ambulatory visit (AV), emergency department (ED), emergency department admit to inpatient hospital
-- stay (EI), inpatient hospital (IP), or observation stay (OS).
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH f2f_pats AS (
    SELECT DISTINCT PATID
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ENC_TYPE IN ('AV','ED','EI','IP','OS')
      AND ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
with_dx AS (
    SELECT DISTINCT PATID FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
with_vital AS (
    SELECT DISTINCT PATID FROM {{ current_schema }}.VITAL
    WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}')
),
counts AS (
    SELECT
        COUNT(*) AS TOTAL,
        COUNT_IF(EXISTS (SELECT 1 FROM with_dx d WHERE d.PATID = f.PATID)
              AND EXISTS (SELECT 1 FROM with_vital v WHERE v.PATID = f.PATID)) AS COMPLETE
    FROM f2f_pats f
)
SELECT
    '2.09'                                                                              AS CHECK_NUM,
    'Less than 80% of face-to-face patients have at least 1 diagnosis and 1 vital'     AS DESCRIPTION,
    CASE WHEN 100.0 * COMPLETE / NULLIF(TOTAL, 0) < 80 THEN 'Fail' ELSE 'Pass' END     AS STATUS
FROM counts
