-- Table IVE. Principal Diagnoses for Institutional Encounters
-- This table shows principal diagnosis code data availability for institutional encounters. Results support Data Check 3.06 (More than 10% of IP (inpatient) or ED to
-- inpatient (EI) encounters with any diagnosis from a known DX_ORIGIN don't have a principal diagnosis from that source) and Data Check 2.07 (the average number
-- of principal diagnoses per known DX_ORIGIN per encounter is above threshold [2.0 for inpatient (IP) and ED to inpatient (EI]). For data check 3.06, exceptions are
-- triggered when the percentage exceeds 10% or when 0 records have a principal diagnosis. Exceptions are highlighted in blue and should be investigated and explained
-- in the ETL ADD.

WITH all_slices AS (
    SELECT * FROM VALUES
    ('EI','BI'),('EI','CL'),('EI','DR'),('EI','OD'),
    ('IP','BI'),('IP','CL'),('IP','DR'),('IP','OD'),
    ('IS','BI'),('IS','CL'),('IS','DR'),('IS','OD'),
    ('OS','BI'),('OS','CL'),('OS','DR'),('OS','OD')
    AS v(ENC_TYPE, DX_ORIGIN)
),
filtered AS (
    SELECT ENCOUNTERID, PDX, ENC_TYPE, DX_ORIGIN, ADMIT_DATE::DATE AS ADMIT_DATE
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
      AND ENC_TYPE IN ('EI','IP','IS','OS')
      AND DX_ORIGIN IN ('BI','CL','DR','OD')
),
pdxgrp_slice AS (
    SELECT ENC_TYPE, DX_ORIGIN, ENCOUNTERID,
           CASE WHEN SUM(CASE WHEN PDX='P' THEN 1 ELSE 0 END) > 0 THEN 'P' ELSE 'U' END AS PDXGRP
    FROM filtered
    GROUP BY ENC_TYPE, DX_ORIGIN, ENCOUNTERID
),
agg_enc AS (
    SELECT ENC_TYPE, DX_ORIGIN,
           COUNT(DISTINCT ENCOUNTERID)                                           AS TOTAL_ENCOUNTERS,
           COUNT(DISTINCT CASE WHEN PDXGRP='P' THEN ENCOUNTERID END)             AS ENCOUNTERS_WITH_P,
           COUNT(DISTINCT CASE WHEN PDXGRP='U' THEN ENCOUNTERID END)             AS ENCOUNTERS_WITHOUT_P
    FROM pdxgrp_slice
    GROUP BY ENC_TYPE, DX_ORIGIN
),
principal AS (
    SELECT ENC_TYPE, DX_ORIGIN, COUNT(*) AS TOTAL_PRINCIPAL_DX
    FROM filtered WHERE PDX='P'
    GROUP BY ENC_TYPE, DX_ORIGIN
),
joined AS (
    SELECT s.ENC_TYPE, s.DX_ORIGIN,
           COALESCE(a.TOTAL_ENCOUNTERS,    0) AS TOTAL_ENCOUNTERS,
           COALESCE(a.ENCOUNTERS_WITH_P,   0) AS ENCOUNTERS_WITH_P,
           COALESCE(a.ENCOUNTERS_WITHOUT_P,0) AS ENCOUNTERS_WITHOUT_P,
           COALESCE(p.TOTAL_PRINCIPAL_DX,  0) AS TOTAL_PRINCIPAL_DX
    FROM all_slices s
    LEFT JOIN agg_enc   a ON a.ENC_TYPE=s.ENC_TYPE AND a.DX_ORIGIN=s.DX_ORIGIN
    LEFT JOIN principal p ON p.ENC_TYPE=s.ENC_TYPE AND p.DX_ORIGIN=s.DX_ORIGIN
)
SELECT
    CASE j.ENC_TYPE WHEN 'EI' THEN 'EI (ED to IP Stay)' WHEN 'IP' THEN 'IP (Inpatient)'
        WHEN 'IS' THEN 'IS (Non-acute Institutional)' WHEN 'OS' THEN 'OS (Observation Stay)' ELSE j.ENC_TYPE END AS ENCOUNTER_TYPE,
    CASE j.DX_ORIGIN WHEN 'BI' THEN 'BI (billing)' WHEN 'CL' THEN 'CL (claim)'
        WHEN 'DR' THEN 'DR (derived)' WHEN 'OD' THEN 'OD (Order/EHR)' ELSE j.DX_ORIGIN END AS DX_ORIGIN,
    j.ENCOUNTERS_WITH_P    AS ENCOUNTERS_WITH_PRINCIPAL_DX,
    j.ENCOUNTERS_WITHOUT_P AS ENCOUNTERS_WITHOUT_PRINCIPAL_DX,
    CASE WHEN j.TOTAL_ENCOUNTERS=0 THEN 0
         ELSE ROUND(100.0 * j.ENCOUNTERS_WITHOUT_P / j.TOTAL_ENCOUNTERS, 2)
    END AS PCT_WITHOUT_PRINCIPAL_DX,
    j.TOTAL_PRINCIPAL_DX,
    CASE WHEN j.ENCOUNTERS_WITH_P>0 THEN ROUND(1.0 * j.TOTAL_PRINCIPAL_DX / j.ENCOUNTERS_WITH_P, 2)
         ELSE 0 END AS AVG_PRINCIPAL_DX_PER_ENC
FROM joined j
ORDER BY
    CASE j.ENC_TYPE WHEN 'EI' THEN 1 WHEN 'IP' THEN 2 WHEN 'IS' THEN 3 WHEN 'OS' THEN 4 ELSE 5 END,
    CASE j.DX_ORIGIN WHEN 'BI' THEN 1 WHEN 'CL' THEN 2 WHEN 'DR' THEN 3 WHEN 'OD' THEN 4 ELSE 5 END
