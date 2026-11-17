-- DC 2.09 | Table IB | Data Plausibility | Investigative
-- Less than 80% of patients with a face-to-face encounter during the past 5 years have at least 1
-- face-to-face diagnosis and 1 vital measurement. Face-to-face = ENC_TYPE in AV, ED, EI, IP, OS.
--
-- SAS source of truth (v7.03):
--   data_curation_base.sas 49-55 + 2867-2891 (%dash): the "past 5 years" DASH window is
--     [first-of-month(run date - 63 months), first-of-month(run date - 3 months)], inclusive.
--     Approximated here with {{ end_date }} (= HARVEST refresh_max) as the run-date anchor.
--   data_curation_tables.sas 1434-1449 (ENC_L3_DASH2 denominator): distinct PATIDs with an
--     ENCOUNTER of type ED/EI/IP/OS/AV and non-missing ADMIT_DATE in the window.
--   data_curation_tables.sas 5748-5784 (XTBL_L3_DASH1 numerator): patients with BOTH a
--     DIAGNOSIS record (ENC_TYPE ED/EI/IP/OS/AV, DX non-missing, ADMIT_DATE in window) AND a
--     VITAL record (MEASURE_DATE in window — no encounter-type restriction on VITAL).
--   edc_report.sas 858-877, 920-931 (Table IB ord=3): exception when denom>0 and pct < 80.0.
-- Parameters: {{ current_schema }}, {{ end_date }}
WITH bounds AS (
    SELECT
        DATE_TRUNC('month', DATEADD(month, -63, TO_DATE('{{ end_date }}'))) AS LO,
        DATE_TRUNC('month', DATEADD(month, -3,  TO_DATE('{{ end_date }}'))) AS HI
),
f2f_pats AS (
    SELECT DISTINCT e.PATID
    FROM {{ current_schema }}.ENCOUNTER e, bounds b
    WHERE e.ENC_TYPE IN ('ED','EI','IP','OS','AV')
      AND e.ADMIT_DATE BETWEEN b.LO AND b.HI
),
with_dx AS (
    SELECT DISTINCT d.PATID
    FROM {{ current_schema }}.DIAGNOSIS d, bounds b
    WHERE d.ENC_TYPE IN ('ED','EI','IP','OS','AV')
      AND d.DX IS NOT NULL
      AND d.ADMIT_DATE BETWEEN b.LO AND b.HI
),
with_vital AS (
    SELECT DISTINCT v.PATID
    FROM {{ current_schema }}.VITAL v, bounds b
    WHERE v.MEASURE_DATE BETWEEN b.LO AND b.HI
),
counts AS (
    SELECT
        (SELECT COUNT(*) FROM f2f_pats)                                        AS DENOM,
        (SELECT COUNT(*) FROM with_dx d
          WHERE d.PATID IN (SELECT PATID FROM with_vital))                     AS NUMER
)
SELECT
    '2.09'                                                                     AS CHECK_NUM,
    'Less than 80% of 5-yr face-to-face patients have at least 1 f2f diagnosis and 1 vital' AS DESCRIPTION,
    CASE WHEN DENOM > 0 AND 100.0 * NUMER / DENOM < 80.0 THEN 'Fail' ELSE 'Pass' END        AS STATUS,
    NUMER                                                                      AS NUMERATOR,
    DENOM                                                                      AS DENOMINATOR,
    ROUND(100.0 * NUMER / NULLIF(DENOM, 0), 1)                                 AS PCT
FROM counts
