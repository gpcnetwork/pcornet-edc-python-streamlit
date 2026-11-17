-- DC 3.18 | Table IB | Data Completeness | Investigative (red / must be corrected)
-- Less than 80% of patients with a face-to-face visit in the past 5 years have ADDRESS_ZIP5 AND
-- COUNTY_FIPS address data for their current physical address.
--
-- SAS source of truth: edc_report.sas 883-931 (Table IB ord=8, row=3.18) +
--   data_curation_tables.sas 1153-1186 (current physical address) and 4667-4729 (XTBL_L3_CFIPSZIP_5Y).
--   Numerator  = distinct patients (with a 5-yr face-to-face encounter) whose CURRENT PHYSICAL address
--                has BOTH a valid ADDRESS_ZIP5 and a valid COUNTY_FIPS.
--   Denominator= distinct patients with a face-to-face encounter in the past 5 years (ENC_L3_DASH2 "5 yrs").
--   Exception  = numerator = 0 OR (numerator/denominator)*100 < 80.0.
--   Current physical address: ADDRESS_PREFERRED='Y' AND ADDRESS_USE IN ('HO','OT','NI','UN','')
--                             AND ADDRESS_PERIOD_END IS NULL AND CURRENT_ADDRESS_FLAG='Y'.
--   Face-to-face encounter types: AV, ED, EI, IP, OS (per DC 2.09).
-- 5-yr window = the SAS DASH window (data_curation_base.sas 49-55, 2867-2891):
--   [first-of-month(run - 63 months), first-of-month(run - 3 months)], inclusive, with the
--   run date approximated by {{ end_date }} (= HARVEST refresh_max).
-- UNVERIFIED: reproduce numerically against drnoc EDCRPT (C4UMO expects an EXCEPTION = Yes).
-- Parameters: {{ current_schema }}, {{ end_date }}
WITH bounds AS (
    SELECT
        DATE_TRUNC('month', DATEADD(month, -63, TO_DATE('{{ end_date }}'))) AS LO,
        DATE_TRUNC('month', DATEADD(month, -3,  TO_DATE('{{ end_date }}'))) AS HI
),
f2f_patients AS (
    SELECT DISTINCT e.PATID
    FROM {{ current_schema }}.ENCOUNTER e, bounds b
    WHERE e.ENC_TYPE IN ('AV','ED','EI','IP','OS')
      AND e.PATID IS NOT NULL
      AND e.ADMIT_DATE BETWEEN b.LO AND b.HI
),
current_address AS (
    -- one row per patient who has a current physical address with BOTH valid ZIP5 and COUNTY_FIPS
    SELECT DISTINCT PATID
    FROM {{ current_schema }}.LDS_ADDRESS_HISTORY
    WHERE PATID IS NOT NULL
      AND ADDRESS_PREFERRED = 'Y'
      AND ADDRESS_USE IN ('HO','OT','NI','UN','')
      AND ADDRESS_PERIOD_END IS NULL
      AND CURRENT_ADDRESS_FLAG = 'Y'
      AND REGEXP_LIKE(TRIM(ADDRESS_ZIP5), '^[0-9]{5}$')
      AND REGEXP_LIKE(TRIM(COUNTY_FIPS), '^[0-9]{5}$')
),
counts AS (
    SELECT
        (SELECT COUNT(*) FROM f2f_patients)                                                  AS DENOM,
        (SELECT COUNT(*) FROM f2f_patients f WHERE f.PATID IN (SELECT PATID FROM current_address)) AS NUMER
)
SELECT
    '3.18'                                                                                            AS CHECK_NUM,
    'Less than 80% of patients w/ face-to-face visit past 5y have ADDRESS_ZIP5 and COUNTY_FIPS'       AS DESCRIPTION,
    CASE WHEN NUMER = 0 OR (DENOM > 0 AND 100.0 * NUMER / DENOM < 80.0) THEN 'Fail' ELSE 'Pass' END   AS STATUS
FROM counts
