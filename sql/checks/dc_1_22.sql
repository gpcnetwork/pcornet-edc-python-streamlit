-- DC 1.22 | Table IID | Data Model Conformance | Required
-- Patients have multiple current physical addresses in the LDS_ADDRESS_HISTORY table.
--
-- SAS source of truth: edc_report.sas 4287-4312, 4613-4637 (row 11 / row=1.22).
--   Rule: "A PATID has multiple ADDRESSIDs where CURRENT_ADDRESS_FLAG='Y'".
--   Exception when RECORD_N > DISTINCT_PATID_N among rows with CURRENT_ADDRESS_FLAG='Y'
--   (i.e. at least one patient has more than one current-flagged address record).
-- Conformance check: full table, no date window.
-- Parameters: {{ current_schema }}
WITH cur AS (
    SELECT PATID
    FROM {{ current_schema }}.LDS_ADDRESS_HISTORY
    WHERE CURRENT_ADDRESS_FLAG = 'Y'
      AND PATID IS NOT NULL
),
counts AS (
    SELECT COUNT(*) AS RECORD_N, COUNT(DISTINCT PATID) AS DISTINCT_PATID_N
    FROM cur
)
SELECT
    '1.22'                                                                  AS CHECK_NUM,
    'Patients have multiple current physical addresses in LDS_ADDRESS_HISTORY' AS DESCRIPTION,
    CASE WHEN RECORD_N > DISTINCT_PATID_N THEN 'Fail' ELSE 'Pass' END        AS STATUS
FROM counts
