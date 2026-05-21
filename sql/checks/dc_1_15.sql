-- DC 1.15: Fields with undefined lengths that are present in more than one table
-- (PATID, ENCOUNTERID, PRESCRIBINGID, PROCEDURESID,
-- PROVIDERID, MEDADMIN_PROVIDERID, OBSGEN_PROVIDERID,
-- OBSCLIN_PROVIDERID, RX_PROVIDERID, and VX_PROVIDERID)
-- do not have harmonized field lengths
-- Parameters: {{ current_schema }}
WITH patid_lengths AS (
    SELECT 'DEMOGRAPHIC' AS T, MAX(LENGTH(PATID)) AS L FROM {{ current_schema }}.DEMOGRAPHIC UNION ALL
    SELECT 'ENCOUNTER',         MAX(LENGTH(PATID))      FROM {{ current_schema }}.ENCOUNTER  UNION ALL
    SELECT 'DIAGNOSIS',         MAX(LENGTH(PATID))      FROM {{ current_schema }}.DIAGNOSIS  UNION ALL
    SELECT 'PROCEDURES',        MAX(LENGTH(PATID))      FROM {{ current_schema }}.PROCEDURES
),
check_harmonized AS (
    SELECT COUNT(DISTINCT L) AS UNIQUE_LENGTHS FROM patid_lengths WHERE L IS NOT NULL
)
SELECT
    '1.15'                                                 AS CHECK_NUM,
    'Fields with undefined lengths that are present in more than one table' AS DESCRIPTION,
    CASE WHEN UNIQUE_LENGTHS > 1 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM check_harmonized
