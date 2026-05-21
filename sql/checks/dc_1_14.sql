-- DC 1.14: Patients in the DEMOGRAPHIC table are not in the HASH_TOKEN table
-- Parameters: {{ current_schema }}
WITH missing AS (
    SELECT COUNT(*) AS N
    FROM {{ current_schema }}.DEMOGRAPHIC
    WHERE PATID NOT IN (SELECT PATID FROM {{ current_schema }}.HASH_TOKEN WHERE PATID IS NOT NULL)
)
SELECT
    '1.14'                                          AS CHECK_NUM,
    'Patients in the DEMOGRAPHIC table are not in the HASH_TOKEN table' AS DESCRIPTION,
    CASE WHEN N > 0 THEN 'Fail' ELSE 'Pass' END     AS STATUS
FROM missing
