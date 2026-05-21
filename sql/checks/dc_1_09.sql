-- DC 1.09: Tables contain orphan ENCOUNTERIDs (ENCOUNTERIDs that are not present in the ENCOUNTER table) for more than 5% of records
-- Parameters: {{ current_schema }}
WITH counts AS (
    SELECT
        COUNT(*) AS TOTAL,
        COUNT_IF(ENCOUNTERID NOT IN (SELECT ENCOUNTERID FROM {{ current_schema }}.ENCOUNTER)) AS ORPHAN_COUNT
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ENCOUNTERID IS NOT NULL
)
SELECT
    '1.09'                                  AS CHECK_NUM,
    'Tables contain orphan ENCOUNTERIDs (ENCOUNTERIDs that are not present in the ENCOUNTER table) for more than 5% of records' AS DESCRIPTION,
    CASE WHEN 100.0 * ORPHAN_COUNT / NULLIF(TOTAL, 0) > 5 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM counts
