-- DC 1.12: Tables contain orphan PROVIDERIDs (PROVIDERIDs that are not present in the PROVIDER table)
-- Parameters: {{ current_schema }}
WITH orphans AS (
    SELECT COUNT(*) AS N
    FROM {{ current_schema }}.ENCOUNTER
    WHERE PROVIDERID IS NOT NULL
      AND PROVIDERID NOT IN (SELECT PROVIDERID FROM {{ current_schema }}.PROVIDER)
)
SELECT
    '1.12'                                  AS CHECK_NUM,
    'Tables contain orphan PROVIDERIDs (PROVIDERIDs that are not present in the PROVIDER table)' AS DESCRIPTION,
    CASE WHEN N > 0 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM orphans
