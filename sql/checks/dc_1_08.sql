-- DC 1.08: Tables contain orphan PATIDs (PATIDS that are not present in the DEMOGRAPHIC table)
-- Parameters: {{ current_schema }}
WITH orphans AS (
    SELECT COUNT(*) AS N FROM {{ current_schema }}.ENCOUNTER WHERE PATID NOT IN (SELECT PATID FROM {{ current_schema }}.DEMOGRAPHIC) UNION ALL
    SELECT COUNT(*) FROM {{ current_schema }}.DIAGNOSIS WHERE PATID NOT IN (SELECT PATID FROM {{ current_schema }}.DEMOGRAPHIC) UNION ALL
    SELECT COUNT(*) FROM {{ current_schema }}.PROCEDURES WHERE PATID NOT IN (SELECT PATID FROM {{ current_schema }}.DEMOGRAPHIC)
),
total AS (SELECT SUM(N) AS TOTAL_ORPHANS FROM orphans)
SELECT
    '1.08'                                  AS CHECK_NUM,
    'Tables contain orphan PATIDs (PATIDS that are not present in the DEMOGRAPHIC table)' AS DESCRIPTION,
    CASE WHEN TOTAL_ORPHANS > 0 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM total
