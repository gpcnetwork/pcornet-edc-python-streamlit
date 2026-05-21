-- DC 1.02: Required tables are populated
-- Required tables are not populated. DEMOGRAPHIC, ENROLLMENT,
-- ENCOUNTER, DIAGNOSIS, PROCEDURES, and HARVEST are
-- required for all network partners; LAB_RESULT_CM, PRESCRIBING,
-- and VITAL are required for network partners with electronic health record
-- data
-- Parameters: {{ current_schema }}
WITH counts AS (
    SELECT CASE WHEN EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC)   THEN 1 ELSE 0 END AS N UNION ALL
    SELECT CASE WHEN EXISTS (SELECT 1 FROM {{ current_schema }}.ENROLLMENT)    THEN 1 ELSE 0 END UNION ALL
    SELECT CASE WHEN EXISTS (SELECT 1 FROM {{ current_schema }}.ENCOUNTER)     THEN 1 ELSE 0 END UNION ALL
    SELECT CASE WHEN EXISTS (SELECT 1 FROM {{ current_schema }}.DIAGNOSIS)     THEN 1 ELSE 0 END UNION ALL
    SELECT CASE WHEN EXISTS (SELECT 1 FROM {{ current_schema }}.PROCEDURES)    THEN 1 ELSE 0 END UNION ALL
    SELECT CASE WHEN EXISTS (SELECT 1 FROM {{ current_schema }}.HARVEST)       THEN 1 ELSE 0 END UNION ALL
    SELECT CASE WHEN EXISTS (SELECT 1 FROM {{ current_schema }}.LAB_RESULT_CM) THEN 1 ELSE 0 END UNION ALL
    SELECT CASE WHEN EXISTS (SELECT 1 FROM {{ current_schema }}.PRESCRIBING)   THEN 1 ELSE 0 END UNION ALL
    SELECT CASE WHEN EXISTS (SELECT 1 FROM {{ current_schema }}.VITAL)         THEN 1 ELSE 0 END
)
SELECT
    '1.02'                                AS CHECK_NUM,
    'Required tables are populated'       AS DESCRIPTION,
    CASE WHEN MIN(N) > 0 THEN 'Pass' ELSE 'Fail' END AS STATUS
FROM counts
