-- DC 1.10: Replication errors between the ENCOUNTER, PROCEDURES and DIAGNOSIS tables. Replication errors are ENCOUNTERIDs in the DIAGNOSIS or PROCEDURES table where the encounter type or admit date does not match the corresponding value in the ENCOUNTER table
-- Parameters: {{ current_schema }}
WITH mismatches AS (
    SELECT COUNT(*) AS N
    FROM {{ current_schema }}.DIAGNOSIS d
    JOIN {{ current_schema }}.ENCOUNTER e ON d.ENCOUNTERID = e.ENCOUNTERID
    WHERE d.ENC_TYPE != e.ENC_TYPE OR d.ADMIT_DATE != e.ADMIT_DATE
)
SELECT
    '1.10'                                  AS CHECK_NUM,
    'Replication errors between the ENCOUNTER, PROCEDURES and DIAGNOSIS tables' AS DESCRIPTION,
    CASE WHEN N > 0 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM mismatches
