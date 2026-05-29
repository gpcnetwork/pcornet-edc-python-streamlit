-- DC 1.10 | Table IIE | Data Model Conformance | Required
-- Replication errors between the ENCOUNTER, PROCEDURES and DIAGNOSIS tables. Replication errors are ENCOUNTERIDs in the
-- DIAGNOSIS or PROCEDURES table where the encounter type or admit date does not match the corresponding value in the ENCOUNTER table
-- Parameters: {{ current_schema }}
WITH mismatches AS (
    SELECT COUNT(*) AS N
    FROM {{ current_schema }}.DIAGNOSIS d
    JOIN {{ current_schema }}.ENCOUNTER e ON d.ENCOUNTERID = e.ENCOUNTERID
    WHERE d.ENC_TYPE != e.ENC_TYPE OR d.ADMIT_DATE != e.ADMIT_DATE
    UNION ALL
    SELECT COUNT(*) AS N
    FROM {{ current_schema }}.PROCEDURES p
    JOIN {{ current_schema }}.ENCOUNTER e ON p.ENCOUNTERID = e.ENCOUNTERID
    WHERE p.ENC_TYPE != e.ENC_TYPE OR p.ADMIT_DATE != e.ADMIT_DATE
),
total AS (SELECT SUM(N) AS TOTAL_MISMATCHES FROM mismatches)
SELECT
    '1.10'                                                                            AS CHECK_NUM,
    'Replication errors in DIAGNOSIS or PROCEDURES vs ENCOUNTER (ENC_TYPE/ADMIT_DATE mismatch)' AS DESCRIPTION,
    CASE WHEN TOTAL_MISMATCHES > 0 THEN 'Fail' ELSE 'Pass' END                        AS STATUS
FROM total
