-- DC 1.07: Required fields have non-permissible missing values
-- Parameters: {{ current_schema }}
WITH nulls AS (
    SELECT COUNT(*) AS N FROM {{ current_schema }}.DEMOGRAPHIC WHERE PATID IS NULL UNION ALL
    SELECT COUNT(*) FROM {{ current_schema }}.ENCOUNTER WHERE ENCOUNTERID IS NULL UNION ALL
    SELECT COUNT(*) FROM {{ current_schema }}.ENCOUNTER WHERE PATID IS NULL UNION ALL
    SELECT COUNT(*) FROM {{ current_schema }}.ENCOUNTER WHERE ADMIT_DATE IS NULL UNION ALL
    SELECT COUNT(*) FROM {{ current_schema }}.ENCOUNTER WHERE ENC_TYPE IS NULL UNION ALL
    SELECT COUNT(*) FROM {{ current_schema }}.DIAGNOSIS WHERE DIAGNOSISID IS NULL UNION ALL
    SELECT COUNT(*) FROM {{ current_schema }}.DIAGNOSIS WHERE PATID IS NULL UNION ALL
    SELECT COUNT(*) FROM {{ current_schema }}.PROCEDURES WHERE PROCEDURESID IS NULL UNION ALL
    SELECT COUNT(*) FROM {{ current_schema }}.PROCEDURES WHERE PATID IS NULL
),
total AS (SELECT SUM(N) AS TOTAL_NULLS FROM nulls)
SELECT
    '1.07'                                              AS CHECK_NUM,
    'Required fields have non-permissible missing values' AS DESCRIPTION,
    CASE WHEN TOTAL_NULLS > 0 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM total
