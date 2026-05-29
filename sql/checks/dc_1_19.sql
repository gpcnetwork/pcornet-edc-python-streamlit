-- DC 1.19 | Table IIE | Data Model Conformance | Investigative
-- More than 10% of the values in the TOKEN_ENCRYPTION_KEY hash token field are assigned to multiple patients.
-- Parameters: {{ current_schema }}
WITH multi AS (
    SELECT
        COUNT(DISTINCT TOKEN_ENCRYPTION_KEY) AS TOTAL_TOKENS,
        COUNT_IF(CNT > 1) AS MULTI_PAT_TOKENS
    FROM (
        SELECT TOKEN_ENCRYPTION_KEY, COUNT(DISTINCT PATID) AS CNT
        FROM {{ current_schema }}.HASH_TOKEN
        WHERE TOKEN_ENCRYPTION_KEY IS NOT NULL
        GROUP BY TOKEN_ENCRYPTION_KEY
    )
)
SELECT
    '1.19'                                                                              AS CHECK_NUM,
    'More than 10% of hash tokens (TOKEN_ENCRYPTION_KEY) are assigned to multiple PATIDs' AS DESCRIPTION,
    CASE WHEN 100.0 * MULTI_PAT_TOKENS / NULLIF(TOTAL_TOKENS, 0) > 10 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM multi
