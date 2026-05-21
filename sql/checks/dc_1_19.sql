-- DC 1.19: More than 10% of the values in any hash token field are assigned to multiple patients
-- Parameters: {{ current_schema }}
WITH multi AS (
    SELECT
        COUNT(DISTINCT TOKEN_VALUE) AS TOTAL_TOKENS,
        COUNT_IF(CNT > 1) AS MULTI_PAT_TOKENS
    FROM (
        SELECT TOKEN_VALUE, COUNT(DISTINCT PATID) AS CNT
        FROM {{ current_schema }}.HASH_TOKEN
        WHERE TOKEN_VALUE IS NOT NULL
        GROUP BY TOKEN_VALUE
    )
)
SELECT
    '1.19'                                                          AS CHECK_NUM,
    'More than 10% hash token values assigned to multiple patients' AS DESCRIPTION,
    CASE WHEN 100.0 * MULTI_PAT_TOKENS / NULLIF(TOTAL_TOKENS, 0) > 10 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM multi
