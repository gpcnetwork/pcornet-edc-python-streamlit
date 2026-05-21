-- DC 2.01: More than 5% of records have future dates. Future dates are defined as those
-- with dates occurring after the maximum refresh date in the HARVEST table
-- Parameters: {{ current_schema }}
WITH max_refresh AS (
    SELECT GREATEST(
        COALESCE(REFRESH_ENCOUNTER_DATE, '1900-01-01'),
        COALESCE(REFRESH_DIAGNOSIS_DATE, '1900-01-01'),
        COALESCE(REFRESH_PROCEDURES_DATE, '1900-01-01')
    ) AS MAX_DT FROM {{ current_schema }}.HARVEST LIMIT 1
),
counts AS (
    SELECT
        COUNT(*) AS TOTAL,
        COUNT_IF(ADMIT_DATE > (SELECT MAX_DT FROM max_refresh)) AS FUTURE_COUNT
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE IS NOT NULL
)
SELECT
    '2.01'                                                              AS CHECK_NUM,
    'More than 5% records with dates after max HARVEST refresh date'    AS DESCRIPTION,
    CASE WHEN 100.0 * FUTURE_COUNT / NULLIF(TOTAL, 0) > 5 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM counts
