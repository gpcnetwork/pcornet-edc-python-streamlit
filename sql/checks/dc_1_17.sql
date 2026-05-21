-- DC 1.17: Zip codes in the ENCOUNTER or LDS_ADDRESS_HISTORY table have
-- non-conforming values (alphabetical characters or the wrong number of digits)
-- Parameters: {{ current_schema }}
WITH all_zips AS (
    SELECT ZIP AS ZIP_VAL FROM {{ current_schema }}.ENCOUNTER WHERE ZIP IS NOT NULL
    UNION ALL
    SELECT ADDRESS_ZIP5 FROM {{ current_schema }}.LDS_ADDRESS_HISTORY WHERE ADDRESS_ZIP5 IS NOT NULL
),
bad_zips AS (
    SELECT COUNT(*) AS N
    FROM all_zips
    WHERE NOT REGEXP_LIKE(ZIP_VAL, '^[0-9]{5}(-[0-9]{4})?$')
)
SELECT
    '1.17'                                                                                    AS CHECK_NUM,
    'Zip codes in the ENCOUNTER or LDS_ADDRESS_HISTORY table have non-conforming values'      AS DESCRIPTION,
    CASE WHEN N > 0 THEN 'Fail' ELSE 'Pass' END                                               AS STATUS
FROM bad_zips
