-- DC 1.21: STATE_FIPS, COUNTY_FIPS and RUCA_ZIP fields have
-- non-conforming values (alphabetical characters or discrepancies between
-- STATE_FIPS and COUNTY_FIPS)
-- Parameters: {{ current_schema }}
WITH bad AS (
    SELECT COUNT(*) AS N
    FROM {{ current_schema }}.ENCOUNTER
    WHERE (STATE_FIPS IS NOT NULL AND NOT REGEXP_LIKE(STATE_FIPS, '^[0-9]{2}$'))
       OR (COUNTY_FIPS IS NOT NULL AND NOT REGEXP_LIKE(COUNTY_FIPS, '^[0-9]{5}$'))
)
SELECT
    '1.21'                                                          AS CHECK_NUM,
    'Any non-conforming STATE_FIPS, COUNTY_FIPS, or RUCA_ZIP'       AS DESCRIPTION,
    CASE WHEN N > 0 THEN 'Fail' ELSE 'Pass' END                     AS STATUS
FROM bad
