-- DC 1.21 | Table IID | Data Model Conformance | Required
-- STATE_FIPS, COUNTY_FIPS, and RUCA_ZIP in LDS_ADDRESS_HISTORY do not conform to expected content.
-- Sub-checks (token exclusion: NULL/N/A/NA/UNKNOWN/NI/UN/OT):
--   * STATE_FIPS  must be exactly 2 digits
--   * COUNTY_FIPS must be exactly 5 digits AND first 2 digits = STATE_FIPS
--   * RUCA_ZIP    must be digits-only (variable length)
-- Parameters: {{ current_schema }}
WITH bad AS (
    SELECT
        SUM(CASE WHEN NULLIF(TRIM(STATE_FIPS), '') IS NOT NULL
                  AND UPPER(TRIM(STATE_FIPS)) NOT IN ('NULL','N/A','NA','UNKNOWN','NI','UN','OT')
                  AND NOT REGEXP_LIKE(TRIM(STATE_FIPS), '^[0-9]{2}$') THEN 1 ELSE 0 END) AS BAD_STATE,
        SUM(CASE WHEN NULLIF(TRIM(COUNTY_FIPS), '') IS NOT NULL
                  AND UPPER(TRIM(COUNTY_FIPS)) NOT IN ('NULL','N/A','NA','UNKNOWN','NI','UN','OT')
                  AND ( NOT REGEXP_LIKE(TRIM(COUNTY_FIPS), '^[0-9]{5}$')
                        OR ( NULLIF(TRIM(STATE_FIPS), '') IS NOT NULL
                             AND LEFT(TRIM(COUNTY_FIPS), 2) <> TRIM(STATE_FIPS) ) )
                  THEN 1 ELSE 0 END) AS BAD_COUNTY,
        SUM(CASE WHEN NULLIF(TRIM(RUCA_ZIP::VARCHAR), '') IS NOT NULL
                  AND UPPER(TRIM(RUCA_ZIP::VARCHAR)) NOT IN ('NULL','N/A','NA','UNKNOWN','NI','UN','OT')
                  AND NOT REGEXP_LIKE(TRIM(RUCA_ZIP::VARCHAR), '^[0-9]+$') THEN 1 ELSE 0 END) AS BAD_RUCA
    FROM {{ current_schema }}.LDS_ADDRESS_HISTORY
)
SELECT
    '1.21'                                                                                         AS CHECK_NUM,
    'STATE_FIPS, COUNTY_FIPS, or RUCA_ZIP in LDS_ADDRESS_HISTORY does not conform to expected content' AS DESCRIPTION,
    CASE WHEN COALESCE(BAD_STATE, 0) + COALESCE(BAD_COUNTY, 0) + COALESCE(BAD_RUCA, 0) > 0
         THEN 'Fail' ELSE 'Pass' END                                                                AS STATUS
FROM bad
