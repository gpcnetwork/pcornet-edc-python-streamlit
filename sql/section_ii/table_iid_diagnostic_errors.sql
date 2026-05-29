-- Table IID. Diagnostic Errors
-- Exceptions to DC 1.01 (required tables not present), 1.02 (required tables not populated),
-- 1.03 (required fields not present), 1.04 (required fields do not conform to type/length),
-- 1.15 (shared identifier fields not harmonized in length), 1.17 (ZIP codes do not conform),
-- 1.21 (STATE_FIPS/COUNTY_FIPS/RUCA_ZIP do not conform). All exceptions highlighted in red.
-- Parameters: {{ current_schema }}, {{ db_name }}, {{ required_structure_fqn }}

WITH
schema_tables AS (
    SELECT UPPER(TABLE_NAME) AS TN
    FROM {{ db_name }}.INFORMATION_SCHEMA.TABLES
    WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}')
),
schema_columns AS (
    SELECT UPPER(TABLE_NAME) AS TN,
           UPPER(COLUMN_NAME) AS CN,
           UPPER(DATA_TYPE) AS DT,
           CHARACTER_MAXIMUM_LENGTH AS CLEN
    FROM {{ db_name }}.INFORMATION_SCHEMA.COLUMNS
    WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}')
),
required_structure AS (
    SELECT UPPER(MEMNAME) AS T,
           UPPER(NAME)    AS F,
           R_TYPE,
           R_LENGTH
    FROM {{ required_structure_fqn }}
),

-- ============ DC 1.01: required tables not present ============
required_tables AS (
    SELECT v.T FROM (VALUES
        ('DEMOGRAPHIC'),('ENROLLMENT'),('ENCOUNTER'),('DIAGNOSIS'),('PROCEDURES'),
        ('VITAL'),('DISPENSING'),('LAB_RESULT_CM'),('CONDITION'),('PRO_CM'),
        ('PRESCRIBING'),('PCORNET_TRIAL'),('DEATH'),('DEATH_CAUSE'),('MED_ADMIN'),
        ('PROVIDER'),('OBS_GEN'),('OBS_CLIN'),('HASH_TOKEN'),('LDS_ADDRESS_HISTORY'),
        ('IMMUNIZATION'),('HARVEST'),('LAB_HISTORY'),('EXTERNAL_MEDS'),('PAT_RELATIONSHIP')
    ) v(T)
),
dc101_missing AS (
    SELECT COALESCE(LISTAGG(r.T, ', ') WITHIN GROUP (ORDER BY r.T), 'None') AS TBLS
    FROM required_tables r
    WHERE NOT EXISTS (SELECT 1 FROM schema_tables s WHERE s.TN = r.T)
),

-- ============ DC 1.02: required tables not populated ============
dc102_empty AS (
    SELECT COALESCE(LISTAGG(T, ', ') WITHIN GROUP (ORDER BY T), 'None') AS TBLS
    FROM (
{% set populated_required = [
    ('DEMOGRAPHIC',   True),
    ('ENROLLMENT',    True),
    ('ENCOUNTER',     True),
    ('DIAGNOSIS',     True),
    ('PROCEDURES',    True),
    ('HARVEST',       True),
    ('LAB_RESULT_CM', False),
    ('PRESCRIBING',   False),
    ('VITAL',         False),
] %}
{% for tbl, _ in populated_required %}
        {% if not loop.first %}UNION ALL {% endif %}SELECT '{{ tbl }}' AS T
        FROM (SELECT 1 X) marker
        WHERE EXISTS (SELECT 1 FROM schema_tables WHERE TN = '{{ tbl }}')
          AND NOT EXISTS (SELECT 1 FROM {{ current_schema }}.{{ tbl }} LIMIT 1)
{% endfor %}
    )
),

-- ============ DC 1.03: required fields not present (split by R_TYPE) ============
dc103_missing_num AS (
    SELECT COALESCE(LISTAGG(DISTINCT rs.T, ', ') WITHIN GROUP (ORDER BY rs.T), 'None') AS TBLS,
           COALESCE(LISTAGG(rs.F || ' (' || rs.T || ')', ', ') WITHIN GROUP (ORDER BY rs.T, rs.F), 'None') AS FLDS
    FROM required_structure rs
    WHERE rs.R_TYPE = '1'
      AND NOT EXISTS (SELECT 1 FROM schema_columns c WHERE c.TN = rs.T AND c.CN = rs.F)
      AND EXISTS     (SELECT 1 FROM schema_tables  s WHERE s.TN = rs.T)
),
dc103_missing_char AS (
    SELECT COALESCE(LISTAGG(DISTINCT rs.T, ', ') WITHIN GROUP (ORDER BY rs.T), 'None') AS TBLS,
           COALESCE(LISTAGG(rs.F || ' (' || rs.T || ')', ', ') WITHIN GROUP (ORDER BY rs.T, rs.F), 'None') AS FLDS
    FROM required_structure rs
    WHERE rs.R_TYPE = '2'
      AND NOT EXISTS (SELECT 1 FROM schema_columns c WHERE c.TN = rs.T AND c.CN = rs.F)
      AND EXISTS     (SELECT 1 FROM schema_tables  s WHERE s.TN = rs.T)
),

-- ============ DC 1.04: type / length mismatches ============
-- Expected character (R_TYPE='2') but actual is numeric
dc104_char_is_num AS (
    SELECT COALESCE(LISTAGG(DISTINCT rs.T, ', ') WITHIN GROUP (ORDER BY rs.T), 'None') AS TBLS,
           COALESCE(LISTAGG(rs.F || ' (' || rs.T || ')', ', ') WITHIN GROUP (ORDER BY rs.T, rs.F), 'None') AS FLDS
    FROM required_structure rs
    JOIN schema_columns c ON c.TN = rs.T AND c.CN = rs.F
    WHERE rs.R_TYPE = '2'
      AND NOT (c.DT LIKE '%CHAR%' OR c.DT LIKE '%TEXT%' OR c.DT LIKE '%STRING%')
),
-- Expected numeric (R_TYPE='1') but actual is character (with %_TIME exception)
dc104_num_is_char AS (
    SELECT COALESCE(LISTAGG(DISTINCT rs.T, ', ') WITHIN GROUP (ORDER BY rs.T), 'None') AS TBLS,
           COALESCE(LISTAGG(rs.F || ' (' || rs.T || ')', ', ') WITHIN GROUP (ORDER BY rs.T, rs.F), 'None') AS FLDS
    FROM required_structure rs
    JOIN schema_columns c ON c.TN = rs.T AND c.CN = rs.F
    WHERE rs.R_TYPE = '1'
      AND (c.DT LIKE '%CHAR%' OR c.DT LIKE '%TEXT%' OR c.DT LIKE '%STRING%')
      AND NOT (
          rs.F LIKE '%_TIME'
          AND (c.DT LIKE '%TIME%' OR (c.CLEN IS NOT NULL AND c.CLEN <= 8))
      )
),
-- Required character field present but CHARACTER_MAXIMUM_LENGTH < required R_LENGTH
dc104_wrong_len AS (
    SELECT COALESCE(LISTAGG(DISTINCT rs.T, ', ') WITHIN GROUP (ORDER BY rs.T), 'None') AS TBLS,
           COALESCE(LISTAGG(rs.F || ' (' || rs.T || ')', ', ') WITHIN GROUP (ORDER BY rs.T, rs.F), 'None') AS FLDS
    FROM required_structure rs
    JOIN schema_columns c ON c.TN = rs.T AND c.CN = rs.F
    WHERE rs.R_TYPE = '2'
      AND rs.R_LENGTH IS NOT NULL
      AND (c.CLEN IS NULL OR c.CLEN < rs.R_LENGTH)
),

-- ============ DC 1.15: harmonized field lengths ============
dc115_harmony AS (
    SELECT CN AS COLUMN_NAME, TN AS TABLE_NAME, CLEN,
           COUNT(DISTINCT CLEN) OVER (PARTITION BY CN) AS VARIANT_COUNT
    FROM schema_columns
    WHERE CN IN (
        'PATID','PATID_1','PATID_2','ENCOUNTERID','PRESCRIBINGID','PROCEDURESID',
        'PROVIDERID','MEDADMIN_PROVIDERID','OBSGEN_PROVIDERID','OBSCLIN_PROVIDERID',
        'RX_PROVIDERID','VX_PROVIDERID'
    )
      AND CLEN IS NOT NULL
),
dc115_offenders AS (
    SELECT
        COALESCE(LISTAGG(DISTINCT TABLE_NAME, ', ') WITHIN GROUP (ORDER BY TABLE_NAME), 'None') AS TBLS,
        COALESCE(LISTAGG(DISTINCT COLUMN_NAME || '(' || CLEN || ')', ', ')
                 WITHIN GROUP (ORDER BY COLUMN_NAME || '(' || CLEN || ')'), 'None') AS FLDS
    FROM dc115_harmony
    WHERE VARIANT_COUNT > 1
),

-- ============ DC 1.17: ZIP code conformance ============
dc117_bad AS (
    SELECT SUM(N) AS TOTAL_BAD FROM (
        SELECT SUM(CASE WHEN NULLIF(TRIM(ADDRESS_ZIP5), '') IS NOT NULL
                         AND UPPER(TRIM(ADDRESS_ZIP5)) NOT IN ('NULL','N/A','NA','UNKNOWN','NI','UN','OT')
                         AND NOT REGEXP_LIKE(TRIM(ADDRESS_ZIP5), '^[0-9]{5}$') THEN 1 ELSE 0 END) AS N
        FROM {{ current_schema }}.LDS_ADDRESS_HISTORY
        UNION ALL
        SELECT SUM(CASE WHEN NULLIF(TRIM(ADDRESS_ZIP9), '') IS NOT NULL
                         AND UPPER(TRIM(ADDRESS_ZIP9)) NOT IN ('NULL','N/A','NA','UNKNOWN','NI','UN','OT')
                         AND NOT REGEXP_LIKE(TRIM(ADDRESS_ZIP9), '^[0-9]{9}$') THEN 1 ELSE 0 END)
        FROM {{ current_schema }}.LDS_ADDRESS_HISTORY
        UNION ALL
        SELECT SUM(CASE WHEN NULLIF(TRIM(FACILITY_LOCATION), '') IS NOT NULL
                         AND UPPER(TRIM(FACILITY_LOCATION)) NOT IN ('NULL','N/A','NA','UNKNOWN','NI','UN','OT')
                         AND NOT REGEXP_LIKE(TRIM(FACILITY_LOCATION), '^[0-9]{5}$') THEN 1 ELSE 0 END)
        FROM {{ current_schema }}.ENCOUNTER
    )
),

-- ============ DC 1.21: STATE_FIPS / COUNTY_FIPS / RUCA_ZIP conformance ============
dc121_bad AS (
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

SELECT "Data Check", "Data Check Description", "Exception", "Table(s)", "Field(s)"
FROM (
    SELECT '1.01' AS "Data Check",
           'Required tables are not present' AS "Data Check Description",
           'Required table is not present. All tables must be present in an instantiation of the CDM.' AS "Exception",
           (SELECT TBLS FROM dc101_missing) AS "Table(s)",
           'n/a' AS "Field(s)",
           10 AS ROW_ORDER
    UNION ALL
    SELECT '1.02',
           'Expected tables are not populated',
           'Table required to be populated is not populated. DEMOGRAPHIC, ENROLLMENT, ENCOUNTER, DIAGNOSIS, PROCEDURES, and HARVEST must be populated. If EHR data is included, LAB_RESULT_CM, PRESCRIBING, and VITAL must also be populated.',
           (SELECT TBLS FROM dc102_empty),
           'n/a',
           20
    UNION ALL
    SELECT '1.03',
           'Required fields are not present',
           'Required numeric field is not present',
           (SELECT TBLS FROM dc103_missing_num),
           (SELECT FLDS FROM dc103_missing_num),
           31
    UNION ALL
    SELECT '1.03',
           'Required fields are not present',
           'Required character field is not present',
           (SELECT TBLS FROM dc103_missing_char),
           (SELECT FLDS FROM dc103_missing_char),
           32
    UNION ALL
    SELECT '1.04',
           'Required fields do not conform to data model specifications for data type, length, or name',
           'Required character field is numeric',
           (SELECT TBLS FROM dc104_char_is_num),
           (SELECT FLDS FROM dc104_char_is_num),
           41
    UNION ALL
    SELECT '1.04',
           'Required fields do not conform to data model specifications for data type, length, or name',
           'Required numeric field is character',
           (SELECT TBLS FROM dc104_num_is_char),
           (SELECT FLDS FROM dc104_num_is_char),
           42
    UNION ALL
    SELECT '1.04',
           'Required fields do not conform to data model specifications for data type, length, or name',
           'Required field is present but of unexpected length',
           (SELECT TBLS FROM dc104_wrong_len),
           (SELECT FLDS FROM dc104_wrong_len),
           43
    UNION ALL
    SELECT '1.15',
           'Fields with undefined lengths that are present in more than one table do not have harmonized field lengths',
           'Field lengths are not harmonized for one or more of the following fields: PATID, PATID_1, PATID_2, ENCOUNTERID, PRESCRIBINGID, PROCEDURESID, PROVIDERID, MEDADMIN_PROVIDERID, OBSGEN_PROVIDERID, OBSCLIN_PROVIDERID, RX_PROVIDERID, and VX_PROVIDERID.',
           (SELECT TBLS FROM dc115_offenders),
           (SELECT FLDS FROM dc115_offenders),
           50
    UNION ALL
    SELECT '1.17',
           'Zip codes in the ENCOUNTER or LDS_ADDRESS_HISTORY table do not conform to expected values',
           'LDS_ADDRESS_HISTORY.ADDRESS_ZIP5, LDS_ADDRESS_HISTORY.ADDRESS_ZIP9 or ENCOUNTER.FACILITY_LOCATION contains alphabetical characters or does not have the expected number of digits.',
           CASE WHEN COALESCE((SELECT TOTAL_BAD FROM dc117_bad), 0) > 0
                THEN 'ENCOUNTER, LDS_ADDRESS_HISTORY' ELSE 'None' END,
           CASE WHEN COALESCE((SELECT TOTAL_BAD FROM dc117_bad), 0) > 0
                THEN 'FACILITY_LOCATION, ADDRESS_ZIP5, ADDRESS_ZIP9' ELSE 'None' END,
           60
    UNION ALL
    SELECT '1.21',
           'Geo codes in the LDS_ADDRESS_HISTORY table do not conform to expected values',
           'LDS_ADDRESS_HISTORY.STATE_FIPS, LDS_ADDRESS_HISTORY.COUNTY_FIPS, or LDS_ADDRESS_HISTORY.RUCA_ZIP contains alphabetical characters or does not have the expected number of digits, or the first 2 digits of COUNTY_FIPS does not match STATE_FIPS.',
           CASE WHEN COALESCE((SELECT BAD_STATE FROM dc121_bad), 0)
                   + COALESCE((SELECT BAD_COUNTY FROM dc121_bad), 0)
                   + COALESCE((SELECT BAD_RUCA FROM dc121_bad), 0) > 0
                THEN 'LDS_ADDRESS_HISTORY' ELSE 'None' END,
           COALESCE(NULLIF(TRIM(
               (CASE WHEN COALESCE((SELECT BAD_STATE FROM dc121_bad), 0)  > 0 THEN 'STATE_FIPS, '  ELSE '' END) ||
               (CASE WHEN COALESCE((SELECT BAD_COUNTY FROM dc121_bad), 0) > 0 THEN 'COUNTY_FIPS, ' ELSE '' END) ||
               (CASE WHEN COALESCE((SELECT BAD_RUCA FROM dc121_bad), 0)   > 0 THEN 'RUCA_ZIP'      ELSE '' END)
           , ', '), ''), 'None'),
           70
)
ORDER BY ROW_ORDER
