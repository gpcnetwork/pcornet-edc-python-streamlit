-- Table IID. Diagnostic Errors
-- Exceptions to DC 1.01 (required tables absent), 1.02 (required tables empty),
-- 1.03 (required fields absent), 1.04 (fields non-conforming type/length),
-- 1.15 (field length harmonization), 1.17 (zip code conformance), 1.21 (FIPS/RUCA conformance).
-- Exceptions highlighted in red and must be corrected.

SELECT DATA_CHECK, DATA_CHECK_DESCRIPTION, EXCEPTION, TABLES, FIELDS, SOURCE_TABLES
FROM (

    -- DC 1.01: Required tables present
    SELECT '1.01' AS DATA_CHECK,
           'Required tables are present in the CDM schema' AS DATA_CHECK_DESCRIPTION,
           CASE WHEN (
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'DEMOGRAPHIC') +
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'ENROLLMENT') +
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'ENCOUNTER') +
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'DIAGNOSIS') +
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'PROCEDURES') +
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'VITAL') +
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'LAB_RESULT_CM') +
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'PRESCRIBING') +
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'DISPENSING') +
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'HARVEST')
           ) >= 10 THEN 'None'
           ELSE 'One or more required CDM tables are absent from the schema' END AS EXCEPTION,
           'DEMOGRAPHIC, ENROLLMENT, ENCOUNTER, DIAGNOSIS, PROCEDURES, VITAL, LAB_RESULT_CM, PRESCRIBING, DISPENSING, HARVEST' AS TABLES,
           '' AS FIELDS,
           'DC1.01' AS SOURCE_TABLES,
           1 AS ROW_ORDER

    UNION ALL

    -- DC 1.02: Required tables populated
    SELECT '1.02',
           'Required tables are populated (contain at least one record)',
           CASE WHEN (
               (SELECT COUNT(*) FROM {{ current_schema }}.DEMOGRAPHIC) > 0 AND
               (SELECT COUNT(*) FROM {{ current_schema }}.ENCOUNTER) > 0 AND
               (SELECT COUNT(*) FROM {{ current_schema }}.DIAGNOSIS) > 0 AND
               (SELECT COUNT(*) FROM {{ current_schema }}.PROCEDURES) > 0 AND
               (SELECT COUNT(*) FROM {{ current_schema }}.VITAL) > 0
           ) THEN 'None'
           ELSE 'One or more required CDM tables contain no records' END,
           'DEMOGRAPHIC, ENCOUNTER, DIAGNOSIS, PROCEDURES, VITAL',
           '',
           'DC1.02',
           2

    UNION ALL

    -- DC 1.03: Required fields present — check key fields via INFORMATION_SCHEMA
    SELECT '1.03',
           'Required fields are present in each CDM table',
           CASE WHEN (
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'DEMOGRAPHIC' AND UPPER(COLUMN_NAME) = 'PATID') > 0 AND
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'ENCOUNTER' AND UPPER(COLUMN_NAME) = 'ENCOUNTERID') > 0 AND
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'DIAGNOSIS' AND UPPER(COLUMN_NAME) = 'DIAGNOSISID') > 0 AND
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'VITAL' AND UPPER(COLUMN_NAME) = 'VITALID') > 0 AND
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'LAB_RESULT_CM' AND UPPER(COLUMN_NAME) = 'LAB_RESULT_CM_ID') > 0
           ) THEN 'None'
           ELSE 'One or more required fields are absent from CDM tables' END,
           'DEMOGRAPHIC, ENCOUNTER, DIAGNOSIS, VITAL, LAB_RESULT_CM',
           'PATID, ENCOUNTERID, DIAGNOSISID, VITALID, LAB_RESULT_CM_ID',
           'DC1.03',
           3

    UNION ALL

    -- DC 1.04: Required fields conform to data model specifications
    SELECT '1.04',
           'Required fields conform to data model specifications (data type, length, name)',
           CASE WHEN (
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'DEMOGRAPHIC' AND UPPER(COLUMN_NAME) = 'PATID' AND UPPER(DATA_TYPE) IN ('TEXT','VARCHAR','CHAR','CHARACTER VARYING','STRING')) > 0 AND
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(TABLE_NAME) = 'ENCOUNTER' AND UPPER(COLUMN_NAME) = 'ADMIT_DATE' AND UPPER(DATA_TYPE) IN ('DATE','TIMESTAMP','TIMESTAMP_NTZ','TIMESTAMP_LTZ')) > 0
           ) THEN 'None'
           ELSE 'One or more required fields do not conform to the expected data type or length' END,
           'DEMOGRAPHIC, ENCOUNTER',
           'PATID (VARCHAR), ADMIT_DATE (DATE)',
           'DC1.04',
           4

    UNION ALL

    -- DC 1.15: Fields with undefined lengths harmonized across tables
    SELECT '1.15',
           'Fields with undefined lengths (e.g., DX in DIAGNOSIS) are harmonized across shared tables',
           CASE WHEN (
               (SELECT MAX(CHARACTER_MAXIMUM_LENGTH) FROM INFORMATION_SCHEMA.COLUMNS
                WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(COLUMN_NAME) = 'DX') =
               (SELECT MIN(CHARACTER_MAXIMUM_LENGTH) FROM INFORMATION_SCHEMA.COLUMNS
                WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(COLUMN_NAME) = 'DX'
                  AND CHARACTER_MAXIMUM_LENGTH IS NOT NULL)
               OR (SELECT COUNT(DISTINCT TABLE_NAME) FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}') AND UPPER(COLUMN_NAME) = 'DX') <= 1
           ) THEN 'None'
           ELSE 'Field DX has inconsistent lengths across tables' END,
           'DIAGNOSIS, CONDITION',
           'DX',
           'DC1.15',
           5

    UNION ALL

    -- DC 1.17: Zip codes conform to expected values
    SELECT '1.17',
           'Zip codes in ENCOUNTER conform to expected 5-digit format',
           CASE WHEN (
               SELECT COUNT(*) FROM {{ current_schema }}.ENCOUNTER
               WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
                 AND ZIP IS NOT NULL
                 AND NOT REGEXP_LIKE(TRIM(ZIP), '^[0-9]{5}$')
           ) = 0 THEN 'None'
           ELSE 'Zip codes present that do not conform to 5-digit numeric format' END,
           'ENCOUNTER',
           'ZIP',
           'ENC_L3_ZIP; LDS_L3_AZIP',
           6

    UNION ALL

    -- DC 1.21: STATE_FIPS, COUNTY_FIPS, RUCA_ZIP conformance
    SELECT '1.21',
           'STATE_FIPS, COUNTY_FIPS, and RUCA_ZIP fields conform to expected content',
           CASE WHEN (
               (SELECT COUNT(*) FROM {{ current_schema }}.ENCOUNTER
                WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
                  AND STATE_FIPS IS NOT NULL
                  AND NOT REGEXP_LIKE(TRIM(STATE_FIPS), '^[0-9]{2}$')) +
               (SELECT COUNT(*) FROM {{ current_schema }}.ENCOUNTER
                WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
                  AND COUNTY_FIPS IS NOT NULL
                  AND NOT REGEXP_LIKE(TRIM(COUNTY_FIPS), '^[0-9]{3}$'))
           ) = 0 THEN 'None'
           ELSE 'STATE_FIPS or COUNTY_FIPS values do not conform to expected FIPS code format' END,
           'ENCOUNTER, LDS_ADDRESS_HISTORY',
           'STATE_FIPS, COUNTY_FIPS, RUCA_ZIP',
           'ENC_L3_FIPS; LDS_L3_FIPS',
           7

) sub
ORDER BY ROW_ORDER
