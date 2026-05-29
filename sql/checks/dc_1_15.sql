-- DC 1.15 | Table IID | Data Model Conformance | Required
-- Fields with undefined lengths (PATID, PATID_1, PATID_2, ENCOUNTERID, PRESCRIBINGID, PROCEDURESID,
-- PROVIDERID, MEDADMIN_PROVIDERID, OBSGEN_PROVIDERID, OBSCLIN_PROVIDERID, RX_PROVIDERID, VX_PROVIDERID)
-- that are present in more than one table must have harmonized CHARACTER_MAXIMUM_LENGTH values.
-- Parameters: {{ db_name }}, {{ current_schema }}
WITH column_metadata AS (
    SELECT
        UPPER(COLUMN_NAME) AS COLUMN_NAME,
        UPPER(TABLE_NAME)  AS TABLE_NAME,
        CHARACTER_MAXIMUM_LENGTH,
        COUNT(DISTINCT CHARACTER_MAXIMUM_LENGTH)
            OVER (PARTITION BY UPPER(COLUMN_NAME)) AS VARIANT_COUNT
    FROM {{ db_name }}.INFORMATION_SCHEMA.COLUMNS
    WHERE UPPER(TABLE_SCHEMA) = UPPER('{{ current_schema }}')
      AND UPPER(COLUMN_NAME) IN (
        'PATID','PATID_1','PATID_2','ENCOUNTERID','PRESCRIBINGID','PROCEDURESID',
        'PROVIDERID','MEDADMIN_PROVIDERID','OBSGEN_PROVIDERID','OBSCLIN_PROVIDERID',
        'RX_PROVIDERID','VX_PROVIDERID'
      )
      AND CHARACTER_MAXIMUM_LENGTH IS NOT NULL
)
SELECT
    '1.15'                                                                                         AS CHECK_NUM,
    'Shared identifier fields do not have harmonized lengths across all tables in which they appear' AS DESCRIPTION,
    CASE WHEN COUNT(DISTINCT CASE WHEN VARIANT_COUNT > 1 THEN COLUMN_NAME END) > 0
         THEN 'Fail' ELSE 'Pass' END                                                                AS STATUS
FROM column_metadata
