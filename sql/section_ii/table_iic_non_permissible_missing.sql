-- Table IIC. Non-Permissible Missing Values
-- Required fields that contain missing or non-permissible values. Supports DC 1.07.
-- Exceptions highlighted in red and must be corrected.
-- Returns 'All fields conform to specifications' row when no violations are found.

WITH violations AS (
    SELECT TABLE_NAME, FIELD_NAME, TO_VARCHAR(RECORDS_MISSING) AS RECORDS_MISSING, SOURCE_TABLE
    FROM (
        -- DEMOGRAPHIC required fields
        SELECT 'DEMOGRAPHIC' AS TABLE_NAME, 'PATID' AS FIELD_NAME,
               SUM(CASE WHEN PATID IS NULL THEN 1 ELSE 0 END) AS RECORDS_MISSING,
               'DEM_L3_N' AS SOURCE_TABLE
        FROM {{ current_schema }}.DEMOGRAPHIC
        HAVING SUM(CASE WHEN PATID IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'DEMOGRAPHIC', 'BIRTH_DATE',
               SUM(CASE WHEN BIRTH_DATE IS NULL THEN 1 ELSE 0 END),
               'DEM_L3_N'
        FROM {{ current_schema }}.DEMOGRAPHIC
        HAVING SUM(CASE WHEN BIRTH_DATE IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'DEMOGRAPHIC', 'SEX',
               SUM(CASE WHEN SEX IS NULL OR SEX IN ('NI','UN') THEN 1 ELSE 0 END),
               'DEM_L3_SEXDIST'
        FROM {{ current_schema }}.DEMOGRAPHIC
        HAVING SUM(CASE WHEN SEX IS NULL OR SEX IN ('NI','UN') THEN 1 ELSE 0 END) > 0

        UNION ALL
        -- ENROLLMENT required fields
        SELECT 'ENROLLMENT', 'PATID',
               SUM(CASE WHEN PATID IS NULL THEN 1 ELSE 0 END),
               'ENR_L3_N'
        FROM {{ current_schema }}.ENROLLMENT
        HAVING SUM(CASE WHEN PATID IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'ENROLLMENT', 'ENR_START_DATE',
               SUM(CASE WHEN ENR_START_DATE IS NULL THEN 1 ELSE 0 END),
               'ENR_L3_N'
        FROM {{ current_schema }}.ENROLLMENT
        HAVING SUM(CASE WHEN ENR_START_DATE IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'ENROLLMENT', 'ENR_BASIS',
               SUM(CASE WHEN ENR_BASIS IS NULL OR ENR_BASIS IN ('NI','UN') THEN 1 ELSE 0 END),
               'ENR_L3_BASIS'
        FROM {{ current_schema }}.ENROLLMENT
        HAVING SUM(CASE WHEN ENR_BASIS IS NULL OR ENR_BASIS IN ('NI','UN') THEN 1 ELSE 0 END) > 0

        UNION ALL
        -- ENCOUNTER required fields
        SELECT 'ENCOUNTER', 'ENCOUNTERID',
               SUM(CASE WHEN ENCOUNTERID IS NULL THEN 1 ELSE 0 END),
               'ENC_L3_N'
        FROM {{ current_schema }}.ENCOUNTER
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN ENCOUNTERID IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'ENCOUNTER', 'PATID',
               SUM(CASE WHEN PATID IS NULL THEN 1 ELSE 0 END),
               'ENC_L3_N'
        FROM {{ current_schema }}.ENCOUNTER
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN PATID IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'ENCOUNTER', 'ENC_TYPE',
               SUM(CASE WHEN ENC_TYPE IS NULL OR ENC_TYPE IN ('NI','UN') THEN 1 ELSE 0 END),
               'ENC_L3_ENCTYPE'
        FROM {{ current_schema }}.ENCOUNTER
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN ENC_TYPE IS NULL OR ENC_TYPE IN ('NI','UN') THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'ENCOUNTER', 'ADMIT_DATE',
               SUM(CASE WHEN ADMIT_DATE IS NULL THEN 1 ELSE 0 END),
               'ENC_L3_N'
        FROM {{ current_schema }}.ENCOUNTER
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN ADMIT_DATE IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        -- DIAGNOSIS required fields
        SELECT 'DIAGNOSIS', 'DIAGNOSISID',
               SUM(CASE WHEN DIAGNOSISID IS NULL THEN 1 ELSE 0 END),
               'DIA_L3_N'
        FROM {{ current_schema }}.DIAGNOSIS
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN DIAGNOSISID IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'DIAGNOSIS', 'PATID',
               SUM(CASE WHEN PATID IS NULL THEN 1 ELSE 0 END),
               'DIA_L3_N'
        FROM {{ current_schema }}.DIAGNOSIS
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN PATID IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'DIAGNOSIS', 'DX',
               SUM(CASE WHEN DX IS NULL THEN 1 ELSE 0 END),
               'DIA_L3_N'
        FROM {{ current_schema }}.DIAGNOSIS
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN DX IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'DIAGNOSIS', 'DX_TYPE',
               SUM(CASE WHEN DX_TYPE IS NULL OR DX_TYPE IN ('NI','UN') THEN 1 ELSE 0 END),
               'DIA_L3_DXTYPE'
        FROM {{ current_schema }}.DIAGNOSIS
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN DX_TYPE IS NULL OR DX_TYPE IN ('NI','UN') THEN 1 ELSE 0 END) > 0

        UNION ALL
        -- PROCEDURES required fields
        SELECT 'PROCEDURES', 'PROCEDURESID',
               SUM(CASE WHEN PROCEDURESID IS NULL THEN 1 ELSE 0 END),
               'PRO_L3_N'
        FROM {{ current_schema }}.PROCEDURES
        WHERE PX_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN PROCEDURESID IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'PROCEDURES', 'PX',
               SUM(CASE WHEN PX IS NULL THEN 1 ELSE 0 END),
               'PRO_L3_N'
        FROM {{ current_schema }}.PROCEDURES
        WHERE PX_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN PX IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'PROCEDURES', 'PX_TYPE',
               SUM(CASE WHEN PX_TYPE IS NULL OR PX_TYPE IN ('NI','UN') THEN 1 ELSE 0 END),
               'PRO_L3_PXTYPE'
        FROM {{ current_schema }}.PROCEDURES
        WHERE PX_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN PX_TYPE IS NULL OR PX_TYPE IN ('NI','UN') THEN 1 ELSE 0 END) > 0

        UNION ALL
        -- VITAL required fields
        SELECT 'VITAL', 'VITALID',
               SUM(CASE WHEN VITALID IS NULL THEN 1 ELSE 0 END),
               'VIT_L3_N'
        FROM {{ current_schema }}.VITAL
        WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN VITALID IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'VITAL', 'PATID',
               SUM(CASE WHEN PATID IS NULL THEN 1 ELSE 0 END),
               'VIT_L3_N'
        FROM {{ current_schema }}.VITAL
        WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN PATID IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'VITAL', 'MEASURE_DATE',
               SUM(CASE WHEN MEASURE_DATE IS NULL THEN 1 ELSE 0 END),
               'VIT_L3_N'
        FROM {{ current_schema }}.VITAL
        WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN MEASURE_DATE IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        -- LAB_RESULT_CM required fields
        SELECT 'LAB_RESULT_CM', 'LAB_RESULT_CM_ID',
               SUM(CASE WHEN LAB_RESULT_CM_ID IS NULL THEN 1 ELSE 0 END),
               'LAB_L3_N'
        FROM {{ current_schema }}.LAB_RESULT_CM
        WHERE RESULT_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN LAB_RESULT_CM_ID IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'LAB_RESULT_CM', 'PATID',
               SUM(CASE WHEN PATID IS NULL THEN 1 ELSE 0 END),
               'LAB_L3_N'
        FROM {{ current_schema }}.LAB_RESULT_CM
        WHERE RESULT_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN PATID IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        -- PRESCRIBING required fields
        SELECT 'PRESCRIBING', 'PRESCRIBINGID',
               SUM(CASE WHEN PRESCRIBINGID IS NULL THEN 1 ELSE 0 END),
               'PRES_L3_N'
        FROM {{ current_schema }}.PRESCRIBING
        WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN PRESCRIBINGID IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'PRESCRIBING', 'PATID',
               SUM(CASE WHEN PATID IS NULL THEN 1 ELSE 0 END),
               'PRES_L3_N'
        FROM {{ current_schema }}.PRESCRIBING
        WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN PATID IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        SELECT 'PRESCRIBING', 'RXNORM_CUI',
               SUM(CASE WHEN RXNORM_CUI IS NULL THEN 1 ELSE 0 END),
               'PRES_L3_N'
        FROM {{ current_schema }}.PRESCRIBING
        WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
        HAVING SUM(CASE WHEN RXNORM_CUI IS NULL THEN 1 ELSE 0 END) > 0

        UNION ALL
        -- HASH_TOKEN required fields
        SELECT 'HASH_TOKEN', 'PATID',
               SUM(CASE WHEN PATID IS NULL THEN 1 ELSE 0 END),
               'HTOK_L3_N'
        FROM {{ current_schema }}.HASH_TOKEN
        HAVING SUM(CASE WHEN PATID IS NULL THEN 1 ELSE 0 END) > 0
    ) sub
)
SELECT TABLE_NAME, FIELD_NAME, RECORDS_MISSING, SOURCE_TABLE
FROM violations
UNION ALL
SELECT 'All fields conform to specifications', '', '', ''
WHERE NOT EXISTS (SELECT 1 FROM violations)
ORDER BY TABLE_NAME, FIELD_NAME
