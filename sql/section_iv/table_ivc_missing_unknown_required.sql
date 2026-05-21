-- Table IVC. Missing or Unknown Values, Required Tables
-- Fields in DEMOGRAPHIC, DIAGNOSIS, ENCOUNTER, ENROLLMENT, and PROCEDURES with missing/unknown values.
-- Supports DC 3.03 (> 10% missing for required fields). Exceptions highlighted in blue.

SELECT TABLE_NAME, FIELD_NAME, ENC_TYPE_CONSTRAINT,
       TO_VARCHAR(NUMERATOR) AS NUMERATOR,
       TO_VARCHAR(DENOMINATOR) AS DENOMINATOR,
       TO_VARCHAR(ROUND(NUMERATOR * 100.0 / NULLIF(DENOMINATOR, 0), 1)) || '%' AS PCT,
       SOURCE_TABLE
FROM (
    SELECT 'DEMOGRAPHIC' AS TABLE_NAME, 'BIRTH_DATE' AS FIELD_NAME, '' AS ENC_TYPE_CONSTRAINT,
           SUM(CASE WHEN BIRTH_DATE IS NULL THEN 1 ELSE 0 END) AS NUMERATOR,
           COUNT(*) AS DENOMINATOR,
           'DEM_L3_N' AS SOURCE_TABLE, 1 AS ROW_ORDER
    FROM {{ current_schema }}.DEMOGRAPHIC

    UNION ALL
    SELECT 'DEMOGRAPHIC', 'SEX', '',
           SUM(CASE WHEN SEX IN ('NI','UN','OT') OR SEX IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'DEM_L3_SEXDIST', 2
    FROM {{ current_schema }}.DEMOGRAPHIC

    UNION ALL
    SELECT 'ENROLLMENT', 'ENR_BASIS', '',
           SUM(CASE WHEN ENR_BASIS IN ('NI','UN') OR ENR_BASIS IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'ENR_L3_BASIS', 3
    FROM {{ current_schema }}.ENROLLMENT
    WHERE ENR_START_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'ENCOUNTER', 'ENC_TYPE', '',
           SUM(CASE WHEN ENC_TYPE IN ('NI','UN','OT') OR ENC_TYPE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'ENC_L3_ENCTYPE', 4
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'ENCOUNTER', 'DISCHARGE_DATE', 'IP/EI only',
           SUM(CASE WHEN DISCHARGE_DATE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'ENC_L3_N', 5
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ENC_TYPE IN ('IP','EI') AND ADMIT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'ENCOUNTER', 'DISCHARGE_DISPOSITION', 'IP/EI only',
           SUM(CASE WHEN DISCHARGE_DISPOSITION IN ('NI','UN','OT') OR DISCHARGE_DISPOSITION IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'ENC_L3_DISSTAT', 6
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ENC_TYPE IN ('IP','EI') AND ADMIT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'ENCOUNTER', 'PROVIDERID', '',
           SUM(CASE WHEN PROVIDERID IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'ENC_L3_N', 7
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'DIAGNOSIS', 'DX_TYPE', '',
           SUM(CASE WHEN DX_TYPE IN ('NI','UN','OT') OR DX_TYPE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'DIA_L3_DXTYPE', 8
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'DIAGNOSIS', 'DX_SOURCE', '',
           SUM(CASE WHEN DX_SOURCE IN ('NI','UN','OT') OR DX_SOURCE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'DIA_L3_ORIGIN', 9
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'DIAGNOSIS', 'DX_ORIGIN', '',
           SUM(CASE WHEN DX_ORIGIN IN ('NI','UN','OT') OR DX_ORIGIN IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'DIA_L3_ORIGIN', 10
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'DIAGNOSIS', 'ENCOUNTERID', '',
           SUM(CASE WHEN ENCOUNTERID IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'DIA_L3_N', 11
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'PROCEDURES', 'PX_TYPE', '',
           SUM(CASE WHEN PX_TYPE IN ('NI','UN','OT') OR PX_TYPE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'PRO_L3_PXTYPE', 12
    FROM {{ current_schema }}.PROCEDURES
    WHERE PX_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'PROCEDURES', 'PX_DATE', '',
           SUM(CASE WHEN PX_DATE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'PRO_L3_N', 13
    FROM {{ current_schema }}.PROCEDURES
    WHERE PX_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'PROCEDURES', 'PX_SOURCE', '',
           SUM(CASE WHEN PX_SOURCE IN ('NI','UN','OT') OR PX_SOURCE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'PRO_L3_N', 14
    FROM {{ current_schema }}.PROCEDURES
    WHERE PX_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'PROCEDURES', 'ENCOUNTERID', '',
           SUM(CASE WHEN ENCOUNTERID IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'PRO_L3_N', 15
    FROM {{ current_schema }}.PROCEDURES
    WHERE PX_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'VITAL', 'ENCOUNTERID', '',
           SUM(CASE WHEN ENCOUNTERID IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'VIT_L3_N', 16
    FROM {{ current_schema }}.VITAL
    WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'VITAL', 'VITAL_SOURCE', '',
           SUM(CASE WHEN VITAL_SOURCE IN ('NI','UN','OT') OR VITAL_SOURCE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'VIT_L3_SOURCE', 17
    FROM {{ current_schema }}.VITAL
    WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'LAB_RESULT_CM', 'ENCOUNTERID', '',
           SUM(CASE WHEN ENCOUNTERID IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'LAB_L3_N', 18
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'PRESCRIBING', 'RXNORM_CUI', '',
           SUM(CASE WHEN RXNORM_CUI IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'PRES_L3_N', 19
    FROM {{ current_schema }}.PRESCRIBING
    WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'PRESCRIBING', 'RX_SOURCE', '',
           SUM(CASE WHEN RX_SOURCE IN ('NI','UN','OT') OR RX_SOURCE IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'PRES_L3_N', 20
    FROM {{ current_schema }}.PRESCRIBING
    WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'DISPENSING', 'DISPENSE_SUP', '',
           SUM(CASE WHEN DISPENSE_SUP IS NULL THEN 1 ELSE 0 END),
           COUNT(*), 'DISP_L3_SUP', 21
    FROM {{ current_schema }}.DISPENSING
    WHERE DISPENSE_DATE >= TO_DATE('{{ start_date }}')

) sub
ORDER BY ROW_ORDER
