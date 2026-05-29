-- DC 4.03 | Table VC | Data Persistence | Investigative
-- More than a 5% decrease in the number of records or distinct codes for CPT/HCPCS, CVX, ICD10,
-- LOINC, NDC, or RXNORM codes between the previous and current DataMart refresh
-- Parameters: {{ current_schema }}, {{ last_schema }}, {{ start_date }}
WITH crt AS (
    SELECT 'DIAGNOSIS'    AS TABLE_NAME, '09' AS CODE, COUNT(*) AS CURRENT_RECORD, COUNT(DISTINCT DX)            AS CURRENT_DISTINCT FROM {{ current_schema }}.DIAGNOSIS    WHERE DX_TYPE       = '09' AND DX            IS NOT NULL AND ADMIT_DATE          >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'DIAGNOSIS',                  '10',          COUNT(*),                   COUNT(DISTINCT DX)            FROM {{ current_schema }}.DIAGNOSIS    WHERE DX_TYPE       = '10' AND DX            IS NOT NULL AND ADMIT_DATE          >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'PROCEDURES',                 '09',          COUNT(*),                   COUNT(DISTINCT PX)            FROM {{ current_schema }}.PROCEDURES   WHERE PX_TYPE       = '09' AND PX            IS NOT NULL AND ADMIT_DATE          >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'PROCEDURES',                 '10',          COUNT(*),                   COUNT(DISTINCT PX)            FROM {{ current_schema }}.PROCEDURES   WHERE PX_TYPE       = '10' AND PX            IS NOT NULL AND ADMIT_DATE          >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'PROCEDURES',                 'CH',          COUNT(*),                   COUNT(DISTINCT PX)            FROM {{ current_schema }}.PROCEDURES   WHERE PX_TYPE       = 'CH' AND PX            IS NOT NULL AND ADMIT_DATE          >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'PROCEDURES',                 'ND',          COUNT(*),                   COUNT(DISTINCT PX)            FROM {{ current_schema }}.PROCEDURES   WHERE PX_TYPE       = 'ND' AND PX            IS NOT NULL AND ADMIT_DATE          >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'DISPENSING',                 'ND',          COUNT(*),                   COUNT(DISTINCT NDC)           FROM {{ current_schema }}.DISPENSING   WHERE NDC           IS NOT NULL                              AND DISPENSE_DATE       >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'IMMUNIZATION',               'CH',          COUNT(*),                   COUNT(DISTINCT VX_CODE)       FROM {{ current_schema }}.IMMUNIZATION WHERE VX_CODE_TYPE  = 'CH' AND VX_CODE      IS NOT NULL AND VX_ADMIN_DATE       >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'IMMUNIZATION',               'CX',          COUNT(*),                   COUNT(DISTINCT VX_CODE)       FROM {{ current_schema }}.IMMUNIZATION WHERE VX_CODE_TYPE  = 'CX' AND VX_CODE      IS NOT NULL AND VX_ADMIN_DATE       >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'IMMUNIZATION',               'ND',          COUNT(*),                   COUNT(DISTINCT VX_CODE)       FROM {{ current_schema }}.IMMUNIZATION WHERE VX_CODE_TYPE  = 'ND' AND VX_CODE      IS NOT NULL AND VX_ADMIN_DATE       >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'IMMUNIZATION',               'RX',          COUNT(*),                   COUNT(DISTINCT VX_CODE)       FROM {{ current_schema }}.IMMUNIZATION WHERE VX_CODE_TYPE  = 'RX' AND VX_CODE      IS NOT NULL AND VX_ADMIN_DATE       >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'MED_ADMIN',                  'ND',          COUNT(*),                   COUNT(DISTINCT MEDADMIN_CODE) FROM {{ current_schema }}.MED_ADMIN   WHERE MEDADMIN_TYPE = 'ND' AND MEDADMIN_CODE IS NOT NULL AND MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'MED_ADMIN',                  'RX',          COUNT(*),                   COUNT(DISTINCT MEDADMIN_CODE) FROM {{ current_schema }}.MED_ADMIN   WHERE MEDADMIN_TYPE = 'RX' AND MEDADMIN_CODE IS NOT NULL AND MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'PRESCRIBING',                'RX',          COUNT(*),                   COUNT(DISTINCT RXNORM_CUI)    FROM {{ current_schema }}.PRESCRIBING  WHERE RXNORM_CUI    IS NOT NULL                              AND RX_ORDER_DATE       >= TO_DATE('{{ start_date }}')
),
old AS (
    SELECT 'DIAGNOSIS'    AS TABLE_NAME, '09' AS CODE, COUNT(*) AS PREVIOUS_RECORD, COUNT(DISTINCT DX)            AS PREVIOUS_DISTINCT FROM {{ last_schema }}.DIAGNOSIS    WHERE DX_TYPE       = '09' AND DX            IS NOT NULL AND ADMIT_DATE          >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'DIAGNOSIS',                  '10',          COUNT(*),                    COUNT(DISTINCT DX)            FROM {{ last_schema }}.DIAGNOSIS    WHERE DX_TYPE       = '10' AND DX            IS NOT NULL AND ADMIT_DATE          >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'PROCEDURES',                 '09',          COUNT(*),                    COUNT(DISTINCT PX)            FROM {{ last_schema }}.PROCEDURES   WHERE PX_TYPE       = '09' AND PX            IS NOT NULL AND ADMIT_DATE          >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'PROCEDURES',                 '10',          COUNT(*),                    COUNT(DISTINCT PX)            FROM {{ last_schema }}.PROCEDURES   WHERE PX_TYPE       = '10' AND PX            IS NOT NULL AND ADMIT_DATE          >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'PROCEDURES',                 'CH',          COUNT(*),                    COUNT(DISTINCT PX)            FROM {{ last_schema }}.PROCEDURES   WHERE PX_TYPE       = 'CH' AND PX            IS NOT NULL AND ADMIT_DATE          >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'PROCEDURES',                 'ND',          COUNT(*),                    COUNT(DISTINCT PX)            FROM {{ last_schema }}.PROCEDURES   WHERE PX_TYPE       = 'ND' AND PX            IS NOT NULL AND ADMIT_DATE          >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'DISPENSING',                 'ND',          COUNT(*),                    COUNT(DISTINCT NDC)           FROM {{ last_schema }}.DISPENSING   WHERE NDC           IS NOT NULL                              AND DISPENSE_DATE       >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'IMMUNIZATION',               'CH',          COUNT(*),                    COUNT(DISTINCT VX_CODE)       FROM {{ last_schema }}.IMMUNIZATION WHERE VX_CODE_TYPE  = 'CH' AND VX_CODE      IS NOT NULL AND VX_ADMIN_DATE       >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'IMMUNIZATION',               'CX',          COUNT(*),                    COUNT(DISTINCT VX_CODE)       FROM {{ last_schema }}.IMMUNIZATION WHERE VX_CODE_TYPE  = 'CX' AND VX_CODE      IS NOT NULL AND VX_ADMIN_DATE       >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'IMMUNIZATION',               'ND',          COUNT(*),                    COUNT(DISTINCT VX_CODE)       FROM {{ last_schema }}.IMMUNIZATION WHERE VX_CODE_TYPE  = 'ND' AND VX_CODE      IS NOT NULL AND VX_ADMIN_DATE       >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'IMMUNIZATION',               'RX',          COUNT(*),                    COUNT(DISTINCT VX_CODE)       FROM {{ last_schema }}.IMMUNIZATION WHERE VX_CODE_TYPE  = 'RX' AND VX_CODE      IS NOT NULL AND VX_ADMIN_DATE       >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'MED_ADMIN',                  'ND',          COUNT(*),                    COUNT(DISTINCT MEDADMIN_CODE) FROM {{ last_schema }}.MED_ADMIN   WHERE MEDADMIN_TYPE = 'ND' AND MEDADMIN_CODE IS NOT NULL AND MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'MED_ADMIN',                  'RX',          COUNT(*),                    COUNT(DISTINCT MEDADMIN_CODE) FROM {{ last_schema }}.MED_ADMIN   WHERE MEDADMIN_TYPE = 'RX' AND MEDADMIN_CODE IS NOT NULL AND MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}') UNION ALL
    SELECT 'PRESCRIBING',                'RX',          COUNT(*),                    COUNT(DISTINCT RXNORM_CUI)    FROM {{ last_schema }}.PRESCRIBING  WHERE RXNORM_CUI    IS NOT NULL                              AND RX_ORDER_DATE       >= TO_DATE('{{ start_date }}')
),
exceptions AS (
    SELECT COUNT(*) AS EXCEPTION_COUNT
    FROM crt JOIN old ON crt.TABLE_NAME = old.TABLE_NAME AND crt.CODE = old.CODE
    WHERE (old.PREVIOUS_RECORD > 0 AND ((crt.CURRENT_RECORD - old.PREVIOUS_RECORD) / old.PREVIOUS_RECORD::FLOAT) * 100 < -5)
       OR (old.PREVIOUS_DISTINCT > 0 AND ((crt.CURRENT_DISTINCT - old.PREVIOUS_DISTINCT) / old.PREVIOUS_DISTINCT::FLOAT) * 100 < -5)
)
SELECT
    '4.03'                                                                        AS CHECK_NUM,
    'More than 5% decrease in records or distinct codes for any code type'        AS DESCRIPTION,
    CASE WHEN EXCEPTION_COUNT > 0 THEN 'Fail' ELSE 'Pass' END                     AS STATUS
FROM exceptions
