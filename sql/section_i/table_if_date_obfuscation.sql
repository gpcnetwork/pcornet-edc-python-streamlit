-- Table IF. Date Obfuscation
-- Date management method codes from the HARVEST table. Supports DC 1.05.

WITH h AS (
    SELECT * FROM {{ current_schema }}.HARVEST LIMIT 1
),
mgmt_decode AS (
    SELECT '01' AS CODE, '01 - No imputation or obfuscation'   AS DESCRIPTION UNION ALL
    SELECT '02',         '02 - Imputation for incomplete dates'                UNION ALL
    SELECT '03',         '03 - Date obfuscation'                               UNION ALL
    SELECT '04',         '04 - Both imputation and obfuscation'                UNION ALL
    SELECT 'NI',         'NI - No information'                                 UNION ALL
    SELECT 'UN',         'UN - Unknown'                                        UNION ALL
    SELECT 'OT',         'OT - Other'
),
fields AS (
    SELECT 'BIRTH_DATE_MGMT'            AS FIELD, TO_VARCHAR(h.BIRTH_DATE_MGMT)            AS RAW_VALUE,  1 AS ROW_ORDER FROM h UNION ALL
    SELECT 'ENR_START_DATE_MGMT',                  TO_VARCHAR(h.ENR_START_DATE_MGMT),                     2 FROM h UNION ALL
    SELECT 'ENR_END_DATE_MGMT',                    TO_VARCHAR(h.ENR_END_DATE_MGMT),                       3 FROM h UNION ALL
    SELECT 'ADMIT_DATE_MGMT',                      TO_VARCHAR(h.ADMIT_DATE_MGMT),                         4 FROM h UNION ALL
    SELECT 'DISCHARGE_DATE_MGMT',                  TO_VARCHAR(h.DISCHARGE_DATE_MGMT),                     5 FROM h UNION ALL
    SELECT 'PX_DATE_MGMT',                         TO_VARCHAR(h.PX_DATE_MGMT),                            6 FROM h UNION ALL
    SELECT 'RX_ORDER_DATE_MGMT',                   TO_VARCHAR(h.RX_ORDER_DATE_MGMT),                      7 FROM h UNION ALL
    SELECT 'RX_START_DATE_MGMT',                   TO_VARCHAR(h.RX_START_DATE_MGMT),                      8 FROM h UNION ALL
    SELECT 'RX_END_DATE_MGMT',                     TO_VARCHAR(h.RX_END_DATE_MGMT),                        9 FROM h UNION ALL
    SELECT 'DISPENSE_DATE_MGMT',                   TO_VARCHAR(h.DISPENSE_DATE_MGMT),                     10 FROM h UNION ALL
    SELECT 'LAB_ORDER_DATE_MGMT',                  TO_VARCHAR(h.LAB_ORDER_DATE_MGMT),                    11 FROM h UNION ALL
    SELECT 'SPECIMEN_DATE_MGMT',                   TO_VARCHAR(h.SPECIMEN_DATE_MGMT),                     12 FROM h UNION ALL
    SELECT 'RESULT_DATE_MGMT',                     TO_VARCHAR(h.RESULT_DATE_MGMT),                       13 FROM h UNION ALL
    SELECT 'MEASURE_DATE_MGMT',                    TO_VARCHAR(h.MEASURE_DATE_MGMT),                      14 FROM h UNION ALL
    SELECT 'ONSET_DATE_MGMT',                      TO_VARCHAR(h.ONSET_DATE_MGMT),                        15 FROM h UNION ALL
    SELECT 'REPORT_DATE_MGMT',                     TO_VARCHAR(h.REPORT_DATE_MGMT),                       16 FROM h UNION ALL
    SELECT 'RESOLVE_DATE_MGMT',                    TO_VARCHAR(h.RESOLVE_DATE_MGMT),                      17 FROM h UNION ALL
    SELECT 'PRO_DATE_MGMT',                        TO_VARCHAR(h.PRO_DATE_MGMT),                          18 FROM h UNION ALL
    SELECT 'DEATH_DATE_MGMT',                      TO_VARCHAR(h.DEATH_DATE_MGMT),                        19 FROM h UNION ALL
    SELECT 'MEDADMIN_START_DATE_MGMT',             TO_VARCHAR(h.MEDADMIN_START_DATE_MGMT),               20 FROM h UNION ALL
    SELECT 'MEDADMIN_STOP_DATE_MGMT',              TO_VARCHAR(h.MEDADMIN_STOP_DATE_MGMT),                21 FROM h UNION ALL
    SELECT 'OBSCLIN_START_DATE_MGMT',              TO_VARCHAR(h.OBSCLIN_START_DATE_MGMT),                22 FROM h UNION ALL
    SELECT 'OBSCLIN_STOP_DATE_MGMT',               TO_VARCHAR(h.OBSCLIN_STOP_DATE_MGMT),                 23 FROM h UNION ALL
    SELECT 'OBSGEN_START_DATE_MGMT',               TO_VARCHAR(h.OBSGEN_START_DATE_MGMT),                 24 FROM h UNION ALL
    SELECT 'OBSGEN_STOP_DATE_MGMT',                TO_VARCHAR(h.OBSGEN_STOP_DATE_MGMT),                  25 FROM h UNION ALL
    SELECT 'DX_DATE_MGMT',                         TO_VARCHAR(h.DX_DATE_MGMT),                           26 FROM h UNION ALL
    SELECT 'ADDRESS_PERIOD_START_MGMT',            TO_VARCHAR(h.ADDRESS_PERIOD_START_MGMT),              27 FROM h UNION ALL
    SELECT 'ADDRESS_PERIOD_END_MGMT',              TO_VARCHAR(h.ADDRESS_PERIOD_END_MGMT),                28 FROM h UNION ALL
    SELECT 'VX_RECORD_DATE_MGMT',                  TO_VARCHAR(h.VX_RECORD_DATE_MGMT),                    29 FROM h UNION ALL
    SELECT 'VX_ADMIN_DATE_MGMT',                   TO_VARCHAR(h.VX_ADMIN_DATE_MGMT),                     30 FROM h UNION ALL
    SELECT 'VX_EXP_DATE_MGMT',                     TO_VARCHAR(h.VX_EXP_DATE_MGMT),                       31 FROM h
)
SELECT
    'HARVEST'                                                      AS "Table",
    f.FIELD                                                        AS "Field",
    COALESCE(d.DESCRIPTION, COALESCE(f.RAW_VALUE, 'Missing'))     AS "Date_MGMT"
FROM fields f
LEFT JOIN mgmt_decode d ON d.CODE = f.RAW_VALUE
ORDER BY f.ROW_ORDER
