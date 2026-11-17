-- DC 2.01 | Table IIIA | Data Plausibility | Investigative
-- More than 5% of records have future dates. Future dates occur after the maximum refresh
-- date in the HARVEST table (= {{ end_date }} at runtime).
--
-- SAS source of truth (v7.03):
--   data_curation_base.sas XTBL_L3_DATES: per date field, FUTURE_DT_N = records with the
--     field > refresh_max; denominator = non-missing values of that field in the loaded table.
--   data_curation_tables.sas table loads: domain tables are restricted to
--     (load_date >= lookback_dt OR load_date IS NULL) with NO upper bound (lines 1069-1839);
--     DEMOGRAPHIC, DEATH, LDS_ADDRESS_HISTORY, PAT_RELATIONSHIP are NOT restricted.
--     (The old SQL's closed [start,end] window excluded the very future dates it must count.)
--   edc_report.sas 5700-5834 (%print_iiia): field list below; exception (row=2.01) when any
--     field's future% > 5.00 (printed at 2 decimals).
-- Parameters: {{ current_schema }}, {{ start_date }}, {{ end_date }}
WITH per_field AS (
    SELECT 'DEMOGRAPHIC' AS TBL, 'BIRTH_DATE' AS FLD, COUNT_IF(BIRTH_DATE > TO_DATE('{{ end_date }}')) AS FUT, COUNT(BIRTH_DATE) AS NONMISS
    FROM {{ current_schema }}.DEMOGRAPHIC
    UNION ALL
    SELECT 'ENROLLMENT', 'ENR_START_DATE', COUNT_IF(ENR_START_DATE > TO_DATE('{{ end_date }}')), COUNT(ENR_START_DATE)
    FROM {{ current_schema }}.ENROLLMENT
    WHERE ENR_START_DATE >= TO_DATE('{{ start_date }}') OR ENR_END_DATE >= TO_DATE('{{ start_date }}')
       OR (ENR_START_DATE IS NULL AND ENR_END_DATE IS NULL)
    UNION ALL
    SELECT 'DEATH', 'DEATH_DATE', COUNT_IF(DEATH_DATE > TO_DATE('{{ end_date }}')), COUNT(DEATH_DATE)
    FROM {{ current_schema }}.DEATH
    UNION ALL
    SELECT 'ENCOUNTER', 'ADMIT_DATE', COUNT_IF(ADMIT_DATE > TO_DATE('{{ end_date }}')), COUNT(ADMIT_DATE)
    FROM {{ current_schema }}.ENCOUNTER WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL
    UNION ALL
    SELECT 'ENCOUNTER', 'DISCHARGE_DATE', COUNT_IF(DISCHARGE_DATE > TO_DATE('{{ end_date }}')), COUNT(DISCHARGE_DATE)
    FROM {{ current_schema }}.ENCOUNTER WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL
    UNION ALL
    SELECT 'DIAGNOSIS', 'ADMIT_DATE', COUNT_IF(ADMIT_DATE > TO_DATE('{{ end_date }}')), COUNT(ADMIT_DATE)
    FROM {{ current_schema }}.DIAGNOSIS WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL
    UNION ALL
    SELECT 'DIAGNOSIS', 'DX_DATE', COUNT_IF(DX_DATE > TO_DATE('{{ end_date }}')), COUNT(DX_DATE)
    FROM {{ current_schema }}.DIAGNOSIS WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL
    UNION ALL
    SELECT 'PROCEDURES', 'ADMIT_DATE', COUNT_IF(ADMIT_DATE > TO_DATE('{{ end_date }}')), COUNT(ADMIT_DATE)
    FROM {{ current_schema }}.PROCEDURES WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL
    UNION ALL
    SELECT 'PROCEDURES', 'PX_DATE', COUNT_IF(PX_DATE > TO_DATE('{{ end_date }}')), COUNT(PX_DATE)
    FROM {{ current_schema }}.PROCEDURES WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL
    UNION ALL
    SELECT 'VITAL', 'MEASURE_DATE', COUNT_IF(MEASURE_DATE > TO_DATE('{{ end_date }}')), COUNT(MEASURE_DATE)
    FROM {{ current_schema }}.VITAL WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}') OR MEASURE_DATE IS NULL
    UNION ALL
    SELECT 'PRESCRIBING', 'RX_ORDER_DATE', COUNT_IF(RX_ORDER_DATE > TO_DATE('{{ end_date }}')), COUNT(RX_ORDER_DATE)
    FROM {{ current_schema }}.PRESCRIBING WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}') OR RX_ORDER_DATE IS NULL
    UNION ALL
    SELECT 'PRESCRIBING', 'RX_START_DATE', COUNT_IF(RX_START_DATE > TO_DATE('{{ end_date }}')), COUNT(RX_START_DATE)
    FROM {{ current_schema }}.PRESCRIBING WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}') OR RX_ORDER_DATE IS NULL
    UNION ALL
    SELECT 'DISPENSING', 'DISPENSE_DATE', COUNT_IF(DISPENSE_DATE > TO_DATE('{{ end_date }}')), COUNT(DISPENSE_DATE)
    FROM {{ current_schema }}.DISPENSING WHERE DISPENSE_DATE >= TO_DATE('{{ start_date }}') OR DISPENSE_DATE IS NULL
    UNION ALL
    SELECT 'LAB_RESULT_CM', 'LAB_ORDER_DATE', COUNT_IF(LAB_ORDER_DATE > TO_DATE('{{ end_date }}')), COUNT(LAB_ORDER_DATE)
    FROM {{ current_schema }}.LAB_RESULT_CM WHERE RESULT_DATE >= TO_DATE('{{ start_date }}') OR RESULT_DATE IS NULL
    UNION ALL
    SELECT 'LAB_RESULT_CM', 'SPECIMEN_DATE', COUNT_IF(SPECIMEN_DATE > TO_DATE('{{ end_date }}')), COUNT(SPECIMEN_DATE)
    FROM {{ current_schema }}.LAB_RESULT_CM WHERE RESULT_DATE >= TO_DATE('{{ start_date }}') OR RESULT_DATE IS NULL
    UNION ALL
    SELECT 'LAB_RESULT_CM', 'RESULT_DATE', COUNT_IF(RESULT_DATE > TO_DATE('{{ end_date }}')), COUNT(RESULT_DATE)
    FROM {{ current_schema }}.LAB_RESULT_CM WHERE RESULT_DATE >= TO_DATE('{{ start_date }}') OR RESULT_DATE IS NULL
    UNION ALL
    SELECT 'CONDITION', 'REPORT_DATE', COUNT_IF(REPORT_DATE > TO_DATE('{{ end_date }}')), COUNT(REPORT_DATE)
    FROM {{ current_schema }}.CONDITION WHERE REPORT_DATE >= TO_DATE('{{ start_date }}') OR REPORT_DATE IS NULL
    UNION ALL
    SELECT 'CONDITION', 'RESOLVE_DATE', COUNT_IF(RESOLVE_DATE > TO_DATE('{{ end_date }}')), COUNT(RESOLVE_DATE)
    FROM {{ current_schema }}.CONDITION WHERE REPORT_DATE >= TO_DATE('{{ start_date }}') OR REPORT_DATE IS NULL
    UNION ALL
    SELECT 'CONDITION', 'ONSET_DATE', COUNT_IF(ONSET_DATE > TO_DATE('{{ end_date }}')), COUNT(ONSET_DATE)
    FROM {{ current_schema }}.CONDITION WHERE REPORT_DATE >= TO_DATE('{{ start_date }}') OR REPORT_DATE IS NULL
    UNION ALL
    SELECT 'PRO_CM', 'PRO_DATE', COUNT_IF(PRO_DATE > TO_DATE('{{ end_date }}')), COUNT(PRO_DATE)
    FROM {{ current_schema }}.PRO_CM WHERE PRO_DATE >= TO_DATE('{{ start_date }}') OR PRO_DATE IS NULL
    UNION ALL
    SELECT 'MED_ADMIN', 'MEDADMIN_START_DATE', COUNT_IF(MEDADMIN_START_DATE > TO_DATE('{{ end_date }}')), COUNT(MEDADMIN_START_DATE)
    FROM {{ current_schema }}.MED_ADMIN WHERE MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}') OR MEDADMIN_START_DATE IS NULL
    UNION ALL
    SELECT 'MED_ADMIN', 'MEDADMIN_STOP_DATE', COUNT_IF(MEDADMIN_STOP_DATE > TO_DATE('{{ end_date }}')), COUNT(MEDADMIN_STOP_DATE)
    FROM {{ current_schema }}.MED_ADMIN WHERE MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}') OR MEDADMIN_START_DATE IS NULL
    UNION ALL
    SELECT 'OBS_CLIN', 'OBSCLIN_START_DATE', COUNT_IF(OBSCLIN_START_DATE > TO_DATE('{{ end_date }}')), COUNT(OBSCLIN_START_DATE)
    FROM {{ current_schema }}.OBS_CLIN WHERE OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}') OR OBSCLIN_START_DATE IS NULL
    UNION ALL
    SELECT 'OBS_CLIN', 'OBSCLIN_STOP_DATE', COUNT_IF(OBSCLIN_STOP_DATE > TO_DATE('{{ end_date }}')), COUNT(OBSCLIN_STOP_DATE)
    FROM {{ current_schema }}.OBS_CLIN WHERE OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}') OR OBSCLIN_START_DATE IS NULL
    UNION ALL
    SELECT 'OBS_GEN', 'OBSGEN_START_DATE', COUNT_IF(OBSGEN_START_DATE > TO_DATE('{{ end_date }}')), COUNT(OBSGEN_START_DATE)
    FROM {{ current_schema }}.OBS_GEN WHERE OBSGEN_START_DATE >= TO_DATE('{{ start_date }}') OR OBSGEN_START_DATE IS NULL
    UNION ALL
    SELECT 'OBS_GEN', 'OBSGEN_STOP_DATE', COUNT_IF(OBSGEN_STOP_DATE > TO_DATE('{{ end_date }}')), COUNT(OBSGEN_STOP_DATE)
    FROM {{ current_schema }}.OBS_GEN WHERE OBSGEN_START_DATE >= TO_DATE('{{ start_date }}') OR OBSGEN_START_DATE IS NULL
    UNION ALL
    SELECT 'LDS_ADDRESS_HISTORY', 'ADDRESS_PERIOD_START', COUNT_IF(ADDRESS_PERIOD_START > TO_DATE('{{ end_date }}')), COUNT(ADDRESS_PERIOD_START)
    FROM {{ current_schema }}.LDS_ADDRESS_HISTORY
    UNION ALL
    SELECT 'LDS_ADDRESS_HISTORY', 'ADDRESS_PERIOD_END', COUNT_IF(ADDRESS_PERIOD_END > TO_DATE('{{ end_date }}')), COUNT(ADDRESS_PERIOD_END)
    FROM {{ current_schema }}.LDS_ADDRESS_HISTORY
    UNION ALL
    SELECT 'IMMUNIZATION', 'VX_RECORD_DATE', COUNT_IF(VX_RECORD_DATE > TO_DATE('{{ end_date }}')), COUNT(VX_RECORD_DATE)
    FROM {{ current_schema }}.IMMUNIZATION WHERE VX_RECORD_DATE >= TO_DATE('{{ start_date }}') OR VX_RECORD_DATE IS NULL
    UNION ALL
    SELECT 'IMMUNIZATION', 'VX_ADMIN_DATE', COUNT_IF(VX_ADMIN_DATE > TO_DATE('{{ end_date }}')), COUNT(VX_ADMIN_DATE)
    FROM {{ current_schema }}.IMMUNIZATION WHERE VX_RECORD_DATE >= TO_DATE('{{ start_date }}') OR VX_RECORD_DATE IS NULL
    UNION ALL
    SELECT 'EXTERNAL_MEDS', 'EXT_RECORD_DATE', COUNT_IF(EXT_RECORD_DATE > TO_DATE('{{ end_date }}')), COUNT(EXT_RECORD_DATE)
    FROM {{ current_schema }}.EXTERNAL_MEDS WHERE EXT_RECORD_DATE >= TO_DATE('{{ start_date }}') OR EXT_RECORD_DATE IS NULL
    UNION ALL
    SELECT 'EXTERNAL_MEDS', 'EXT_PAT_START_DATE', COUNT_IF(EXT_PAT_START_DATE > TO_DATE('{{ end_date }}')), COUNT(EXT_PAT_START_DATE)
    FROM {{ current_schema }}.EXTERNAL_MEDS WHERE EXT_RECORD_DATE >= TO_DATE('{{ start_date }}') OR EXT_RECORD_DATE IS NULL
    UNION ALL
    SELECT 'EXTERNAL_MEDS', 'EXT_PAT_END_DATE', COUNT_IF(EXT_PAT_END_DATE > TO_DATE('{{ end_date }}')), COUNT(EXT_PAT_END_DATE)
    FROM {{ current_schema }}.EXTERNAL_MEDS WHERE EXT_RECORD_DATE >= TO_DATE('{{ start_date }}') OR EXT_RECORD_DATE IS NULL
    UNION ALL
    SELECT 'EXTERNAL_MEDS', 'EXT_END_DATE', COUNT_IF(EXT_END_DATE > TO_DATE('{{ end_date }}')), COUNT(EXT_END_DATE)
    FROM {{ current_schema }}.EXTERNAL_MEDS WHERE EXT_RECORD_DATE >= TO_DATE('{{ start_date }}') OR EXT_RECORD_DATE IS NULL
    UNION ALL
    SELECT 'PAT_RELATIONSHIP', 'RELATIONSHIP_START', COUNT_IF(RELATIONSHIP_START > TO_DATE('{{ end_date }}')), COUNT(RELATIONSHIP_START)
    FROM {{ current_schema }}.PAT_RELATIONSHIP
    UNION ALL
    SELECT 'PAT_RELATIONSHIP', 'RELATIONSHIP_END', COUNT_IF(RELATIONSHIP_END > TO_DATE('{{ end_date }}')), COUNT(RELATIONSHIP_END)
    FROM {{ current_schema }}.PAT_RELATIONSHIP
),
scored AS (
    SELECT TBL, FLD, FUT, NONMISS,
           ROUND(100.0 * FUT / NULLIF(NONMISS, 0), 2) AS FUTURE_PCT
    FROM per_field
),
summary AS (
    SELECT
        '2.01'                                                                 AS CHECK_NUM,
        'More than 5% of records have future dates (after the HARVEST maximum refresh date)' AS DESCRIPTION,
        CASE WHEN MAX(FUTURE_PCT) > 5.00 THEN 'Fail' ELSE 'Pass' END           AS STATUS,
        'SUMMARY'                                                              AS ROW_TYPE,
        ''                                                                     AS EXC_TABLE,
        ''                                                                     AS EXC_FIELD,
        'max future pct = ' || TO_VARCHAR(COALESCE(MAX(FUTURE_PCT), 0))        AS EXC_DETAIL,
        NULL                                                                   AS EXC_COUNT,
        0                                                                      AS ROW_ORDER
    FROM scored
),
details AS (
    SELECT
        '2.01', 'More than 5% of records have future dates', 'Fail', 'DETAIL',
        TBL, FLD,
        'future dates ' || TO_VARCHAR(FUTURE_PCT) || '% (' || TO_VARCHAR(FUT) || ' of ' || TO_VARCHAR(NONMISS) || ')',
        FUT,
        ROW_NUMBER() OVER (ORDER BY FUTURE_PCT DESC)
    FROM scored
    WHERE FUTURE_PCT > 5.00
)
SELECT CHECK_NUM, DESCRIPTION, STATUS, ROW_TYPE, EXC_TABLE, EXC_FIELD, EXC_DETAIL, EXC_COUNT
FROM (
    SELECT * FROM summary
    UNION ALL
    SELECT * FROM details
)
ORDER BY ROW_ORDER
