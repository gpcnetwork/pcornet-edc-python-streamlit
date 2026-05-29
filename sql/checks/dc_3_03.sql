-- DC 3.03 | Table IVC / Table IVD | Data Completeness | Investigative
-- More than 10% of records have missing or unknown values for required fields including:
-- DISCHARGE_DISPOSITION (IP/EI), DISPENSE_SUP, DX_SOURCE, SEX, code fields, code type fields
-- (DX_TYPE, PX_TYPE, etc.), date fields (BIRTH_DATE, DISCHARGE_DATE, etc.), ENCOUNTERID fields,
-- and provenance fields (DX_ORIGIN, RX_SOURCE, VITAL_SOURCE, etc.)
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH sex_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(SEX IN ('NI','UN','OT') OR SEX IS NULL) AS BAD
    FROM {{ current_schema }}.DEMOGRAPHIC
),
birth_date_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(BIRTH_DATE IS NULL) AS BAD
    FROM {{ current_schema }}.DEMOGRAPHIC
),
dx_type_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(DX_TYPE IN ('NI','UN','OT') OR DX_TYPE IS NULL) AS BAD
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
dx_origin_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(DX_ORIGIN IN ('NI','UN','OT') OR DX_ORIGIN IS NULL) AS BAD
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
dx_source_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(DX_SOURCE IN ('NI','UN','OT') OR DX_SOURCE IS NULL) AS BAD
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
px_type_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(PX_TYPE IN ('NI','UN','OT') OR PX_TYPE IS NULL) AS BAD
    FROM {{ current_schema }}.PROCEDURES
    WHERE PX_DATE >= TO_DATE('{{ start_date }}')
),
px_source_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(PX_SOURCE IN ('NI','UN','OT') OR PX_SOURCE IS NULL) AS BAD
    FROM {{ current_schema }}.PROCEDURES
    WHERE PX_DATE >= TO_DATE('{{ start_date }}')
),
discharge_disp_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(DISCHARGE_DISPOSITION IN ('NI','UN','OT') OR DISCHARGE_DISPOSITION IS NULL) AS BAD
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ENC_TYPE IN ('IP','EI')
      AND ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
discharge_date_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(DISCHARGE_DATE IS NULL) AS BAD
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ENC_TYPE IN ('IP','EI')
      AND ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
dispense_sup_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(DISPENSE_SUP IS NULL) AS BAD
    FROM {{ current_schema }}.DISPENSING
    WHERE DISPENSE_DATE >= TO_DATE('{{ start_date }}')
),
vital_source_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(VITAL_SOURCE IN ('NI','UN','OT') OR VITAL_SOURCE IS NULL) AS BAD
    FROM {{ current_schema }}.VITAL
    WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}')
),
rx_source_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(RX_SOURCE IN ('NI','UN','OT') OR RX_SOURCE IS NULL) AS BAD
    FROM {{ current_schema }}.PRESCRIBING
    WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
),
lab_source_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(LAB_RESULT_SOURCE IN ('NI','UN','OT') OR LAB_RESULT_SOURCE IS NULL) AS BAD
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE >= TO_DATE('{{ start_date }}')
),
medadmin_type_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(MEDADMIN_TYPE IN ('NI','UN','OT') OR MEDADMIN_TYPE IS NULL) AS BAD
    FROM {{ current_schema }}.MED_ADMIN
    WHERE MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}')
),
obsclin_type_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(OBSCLIN_TYPE IN ('NI','UN','OT') OR OBSCLIN_TYPE IS NULL) AS BAD
    FROM {{ current_schema }}.OBS_CLIN
    WHERE OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}')
),
obsgen_type_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(OBSGEN_TYPE IN ('NI','UN','OT') OR OBSGEN_TYPE IS NULL) AS BAD
    FROM {{ current_schema }}.OBS_GEN
    WHERE OBSGEN_START_DATE >= TO_DATE('{{ start_date }}')
),
condition_type_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(CONDITION_TYPE IN ('NI','UN','OT') OR CONDITION_TYPE IS NULL) AS BAD
    FROM {{ current_schema }}.CONDITION
    WHERE REPORT_DATE >= TO_DATE('{{ start_date }}')
),
imm_code_type_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(VX_CODE_TYPE IN ('NI','UN','OT') OR VX_CODE_TYPE IS NULL) AS BAD
    FROM {{ current_schema }}.IMMUNIZATION
    WHERE VX_ADMIN_DATE >= TO_DATE('{{ start_date }}')
),
all_checks AS (
    SELECT TOTAL, BAD FROM sex_check          UNION ALL
    SELECT TOTAL, BAD FROM birth_date_check   UNION ALL
    SELECT TOTAL, BAD FROM dx_type_check      UNION ALL
    SELECT TOTAL, BAD FROM dx_origin_check    UNION ALL
    SELECT TOTAL, BAD FROM dx_source_check    UNION ALL
    SELECT TOTAL, BAD FROM px_type_check      UNION ALL
    SELECT TOTAL, BAD FROM px_source_check    UNION ALL
    SELECT TOTAL, BAD FROM discharge_disp_check UNION ALL
    SELECT TOTAL, BAD FROM discharge_date_check UNION ALL
    SELECT TOTAL, BAD FROM dispense_sup_check UNION ALL
    SELECT TOTAL, BAD FROM vital_source_check UNION ALL
    SELECT TOTAL, BAD FROM rx_source_check    UNION ALL
    SELECT TOTAL, BAD FROM lab_source_check   UNION ALL
    SELECT TOTAL, BAD FROM medadmin_type_check UNION ALL
    SELECT TOTAL, BAD FROM obsclin_type_check  UNION ALL
    SELECT TOTAL, BAD FROM obsgen_type_check   UNION ALL
    SELECT TOTAL, BAD FROM condition_type_check UNION ALL
    SELECT TOTAL, BAD FROM imm_code_type_check
),
summary AS (SELECT MAX(ROUND(100.0 * BAD / NULLIF(TOTAL, 0), 2)) AS MAX_PCT FROM all_checks)
SELECT
    '3.03'                                                                AS CHECK_NUM,
    'More than 10% of records have missing or unknown values for required fields' AS DESCRIPTION,
    CASE WHEN MAX_PCT > 10 THEN 'Fail' ELSE 'Pass' END                    AS STATUS
FROM summary
