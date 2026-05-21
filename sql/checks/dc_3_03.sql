-- DC 3.03: More than 10% of records have missing or unknown values for the
-- following fields: DISCHARGE_DISPOSITION (IP/EI encounters only),
-- DISPENSE_SUP, DX_SOURCE, SEX, code fields
-- [DEATH_CAUSE_CODE, MEDADMIN_CODE, OBSCLIN_CODE,
-- OBSGEN_CODE], code type fields [DX_TYPE, CONDITION_TYPE,
-- MEDADMIN_TYPE, OBSCLIN_TYPE, OBSGEN_TYPE, PX_TYPE,
-- VX_CODE_TYPE], date fields [BIRTH_DATE, DISCHARGE_DATE
-- (IP/EI encounters only), EXT_RECORD_DATE, RX_ORDER_DATE,
-- PX_DATE, VX_RECORD_DATE], ENCOUNTERID fields in selected
-- tables [DIAGNOSIS, LAB_RESULT_CM, PRESCRIBING,
-- MED_ADMIN, OBS_CLIN, PROCEDURES, and VITAL], and provenance
-- fields [CONDITION_SOURCE, DEATH_CAUSE_SOURCE,
-- DEATH_SOURCE, DISPENSE_SOURCE, DX_ORIGIN,
-- EXTMED_SOURCE, EXT_BASIS, MEDADMIN_SOURCE,
-- LAB_RESULT_SOURCE, PX_SOURCE, RX_SOURCE,
-- VITAL_SOURCE, VX_SOURCE]
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH medadmin_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(MEDADMIN_TYPE IN ('NI','UN','OT') OR MEDADMIN_TYPE IS NULL) AS BAD
    FROM {{ current_schema }}.MED_ADMIN WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND MEDADMIN_START_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
),
obsclin_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(OBSCLIN_TYPE IN ('NI','UN','OT') OR OBSCLIN_TYPE IS NULL) AS BAD
    FROM {{ current_schema }}.OBS_CLIN WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND OBSCLIN_START_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
),
obsgen_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(OBSGEN_TYPE IN ('NI','UN','OT') OR OBSGEN_TYPE IS NULL) AS BAD
    FROM {{ current_schema }}.OBS_GEN WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND OBSGEN_START_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
),
condition_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(CONDITION_TYPE IN ('NI','UN','OT') OR CONDITION_TYPE IS NULL) AS BAD
    FROM {{ current_schema }}.CONDITION WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND REPORT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
),
imm_check AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(VX_CODE_TYPE IN ('NI','UN','OT') OR VX_CODE_TYPE IS NULL) AS BAD
    FROM {{ current_schema }}.IMMUNIZATION WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND VX_ADMIN_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
),
all_checks AS (
    SELECT TOTAL, BAD FROM medadmin_check UNION ALL
    SELECT TOTAL, BAD FROM obsclin_check UNION ALL
    SELECT TOTAL, BAD FROM obsgen_check UNION ALL
    SELECT TOTAL, BAD FROM condition_check UNION ALL
    SELECT TOTAL, BAD FROM imm_check
),
summary AS (SELECT MAX(ROUND(100.0 * BAD / NULLIF(TOTAL, 0), 2)) AS MAX_PCT FROM all_checks)
SELECT
    '3.03'                                                      AS CHECK_NUM,
    '> 10% missing or unknown values in optional table fields'  AS DESCRIPTION,
    CASE WHEN MAX_PCT > 10 THEN 'Fail' ELSE 'Pass' END          AS STATUS
FROM summary
