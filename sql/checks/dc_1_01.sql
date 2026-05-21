-- DC 1.01: Required tables are present
-- Parameters: {{ db_name }}, {{ current_schema }}
WITH required AS (
    SELECT v.TABLE_NAME FROM (VALUES
        ('DEMOGRAPHIC'),('ENROLLMENT'),('ENCOUNTER'),('DIAGNOSIS'),('PROCEDURES'),
        ('VITAL'),('DISPENSING'),('LAB_RESULT_CM'),('CONDITION'),('PRO_CM'),
        ('PRESCRIBING'),('PCORNET_TRIAL'),('DEATH'),('DEATH_CAUSE'),('MED_ADMIN'),
        ('PROVIDER'),('OBS_GEN'),('OBS_CLIN'),('HASH_TOKEN'),('LDS_ADDRESS_HISTORY'),
        ('IMMUNIZATION'),('HARVEST'),('LAB_HISTORY'),('EXTERNAL_MEDS'),('PAT_RELATIONSHIP')
    ) v(TABLE_NAME)
),
existing AS (
    SELECT TABLE_NAME FROM {{ db_name }}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{{ current_schema }}'
)
SELECT
    '1.01'                                                        AS CHECK_NUM,
    'Required tables are present'                                    AS DESCRIPTION,
    CASE WHEN COUNT(*) = 0 THEN 'Pass' ELSE 'Fail' END              AS STATUS
FROM required r
WHERE r.TABLE_NAME NOT IN (SELECT TABLE_NAME FROM existing)
