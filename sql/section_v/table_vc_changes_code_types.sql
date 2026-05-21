-- Table VC. Changes in Selected Code Types
-- This table shows changes in key DataMart attributes between the most recent approved DataMart refresh and the current DataMart refresh and supports Data Check
-- 4.03 (more than a 5% decrease in the number of records or distinct codes for CPT/HCPCS, CVX, ICD10, NDC, LOINC or RXCUI codes). The data check is not
-- applied to ICD9 codes because these codes will decrease between refreshes because of the 10 year lookback. Data check exceptions are highlighted in blue and should
-- be investigated and explained in the ETL ADD.

WITH crt AS (
    SELECT 'DIAGNOSIS'   AS TABLE_NAME, '09' AS CODE, COUNT(*) AS CURRENT_RECORD, COUNT(DISTINCT DX)           AS CURRENT_DISTINCT_CODES FROM {{ current_schema }}.DIAGNOSIS    WHERE DX_TYPE       = '09' AND DX          IS NOT NULL {% if cutoff_date %}AND ADMIT_DATE          >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'DIAGNOSIS',              '10', COUNT(*), COUNT(DISTINCT DX)           FROM {{ current_schema }}.DIAGNOSIS    WHERE DX_TYPE       = '10' AND DX          IS NOT NULL {% if cutoff_date %}AND ADMIT_DATE          >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'PROCEDURES', '09', COUNT(*), COUNT(DISTINCT PX)           FROM {{ current_schema }}.PROCEDURES   WHERE PX_TYPE       = '09' AND PX          IS NOT NULL {% if cutoff_date %}AND ADMIT_DATE          >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'PROCEDURES', '10', COUNT(*), COUNT(DISTINCT PX)           FROM {{ current_schema }}.PROCEDURES   WHERE PX_TYPE       = '10' AND PX          IS NOT NULL {% if cutoff_date %}AND ADMIT_DATE          >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'PROCEDURES', 'CH', COUNT(*), COUNT(DISTINCT PX)           FROM {{ current_schema }}.PROCEDURES   WHERE PX_TYPE       = 'CH' AND PX          IS NOT NULL {% if cutoff_date %}AND ADMIT_DATE          >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'PROCEDURES', 'ND', COUNT(*), COUNT(DISTINCT PX)           FROM {{ current_schema }}.PROCEDURES   WHERE PX_TYPE       = 'ND' AND PX          IS NOT NULL {% if cutoff_date %}AND ADMIT_DATE          >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'DISPENSING',  'ND', COUNT(*), COUNT(DISTINCT NDC)          FROM {{ current_schema }}.DISPENSING   WHERE NDC          IS NOT NULL                         {% if cutoff_date %}AND DISPENSE_DATE       >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'IMMUNIZATION','CH', COUNT(*), COUNT(DISTINCT VX_CODE)      FROM {{ current_schema }}.IMMUNIZATION WHERE VX_CODE_TYPE  = 'CH' AND VX_CODE     IS NOT NULL {% if cutoff_date %}AND VX_ADMIN_DATE       >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'IMMUNIZATION','CX', COUNT(*), COUNT(DISTINCT VX_CODE)      FROM {{ current_schema }}.IMMUNIZATION WHERE VX_CODE_TYPE  = 'CX' AND VX_CODE     IS NOT NULL {% if cutoff_date %}AND VX_ADMIN_DATE       >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'IMMUNIZATION','ND', COUNT(*), COUNT(DISTINCT VX_CODE)      FROM {{ current_schema }}.IMMUNIZATION WHERE VX_CODE_TYPE  = 'ND' AND VX_CODE     IS NOT NULL {% if cutoff_date %}AND VX_ADMIN_DATE       >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'IMMUNIZATION','RX', COUNT(*), COUNT(DISTINCT VX_CODE)      FROM {{ current_schema }}.IMMUNIZATION WHERE VX_CODE_TYPE  = 'RX' AND VX_CODE     IS NOT NULL {% if cutoff_date %}AND VX_ADMIN_DATE       >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'MED_ADMIN',   'ND', COUNT(*), COUNT(DISTINCT MEDADMIN_CODE) FROM {{ current_schema }}.MED_ADMIN   WHERE MEDADMIN_TYPE = 'ND' AND MEDADMIN_CODE IS NOT NULL {% if cutoff_date %}AND MEDADMIN_START_DATE >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'MED_ADMIN',   'RX', COUNT(*), COUNT(DISTINCT MEDADMIN_CODE) FROM {{ current_schema }}.MED_ADMIN   WHERE MEDADMIN_TYPE = 'RX' AND MEDADMIN_CODE IS NOT NULL {% if cutoff_date %}AND MEDADMIN_START_DATE >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'PRESCRIBING', 'RX', COUNT(*), COUNT(DISTINCT RXNORM_CUI)   FROM {{ current_schema }}.PRESCRIBING  WHERE RXNORM_CUI   IS NOT NULL                         {% if cutoff_date %}AND RX_ORDER_DATE       >= '{{ cutoff_date }}'{% endif %}
),
old AS (
    SELECT 'DIAGNOSIS'   AS TABLE_NAME, '09' AS CODE, COUNT(*) AS PREVIOUS_RECORD, COUNT(DISTINCT DX)           AS PREVIOUS_DISTINCT_CODES FROM {{ last_schema }}.DIAGNOSIS    WHERE DX_TYPE       = '09' AND DX          IS NOT NULL {% if cutoff_date %}AND ADMIT_DATE          >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'DIAGNOSIS',              '10', COUNT(*), COUNT(DISTINCT DX)           FROM {{ last_schema }}.DIAGNOSIS    WHERE DX_TYPE       = '10' AND DX          IS NOT NULL {% if cutoff_date %}AND ADMIT_DATE          >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'PROCEDURES', '09', COUNT(*), COUNT(DISTINCT PX)           FROM {{ last_schema }}.PROCEDURES   WHERE PX_TYPE       = '09' AND PX          IS NOT NULL {% if cutoff_date %}AND ADMIT_DATE          >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'PROCEDURES', '10', COUNT(*), COUNT(DISTINCT PX)           FROM {{ last_schema }}.PROCEDURES   WHERE PX_TYPE       = '10' AND PX          IS NOT NULL {% if cutoff_date %}AND ADMIT_DATE          >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'PROCEDURES', 'CH', COUNT(*), COUNT(DISTINCT PX)           FROM {{ last_schema }}.PROCEDURES   WHERE PX_TYPE       = 'CH' AND PX          IS NOT NULL {% if cutoff_date %}AND ADMIT_DATE          >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'PROCEDURES', 'ND', COUNT(*), COUNT(DISTINCT PX)           FROM {{ last_schema }}.PROCEDURES   WHERE PX_TYPE       = 'ND' AND PX          IS NOT NULL {% if cutoff_date %}AND ADMIT_DATE          >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'DISPENSING',  'ND', COUNT(*), COUNT(DISTINCT NDC)          FROM {{ last_schema }}.DISPENSING   WHERE NDC          IS NOT NULL                         {% if cutoff_date %}AND DISPENSE_DATE       >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'IMMUNIZATION','CH', COUNT(*), COUNT(DISTINCT VX_CODE)      FROM {{ last_schema }}.IMMUNIZATION WHERE VX_CODE_TYPE  = 'CH' AND VX_CODE     IS NOT NULL {% if cutoff_date %}AND VX_ADMIN_DATE       >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'IMMUNIZATION','CX', COUNT(*), COUNT(DISTINCT VX_CODE)      FROM {{ last_schema }}.IMMUNIZATION WHERE VX_CODE_TYPE  = 'CX' AND VX_CODE     IS NOT NULL {% if cutoff_date %}AND VX_ADMIN_DATE       >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'IMMUNIZATION','ND', COUNT(*), COUNT(DISTINCT VX_CODE)      FROM {{ last_schema }}.IMMUNIZATION WHERE VX_CODE_TYPE  = 'ND' AND VX_CODE     IS NOT NULL {% if cutoff_date %}AND VX_ADMIN_DATE       >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'IMMUNIZATION','RX', COUNT(*), COUNT(DISTINCT VX_CODE)      FROM {{ last_schema }}.IMMUNIZATION WHERE VX_CODE_TYPE  = 'RX' AND VX_CODE     IS NOT NULL {% if cutoff_date %}AND VX_ADMIN_DATE       >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'MED_ADMIN',   'ND', COUNT(*), COUNT(DISTINCT MEDADMIN_CODE) FROM {{ last_schema }}.MED_ADMIN   WHERE MEDADMIN_TYPE = 'ND' AND MEDADMIN_CODE IS NOT NULL {% if cutoff_date %}AND MEDADMIN_START_DATE >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'MED_ADMIN',   'RX', COUNT(*), COUNT(DISTINCT MEDADMIN_CODE) FROM {{ last_schema }}.MED_ADMIN   WHERE MEDADMIN_TYPE = 'RX' AND MEDADMIN_CODE IS NOT NULL {% if cutoff_date %}AND MEDADMIN_START_DATE >= '{{ cutoff_date }}'{% endif %} UNION
    SELECT 'PRESCRIBING', 'RX', COUNT(*), COUNT(DISTINCT RXNORM_CUI)   FROM {{ last_schema }}.PRESCRIBING  WHERE RXNORM_CUI   IS NOT NULL                         {% if cutoff_date %}AND RX_ORDER_DATE       >= '{{ cutoff_date }}'{% endif %}
),
detail AS (
    SELECT
        crt.TABLE_NAME,
        crt.CODE,
        old.PREVIOUS_RECORD                                                                                               AS PREV_RECORDS,
        crt.CURRENT_RECORD                                                                                                AS CURR_RECORDS,
        CASE WHEN COALESCE(old.PREVIOUS_RECORD, 0) = 0 THEN NULL
             ELSE ROUND(((crt.CURRENT_RECORD - old.PREVIOUS_RECORD) / old.PREVIOUS_RECORD::FLOAT) * 100, 1)
        END                                                                                                               AS PCT_CHANGE_RECORDS,
        old.PREVIOUS_DISTINCT_CODES                                                                                       AS PREV_CODES,
        crt.CURRENT_DISTINCT_CODES                                                                                        AS CURR_CODES,
        CASE WHEN COALESCE(old.PREVIOUS_DISTINCT_CODES, 0) = 0 THEN NULL
             ELSE ROUND(((crt.CURRENT_DISTINCT_CODES - old.PREVIOUS_DISTINCT_CODES) / old.PREVIOUS_DISTINCT_CODES::FLOAT) * 100, 1)
        END                                                                                                               AS PCT_CHANGE_CODES,
        CASE crt.TABLE_NAME || '|' || crt.CODE
            WHEN 'DIAGNOSIS|09'    THEN 11
            WHEN 'DIAGNOSIS|10'    THEN 12
            WHEN 'PROCEDURES|09'   THEN 21
            WHEN 'PROCEDURES|10'   THEN 22
            WHEN 'PROCEDURES|CH'   THEN 23
            WHEN 'PROCEDURES|ND'   THEN 24
            WHEN 'DISPENSING|ND'   THEN 31
            WHEN 'IMMUNIZATION|CH' THEN 41
            WHEN 'IMMUNIZATION|CX' THEN 42
            WHEN 'IMMUNIZATION|ND' THEN 43
            WHEN 'IMMUNIZATION|RX' THEN 44
            WHEN 'MED_ADMIN|ND'    THEN 51
            WHEN 'MED_ADMIN|RX'    THEN 52
            WHEN 'PRESCRIBING|RX'  THEN 61
            ELSE 99
        END AS ROW_ORDER
    FROM crt JOIN old ON crt.TABLE_NAME = old.TABLE_NAME AND crt.CODE = old.CODE
)
SELECT
    CASE WHEN CODE = '' THEN TABLE_NAME ELSE CODE END  AS LABEL,
    COALESCE(TO_VARCHAR(PREV_RECORDS), '')              AS "RECORDS__Previous Refresh",
    COALESCE(TO_VARCHAR(CURR_RECORDS), '')              AS "RECORDS__Current Refresh",
    PCT_CHANGE_RECORDS                                  AS "RECORDS__PCT_CHANGE",
    COALESCE(TO_VARCHAR(PREV_CODES), '')                AS "DISTINCT CODES__Previous Refresh",
    COALESCE(TO_VARCHAR(CURR_CODES), '')                AS "DISTINCT CODES__Current Refresh",
    PCT_CHANGE_CODES                                    AS "DISTINCT CODES__PCT_CHANGE"
FROM (
    SELECT TABLE_NAME, CODE, PREV_RECORDS, CURR_RECORDS, PCT_CHANGE_RECORDS, PREV_CODES, CURR_CODES, PCT_CHANGE_CODES, ROW_ORDER
    FROM detail
    UNION ALL
    SELECT 'DIAGNOSIS',    '', NULL, NULL, NULL, NULL, NULL, NULL, 10
    UNION ALL
    SELECT 'PROCEDURES',   '', NULL, NULL, NULL, NULL, NULL, NULL, 20
    UNION ALL
    SELECT 'DISPENSING',   '', NULL, NULL, NULL, NULL, NULL, NULL, 30
    UNION ALL
    SELECT 'IMMUNIZATION', '', NULL, NULL, NULL, NULL, NULL, NULL, 40
    UNION ALL
    SELECT 'MED_ADMIN',    '', NULL, NULL, NULL, NULL, NULL, NULL, 50
    UNION ALL
    SELECT 'PRESCRIBING',  '', NULL, NULL, NULL, NULL, NULL, NULL, 60
) combined
ORDER BY ROW_ORDER
