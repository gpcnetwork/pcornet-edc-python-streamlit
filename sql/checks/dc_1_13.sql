-- DC 1.13 | Table IIF | Data Model Conformance | Required
-- More than 5% of CPT/HCPCS, CVX, ICD, NDC, LOINC or RXNORM codes do not conform to the expected length
-- or content based on terminology-specific heuristics. Fails if the maximum bad-percentage across any
-- (table, code_type) pair exceeds 5.
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH
condition_rates AS (
    SELECT code_type, COUNT(*) AS TOTAL, SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) AS BAD
    FROM (
        SELECT code_type,
               CASE
                   WHEN code_type = '09' THEN
                       (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*')
                                  AND LEFT(code_clean, 1) NOT IN ('E', 'V') THEN 1 ELSE 0 END
                        + CASE WHEN code_length NOT IN (3, 4, 5) THEN 1 ELSE 0 END
                        + CASE WHEN LEFT(code_clean, 3) = '000' THEN 1 ELSE 0 END
                        + CASE WHEN NOT REGEXP_LIKE(code_clean, '.*[0-9].*') THEN 1 ELSE 0 END)
                   WHEN code_type = '10' THEN
                       (CASE WHEN NOT REGEXP_LIKE(code_clean, '^[A-Z].*') THEN 1 ELSE 0 END
                        + CASE WHEN code_length NOT IN (3, 4, 5, 6, 7) THEN 1 ELSE 0 END
                        + CASE WHEN LEFT(code_clean, 3) IN ('000', '999') THEN 1 ELSE 0 END
                        + CASE WHEN NOT REGEXP_LIKE(code_clean, '.*[0-9].*') THEN 1 ELSE 0 END)
                   ELSE 0
               END AS has_error
        FROM (
            SELECT CONDITION_TYPE                                         AS code_type,
                   UPPER(REGEXP_REPLACE(CONDITION, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(CONDITION, '[., ]', ''))) AS code_length
            FROM {{ current_schema }}.CONDITION
            WHERE CONDITION IS NOT NULL
              AND CONDITION_TYPE IN ('09', '10')
              AND REPORT_DATE >= TO_DATE('{{ start_date }}')
        )
    )
    GROUP BY code_type
),
diagnosis_rates AS (
    SELECT code_type, COUNT(*) AS TOTAL, SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) AS BAD
    FROM (
        SELECT code_type,
               CASE
                   WHEN code_type = '09' THEN
                       (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*')
                                  AND LEFT(code_clean, 1) NOT IN ('E', 'V') THEN 1 ELSE 0 END
                        + CASE WHEN code_length NOT IN (3, 4, 5) THEN 1 ELSE 0 END
                        + CASE WHEN LEFT(code_clean, 3) = '000' THEN 1 ELSE 0 END
                        + CASE WHEN NOT REGEXP_LIKE(code_clean, '.*[0-9].*') THEN 1 ELSE 0 END)
                   WHEN code_type = '10' THEN
                       (CASE WHEN NOT REGEXP_LIKE(code_clean, '^[A-Z].*') THEN 1 ELSE 0 END
                        + CASE WHEN code_length NOT IN (3, 4, 5, 6, 7) THEN 1 ELSE 0 END
                        + CASE WHEN LEFT(code_clean, 3) IN ('000', '999') THEN 1 ELSE 0 END
                        + CASE WHEN NOT REGEXP_LIKE(code_clean, '.*[0-9].*') THEN 1 ELSE 0 END)
                   ELSE 0
               END AS has_error
        FROM (
            SELECT DX_TYPE                                         AS code_type,
                   UPPER(REGEXP_REPLACE(DX, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(DX, '[., ]', ''))) AS code_length
            FROM {{ current_schema }}.DIAGNOSIS
            WHERE DX IS NOT NULL
              AND DX_TYPE IN ('09', '10')
              AND ADMIT_DATE >= TO_DATE('{{ start_date }}')
        )
    )
    GROUP BY code_type
),
dispensing_rate AS (
    SELECT 'ND' AS code_type, COUNT(*) AS TOTAL, SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) AS BAD
    FROM (
        SELECT (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                + CASE WHEN code_length <> 11 THEN 1 ELSE 0 END
                + CASE WHEN code_clean IN ('00000000000', '99999999999') THEN 1 ELSE 0 END) AS has_error
        FROM (
            SELECT UPPER(REGEXP_REPLACE(NDC, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(NDC, '[., ]', ''))) AS code_length
            FROM {{ current_schema }}.DISPENSING
            WHERE NDC IS NOT NULL
              AND DISPENSE_DATE >= TO_DATE('{{ start_date }}')
        )
    )
),
external_meds_rate AS (
    SELECT 'RX' AS code_type, COUNT(*) AS TOTAL, SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) AS BAD
    FROM (
        SELECT (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                + CASE WHEN code_length < 2 OR code_length > 7 THEN 1 ELSE 0 END) AS has_error
        FROM (
            SELECT UPPER(REGEXP_REPLACE(RXNORM_CUI, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(RXNORM_CUI, '[., ]', ''))) AS code_length
            FROM {{ current_schema }}.EXTERNAL_MEDS
            WHERE RXNORM_CUI IS NOT NULL
              AND EXT_RECORD_DATE >= TO_DATE('{{ start_date }}')
        )
    )
),
immunization_rates AS (
    SELECT code_type, COUNT(*) AS TOTAL, SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) AS BAD
    FROM (
        SELECT code_type,
               CASE
                   WHEN code_type = 'RX' THEN
                       (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                        + CASE WHEN code_length < 2 OR code_length > 7 THEN 1 ELSE 0 END)
                   WHEN code_type = 'ND' THEN
                       (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                        + CASE WHEN code_length <> 11 THEN 1 ELSE 0 END
                        + CASE WHEN code_clean IN ('00000000000', '99999999999') THEN 1 ELSE 0 END)
                   WHEN code_type = 'CH' THEN
                       (CASE WHEN code_length < 5 THEN 1 ELSE 0 END
                        + CASE WHEN code_clean IN ('00000', '99999') THEN 1 ELSE 0 END
                        + CASE WHEN NOT REGEXP_LIKE(code_clean, '.*[0-9].*') THEN 1 ELSE 0 END)
                   WHEN code_type = 'CX' THEN
                       (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                        + CASE WHEN code_length NOT BETWEEN 2 AND 3 THEN 1 ELSE 0 END)
                   ELSE 0
               END AS has_error
        FROM (
            SELECT VX_CODE_TYPE AS code_type,
                   CASE WHEN VX_CODE_TYPE = 'CH'
                        THEN LEFT(UPPER(REGEXP_REPLACE(VX_CODE, '[., ]', '')), 5)
                        ELSE UPPER(REGEXP_REPLACE(VX_CODE, '[., ]', ''))
                   END AS code_clean,
                   LENGTH(
                       CASE WHEN VX_CODE_TYPE = 'CH'
                            THEN LEFT(UPPER(REGEXP_REPLACE(VX_CODE, '[., ]', '')), 5)
                            ELSE UPPER(REGEXP_REPLACE(VX_CODE, '[., ]', ''))
                       END
                   ) AS code_length
            FROM {{ current_schema }}.IMMUNIZATION
            WHERE VX_CODE IS NOT NULL
              AND VX_CODE_TYPE IN ('CX', 'ND', 'RX', 'CH')
              AND VX_ADMIN_DATE >= TO_DATE('{{ start_date }}')
        )
    )
    GROUP BY code_type
),
lab_history_rate AS (
    SELECT 'LC' AS code_type, COUNT(*) AS TOTAL, SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) AS BAD
    FROM (
        SELECT (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                + CASE WHEN code_length < 3 OR code_length > 8 THEN 1 ELSE 0 END
                + CASE WHEN SUBSTR(REVERSE(code_clean), 2, 1) <> '-' THEN 1 ELSE 0 END) AS has_error
        FROM (
            SELECT UPPER(REGEXP_REPLACE(LAB_LOINC, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(LAB_LOINC, '[., ]', ''))) AS code_length
            FROM {{ current_schema }}.LAB_HISTORY
            WHERE LAB_LOINC IS NOT NULL
        )
    )
),
lab_result_cm_rate AS (
    SELECT 'LC' AS code_type, COUNT(*) AS TOTAL, SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) AS BAD
    FROM (
        SELECT (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                + CASE WHEN code_length < 3 OR code_length > 8 THEN 1 ELSE 0 END
                + CASE WHEN SUBSTR(REVERSE(code_clean), 2, 1) <> '-' THEN 1 ELSE 0 END) AS has_error
        FROM (
            SELECT UPPER(REGEXP_REPLACE(LAB_LOINC, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(LAB_LOINC, '[., ]', ''))) AS code_length
            FROM {{ current_schema }}.LAB_RESULT_CM
            WHERE LAB_LOINC IS NOT NULL
              AND RESULT_DATE >= TO_DATE('{{ start_date }}')
        )
    )
),
med_admin_rates AS (
    SELECT code_type, COUNT(*) AS TOTAL, SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) AS BAD
    FROM (
        SELECT code_type,
               CASE
                   WHEN code_type = 'RX' THEN
                       (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                        + CASE WHEN code_length < 2 OR code_length > 7 THEN 1 ELSE 0 END)
                   WHEN code_type = 'ND' THEN
                       (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                        + CASE WHEN code_length <> 11 THEN 1 ELSE 0 END
                        + CASE WHEN code_clean IN ('00000000000', '99999999999') THEN 1 ELSE 0 END)
                   ELSE 0
               END AS has_error
        FROM (
            SELECT MEDADMIN_TYPE                                              AS code_type,
                   UPPER(REGEXP_REPLACE(MEDADMIN_CODE, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(MEDADMIN_CODE, '[., ]', ''))) AS code_length
            FROM {{ current_schema }}.MED_ADMIN
            WHERE MEDADMIN_CODE IS NOT NULL
              AND MEDADMIN_TYPE IN ('RX', 'ND')
              AND MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}')
        )
    )
    GROUP BY code_type
),
obs_clin_rate AS (
    SELECT 'LC' AS code_type, COUNT(*) AS TOTAL, SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) AS BAD
    FROM (
        SELECT (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                + CASE WHEN code_length < 3 OR code_length > 8 THEN 1 ELSE 0 END
                + CASE WHEN SUBSTR(REVERSE(code_clean), 2, 1) <> '-' THEN 1 ELSE 0 END) AS has_error
        FROM (
            SELECT UPPER(REGEXP_REPLACE(OBSCLIN_CODE, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(OBSCLIN_CODE, '[., ]', ''))) AS code_length
            FROM {{ current_schema }}.OBS_CLIN
            WHERE OBSCLIN_CODE IS NOT NULL
              AND OBSCLIN_TYPE = 'LC'
              AND OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}')
        )
    )
),
obs_gen_rates AS (
    SELECT code_type, COUNT(*) AS TOTAL, SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) AS BAD
    FROM (
        SELECT code_type,
               CASE
                   WHEN code_type = 'LC' THEN
                       (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                        + CASE WHEN code_length < 3 OR code_length > 8 THEN 1 ELSE 0 END
                        + CASE WHEN SUBSTR(REVERSE(code_clean), 2, 1) <> '-' THEN 1 ELSE 0 END)
                   WHEN code_type = 'RX' THEN
                       (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                        + CASE WHEN code_length < 2 OR code_length > 7 THEN 1 ELSE 0 END)
                   WHEN code_type = 'ND' THEN
                       (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                        + CASE WHEN code_length <> 11 THEN 1 ELSE 0 END
                        + CASE WHEN code_clean IN ('00000000000', '99999999999') THEN 1 ELSE 0 END)
                   WHEN code_type = '09DX' THEN
                       (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*')
                                  AND LEFT(code_clean, 1) NOT IN ('E', 'V') THEN 1 ELSE 0 END
                        + CASE WHEN code_length NOT IN (3, 4, 5) THEN 1 ELSE 0 END
                        + CASE WHEN LEFT(code_clean, 3) = '000' THEN 1 ELSE 0 END
                        + CASE WHEN NOT REGEXP_LIKE(code_clean, '.*[0-9].*') THEN 1 ELSE 0 END)
                   WHEN code_type = '10DX' THEN
                       (CASE WHEN REGEXP_LIKE(code_clean, '^[0-9].*') THEN 1 ELSE 0 END
                        + CASE WHEN code_length NOT IN (3, 4, 5, 6, 7) THEN 1 ELSE 0 END
                        + CASE WHEN LEFT(code_clean, 3) IN ('000', '999') THEN 1 ELSE 0 END
                        + CASE WHEN NOT REGEXP_LIKE(code_clean, '.*[0-9].*') THEN 1 ELSE 0 END)
                   WHEN code_type = '09PX' THEN
                       (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                        + CASE WHEN code_length NOT IN (3, 4) THEN 1 ELSE 0 END
                        + CASE WHEN code_clean = '0000' THEN 1 ELSE 0 END)
                   WHEN code_type = '10PX' THEN
                       (CASE WHEN code_length <> 7 THEN 1 ELSE 0 END
                        + CASE WHEN code_clean IN ('0000000', '9999999') THEN 1 ELSE 0 END)
                   WHEN code_type = 'CH' THEN
                       (CASE WHEN code_length < 5 THEN 1 ELSE 0 END
                        + CASE WHEN code_clean IN ('00000', '99999') THEN 1 ELSE 0 END
                        + CASE WHEN NOT REGEXP_LIKE(code_clean, '.*[0-9].*') THEN 1 ELSE 0 END)
                   ELSE 0
               END AS has_error
        FROM (
            SELECT OBSGEN_TYPE  AS code_type,
                   CASE WHEN OBSGEN_TYPE = 'CH'
                        THEN LEFT(UPPER(REGEXP_REPLACE(OBSGEN_CODE, '[., ]', '')), 5)
                        ELSE UPPER(REGEXP_REPLACE(OBSGEN_CODE, '[., ]', ''))
                   END AS code_clean,
                   LENGTH(
                       CASE WHEN OBSGEN_TYPE = 'CH'
                            THEN LEFT(UPPER(REGEXP_REPLACE(OBSGEN_CODE, '[., ]', '')), 5)
                            ELSE UPPER(REGEXP_REPLACE(OBSGEN_CODE, '[., ]', ''))
                       END
                   ) AS code_length
            FROM {{ current_schema }}.OBS_GEN
            WHERE OBSGEN_CODE IS NOT NULL
              AND OBSGEN_TYPE IN ('LC', 'ND', 'RX', '09DX', '09PX', '10DX', '10PX', 'CH')
              AND OBSGEN_START_DATE >= TO_DATE('{{ start_date }}')
        )
    )
    GROUP BY code_type
),
prescribing_rate AS (
    SELECT 'RX' AS code_type, COUNT(*) AS TOTAL, SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) AS BAD
    FROM (
        SELECT (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                + CASE WHEN code_length < 2 OR code_length > 7 THEN 1 ELSE 0 END) AS has_error
        FROM (
            SELECT UPPER(REGEXP_REPLACE(RXNORM_CUI::VARCHAR, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(RXNORM_CUI::VARCHAR, '[., ]', ''))) AS code_length
            FROM {{ current_schema }}.PRESCRIBING
            WHERE RXNORM_CUI IS NOT NULL
              AND RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
        )
    )
),
pro_cm_rate AS (
    SELECT 'LC' AS code_type, COUNT(*) AS TOTAL, SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) AS BAD
    FROM (
        SELECT (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                + CASE WHEN code_length < 3 OR code_length > 8 THEN 1 ELSE 0 END
                + CASE WHEN SUBSTR(REVERSE(code_clean), 2, 1) <> '-' THEN 1 ELSE 0 END) AS has_error
        FROM (
            SELECT UPPER(REGEXP_REPLACE(PRO_CODE, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(PRO_CODE, '[., ]', ''))) AS code_length
            FROM {{ current_schema }}.PRO_CM
            WHERE PRO_CODE IS NOT NULL
              AND UPPER(TRIM(PRO_TYPE)) = 'LC'
              AND PRO_DATE >= TO_DATE('{{ start_date }}')
        )
    )
),
procedures_rates AS (
    SELECT code_type, COUNT(*) AS TOTAL, SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) AS BAD
    FROM (
        SELECT code_type,
               CASE
                   WHEN code_type = 'CH' THEN
                       (CASE WHEN code_length < 5 THEN 1 ELSE 0 END
                        + CASE WHEN code_clean IN ('00000', '99999') THEN 1 ELSE 0 END
                        + CASE WHEN NOT REGEXP_LIKE(code_clean, '.*[0-9].*') THEN 1 ELSE 0 END)
                   WHEN code_type = '09' THEN
                       (CASE WHEN code_length NOT IN (3, 4) THEN 1 ELSE 0 END
                        + CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                        + CASE WHEN code_clean = '0000' THEN 1 ELSE 0 END)
                   WHEN code_type = '10' THEN
                       (CASE WHEN code_length <> 7 THEN 1 ELSE 0 END
                        + CASE WHEN code_clean IN ('0000000', '9999999') THEN 1 ELSE 0 END)
                   WHEN code_type = 'ND' THEN
                       (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                        + CASE WHEN code_length <> 11 THEN 1 ELSE 0 END
                        + CASE WHEN code_clean IN ('00000000000', '99999999999') THEN 1 ELSE 0 END)
                   ELSE 0
               END AS has_error
        FROM (
            SELECT PX_TYPE   AS code_type,
                   CASE WHEN PX_TYPE = 'CH'
                        THEN LEFT(UPPER(REGEXP_REPLACE(PX, '[., ]', '')), 5)
                        ELSE UPPER(REGEXP_REPLACE(PX, '[., ]', ''))
                   END AS code_clean,
                   LENGTH(
                       CASE WHEN PX_TYPE = 'CH'
                            THEN LEFT(UPPER(REGEXP_REPLACE(PX, '[., ]', '')), 5)
                            ELSE UPPER(REGEXP_REPLACE(PX, '[., ]', ''))
                       END
                   ) AS code_length
            FROM {{ current_schema }}.PROCEDURES
            WHERE PX IS NOT NULL
              AND PX_TYPE IN ('CH', '09', '10', 'ND')
              AND PX_DATE >= TO_DATE('{{ start_date }}')
        )
    )
    GROUP BY code_type
),
all_rates AS (
    SELECT code_type, TOTAL, BAD FROM condition_rates
    UNION ALL SELECT code_type, TOTAL, BAD FROM diagnosis_rates
    UNION ALL SELECT code_type, TOTAL, BAD FROM dispensing_rate
    UNION ALL SELECT code_type, TOTAL, BAD FROM external_meds_rate
    UNION ALL SELECT code_type, TOTAL, BAD FROM immunization_rates
    UNION ALL SELECT code_type, TOTAL, BAD FROM lab_history_rate
    UNION ALL SELECT code_type, TOTAL, BAD FROM lab_result_cm_rate
    UNION ALL SELECT code_type, TOTAL, BAD FROM med_admin_rates
    UNION ALL SELECT code_type, TOTAL, BAD FROM obs_clin_rate
    UNION ALL SELECT code_type, TOTAL, BAD FROM obs_gen_rates
    UNION ALL SELECT code_type, TOTAL, BAD FROM prescribing_rate
    UNION ALL SELECT code_type, TOTAL, BAD FROM pro_cm_rate
    UNION ALL SELECT code_type, TOTAL, BAD FROM procedures_rates
),
summary AS (
    SELECT MAX(ROUND(100.0 * BAD / NULLIF(TOTAL, 0), 2)) AS MAX_PCT_BAD FROM all_rates
)
SELECT
    '1.13'                                                                                                               AS CHECK_NUM,
    'More than 5% of CPT/HCPCS, CVX, ICD, NDC, LOINC or RXNORM codes do not conform to terminology-specific heuristics' AS DESCRIPTION,
    CASE WHEN COALESCE(MAX_PCT_BAD, 0) > 5 THEN 'Fail' ELSE 'Pass' END                                                   AS STATUS
FROM summary
