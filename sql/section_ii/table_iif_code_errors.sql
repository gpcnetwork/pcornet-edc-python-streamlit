-- Table IIF. Potential Code Errors
-- Exceptions to DC 1.13 (more than 5% of ICD/CPT/HCPCS/CVX/LOINC/RXNORM/NDC codes do not conform to
-- terminology-specific heuristics) and DC 1.16 (lab results or clinical observations are recorded in
-- the wrong table based on the LOINC classtype).
-- DC 1.13 exceptions highlighted in red; DC 1.16 exceptions highlighted in blue.

WITH loinc_class AS (
    {% if loinc_ref_fqn %}
    SELECT DISTINCT TRIM(UPPER(LOINC_NUM)) AS LOINC_NUM,
                    TRIM(CLASSTYPE)        AS CLASSTYPE
    FROM {{ loinc_ref_fqn }}
    WHERE LOINC_NUM IS NOT NULL AND TRIM(LOINC_NUM) <> ''
    {% else %}
    SELECT NULL::VARCHAR AS LOINC_NUM, NULL::VARCHAR AS CLASSTYPE WHERE 1=0
    {% endif %}
),
condition_rows AS (
    SELECT 'CONDITION' AS "Table",
           code_type   AS "Code Type",
           TO_VARCHAR(COUNT(DISTINCT code))                                                                AS "Distinct Codes",
           TO_VARCHAR(COUNT(*))                                                                            AS "Records",
           TO_VARCHAR(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END))                                      AS "Records with Code Type Errors",
           TO_VARCHAR(ROUND(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)) || '%' AS "Percent of Records with Code Type Errors (Data Check 1.13)",
           '0'                                                                                             AS "Records in the Wrong Table",
           '0.00%'                                                                                         AS "Percentage of Records in the Wrong Table (Data Check 1.16)"
    FROM (
        SELECT code, code_type, code_clean, code_length,
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
            SELECT CONDITION                                              AS code,
                   CONDITION_TYPE                                         AS code_type,
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
diagnosis_rows AS (
    SELECT 'DIAGNOSIS' AS "Table",
           code_type   AS "Code Type",
           TO_VARCHAR(COUNT(DISTINCT code))                                                                AS "Distinct Codes",
           TO_VARCHAR(COUNT(*))                                                                            AS "Records",
           TO_VARCHAR(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END))                                      AS "Records with Code Type Errors",
           TO_VARCHAR(ROUND(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)) || '%' AS "Percent of Records with Code Type Errors (Data Check 1.13)",
           '0'                                                                                             AS "Records in the Wrong Table",
           '0.00%'                                                                                         AS "Percentage of Records in the Wrong Table (Data Check 1.16)"
    FROM (
        SELECT code, code_type, code_clean, code_length,
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
            SELECT DX                                              AS code,
                   DX_TYPE                                         AS code_type,
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
dispensing_rows AS (
    SELECT 'DISPENSING' AS "Table",
           'ND'         AS "Code Type",
           TO_VARCHAR(COUNT(DISTINCT code))                                                                AS "Distinct Codes",
           TO_VARCHAR(COUNT(*))                                                                            AS "Records",
           TO_VARCHAR(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END))                                      AS "Records with Code Type Errors",
           TO_VARCHAR(ROUND(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)) || '%' AS "Percent of Records with Code Type Errors (Data Check 1.13)",
           '0'                                                                                             AS "Records in the Wrong Table",
           '0.00%'                                                                                         AS "Percentage of Records in the Wrong Table (Data Check 1.16)"
    FROM (
        SELECT code, code_clean, code_length,
               (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                + CASE WHEN code_length <> 11 THEN 1 ELSE 0 END
                + CASE WHEN code_clean IN ('00000000000', '99999999999') THEN 1 ELSE 0 END) AS has_error
        FROM (
            SELECT NDC                                              AS code,
                   UPPER(REGEXP_REPLACE(NDC, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(NDC, '[., ]', ''))) AS code_length
            FROM {{ current_schema }}.DISPENSING
            WHERE NDC IS NOT NULL
              AND DISPENSE_DATE >= TO_DATE('{{ start_date }}')
        )
    )
),
external_meds_rows AS (
    SELECT 'EXTERNAL_MEDS' AS "Table",
           'RX'            AS "Code Type",
           TO_VARCHAR(COUNT(DISTINCT code))                                                                AS "Distinct Codes",
           TO_VARCHAR(COUNT(*))                                                                            AS "Records",
           TO_VARCHAR(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END))                                      AS "Records with Code Type Errors",
           TO_VARCHAR(ROUND(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)) || '%' AS "Percent of Records with Code Type Errors (Data Check 1.13)",
           '0'                                                                                             AS "Records in the Wrong Table",
           '0.00%'                                                                                         AS "Percentage of Records in the Wrong Table (Data Check 1.16)"
    FROM (
        SELECT code, code_clean, code_length,
               (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                + CASE WHEN code_length < 2 OR code_length > 7 THEN 1 ELSE 0 END) AS has_error
        FROM (
            SELECT RXNORM_CUI                                              AS code,
                   UPPER(REGEXP_REPLACE(RXNORM_CUI, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(RXNORM_CUI, '[., ]', ''))) AS code_length
            FROM {{ current_schema }}.EXTERNAL_MEDS
            WHERE RXNORM_CUI IS NOT NULL
              AND EXT_RECORD_DATE >= TO_DATE('{{ start_date }}')
        )
    )
),
immunization_rows AS (
    SELECT 'IMMUNIZATION' AS "Table",
           code_type      AS "Code Type",
           TO_VARCHAR(COUNT(DISTINCT code))                                                                AS "Distinct Codes",
           TO_VARCHAR(COUNT(*))                                                                            AS "Records",
           TO_VARCHAR(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END))                                      AS "Records with Code Type Errors",
           TO_VARCHAR(ROUND(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)) || '%' AS "Percent of Records with Code Type Errors (Data Check 1.13)",
           '0'                                                                                             AS "Records in the Wrong Table",
           '0.00%'                                                                                         AS "Percentage of Records in the Wrong Table (Data Check 1.16)"
    FROM (
        SELECT code, code_type, code_clean, code_length,
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
            SELECT VX_CODE      AS code,
                   VX_CODE_TYPE AS code_type,
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
lab_history_rows AS (
    SELECT 'LAB_HISTORY' AS "Table",
           'LC'          AS "Code Type",
           TO_VARCHAR(COUNT(DISTINCT code))                                                                AS "Distinct Codes",
           TO_VARCHAR(COUNT(*))                                                                            AS "Records",
           TO_VARCHAR(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END))                                      AS "Records with Code Type Errors",
           TO_VARCHAR(ROUND(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)) || '%' AS "Percent of Records with Code Type Errors (Data Check 1.13)",
           '0'                                                                                             AS "Records in the Wrong Table",
           '0.00%'                                                                                         AS "Percentage of Records in the Wrong Table (Data Check 1.16)"
    FROM (
        SELECT code, code_clean, code_length,
               (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                + CASE WHEN code_length < 3 OR code_length > 8 THEN 1 ELSE 0 END
                + CASE WHEN SUBSTR(REVERSE(code_clean), 2, 1) <> '-' THEN 1 ELSE 0 END) AS has_error
        FROM (
            SELECT LAB_LOINC                                              AS code,
                   UPPER(REGEXP_REPLACE(LAB_LOINC, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(LAB_LOINC, '[., ]', ''))) AS code_length
            FROM {{ current_schema }}.LAB_HISTORY
            WHERE LAB_LOINC IS NOT NULL
        )
    )
),
lab_result_cm_rows AS (
    SELECT 'LAB_RESULT_CM' AS "Table",
           'LC'            AS "Code Type",
           TO_VARCHAR(COUNT(DISTINCT code))                                                                AS "Distinct Codes",
           TO_VARCHAR(COUNT(*))                                                                            AS "Records",
           TO_VARCHAR(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END))                                      AS "Records with Code Type Errors",
           TO_VARCHAR(ROUND(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)) || '%' AS "Percent of Records with Code Type Errors (Data Check 1.13)",
           TO_VARCHAR(SUM(CASE WHEN wrong_table = 1 THEN 1 ELSE 0 END))                                    AS "Records in the Wrong Table",
           TO_VARCHAR(ROUND(SUM(CASE WHEN wrong_table = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)) || '%' AS "Percentage of Records in the Wrong Table (Data Check 1.16)"
    FROM (
        SELECT code, code_clean, code_length, classtype,
               (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                + CASE WHEN code_length < 3 OR code_length > 8 THEN 1 ELSE 0 END
                + CASE WHEN SUBSTR(REVERSE(code_clean), 2, 1) <> '-' THEN 1 ELSE 0 END) AS has_error,
               CASE WHEN classtype IS NOT NULL AND classtype <> '1' THEN 1 ELSE 0 END AS wrong_table
        FROM (
            SELECT l.LAB_LOINC                                              AS code,
                   UPPER(REGEXP_REPLACE(l.LAB_LOINC, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(l.LAB_LOINC, '[., ]', ''))) AS code_length,
                   lc.CLASSTYPE                                             AS classtype
            FROM {{ current_schema }}.LAB_RESULT_CM l
            LEFT JOIN loinc_class lc ON TRIM(UPPER(l.LAB_LOINC)) = lc.LOINC_NUM
            WHERE l.LAB_LOINC IS NOT NULL
              AND l.RESULT_DATE >= TO_DATE('{{ start_date }}')
        )
    )
),
med_admin_rows AS (
    SELECT 'MED_ADMIN' AS "Table",
           code_type   AS "Code Type",
           TO_VARCHAR(COUNT(DISTINCT code))                                                                AS "Distinct Codes",
           TO_VARCHAR(COUNT(*))                                                                            AS "Records",
           TO_VARCHAR(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END))                                      AS "Records with Code Type Errors",
           TO_VARCHAR(ROUND(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)) || '%' AS "Percent of Records with Code Type Errors (Data Check 1.13)",
           '0'                                                                                             AS "Records in the Wrong Table",
           '0.00%'                                                                                         AS "Percentage of Records in the Wrong Table (Data Check 1.16)"
    FROM (
        SELECT code, code_type, code_clean, code_length,
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
            SELECT MEDADMIN_CODE                                              AS code,
                   MEDADMIN_TYPE                                              AS code_type,
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
obs_clin_rows AS (
    SELECT 'OBS_CLIN' AS "Table",
           'LC'       AS "Code Type",
           TO_VARCHAR(COUNT(DISTINCT code))                                                                AS "Distinct Codes",
           TO_VARCHAR(COUNT(*))                                                                            AS "Records",
           TO_VARCHAR(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END))                                      AS "Records with Code Type Errors",
           TO_VARCHAR(ROUND(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)) || '%' AS "Percent of Records with Code Type Errors (Data Check 1.13)",
           TO_VARCHAR(SUM(CASE WHEN wrong_table = 1 THEN 1 ELSE 0 END))                                    AS "Records in the Wrong Table",
           TO_VARCHAR(ROUND(SUM(CASE WHEN wrong_table = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)) || '%' AS "Percentage of Records in the Wrong Table (Data Check 1.16)"
    FROM (
        SELECT code, code_clean, code_length, classtype,
               (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                + CASE WHEN code_length < 3 OR code_length > 8 THEN 1 ELSE 0 END
                + CASE WHEN SUBSTR(REVERSE(code_clean), 2, 1) <> '-' THEN 1 ELSE 0 END) AS has_error,
               CASE WHEN classtype = '1' THEN 1 ELSE 0 END AS wrong_table
        FROM (
            SELECT o.OBSCLIN_CODE                                              AS code,
                   UPPER(REGEXP_REPLACE(o.OBSCLIN_CODE, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(o.OBSCLIN_CODE, '[., ]', ''))) AS code_length,
                   lc.CLASSTYPE                                                AS classtype
            FROM {{ current_schema }}.OBS_CLIN o
            LEFT JOIN loinc_class lc ON TRIM(UPPER(o.OBSCLIN_CODE)) = lc.LOINC_NUM
            WHERE o.OBSCLIN_CODE IS NOT NULL
              AND o.OBSCLIN_TYPE = 'LC'
              AND o.OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}')
        )
    )
),
obs_gen_rows AS (
    SELECT 'OBS_GEN' AS "Table",
           code_type AS "Code Type",
           TO_VARCHAR(COUNT(DISTINCT code))                                                                AS "Distinct Codes",
           TO_VARCHAR(COUNT(*))                                                                            AS "Records",
           TO_VARCHAR(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END))                                      AS "Records with Code Type Errors",
           TO_VARCHAR(ROUND(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)) || '%' AS "Percent of Records with Code Type Errors (Data Check 1.13)",
           '0'                                                                                             AS "Records in the Wrong Table",
           '0.00%'                                                                                         AS "Percentage of Records in the Wrong Table (Data Check 1.16)"
    FROM (
        SELECT code, code_type, code_clean, code_length,
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
            SELECT OBSGEN_CODE  AS code,
                   OBSGEN_TYPE  AS code_type,
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
prescribing_rows AS (
    SELECT 'PRESCRIBING' AS "Table",
           'RX'          AS "Code Type",
           TO_VARCHAR(COUNT(DISTINCT code))                                                                AS "Distinct Codes",
           TO_VARCHAR(COUNT(*))                                                                            AS "Records",
           TO_VARCHAR(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END))                                      AS "Records with Code Type Errors",
           TO_VARCHAR(ROUND(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)) || '%' AS "Percent of Records with Code Type Errors (Data Check 1.13)",
           '0'                                                                                             AS "Records in the Wrong Table",
           '0.00%'                                                                                         AS "Percentage of Records in the Wrong Table (Data Check 1.16)"
    FROM (
        SELECT code, code_clean, code_length,
               (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                + CASE WHEN code_length < 2 OR code_length > 7 THEN 1 ELSE 0 END) AS has_error
        FROM (
            SELECT RXNORM_CUI::VARCHAR                                              AS code,
                   UPPER(REGEXP_REPLACE(RXNORM_CUI::VARCHAR, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(RXNORM_CUI::VARCHAR, '[., ]', ''))) AS code_length
            FROM {{ current_schema }}.PRESCRIBING
            WHERE RXNORM_CUI IS NOT NULL
              AND RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
        )
    )
),
pro_cm_rows AS (
    SELECT 'PRO_CM' AS "Table",
           'LC'     AS "Code Type",
           TO_VARCHAR(COUNT(DISTINCT code))                                                                AS "Distinct Codes",
           TO_VARCHAR(COUNT(*))                                                                            AS "Records",
           TO_VARCHAR(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END))                                      AS "Records with Code Type Errors",
           TO_VARCHAR(ROUND(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)) || '%' AS "Percent of Records with Code Type Errors (Data Check 1.13)",
           '0'                                                                                             AS "Records in the Wrong Table",
           '0.00%'                                                                                         AS "Percentage of Records in the Wrong Table (Data Check 1.16)"
    FROM (
        SELECT code, code_clean, code_length,
               (CASE WHEN REGEXP_LIKE(code_clean, '.*[A-Z].*') THEN 1 ELSE 0 END
                + CASE WHEN code_length < 3 OR code_length > 8 THEN 1 ELSE 0 END
                + CASE WHEN SUBSTR(REVERSE(code_clean), 2, 1) <> '-' THEN 1 ELSE 0 END) AS has_error
        FROM (
            SELECT PRO_CODE                                              AS code,
                   UPPER(REGEXP_REPLACE(PRO_CODE, '[., ]', ''))          AS code_clean,
                   LENGTH(UPPER(REGEXP_REPLACE(PRO_CODE, '[., ]', ''))) AS code_length
            FROM {{ current_schema }}.PRO_CM
            WHERE PRO_CODE IS NOT NULL
              AND UPPER(TRIM(PRO_TYPE)) = 'LC'
              AND PRO_DATE >= TO_DATE('{{ start_date }}')
        )
    )
),
procedures_rows AS (
    SELECT 'PROCEDURES' AS "Table",
           code_type    AS "Code Type",
           TO_VARCHAR(COUNT(DISTINCT code))                                                                AS "Distinct Codes",
           TO_VARCHAR(COUNT(*))                                                                            AS "Records",
           TO_VARCHAR(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END))                                      AS "Records with Code Type Errors",
           TO_VARCHAR(ROUND(SUM(CASE WHEN has_error > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2)) || '%' AS "Percent of Records with Code Type Errors (Data Check 1.13)",
           '0'                                                                                             AS "Records in the Wrong Table",
           '0.00%'                                                                                         AS "Percentage of Records in the Wrong Table (Data Check 1.16)"
    FROM (
        SELECT code, code_type, code_clean, code_length,
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
            SELECT PX        AS code,
                   PX_TYPE   AS code_type,
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
)
SELECT "Table", "Code Type", "Distinct Codes", "Records",
       "Records with Code Type Errors",
       "Percent of Records with Code Type Errors (Data Check 1.13)",
       "Records in the Wrong Table",
       "Percentage of Records in the Wrong Table (Data Check 1.16)"
FROM (
    SELECT * FROM condition_rows
    UNION ALL SELECT * FROM diagnosis_rows
    UNION ALL SELECT * FROM dispensing_rows
    UNION ALL SELECT * FROM external_meds_rows
    UNION ALL SELECT * FROM immunization_rows
    UNION ALL SELECT * FROM lab_history_rows
    UNION ALL SELECT * FROM lab_result_cm_rows
    UNION ALL SELECT * FROM med_admin_rows
    UNION ALL SELECT * FROM obs_clin_rows
    UNION ALL SELECT * FROM obs_gen_rows
    UNION ALL SELECT * FROM prescribing_rows
    UNION ALL SELECT * FROM pro_cm_rows
    UNION ALL SELECT * FROM procedures_rows
)
ORDER BY "Table", "Code Type"
