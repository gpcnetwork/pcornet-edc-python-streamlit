-- DC 1.13 | Table IIF | Data Model Conformance | Required
-- More than 5% of CPT/HCPCS, CVX, ICD, NDC, LOINC, RXNORM or SNOMED codes do not conform to the
-- expected length or content based on terminology-specific heuristics.
--
-- SAS source of truth: potential_code_errors.sas (PCE_V16):
--   Cleaning (all tables): code_clean = strip(upcase(compress(code,"., "))), i.e. uppercase and
--     remove ALL periods, commas and spaces; CH codes are truncated to the first 5 characters
--     (PROCEDURES 440, IMMUNIZATION 513, OBS_GEN 948). Full table scans - NO date filter.
--   Heuristics (a record is "bad" when ANY flag = 1):
--     ICD-9 DX   (322-328): alpha present and 1st char not E/V; length not in (3,4,5);
--                           first 3 chars = '000'; no digit anywhere.
--     ICD-10 DX  (330-335): 1st char is a digit (anydigit(code_clean)=1); length not in (3..7);
--                           first 3 chars in ('000','999'); no digit anywhere.
--     SNOMED SM  (337-342): any alpha; length <6 or >18; all chars '0' or all chars '9'.
--     CPT/HCPCS  (453-458): length <5; first 5 chars in ('00000','99999'); no digit anywhere.
--     ICD-9 PX   (460-466): length not in (3,4); any alpha; code = '0000'.
--     ICD-10 PX  (468-473): length <> 7; code in ('0000000','9999999').  (No alpha/numeric flag.)
--     NDC ND     (649-654): any alpha; code in ('00000000000','99999999999'); length <> 11.
--     RXNORM RX  (597-602): any alpha; length <2 or >7.
--     CVX CX     (544-549): any alpha; length not in (2,3).
--     LOINC LC   (774-780): any alpha; length <3 or >8; '-' not in next-to-last position.
--   Table/code_type population: DIAGNOSIS 301-344 (09/10/SM), CONDITION 366-405 (09/10/SM),
--     PROCEDURES 431-482 (CH/09/10/ND), IMMUNIZATION 504-551 (CX/ND/RX/CH),
--     PRESCRIBING 577-603 (RX), DISPENSING 629-655 (ND), MED_ADMIN 681-714 (RX/ND),
--     LAB_RESULT_CM 740-789 (LC from LAB_LOINC, SM from RESULT_SNOMED),
--     LAB_HISTORY 815-843 (LC), OBS_CLIN 867-915 (LC/SM from OBSCLIN_CODE where
--     OBSCLIN_TYPE in (LC,SM), plus SM from OBSCLIN_RESULT_SNOMED for those rows),
--     OBS_GEN 939-1017 (LC/ND/RX/09DX/09PX/10DX/10PX/CH/SM), EXTERNAL_MEDS 1043-1069 (RX),
--     PRO_CM 1095-1123 (LC where upcase(strip(PRO_TYPE))='LC').
--   Exception threshold: edc_report.sas 5466-5476 - bad_record_pct > 5.00 for ANY
--     table/code_type stratum (bad_record_pct = bad_records/records*100, potential_code_errors.sas 183).
-- Conformance check: full table, no date window.
-- Parameters: {{ current_schema }}
WITH recs AS (
    -- DIAGNOSIS (potential_code_errors.sas 301-344)
    SELECT 'DIAGNOSIS' AS EXC_TABLE,
           DX_TYPE     AS CODE_TYPE,
           CASE DX_TYPE WHEN '09' THEN 'DX09' WHEN '10' THEN 'DX10' ELSE 'SM' END AS RULE,
           UPPER(REGEXP_REPLACE(DX, '[., ]', '')) AS CODE_CLEAN
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE NULLIF(TRIM(DX), '') IS NOT NULL
      AND DX_TYPE IN ('09', '10', 'SM')
    UNION ALL
    -- CONDITION (366-405)
    SELECT 'CONDITION',
           CONDITION_TYPE,
           CASE CONDITION_TYPE WHEN '09' THEN 'DX09' WHEN '10' THEN 'DX10' ELSE 'SM' END,
           UPPER(REGEXP_REPLACE(CONDITION, '[., ]', ''))
    FROM {{ current_schema }}.CONDITION
    WHERE NULLIF(TRIM(CONDITION), '') IS NOT NULL
      AND CONDITION_TYPE IN ('09', '10', 'SM')
    UNION ALL
    -- PROCEDURES (431-482); CH truncated to first 5 chars after cleaning (440)
    SELECT 'PROCEDURES',
           PX_TYPE,
           CASE PX_TYPE WHEN 'CH' THEN 'CH' WHEN '09' THEN 'PX09' WHEN '10' THEN 'PX10' ELSE 'ND' END,
           CASE WHEN PX_TYPE = 'CH'
                THEN LEFT(UPPER(REGEXP_REPLACE(PX, '[., ]', '')), 5)
                ELSE UPPER(REGEXP_REPLACE(PX, '[., ]', ''))
           END
    FROM {{ current_schema }}.PROCEDURES
    WHERE NULLIF(TRIM(PX), '') IS NOT NULL
      AND PX_TYPE IN ('CH', '09', '10', 'ND')
    UNION ALL
    -- IMMUNIZATION (504-551); CH truncated to first 5 chars (513)
    SELECT 'IMMUNIZATION',
           VX_CODE_TYPE,
           VX_CODE_TYPE,  -- CX/ND/RX/CH rules carry the same names
           CASE WHEN VX_CODE_TYPE = 'CH'
                THEN LEFT(UPPER(REGEXP_REPLACE(VX_CODE, '[., ]', '')), 5)
                ELSE UPPER(REGEXP_REPLACE(VX_CODE, '[., ]', ''))
           END
    FROM {{ current_schema }}.IMMUNIZATION
    WHERE NULLIF(TRIM(VX_CODE), '') IS NOT NULL
      AND VX_CODE_TYPE IN ('CX', 'ND', 'RX', 'CH')
    UNION ALL
    -- PRESCRIBING (577-603)
    SELECT 'PRESCRIBING', 'RX', 'RX',
           UPPER(REGEXP_REPLACE(RXNORM_CUI::VARCHAR, '[., ]', ''))
    FROM {{ current_schema }}.PRESCRIBING
    WHERE NULLIF(TRIM(RXNORM_CUI::VARCHAR), '') IS NOT NULL
    UNION ALL
    -- DISPENSING (629-655)
    SELECT 'DISPENSING', 'ND', 'ND',
           UPPER(REGEXP_REPLACE(NDC, '[., ]', ''))
    FROM {{ current_schema }}.DISPENSING
    WHERE NULLIF(TRIM(NDC), '') IS NOT NULL
    UNION ALL
    -- MED_ADMIN (681-714)
    SELECT 'MED_ADMIN', MEDADMIN_TYPE, MEDADMIN_TYPE,
           UPPER(REGEXP_REPLACE(MEDADMIN_CODE, '[., ]', ''))
    FROM {{ current_schema }}.MED_ADMIN
    WHERE NULLIF(TRIM(MEDADMIN_CODE), '') IS NOT NULL
      AND MEDADMIN_TYPE IN ('RX', 'ND')
    UNION ALL
    -- LAB_RESULT_CM (740-789): LC from LAB_LOINC and SM from RESULT_SNOMED
    -- (a row populating both fields contributes one record to each stratum, 743-746)
    SELECT 'LAB_RESULT_CM', 'LC', 'LC',
           UPPER(REGEXP_REPLACE(LAB_LOINC, '[., ]', ''))
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE NULLIF(TRIM(LAB_LOINC), '') IS NOT NULL
    UNION ALL
    SELECT 'LAB_RESULT_CM', 'SM', 'SM',
           UPPER(REGEXP_REPLACE(RESULT_SNOMED, '[., ]', ''))
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE NULLIF(TRIM(RESULT_SNOMED), '') IS NOT NULL
    UNION ALL
    -- LAB_HISTORY (815-843)
    SELECT 'LAB_HISTORY', 'LC', 'LC',
           UPPER(REGEXP_REPLACE(LAB_LOINC, '[., ]', ''))
    FROM {{ current_schema }}.LAB_HISTORY
    WHERE NULLIF(TRIM(LAB_LOINC), '') IS NOT NULL
    UNION ALL
    -- OBS_CLIN (867-915): OBSCLIN_CODE keeps OBSCLIN_TYPE (LC or SM); OBSCLIN_RESULT_SNOMED
    -- rows (for OBSCLIN_TYPE in LC/SM) are typed SM (871-884)
    SELECT 'OBS_CLIN', OBSCLIN_TYPE, OBSCLIN_TYPE,
           UPPER(REGEXP_REPLACE(OBSCLIN_CODE, '[., ]', ''))
    FROM {{ current_schema }}.OBS_CLIN
    WHERE NULLIF(TRIM(OBSCLIN_CODE), '') IS NOT NULL
      AND OBSCLIN_TYPE IN ('LC', 'SM')
    UNION ALL
    SELECT 'OBS_CLIN', 'SM', 'SM',
           UPPER(REGEXP_REPLACE(OBSCLIN_RESULT_SNOMED, '[., ]', ''))
    FROM {{ current_schema }}.OBS_CLIN
    WHERE NULLIF(TRIM(OBSCLIN_RESULT_SNOMED), '') IS NOT NULL
      AND OBSCLIN_TYPE IN ('LC', 'SM')
    UNION ALL
    -- OBS_GEN (939-1017); CH truncated to first 5 chars (948)
    SELECT 'OBS_GEN',
           OBSGEN_TYPE,
           CASE OBSGEN_TYPE
                WHEN '09DX' THEN 'DX09' WHEN '10DX' THEN 'DX10'
                WHEN '09PX' THEN 'PX09' WHEN '10PX' THEN 'PX10'
                ELSE OBSGEN_TYPE  -- LC/ND/RX/CH/SM
           END,
           CASE WHEN OBSGEN_TYPE = 'CH'
                THEN LEFT(UPPER(REGEXP_REPLACE(OBSGEN_CODE, '[., ]', '')), 5)
                ELSE UPPER(REGEXP_REPLACE(OBSGEN_CODE, '[., ]', ''))
           END
    FROM {{ current_schema }}.OBS_GEN
    WHERE NULLIF(TRIM(OBSGEN_CODE), '') IS NOT NULL
      AND OBSGEN_TYPE IN ('LC', 'ND', 'RX', '09DX', '09PX', '10DX', '10PX', 'CH', 'SM')
    UNION ALL
    -- EXTERNAL_MEDS (1043-1069)
    SELECT 'EXTERNAL_MEDS', 'RX', 'RX',
           UPPER(REGEXP_REPLACE(RXNORM_CUI::VARCHAR, '[., ]', ''))
    FROM {{ current_schema }}.EXTERNAL_MEDS
    WHERE NULLIF(TRIM(RXNORM_CUI::VARCHAR), '') IS NOT NULL
    UNION ALL
    -- PRO_CM (1095-1123)
    SELECT 'PRO_CM', 'LC', 'LC',
           UPPER(REGEXP_REPLACE(PRO_CODE, '[., ]', ''))
    FROM {{ current_schema }}.PRO_CM
    WHERE NULLIF(TRIM(PRO_CODE), '') IS NOT NULL
      AND UPPER(TRIM(PRO_TYPE)) = 'LC'
),
scored AS (
    SELECT EXC_TABLE,
           CODE_TYPE,
           CASE
               -- ICD-9 diagnosis heuristic (322-328)
               WHEN RULE = 'DX09' THEN IFF(
                      (REGEXP_LIKE(CODE_CLEAN, '.*[A-Z].*') AND LEFT(CODE_CLEAN, 1) NOT IN ('E', 'V'))
                   OR CODE_LENGTH NOT IN (3, 4, 5)
                   OR LEFT(CODE_CLEAN, 3) = '000'
                   OR NOT REGEXP_LIKE(CODE_CLEAN, '.*[0-9].*'), 1, 0)
               -- ICD-10 diagnosis heuristic (330-335); anydigit(code_clean)=1 means 1st char is a digit
               WHEN RULE = 'DX10' THEN IFF(
                      REGEXP_LIKE(CODE_CLEAN, '^[0-9].*')
                   OR CODE_LENGTH NOT IN (3, 4, 5, 6, 7)
                   OR LEFT(CODE_CLEAN, 3) IN ('000', '999')
                   OR NOT REGEXP_LIKE(CODE_CLEAN, '.*[0-9].*'), 1, 0)
               -- SNOMED heuristic (337-342); Snowflake REGEXP_LIKE matches the whole string,
               -- so '0+'/'9+' mean "all zeros"/"all nines" (= countc test in SAS 340)
               WHEN RULE = 'SM' THEN IFF(
                      REGEXP_LIKE(CODE_CLEAN, '.*[A-Z].*')
                   OR CODE_LENGTH < 6 OR CODE_LENGTH > 18
                   OR REGEXP_LIKE(CODE_CLEAN, '0+')
                   OR REGEXP_LIKE(CODE_CLEAN, '9+'), 1, 0)
               -- CPT/HCPCS heuristic (453-458); code already truncated to 5 chars
               WHEN RULE = 'CH' THEN IFF(
                      CODE_LENGTH < 5
                   OR LEFT(CODE_CLEAN, 5) IN ('00000', '99999')
                   OR NOT REGEXP_LIKE(CODE_CLEAN, '.*[0-9].*'), 1, 0)
               -- ICD-9 procedure heuristic (460-466)
               WHEN RULE = 'PX09' THEN IFF(
                      CODE_LENGTH NOT IN (3, 4)
                   OR REGEXP_LIKE(CODE_CLEAN, '.*[A-Z].*')
                   OR CODE_CLEAN = '0000', 1, 0)
               -- ICD-10 procedure heuristic (468-473); SAS has no alpha/numeric flag here
               WHEN RULE = 'PX10' THEN IFF(
                      CODE_LENGTH <> 7
                   OR CODE_CLEAN IN ('0000000', '9999999'), 1, 0)
               -- NDC heuristic (649-654)
               WHEN RULE = 'ND' THEN IFF(
                      REGEXP_LIKE(CODE_CLEAN, '.*[A-Z].*')
                   OR CODE_CLEAN IN ('00000000000', '99999999999')
                   OR CODE_LENGTH <> 11, 1, 0)
               -- RXNORM heuristic (597-602)
               WHEN RULE = 'RX' THEN IFF(
                      REGEXP_LIKE(CODE_CLEAN, '.*[A-Z].*')
                   OR CODE_LENGTH < 2 OR CODE_LENGTH > 7, 1, 0)
               -- CVX heuristic (544-549)
               WHEN RULE = 'CX' THEN IFF(
                      REGEXP_LIKE(CODE_CLEAN, '.*[A-Z].*')
                   OR CODE_LENGTH NOT BETWEEN 2 AND 3, 1, 0)
               -- LOINC heuristic (774-780): dash must be the next-to-last character
               WHEN RULE = 'LC' THEN IFF(
                      REGEXP_LIKE(CODE_CLEAN, '.*[A-Z].*')
                   OR CODE_LENGTH < 3 OR CODE_LENGTH > 8
                   OR CODE_CLEAN NOT LIKE '%-_', 1, 0)
               ELSE 0
           END AS IS_BAD
    FROM (SELECT EXC_TABLE, CODE_TYPE, RULE, CODE_CLEAN, LENGTH(CODE_CLEAN) AS CODE_LENGTH FROM recs)
),
strata AS (
    SELECT EXC_TABLE,
           CODE_TYPE,
           COUNT(*)                          AS TOTAL_RECORDS,
           SUM(IS_BAD)                       AS BAD_RECORDS,
           100.0 * SUM(IS_BAD) / COUNT(*)    AS BAD_RECORD_PCT
    FROM scored
    GROUP BY EXC_TABLE, CODE_TYPE
),
summary AS (
    SELECT
        '1.13'                                                                                               AS CHECK_NUM,
        'More than 5% of CPT/HCPCS, CVX, ICD, NDC, LOINC, RXNORM or SNOMED codes do not conform to expected length or content based on terminology-specific heuristics' AS DESCRIPTION,
        CASE WHEN EXISTS (SELECT 1 FROM strata WHERE BAD_RECORD_PCT > 5) THEN 'Fail' ELSE 'Pass' END         AS STATUS,
        'SUMMARY'                                                                                            AS ROW_TYPE,
        CAST(NULL AS VARCHAR)                                                                                AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                                                AS EXC_FIELD,
        CAST(NULL AS VARCHAR)                                                                                AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                                                 AS EXC_COUNT,
        0                                                                                                    AS ROW_ORDER
),
details AS (
    -- One row per table/code_type stratum (mirrors dmlocal.code_summary, potential_code_errors.sas 1294-1335)
    SELECT
        '1.13'                                                                                               AS CHECK_NUM,
        'More than 5% of CPT/HCPCS, CVX, ICD, NDC, LOINC, RXNORM or SNOMED codes do not conform to expected length or content based on terminology-specific heuristics' AS DESCRIPTION,
        CASE WHEN BAD_RECORD_PCT > 5 THEN 'Fail' ELSE 'Pass' END                                             AS STATUS,
        'DETAIL'                                                                                             AS ROW_TYPE,
        EXC_TABLE                                                                                            AS EXC_TABLE,
        CODE_TYPE                                                                                            AS EXC_FIELD,
        'bad_record_pct=' || TO_CHAR(ROUND(BAD_RECORD_PCT, 2), 'FM999990.00')
            || ' (' || BAD_RECORDS || ' of ' || TOTAL_RECORDS || ' records)'                                 AS EXC_DETAIL,
        BAD_RECORDS                                                                                          AS EXC_COUNT,
        ROW_NUMBER() OVER (ORDER BY EXC_TABLE, CODE_TYPE)                                                    AS ROW_ORDER
    FROM strata
)
SELECT * FROM summary
UNION ALL
SELECT * FROM details
ORDER BY ROW_ORDER
