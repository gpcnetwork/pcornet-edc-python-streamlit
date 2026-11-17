-- DC 1.16 | Table IIF | Data Model Conformance | Investigative
-- Laboratory results or clinical observations are recorded in the wrong table based on the
-- LOINC classtype of the code.
--
-- SAS source of truth: potential_code_errors.sas (PCE_V16) 1144-1270 (MISPLACED_LOINCS):
--   Codes are joined (raw code, not cleaned) to the LOINC reference (inner join on
--   code = loinc_num) and flagged per table:
--     LAB_RESULT_CM (1146-1160): misplaced when strip(classtype) ne '1'  (expected CLASSTYPE=1).
--       Input rows (740-765): LC from LAB_LOINC plus SM from RESULT_SNOMED.
--     LAB_HISTORY   (1169-1183): misplaced when strip(classtype) ne '1'  (expected CLASSTYPE=1).
--       Input rows (815-828): LC from LAB_LOINC.
--     OBS_CLIN      (1191-1205): misplaced when strip(classtype) ne '2'  (expected CLASSTYPE=2).
--       Input rows (867-891): OBSCLIN_CODE where OBSCLIN_TYPE in (LC,SM), plus
--       OBSCLIN_RESULT_SNOMED for those rows.
--     OBS_GEN       (1213-1227): misplaced when strip(classtype) =  '1'  (expected CLASSTYPE=2, 3 or 4).
--       Input rows (939-953): OBSGEN_CODE where OBSGEN_TYPE in
--       (LC,ND,RX,09DX,09PX,10DX,10PX,CH,SM).
--   misplaced_record_pct = misplaced_records / records * 100 per table/code_type stratum
--   (misp_sum macro, 228-265). Full table scans - NO date filter.
--   Exception threshold: edc_report.sas 5466-5480 - misplaced_record_pct > 0 for any stratum.
--   (Note: in edc_report.sas the 1.16 test is an "else if" after the 1.13 bad_record_pct>5.00
--   test on the same stratum; implemented here simply as any misplaced_records > 0.)
-- Conformance check: full table, no date window.
-- Parameters: {{ current_schema }}, {{ loinc_ref_fqn }}
WITH loinc_class AS (
    {% if loinc_ref_fqn %}
    SELECT DISTINCT TRIM(UPPER(LOINC_NUM))               AS LOINC_NUM,
                    COALESCE(TRIM(CLASSTYPE::VARCHAR), '') AS CLASSTYPE
    FROM {{ loinc_ref_fqn }}
    WHERE LOINC_NUM IS NOT NULL AND TRIM(LOINC_NUM) <> ''
    {% else %}
    SELECT NULL::VARCHAR AS LOINC_NUM, NULL::VARCHAR AS CLASSTYPE WHERE 1=0
    {% endif %}
),
recs AS (
    -- LAB_RESULT_CM (potential_code_errors.sas 740-765): expected CLASSTYPE=1
    SELECT 'LAB_RESULT_CM' AS EXC_TABLE, 'LC' AS CODE_TYPE, 'LAB_LOINC' AS EXC_FIELD,
           '1' AS EXPECTED_CLASSTYPE, LAB_LOINC AS CODE
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE NULLIF(TRIM(LAB_LOINC), '') IS NOT NULL
    UNION ALL
    SELECT 'LAB_RESULT_CM', 'SM', 'RESULT_SNOMED', '1', RESULT_SNOMED
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE NULLIF(TRIM(RESULT_SNOMED), '') IS NOT NULL
    UNION ALL
    -- LAB_HISTORY (815-828): expected CLASSTYPE=1
    SELECT 'LAB_HISTORY', 'LC', 'LAB_LOINC', '1', LAB_LOINC
    FROM {{ current_schema }}.LAB_HISTORY
    WHERE NULLIF(TRIM(LAB_LOINC), '') IS NOT NULL
    UNION ALL
    -- OBS_CLIN (867-891): expected CLASSTYPE=2
    SELECT 'OBS_CLIN', OBSCLIN_TYPE, 'OBSCLIN_CODE', '2', OBSCLIN_CODE
    FROM {{ current_schema }}.OBS_CLIN
    WHERE NULLIF(TRIM(OBSCLIN_CODE), '') IS NOT NULL
      AND OBSCLIN_TYPE IN ('LC', 'SM')
    UNION ALL
    SELECT 'OBS_CLIN', 'SM', 'OBSCLIN_RESULT_SNOMED', '2', OBSCLIN_RESULT_SNOMED
    FROM {{ current_schema }}.OBS_CLIN
    WHERE NULLIF(TRIM(OBSCLIN_RESULT_SNOMED), '') IS NOT NULL
      AND OBSCLIN_TYPE IN ('LC', 'SM')
    UNION ALL
    -- OBS_GEN (939-953): flag Laboratory LOINCs (expected CLASSTYPE=2, 3 or 4)
    SELECT 'OBS_GEN', OBSGEN_TYPE, 'OBSGEN_CODE', '2,3,or 4', OBSGEN_CODE
    FROM {{ current_schema }}.OBS_GEN
    WHERE NULLIF(TRIM(OBSGEN_CODE), '') IS NOT NULL
      AND OBSGEN_TYPE IN ('LC', 'ND', 'RX', '09DX', '09PX', '10DX', '10PX', 'CH', 'SM')
),
totals AS (
    -- SAS strata are (table, code_type): OBS_CLIN SM combines OBSCLIN_CODE (type SM)
    -- and OBSCLIN_RESULT_SNOMED records (tblsum/misp_sum macros, 125-135 and 228-265)
    SELECT EXC_TABLE, CODE_TYPE, EXPECTED_CLASSTYPE,
           LISTAGG(DISTINCT EXC_FIELD, '/') WITHIN GROUP (ORDER BY EXC_FIELD) AS EXC_FIELD,
           COUNT(*) AS TOTAL_RECORDS
    FROM recs
    GROUP BY EXC_TABLE, CODE_TYPE, EXPECTED_CLASSTYPE
),
misplaced AS (
    -- inner join on code = loinc_num, as in SAS (1146-1227)
    SELECT r.EXC_TABLE, r.CODE_TYPE, COUNT(*) AS MISPLACED_RECORDS
    FROM recs r
    JOIN loinc_class lc
      ON TRIM(UPPER(r.CODE)) = lc.LOINC_NUM
    WHERE (r.EXC_TABLE IN ('LAB_RESULT_CM', 'LAB_HISTORY') AND lc.CLASSTYPE <> '1')
       OR (r.EXC_TABLE = 'OBS_CLIN'                        AND lc.CLASSTYPE <> '2')
       OR (r.EXC_TABLE = 'OBS_GEN'                         AND lc.CLASSTYPE =  '1')
    GROUP BY r.EXC_TABLE, r.CODE_TYPE
),
strata AS (
    SELECT t.EXC_TABLE,
           t.CODE_TYPE,
           t.EXC_FIELD,
           t.EXPECTED_CLASSTYPE,
           t.TOTAL_RECORDS,
           COALESCE(m.MISPLACED_RECORDS, 0)                        AS MISPLACED_RECORDS,
           100.0 * COALESCE(m.MISPLACED_RECORDS, 0) / t.TOTAL_RECORDS AS MISPLACED_RECORD_PCT
    FROM totals t
    LEFT JOIN misplaced m
      ON t.EXC_TABLE = m.EXC_TABLE AND t.CODE_TYPE = m.CODE_TYPE
),
summary AS (
    SELECT
        '1.16'                                                                                            AS CHECK_NUM,
        'Laboratory results or clinical observations are recorded in the wrong table based on the LOINC classtype' AS DESCRIPTION,
        CASE
            -- n/a when LAB_RESULT_CM, OBS_GEN and OBS_CLIN all have no data (edc_report.sas 11314-11318)
            WHEN (SELECT COUNT(*) FROM {{ current_schema }}.LAB_RESULT_CM LIMIT 1) = 0
             AND (SELECT COUNT(*) FROM {{ current_schema }}.OBS_GEN       LIMIT 1) = 0
             AND (SELECT COUNT(*) FROM {{ current_schema }}.OBS_CLIN      LIMIT 1) = 0
            THEN 'n/a'
            WHEN EXISTS (SELECT 1 FROM strata WHERE MISPLACED_RECORDS > 0) THEN 'Fail'
            ELSE 'Pass'
        END                                                                                               AS STATUS,
        'SUMMARY'                                                                                         AS ROW_TYPE,
        CAST(NULL AS VARCHAR)                                                                             AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                                             AS EXC_FIELD,
        CAST(NULL AS VARCHAR)                                                                             AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                                              AS EXC_COUNT,
        0                                                                                                 AS ROW_ORDER
),
details AS (
    -- One row per table/code_type stratum with misplaced counts (mirrors misp_sum + code_summary)
    SELECT
        '1.16'                                                                                            AS CHECK_NUM,
        'Laboratory results or clinical observations are recorded in the wrong table based on the LOINC classtype' AS DESCRIPTION,
        CASE WHEN MISPLACED_RECORDS > 0 THEN 'Fail' ELSE 'Pass' END                                       AS STATUS,
        'DETAIL'                                                                                          AS ROW_TYPE,
        EXC_TABLE                                                                                         AS EXC_TABLE,
        EXC_FIELD || ' (' || CODE_TYPE || ')'                                                             AS EXC_FIELD,
        'misplaced_record_pct=' || TO_CHAR(ROUND(MISPLACED_RECORD_PCT, 2), 'FM999990.00')
            || ' (' || MISPLACED_RECORDS || ' of ' || TOTAL_RECORDS
            || ' records; expected CLASSTYPE=' || EXPECTED_CLASSTYPE || ')'                               AS EXC_DETAIL,
        MISPLACED_RECORDS                                                                                 AS EXC_COUNT,
        ROW_NUMBER() OVER (ORDER BY EXC_TABLE, CODE_TYPE, EXC_FIELD)                                      AS ROW_ORDER
    FROM strata
)
SELECT * FROM summary
UNION ALL
SELECT * FROM details
ORDER BY ROW_ORDER
