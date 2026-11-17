-- DC 3.09 | Table IVI | Data Completeness | Investigative
-- Less than 80% of laboratory results are mapped to LAB_LOINC and have either a quantitative
-- result (RESULT_NUM non-missing with RESULT_MODIFIER not null/NI/UN/OT) or a qualitative
-- result (RESULT_QUAL not null/NI/UN/OT).
--
-- SAS source of truth (v7.03): edc_report.sas 9822-10032 (Table IVI, ord=3 -> row=3.09):
--   * Numerator (9877-9884, col3): LAB_L3_RECORDC KNOWN_TEST_RESULT ALL_N. Flag defined in
--     data_curation_tables.sas 1791: not missing(lab_loinc) and ((not missing(result_num) and
--     result_modifier not in ('' 'NI' 'UN' 'OT')) or result_qual not in ('' 'NI' 'UN' 'OT')).
--     SAS blank ('') and null are the same missing value -> COALESCE(col,'') comparisons here.
--   * Denominator (9826-9827, 9839-9844, 9857): LAB_L3_N LAB_RESULT_CM_ID ALL_N + NULL_N
--     = ALL records of the loaded LAB_RESULT_CM dataset.
--   * Date window: data_curation_tables.sas 1781 loads LAB_RESULT_CM with
--     "if result_date>=&lookback_dt or missing(result_date)" -- lower bound only, null dates
--     kept, NO upper bound.
--   * Exception (10024): ord=3 and (.<col5p<80.0 or (numerator=0 and denominator>0)); col5p is
--     the raw (unrounded) percent, missing when denominator<=0 -> denominator=0 is NOT an
--     exception.
--   * n/a (11335-11339): row 3.09 is "n/a" when &_anylabdata=0, i.e. no LAB_RESULT_CM data in
--     the DataMart (data_curation_base.sas 558: queries are skipped when the raw table has 0
--     rows) -> STATUS 'n/a' when LAB_RESULT_CM is empty.
-- Ground truth: C4UMO 2026-07-10 -> No (Pass).
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH lab AS (
    SELECT
        COUNT(*) AS DENOM,
        COUNT_IF(
            COALESCE(LAB_LOINC, '') <> '' AND (
                (RESULT_NUM IS NOT NULL AND COALESCE(RESULT_MODIFIER, '') NOT IN ('', 'NI', 'UN', 'OT'))
                OR COALESCE(RESULT_QUAL, '') NOT IN ('', 'NI', 'UN', 'OT')
            )
        ) AS NUMER
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE >= TO_DATE('{{ start_date }}') OR RESULT_DATE IS NULL
),
tbl AS (
    -- n/a parity: SAS skips the LAB queries only when the raw table has no rows at all
    SELECT COUNT(*) AS N_ALL FROM {{ current_schema }}.LAB_RESULT_CM
),
summary AS (
    SELECT
        '3.09'                                                                 AS CHECK_NUM,
        'Table IVI: less than 80% of lab results mapped to LAB_LOINC with a quantitative or qualitative result' AS DESCRIPTION,
        CASE WHEN t.N_ALL = 0 THEN 'n/a'
             WHEN l.DENOM > 0 AND 100.0 * l.NUMER / l.DENOM < 80.0 THEN 'Fail'
             ELSE 'Pass' END                                                   AS STATUS,
        'SUMMARY'                                                              AS ROW_TYPE,
        CAST(NULL AS VARCHAR)                                                  AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                  AS EXC_FIELD,
        CAST(NULL AS VARCHAR)                                                  AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                   AS EXC_COUNT,
        0                                                                      AS ROW_ORDER
    FROM lab l CROSS JOIN tbl t
),
details AS (
    SELECT
        '3.09'                                                                 AS CHECK_NUM,
        'Table IVI: less than 80% of lab results mapped to LAB_LOINC with a quantitative or qualitative result' AS DESCRIPTION,
        CASE WHEN l.DENOM > 0 AND 100.0 * l.NUMER / l.DENOM < 80.0 THEN 'Fail' ELSE 'Pass' END AS STATUS,
        'DETAIL'                                                               AS ROW_TYPE,
        'LAB_RESULT_CM'                                                        AS EXC_TABLE,
        'LAB_LOINC/RESULT_NUM/RESULT_QUAL'                                     AS EXC_FIELD,
        'known test result ' ||
            COALESCE(TO_VARCHAR(ROUND(100.0 * l.NUMER / NULLIF(l.DENOM, 0), 2)::DECIMAL(10,2)), 'n/a') ||
            '% (' || TO_VARCHAR(l.NUMER) || '/' || TO_VARCHAR(l.DENOM) || ')'  AS EXC_DETAIL,
        l.NUMER                                                                AS EXC_COUNT,
        1                                                                      AS ROW_ORDER
    FROM lab l CROSS JOIN tbl t
    WHERE t.N_ALL > 0
)
SELECT * FROM summary
UNION ALL
SELECT * FROM details
ORDER BY ROW_ORDER
