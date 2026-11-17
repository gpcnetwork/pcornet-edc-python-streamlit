-- DC 3.12 | Table IVI | Data Completeness | Investigative
-- Less than 80% of quantitative lab results (mapped to LAB_LOINC, RESULT_NUM non-missing,
-- RESULT_MODIFIER not null/NI/UN/OT) fully specify the RESULT_UNIT (not null/NI/UN/OT).
--
-- SAS source of truth (v7.03): edc_report.sas 9822-10032 (Table IVI, ord=8 -> row=3.12):
--   * Numerator (9901-9910, col3): LAB_L3_RECORDC KNOWN_TEST_RESULT_NUM_UNIT ALL_N. Flag in
--     data_curation_tables.sas 1794: not missing(lab_loinc) and not missing(result_num) and
--     result_modifier not in ('' 'NI' 'UN' 'OT') and result_unit not in ('' 'NI' 'UN' 'OT').
--   * Denominator (9859, 9894/9904, denom_8): LAB_L3_RECORDC KNOWN_TEST_RESULT_NUM ALL_N.
--     Flag in data_curation_tables.sas 1792: not missing(lab_loinc) and not missing(result_num)
--     and result_modifier not in ('' 'NI' 'UN' 'OT').
--   * Date window: data_curation_tables.sas 1781 loads LAB_RESULT_CM with
--     "if result_date>=&lookback_dt or missing(result_date)" -- lower bound only, null dates
--     kept, NO upper bound.
--   * Exception (10024): ord=8 and (.<col5p<80.0 or (numerator=0 and denominator>0)); raw
--     unrounded percent; denominator=0 (no quantitative results) is NOT an exception.
--   * n/a (11335-11339): row 3.12 is "n/a" when &_anylabdata=0 -> STATUS 'n/a' when
--     LAB_RESULT_CM is empty.
-- Ground truth: C4UMO 2026-07-10 -> No (Pass).
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH quant AS (
    SELECT
        COUNT(*) AS DENOM,
        COUNT_IF(COALESCE(RESULT_UNIT, '') NOT IN ('', 'NI', 'UN', 'OT')) AS NUMER
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE (RESULT_DATE >= TO_DATE('{{ start_date }}') OR RESULT_DATE IS NULL)
      AND COALESCE(LAB_LOINC, '') <> ''
      AND RESULT_NUM IS NOT NULL
      AND COALESCE(RESULT_MODIFIER, '') NOT IN ('', 'NI', 'UN', 'OT')
),
tbl AS (
    -- n/a parity: SAS skips the LAB queries only when the raw table has no rows at all
    SELECT COUNT(*) AS N_ALL FROM {{ current_schema }}.LAB_RESULT_CM
),
summary AS (
    SELECT
        '3.12'                                                                 AS CHECK_NUM,
        'Table IVI: less than 80% of quantitative LAB_LOINC results fully specify RESULT_UNIT' AS DESCRIPTION,
        CASE WHEN t.N_ALL = 0 THEN 'n/a'
             WHEN q.DENOM > 0 AND 100.0 * q.NUMER / q.DENOM < 80.0 THEN 'Fail'
             ELSE 'Pass' END                                                   AS STATUS,
        'SUMMARY'                                                              AS ROW_TYPE,
        CAST(NULL AS VARCHAR)                                                  AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                  AS EXC_FIELD,
        CAST(NULL AS VARCHAR)                                                  AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                   AS EXC_COUNT,
        0                                                                      AS ROW_ORDER
    FROM quant q CROSS JOIN tbl t
),
details AS (
    SELECT
        '3.12'                                                                 AS CHECK_NUM,
        'Table IVI: less than 80% of quantitative LAB_LOINC results fully specify RESULT_UNIT' AS DESCRIPTION,
        CASE WHEN q.DENOM > 0 AND 100.0 * q.NUMER / q.DENOM < 80.0 THEN 'Fail' ELSE 'Pass' END AS STATUS,
        'DETAIL'                                                               AS ROW_TYPE,
        'LAB_RESULT_CM'                                                        AS EXC_TABLE,
        'RESULT_UNIT'                                                          AS EXC_FIELD,
        'known unit among quantitative results ' ||
            COALESCE(TO_VARCHAR(ROUND(100.0 * q.NUMER / NULLIF(q.DENOM, 0), 2)::DECIMAL(10,2)), 'n/a') ||
            '% (' || TO_VARCHAR(q.NUMER) || '/' || TO_VARCHAR(q.DENOM) || ')'  AS EXC_DETAIL,
        q.NUMER                                                                AS EXC_COUNT,
        1                                                                      AS ROW_ORDER
    FROM quant q CROSS JOIN tbl t
    WHERE t.N_ALL > 0
)
SELECT * FROM summary
UNION ALL
SELECT * FROM details
ORDER BY ROW_ORDER
