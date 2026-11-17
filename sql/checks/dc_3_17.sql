-- DC 3.17 | Table IVI | Data Completeness | Investigative
-- Less than 80% of quantitative clinical observations (mapped to OBSCLIN_CODE,
-- OBSCLIN_RESULT_NUM non-missing, OBSCLIN_RESULT_MODIFIER not null/NI/UN/OT) fully specify the
-- OBSCLIN_RESULT_UNIT (not null/NI/UN/OT).
--
-- SAS source of truth (v7.03): edc_report.sas 9922-10032 (Table IVI, ord=16 -> row=3.17):
--   * Numerator (9989-9996, col3): OBSCLIN_L3_RECORDC KNOWN_TEST_RESULT_NUM_UNIT ALL_N. Flag in
--     data_curation_tables.sas 1857: not missing(obsclin_code) and not
--     missing(obsclin_result_num) and obsclin_result_modifier not in ('' 'NI' 'UN' 'OT') and
--     obsclin_result_unit not in ('' 'NI' 'UN' 'OT').
--   * Denominator (9934-9945, denom_obsknown): OBSCLIN_L3_RECORDC KNOWN_TEST_RESULT_NUM ALL_N.
--     Flag in data_curation_tables.sas 1856: not missing(obsclin_code) and not
--     missing(obsclin_result_num) and obsclin_result_modifier not in ('' 'NI' 'UN' 'OT').
--   * Date window: data_curation_tables.sas 1839 loads OBS_CLIN with
--     "if obsclin_start_date>=&lookback_dt or missing(obsclin_start_date)" -- lower bound only,
--     null dates kept, NO upper bound.
--   * Exception (10024): ord=16 and (.<col5p<80.0 or (numerator=0 and denominator>0)); raw
--     unrounded percent; denominator=0 (no quantitative results) is NOT an exception.
--   * n/a (11350-11354): row 3.17 is "n/a" when &_anyobsclindata=0 -> STATUS 'n/a' when
--     OBS_CLIN is empty.
-- Ground truth: C4UMO 2026-07-10 -> No (Pass).
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH quant AS (
    SELECT
        COUNT(*) AS DENOM,
        COUNT_IF(COALESCE(OBSCLIN_RESULT_UNIT, '') NOT IN ('', 'NI', 'UN', 'OT')) AS NUMER
    FROM {{ current_schema }}.OBS_CLIN
    WHERE (OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}') OR OBSCLIN_START_DATE IS NULL)
      AND COALESCE(OBSCLIN_CODE, '') <> ''
      AND OBSCLIN_RESULT_NUM IS NOT NULL
      AND COALESCE(OBSCLIN_RESULT_MODIFIER, '') NOT IN ('', 'NI', 'UN', 'OT')
),
tbl AS (
    -- n/a parity: SAS skips the OBSCLIN queries only when the raw table has no rows at all
    SELECT COUNT(*) AS N_ALL FROM {{ current_schema }}.OBS_CLIN
),
summary AS (
    SELECT
        '3.17'                                                                 AS CHECK_NUM,
        'Table IVI: less than 80% of quantitative OBS_CLIN results fully specify OBSCLIN_RESULT_UNIT' AS DESCRIPTION,
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
        '3.17'                                                                 AS CHECK_NUM,
        'Table IVI: less than 80% of quantitative OBS_CLIN results fully specify OBSCLIN_RESULT_UNIT' AS DESCRIPTION,
        CASE WHEN q.DENOM > 0 AND 100.0 * q.NUMER / q.DENOM < 80.0 THEN 'Fail' ELSE 'Pass' END AS STATUS,
        'DETAIL'                                                               AS ROW_TYPE,
        'OBS_CLIN'                                                             AS EXC_TABLE,
        'OBSCLIN_RESULT_UNIT'                                                  AS EXC_FIELD,
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
