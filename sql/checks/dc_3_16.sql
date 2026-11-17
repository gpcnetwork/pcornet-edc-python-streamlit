-- DC 3.16 | Table IVI | Data Completeness | Investigative
-- Less than 80% of clinical observations are mapped to an OBSCLIN_CODE and have a quantitative
-- result (OBSCLIN_RESULT_NUM non-missing with OBSCLIN_RESULT_MODIFIER not null/NI/UN/OT), a
-- qualitative result (OBSCLIN_RESULT_QUAL not null/NI/UN/OT), or a narrative result
-- (OBSCLIN_RESULT_TEXT non-missing).
--
-- SAS source of truth (v7.03): edc_report.sas 9922-10032 (Table IVI, ord=14 -> row=3.16):
--   * Numerator (9975-9982, col3): OBSCLIN_L3_RECORDC KNOWN_TEST_RESULT ALL_N. Flag in
--     data_curation_tables.sas 1854-1855: not missing(obsclin_code) and ((not
--     missing(obsclin_result_num) and obsclin_result_modifier not in ('' 'NI' 'UN' 'OT')) or
--     obsclin_result_qual not in ('' 'NI' 'UN' 'OT') or not missing(obsclin_result_text)).
--   * Denominator (9934-9945, denom_obsclin): OBSCLIN_L3_N OBSCLINID ALL_N + NULL_N
--     = ALL records of the loaded OBS_CLIN dataset.
--   * Date window: data_curation_tables.sas 1839 loads OBS_CLIN with
--     "if obsclin_start_date>=&lookback_dt or missing(obsclin_start_date)" -- lower bound only,
--     null dates kept, NO upper bound.
--   * Exception (10024): ord=14 and (.<col5p<80.0 or (numerator=0 and denominator>0)); raw
--     unrounded percent; denominator=0 is NOT an exception.
--   * n/a (11350-11354): row 3.16 is "n/a" when &_anyobsclindata=0, i.e. no OBS_CLIN data in
--     the DataMart -> STATUS 'n/a' when OBS_CLIN is empty.
-- Ground truth: C4UMO 2026-07-10 -> No (Pass).
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH obs AS (
    SELECT
        COUNT(*) AS DENOM,
        COUNT_IF(
            COALESCE(OBSCLIN_CODE, '') <> '' AND (
                (OBSCLIN_RESULT_NUM IS NOT NULL AND COALESCE(OBSCLIN_RESULT_MODIFIER, '') NOT IN ('', 'NI', 'UN', 'OT'))
                OR COALESCE(OBSCLIN_RESULT_QUAL, '') NOT IN ('', 'NI', 'UN', 'OT')
                OR COALESCE(OBSCLIN_RESULT_TEXT, '') <> ''
            )
        ) AS NUMER
    FROM {{ current_schema }}.OBS_CLIN
    WHERE OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}') OR OBSCLIN_START_DATE IS NULL
),
tbl AS (
    -- n/a parity: SAS skips the OBSCLIN queries only when the raw table has no rows at all
    SELECT COUNT(*) AS N_ALL FROM {{ current_schema }}.OBS_CLIN
),
summary AS (
    SELECT
        '3.16'                                                                 AS CHECK_NUM,
        'Table IVI: less than 80% of OBS_CLIN records mapped to OBSCLIN_CODE with a quantitative, qualitative or narrative result' AS DESCRIPTION,
        CASE WHEN t.N_ALL = 0 THEN 'n/a'
             WHEN o.DENOM > 0 AND 100.0 * o.NUMER / o.DENOM < 80.0 THEN 'Fail'
             ELSE 'Pass' END                                                   AS STATUS,
        'SUMMARY'                                                              AS ROW_TYPE,
        CAST(NULL AS VARCHAR)                                                  AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                  AS EXC_FIELD,
        CAST(NULL AS VARCHAR)                                                  AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                   AS EXC_COUNT,
        0                                                                      AS ROW_ORDER
    FROM obs o CROSS JOIN tbl t
),
details AS (
    SELECT
        '3.16'                                                                 AS CHECK_NUM,
        'Table IVI: less than 80% of OBS_CLIN records mapped to OBSCLIN_CODE with a quantitative, qualitative or narrative result' AS DESCRIPTION,
        CASE WHEN o.DENOM > 0 AND 100.0 * o.NUMER / o.DENOM < 80.0 THEN 'Fail' ELSE 'Pass' END AS STATUS,
        'DETAIL'                                                               AS ROW_TYPE,
        'OBS_CLIN'                                                             AS EXC_TABLE,
        'OBSCLIN_CODE/OBSCLIN_RESULT_NUM/QUAL/TEXT'                            AS EXC_FIELD,
        'known test result ' ||
            COALESCE(TO_VARCHAR(ROUND(100.0 * o.NUMER / NULLIF(o.DENOM, 0), 2)::DECIMAL(10,2)), 'n/a') ||
            '% (' || TO_VARCHAR(o.NUMER) || '/' || TO_VARCHAR(o.DENOM) || ')'  AS EXC_DETAIL,
        o.NUMER                                                                AS EXC_COUNT,
        1                                                                      AS ROW_ORDER
    FROM obs o CROSS JOIN tbl t
    WHERE t.N_ALL > 0
)
SELECT * FROM summary
UNION ALL
SELECT * FROM details
ORDER BY ROW_ORDER
