-- DC 1.19 | Table IIE | Data Model Conformance | Investigative
-- More than 10% of the values in any hash token field are assigned to multiple patients.
--
-- SAS source of truth (v7.03): edc_report.sas 5130-5148 (Table IIE row=19):
--   for every HASH_L3_N token variable (all tokens except PATID, TOKEN_ENCRYPTION_KEY, HASHID),
--   nondistinct_pct = 100 - (VALID_DISTINCT_N / VALID_N * 100); exception when >= 10.0.
--   Token field list from the HASH_TOKEN load (data_curation_tables.sas 532):
--   TOKEN_01-09, 12, 14-18, 23-26, 29, 30, 101-111. Full table, no date window.
--   "Valid" approximated as non-null/non-blank (SAS VALID_N definition).
--   n/a when the HASH_TOKEN table has no data (edc_report.sas 11319-11323).
--   (The old SQL evaluated TOKEN_ENCRYPTION_KEY, which SAS explicitly excludes.)
-- Parameters: {{ current_schema }}
{% set tokens = [
    'TOKEN_01','TOKEN_02','TOKEN_03','TOKEN_04','TOKEN_05','TOKEN_06','TOKEN_07','TOKEN_08','TOKEN_09',
    'TOKEN_12','TOKEN_14','TOKEN_15','TOKEN_16','TOKEN_17','TOKEN_18',
    'TOKEN_23','TOKEN_24','TOKEN_25','TOKEN_26','TOKEN_29','TOKEN_30',
    'TOKEN_101','TOKEN_102','TOKEN_103','TOKEN_104','TOKEN_105','TOKEN_106','TOKEN_107','TOKEN_108',
    'TOKEN_109','TOKEN_110','TOKEN_111'
] %}
WITH agg AS (
    SELECT
        COUNT(*) AS TABLE_ROWS
        {%- for t in tokens %},
        COUNT(NULLIF(TRIM({{ t }}), ''))          AS VALID_{{ t }},
        COUNT(DISTINCT NULLIF(TRIM({{ t }}), '')) AS DIST_{{ t }}
        {%- endfor %}
    FROM {{ current_schema }}.HASH_TOKEN
),
per_token AS (
    SELECT v.TOKEN_FIELD, v.VALID_N, v.DIST_N,
           CASE WHEN v.VALID_N > 0
                THEN 100.0 - (100.0 * v.DIST_N / v.VALID_N) END AS NONDISTINCT_PCT
    FROM agg,
    LATERAL (
        SELECT * FROM VALUES
        {%- for t in tokens %}
            ('{{ t }}', agg.VALID_{{ t }}, agg.DIST_{{ t }}){{ "," if not loop.last }}
        {%- endfor %}
        AS x(TOKEN_FIELD, VALID_N, DIST_N)
    ) v
),
summary AS (
    SELECT
        '1.19'                                                                 AS CHECK_NUM,
        'More than 10% of the values in any hash token field are assigned to multiple patients' AS DESCRIPTION,
        CASE
            WHEN (SELECT TABLE_ROWS FROM agg) = 0 THEN 'n/a'
            WHEN (SELECT MAX(NONDISTINCT_PCT) FROM per_token) >= 10.0 THEN 'Fail'
            ELSE 'Pass'
        END                                                                    AS STATUS,
        'SUMMARY'                                                              AS ROW_TYPE,
        ''                                                                     AS EXC_TABLE,
        ''                                                                     AS EXC_FIELD,
        'max non-distinct pct = ' ||
            TO_VARCHAR(COALESCE(ROUND((SELECT MAX(NONDISTINCT_PCT) FROM per_token), 2), 0)) AS EXC_DETAIL,
        NULL                                                                   AS EXC_COUNT,
        0                                                                      AS ROW_ORDER
),
details AS (
    SELECT
        '1.19', 'Hash token values assigned to multiple patients', 'Fail', 'DETAIL',
        'HASH_TOKEN', TOKEN_FIELD,
        'non-distinct ' || TO_VARCHAR(ROUND(NONDISTINCT_PCT, 2)) || '% (' ||
            TO_VARCHAR(VALID_N - DIST_N) || ' of ' || TO_VARCHAR(VALID_N) || ' valid values)',
        VALID_N - DIST_N,
        ROW_NUMBER() OVER (ORDER BY NONDISTINCT_PCT DESC)
    FROM per_token
    WHERE NONDISTINCT_PCT >= 10.0
)
SELECT CHECK_NUM, DESCRIPTION, STATUS, ROW_TYPE, EXC_TABLE, EXC_FIELD, EXC_DETAIL, EXC_COUNT
FROM (
    SELECT * FROM summary
    UNION ALL
    SELECT * FROM details
)
ORDER BY ROW_ORDER
