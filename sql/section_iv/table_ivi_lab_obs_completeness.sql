-- Table IVI. Laboratory and Clinical Observation Result Data Completeness
-- Supports DC 3.09 (< 80% LAB_RESULT_CM mapped to LAB_LOINC with valid result),
-- DC 3.10 (< 80% quantitative with normal range), DC 3.12 (< 80% quantitative with unit),
-- DC 3.16 (< 80% OBS_CLIN with code and valid result), DC 3.17 (< 80% quantitative with unit).
-- Exceptions highlighted in blue; must be investigated and explained in ETL ADD.

SELECT TABLE_NAME, CODE_FIELD, RESULT_TYPE,
       TO_VARCHAR(NUMERATOR) AS NUMERATOR,
       TO_VARCHAR(DENOMINATOR) AS DENOMINATOR,
       TO_VARCHAR(ROUND(NUMERATOR * 100.0 / NULLIF(DENOMINATOR, 0), 1)) || '%' AS PCT,
       SOURCE_TABLE
FROM (
    -- DC 3.09: LAB_RESULT_CM — records mapped to LAB_LOINC with valid result
    SELECT 'LAB_RESULT_CM' AS TABLE_NAME, 'LAB_LOINC' AS CODE_FIELD,
           'Mapped with quantitative or qualitative result (DC 3.09)' AS RESULT_TYPE,
           SUM(CASE WHEN LAB_LOINC IS NOT NULL
                     AND ((RESULT_NUM IS NOT NULL AND RESULT_MODIFIER NOT IN ('NI','UN','OT') AND RESULT_MODIFIER IS NOT NULL)
                          OR (RESULT_QUAL IS NOT NULL AND RESULT_QUAL NOT IN ('NI','UN','OT')))
                    THEN 1 ELSE 0 END) AS NUMERATOR,
           COUNT(*) AS DENOMINATOR,
           'LAB_L3_LOINC' AS SOURCE_TABLE,
           1 AS ROW_ORDER
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- DC 3.10: Quantitative lab results with normal range specified
    SELECT 'LAB_RESULT_CM', 'LAB_LOINC',
           'Quantitative results with normal range (DC 3.10)',
           SUM(CASE WHEN NORM_RANGE_LOW IS NOT NULL AND NORM_RANGE_HIGH IS NOT NULL
                     AND NORM_MODIFIER_LOW NOT IN ('NI','UN','OT') AND NORM_MODIFIER_LOW IS NOT NULL
                     AND NORM_MODIFIER_HIGH NOT IN ('NI','UN','OT') AND NORM_MODIFIER_HIGH IS NOT NULL
                    THEN 1 ELSE 0 END),
           SUM(CASE WHEN LAB_LOINC IS NOT NULL AND RESULT_NUM IS NOT NULL
                     AND RESULT_MODIFIER NOT IN ('NI','UN','OT') AND RESULT_MODIFIER IS NOT NULL
                    THEN 1 ELSE 0 END),
           'LAB_L3_LOINC',
           2
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- DC 3.12: Quantitative lab results with result unit
    SELECT 'LAB_RESULT_CM', 'LAB_LOINC',
           'Quantitative results with RESULT_UNIT (DC 3.12)',
           SUM(CASE WHEN RESULT_UNIT IS NOT NULL AND RESULT_UNIT NOT IN ('NI','UN','OT')
                    THEN 1 ELSE 0 END),
           SUM(CASE WHEN LAB_LOINC IS NOT NULL AND RESULT_NUM IS NOT NULL
                     AND RESULT_MODIFIER NOT IN ('NI','UN','OT') AND RESULT_MODIFIER IS NOT NULL
                    THEN 1 ELSE 0 END),
           'LAB_L3_LOINC',
           3
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- DC 3.16: OBS_CLIN records with code and valid result
    SELECT 'OBS_CLIN', 'OBSCLIN_CODE',
           'Mapped with quantitative, qualitative, or narrative result (DC 3.16)',
           SUM(CASE WHEN OBSCLIN_CODE IS NOT NULL
                     AND ((OBSCLIN_RESULT_NUM IS NOT NULL AND OBSCLIN_RESULT_MODIFIER NOT IN ('NI','UN','OT') AND OBSCLIN_RESULT_MODIFIER IS NOT NULL)
                          OR (OBSCLIN_RESULT_QUAL IS NOT NULL AND OBSCLIN_RESULT_QUAL NOT IN ('NI','UN','OT'))
                          OR OBSCLIN_RESULT_TEXT IS NOT NULL)
                    THEN 1 ELSE 0 END),
           COUNT(*),
           'OBSCLIN_L3_CODE',
           4
    FROM {{ current_schema }}.OBS_CLIN
    WHERE OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- DC 3.17: OBS_CLIN quantitative results with result unit
    SELECT 'OBS_CLIN', 'OBSCLIN_CODE',
           'Quantitative results with OBSCLIN_RESULT_UNIT (DC 3.17)',
           SUM(CASE WHEN OBSCLIN_RESULT_UNIT IS NOT NULL AND OBSCLIN_RESULT_UNIT NOT IN ('NI','UN','OT')
                    THEN 1 ELSE 0 END),
           SUM(CASE WHEN OBSCLIN_CODE IS NOT NULL AND OBSCLIN_RESULT_NUM IS NOT NULL
                     AND OBSCLIN_RESULT_MODIFIER NOT IN ('NI','UN','OT') AND OBSCLIN_RESULT_MODIFIER IS NOT NULL
                    THEN 1 ELSE 0 END),
           'OBSCLIN_L3_CODE',
           5
    FROM {{ current_schema }}.OBS_CLIN
    WHERE OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}')

) sub
ORDER BY ROW_ORDER
