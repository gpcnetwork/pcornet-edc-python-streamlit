-- Table IVI. Laboratory and Clinical Observation Result Data Completeness
-- Supports DC 3.09 (< 80% LAB_RESULT_CM mapped to LAB_LOINC with valid result),
-- DC 3.10 (< 80% quantitative with normal range), DC 3.12 (< 80% quantitative with unit),
-- DC 3.16 (< 80% OBS_CLIN with code and valid result), DC 3.17 (< 80% quantitative with unit).
-- Exceptions highlighted in blue; must be investigated and explained in ETL ADD.

WITH lab_counts AS (
    SELECT
        COUNT(DISTINCT CASE WHEN LAB_LOINC IS NOT NULL AND LAB_LOINC NOT IN ('NI','UN','OT')
                            THEN LAB_LOINC END)                                                          AS distinct_loincs,
        COUNT(*)                                                                                          AS total,
        SUM(CASE WHEN LAB_LOINC IS NOT NULL AND LAB_LOINC NOT IN ('NI','UN','OT')
                 THEN 1 ELSE 0 END)                                                                      AS mapped_loinc,
        SUM(CASE WHEN LAB_LOINC IS NOT NULL
                  AND ((RESULT_NUM IS NOT NULL AND RESULT_MODIFIER NOT IN ('NI','UN','OT') AND RESULT_MODIFIER IS NOT NULL)
                       OR (RESULT_QUAL IS NOT NULL AND RESULT_QUAL NOT IN ('NI','UN','OT')))
                 THEN 1 ELSE 0 END)                                                                      AS mapped_with_result,
        SUM(CASE WHEN LAB_LOINC IS NOT NULL
                  AND RESULT_NUM IS NOT NULL AND RESULT_MODIFIER NOT IN ('NI','UN','OT') AND RESULT_MODIFIER IS NOT NULL
                 THEN 1 ELSE 0 END)                                                                      AS quant_with_loinc,
        SUM(CASE WHEN NORM_RANGE_LOW IS NOT NULL AND NORM_RANGE_HIGH IS NOT NULL
                  AND NORM_MODIFIER_LOW  NOT IN ('NI','UN','OT') AND NORM_MODIFIER_LOW  IS NOT NULL
                  AND NORM_MODIFIER_HIGH NOT IN ('NI','UN','OT') AND NORM_MODIFIER_HIGH IS NOT NULL
                 THEN 1 ELSE 0 END)                                                                      AS quant_with_range,
        SUM(CASE WHEN RESULT_UNIT IS NOT NULL AND RESULT_UNIT NOT IN ('NI','UN','OT')
                 THEN 1 ELSE 0 END)                                                                      AS quant_with_unit
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE >= TO_DATE('{{ start_date }}') AND RESULT_DATE <= TO_DATE('{{ end_date }}')
),
obs_counts AS (
    SELECT
        COUNT(DISTINCT CASE WHEN OBSCLIN_CODE IS NOT NULL AND OBSCLIN_CODE NOT IN ('NI','UN','OT')
                            THEN OBSCLIN_CODE END)                                                       AS distinct_codes,
        COUNT(*)                                                                                          AS total,
        SUM(CASE WHEN OBSCLIN_CODE IS NOT NULL AND OBSCLIN_CODE NOT IN ('NI','UN','OT')
                 THEN 1 ELSE 0 END)                                                                      AS mapped_code,
        SUM(CASE WHEN OBSCLIN_CODE IS NOT NULL
                  AND ((OBSCLIN_RESULT_NUM IS NOT NULL AND OBSCLIN_RESULT_MODIFIER NOT IN ('NI','UN','OT') AND OBSCLIN_RESULT_MODIFIER IS NOT NULL)
                       OR (OBSCLIN_RESULT_QUAL IS NOT NULL AND OBSCLIN_RESULT_QUAL NOT IN ('NI','UN','OT'))
                       OR OBSCLIN_RESULT_TEXT IS NOT NULL)
                 THEN 1 ELSE 0 END)                                                                      AS mapped_with_result,
        SUM(CASE WHEN OBSCLIN_CODE IS NOT NULL
                  AND OBSCLIN_RESULT_NUM IS NOT NULL AND OBSCLIN_RESULT_MODIFIER NOT IN ('NI','UN','OT') AND OBSCLIN_RESULT_MODIFIER IS NOT NULL
                 THEN 1 ELSE 0 END)                                                                      AS quant_with_code,
        SUM(CASE WHEN OBSCLIN_RESULT_UNIT IS NOT NULL AND OBSCLIN_RESULT_UNIT NOT IN ('NI','UN','OT')
                 THEN 1 ELSE 0 END)                                                                      AS quant_with_unit
    FROM {{ current_schema }}.OBS_CLIN
    WHERE OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}') AND OBSCLIN_START_DATE <= TO_DATE('{{ end_date }}')
)
SELECT TABLE_NAME, DATA_CHECK, DESCRIPTION,
       TO_VARCHAR(NUMERATOR)   AS NUMERATOR,
       TO_VARCHAR(DENOMINATOR) AS DENOMINATOR,
       CASE WHEN DENOMINATOR IS NULL OR DENOMINATOR = 0 THEN NULL
            ELSE TO_VARCHAR(ROUND(100.0 * NUMERATOR / DENOMINATOR, 1)) || '%'
       END AS PCT
FROM (
    SELECT 'LAB_RESULT_CM' AS TABLE_NAME, 'n/a' AS DATA_CHECK,
           'Number of distinct LAB_LOINCs'                              AS DESCRIPTION,
           distinct_loincs   AS NUMERATOR, NULL             AS DENOMINATOR, 1 AS ROW_ORDER FROM lab_counts
    UNION ALL
    SELECT 'LAB_RESULT_CM', 'n/a',
           'Results mapped to a known LAB_LOINC',
           mapped_loinc,      total,          2 FROM lab_counts
    UNION ALL
    SELECT 'LAB_RESULT_CM', '3.09',
           'Results mapped to a known LAB_LOINC with a known result',
           mapped_with_result, total,          3 FROM lab_counts
    UNION ALL
    SELECT 'LAB_RESULT_CM', 'n/a',
           'Quantitative results',
           quant_with_loinc,  mapped_loinc,   4 FROM lab_counts
    UNION ALL
    SELECT 'LAB_RESULT_CM', '3.10',
           'Quantitative results which fully specify the normal range',
           quant_with_range,  quant_with_loinc, 5 FROM lab_counts
    UNION ALL
    SELECT 'LAB_RESULT_CM', '3.12',
           'Quantitative results which specify the result unit',
           quant_with_unit,   quant_with_loinc, 6 FROM lab_counts
    UNION ALL
    SELECT 'OBS_CLIN', 'n/a',
           'Number of distinct OBSCLIN_CODEs',
           distinct_codes,    NULL,            7 FROM obs_counts
    UNION ALL
    SELECT 'OBS_CLIN', 'n/a',
           'Results mapped to a known OBSCLIN_CODE',
           mapped_code,       total,           8 FROM obs_counts
    UNION ALL
    SELECT 'OBS_CLIN', '3.16',
           'Results mapped to a known OBSCLIN_CODE with a known result',
           mapped_with_result, total,          9 FROM obs_counts
    UNION ALL
    SELECT 'OBS_CLIN', 'n/a',
           'Quantitative results',
           quant_with_code,   mapped_code,    10 FROM obs_counts
    UNION ALL
    SELECT 'OBS_CLIN', '3.17',
           'Quantitative results which specify the result unit',
           quant_with_unit,   quant_with_code, 11 FROM obs_counts
) sub
ORDER BY ROW_ORDER
