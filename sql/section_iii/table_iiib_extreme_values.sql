-- Table IIIB. Records With Extreme Values
-- Extreme low/high value counts for age, height, weight, BP, and dispensed days supply.
-- Supports DC 2.02 (> 10% in extreme categories). Exceptions highlighted in blue.

SELECT TABLE_NAME, FIELD_NAME, LOW_THRESHOLD, HIGH_THRESHOLD,
       TO_VARCHAR(RECORDS) AS RECORDS,
       TO_VARCHAR(N_LOW) AS N_LOW,
       TO_VARCHAR(ROUND(N_LOW * 100.0 / NULLIF(RECORDS, 0), 1)) || '%' AS PCT_LOW,
       TO_VARCHAR(N_HIGH) AS N_HIGH,
       TO_VARCHAR(ROUND(N_HIGH * 100.0 / NULLIF(RECORDS, 0), 1)) || '%' AS PCT_HIGH,
       TO_VARCHAR(MEDIAN_VAL) AS MEDIAN_VAL,
       SOURCE_TABLE
FROM (
    SELECT 'DEMOGRAPHIC' AS TABLE_NAME, 'AGE (years)' AS FIELD_NAME,
           '< 0' AS LOW_THRESHOLD, '> 89' AS HIGH_THRESHOLD,
           COUNT(*) AS RECORDS,
           SUM(CASE WHEN DATEDIFF('year', BIRTH_DATE, CURRENT_DATE) < 0 THEN 1 ELSE 0 END) AS N_LOW,
           SUM(CASE WHEN DATEDIFF('year', BIRTH_DATE, CURRENT_DATE) > 89 THEN 1 ELSE 0 END) AS N_HIGH,
           CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF('year', BIRTH_DATE, CURRENT_DATE)) AS INT) AS MEDIAN_VAL,
           'DEM_L3_AGEYRSDIST1' AS SOURCE_TABLE,
           1 AS ROW_ORDER
    FROM {{ current_schema }}.DEMOGRAPHIC
    WHERE BIRTH_DATE IS NOT NULL

    UNION ALL
    SELECT 'VITAL', 'HT (inches)',
           '< 21', '> 76',
           COUNT(*),
           SUM(CASE WHEN HT < 21 THEN 1 ELSE 0 END),
           SUM(CASE WHEN HT > 76 THEN 1 ELSE 0 END),
           CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY HT) AS INT),
           'VIT_L3_HT_DIST',
           2
    FROM {{ current_schema }}.VITAL
    WHERE HT IS NOT NULL AND MEASURE_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'VITAL', 'WT (lbs)',
           '< 0', '> 350',
           COUNT(*),
           SUM(CASE WHEN WT < 0 THEN 1 ELSE 0 END),
           SUM(CASE WHEN WT > 350 THEN 1 ELSE 0 END),
           CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY WT) AS INT),
           'VIT_L3_WT_DIST',
           3
    FROM {{ current_schema }}.VITAL
    WHERE WT IS NOT NULL AND MEASURE_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'VITAL', 'DIASTOLIC (mmHg)',
           '< 40', '> 120',
           COUNT(*),
           SUM(CASE WHEN DIASTOLIC < 40 THEN 1 ELSE 0 END),
           SUM(CASE WHEN DIASTOLIC > 120 THEN 1 ELSE 0 END),
           CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DIASTOLIC) AS INT),
           'VIT_L3_BP',
           4
    FROM {{ current_schema }}.VITAL
    WHERE DIASTOLIC IS NOT NULL AND MEASURE_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'VITAL', 'SYSTOLIC (mmHg)',
           '< 40', '> 210',
           COUNT(*),
           SUM(CASE WHEN SYSTOLIC < 40 THEN 1 ELSE 0 END),
           SUM(CASE WHEN SYSTOLIC > 210 THEN 1 ELSE 0 END),
           CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SYSTOLIC) AS INT),
           'VIT_L3_BP',
           5
    FROM {{ current_schema }}.VITAL
    WHERE SYSTOLIC IS NOT NULL AND MEASURE_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    SELECT 'DISPENSING', 'DISPENSE_SUP (days)',
           '< 1', '> 90',
           COUNT(*),
           SUM(CASE WHEN DISPENSE_SUP < 1 THEN 1 ELSE 0 END),
           SUM(CASE WHEN DISPENSE_SUP > 90 THEN 1 ELSE 0 END),
           CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DISPENSE_SUP) AS INT),
           'DISP_L3_SUP',
           6
    FROM {{ current_schema }}.DISPENSING
    WHERE DISPENSE_SUP IS NOT NULL AND DISPENSE_DATE >= TO_DATE('{{ start_date }}')

) sub
ORDER BY ROW_ORDER
