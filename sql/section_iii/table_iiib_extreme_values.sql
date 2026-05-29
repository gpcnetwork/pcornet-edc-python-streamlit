-- Table IIIB. Records With Extreme Values
-- Extreme low/high value counts for age, height, weight, BP, and dispensed days supply.
-- Supports DC 2.02 (> 10% in extreme categories). Exceptions highlighted in blue.

SELECT TABLE_NAME  AS "Table",
       FIELD_NAME  AS "Field",
       LOW_THRESHOLD  AS "Data Check Parameters__Low",
       HIGH_THRESHOLD AS "Data Check Parameters__High",
       TO_VARCHAR(RECORDS)   AS "Records",
       TO_VARCHAR(N_LOW)     AS "Records with values in the lowest category__N",
       TO_VARCHAR(ROUND(N_LOW  * 100.0 / NULLIF(RECORDS, 0), 1)) || '%' AS "Records with values in the lowest category__%",
       TO_VARCHAR(N_HIGH)    AS "Records with values in the highest category__N",
       TO_VARCHAR(ROUND(N_HIGH * 100.0 / NULLIF(RECORDS, 0), 1)) || '%' AS "Records with values in the highest category__%",
       TO_VARCHAR(MEDIAN_VAL) AS "Median"
FROM (
    SELECT 'DEMOGRAPHIC' AS TABLE_NAME, 'AGE (years)' AS FIELD_NAME,
           '< 0' AS LOW_THRESHOLD, '> 89' AS HIGH_THRESHOLD,
           COUNT(*) AS RECORDS,
           SUM(CASE WHEN DATEDIFF('year', BIRTH_DATE, CURRENT_DATE) < 0 THEN 1 ELSE 0 END) AS N_LOW,
           SUM(CASE WHEN DATEDIFF('year', BIRTH_DATE, CURRENT_DATE) > 89 THEN 1 ELSE 0 END) AS N_HIGH,
           CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF('year', BIRTH_DATE, CURRENT_DATE)) AS INT) AS MEDIAN_VAL,
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
           6
    FROM {{ current_schema }}.DISPENSING
    WHERE DISPENSE_SUP IS NOT NULL AND DISPENSE_DATE >= TO_DATE('{{ start_date }}')

) sub
ORDER BY ROW_ORDER
