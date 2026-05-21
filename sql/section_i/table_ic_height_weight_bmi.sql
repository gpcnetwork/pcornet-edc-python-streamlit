-- Table IC. Height, Weight, and Body Mass Index (BMI)
-- Descriptive statistics and frequencies of VITAL measurements.

WITH bmi_total AS (
    SELECT COUNT(*) AS bmi_n FROM {{ current_schema }}.VITAL WHERE ORIGINAL_BMI IS NOT NULL
)
SELECT CATEGORY, GROUP_NAME, RESULT,
    CASE
        WHEN row_order IN (11, 12, 13) AND bmi_n != 0
            THEN TO_VARCHAR(ROUND((RESULT::FLOAT / bmi_n) * 100, 1)) || '%'
        ELSE ''
    END AS percentage
FROM (
    SELECT 'Height measurements' AS CATEGORY, '' AS GROUP_NAME, '' AS RESULT, 1 AS row_order UNION
    SELECT '', 'Records',               TO_VARCHAR(COUNT(*)),                                                                      2 FROM {{ current_schema }}.VITAL WHERE HT IS NOT NULL UNION
    SELECT '', 'Height (inches), mean', TO_VARCHAR(ROUND(AVG(HT), 0)),                                                             3 FROM {{ current_schema }}.VITAL WHERE HT IS NOT NULL UNION
    SELECT '', 'Height (inches), median',TO_VARCHAR(ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY HT), 0)),                    4 FROM {{ current_schema }}.VITAL WHERE HT IS NOT NULL UNION
    SELECT 'Weight measurements', '', '', 5 UNION
    SELECT '', 'Records',               TO_VARCHAR(COUNT(*)),                                                                      6 FROM {{ current_schema }}.VITAL WHERE WT IS NOT NULL UNION
    SELECT '', 'Weight (lbs.), mean',   TO_VARCHAR(ROUND(AVG(WT), 0)),                                                             7 FROM {{ current_schema }}.VITAL WHERE WT IS NOT NULL UNION
    SELECT '', 'Weight (lbs.), median', TO_VARCHAR(ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY WT), 0)),                    8 FROM {{ current_schema }}.VITAL WHERE WT IS NOT NULL UNION
    SELECT 'Body Mass Index (BMI) measurements', '', '', 9 UNION
    SELECT '', 'Records',  TO_VARCHAR(bmi_n), 10 FROM bmi_total UNION
    SELECT '', 'BMI <=25', TO_VARCHAR(COUNT(*)), 11 FROM {{ current_schema }}.VITAL WHERE ORIGINAL_BMI IS NOT NULL AND ORIGINAL_BMI <= 25 UNION
    SELECT '', 'BMI 26-30',TO_VARCHAR(COUNT(*)), 12 FROM {{ current_schema }}.VITAL WHERE ORIGINAL_BMI BETWEEN 26 AND 30 UNION
    SELECT '', 'BMI >=31', TO_VARCHAR(COUNT(*)), 13 FROM {{ current_schema }}.VITAL WHERE ORIGINAL_BMI >= 31
) subq, bmi_total
ORDER BY row_order
