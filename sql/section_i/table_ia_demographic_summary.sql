-- Table IA. Demographic Summary
-- General descriptive information about patients in the DEMOGRAPHIC table.
-- Patients may or may not be represented in other CDM tables.

WITH total AS (
    SELECT COUNT(*) AS total_patients FROM {{ current_schema }}.demographic
),
least_encounter AS (
    SELECT COUNT(DISTINCT patid) AS total_patients_with_enc
    FROM {{ current_schema }}.encounter WHERE ADMIT_DATE > '2011-12-01'
)
SELECT CATEGORY, GROUP_NAME, N,
    CASE
        WHEN N != '' AND CATEGORY NOT IN ('Patients') AND GROUP_NAME NOT IN ('Mean', 'Median')
             AND row_order NOT IN (25, 26, 27) AND total_patients != 0
            THEN TO_VARCHAR(ROUND((N::FLOAT / total_patients) * 100, 1)) || '%'
        WHEN row_order IN (25, 26, 27) AND total_patients_with_enc != 0
            THEN TO_VARCHAR(ROUND((N::FLOAT / total_patients_with_enc) * 100, 1)) || '%'
        ELSE ''
    END AS percentage
FROM (
    SELECT 'Patients' AS CATEGORY, '' AS GROUP_NAME, TO_VARCHAR(COUNT(*)) AS N, 1 AS row_order FROM {{ current_schema }}.demographic UNION
    SELECT 'Age', '', '', 2 UNION
    SELECT '', 'Mean',   TO_VARCHAR(CAST(AVG(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE)) AS INT)), 3 FROM {{ current_schema }}.demographic UNION
    SELECT '', 'Median', TO_VARCHAR(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE)) AS INT)), 4 FROM {{ current_schema }}.demographic UNION
    SELECT 'Age group', '', '', 5 UNION
    SELECT '', '0-4',     TO_VARCHAR(COUNT(*)), 6  FROM {{ current_schema }}.demographic WHERE CAST(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE) AS INT) <= 4 UNION
    SELECT '', '5-14',    TO_VARCHAR(COUNT(*)), 7  FROM {{ current_schema }}.demographic WHERE CAST(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE) AS INT) > 4  AND CAST(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE) AS INT) <= 14 UNION
    SELECT '', '15-21',   TO_VARCHAR(COUNT(*)), 8  FROM {{ current_schema }}.demographic WHERE CAST(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE) AS INT) > 14 AND CAST(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE) AS INT) <= 21 UNION
    SELECT '', '22-64',   TO_VARCHAR(COUNT(*)), 9  FROM {{ current_schema }}.demographic WHERE CAST(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE) AS INT) > 21 AND CAST(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE) AS INT) <= 64 UNION
    SELECT '', '65+',     TO_VARCHAR(COUNT(*)), 10 FROM {{ current_schema }}.demographic WHERE CAST(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE) AS INT) > 64 UNION
    SELECT '', 'Missing', TO_VARCHAR(COUNT(*)), 11 FROM {{ current_schema }}.demographic WHERE BIRTH_DATE IS NULL UNION
    SELECT 'Hispanic', '', '', 12 UNION
    SELECT '', 'N (No)',             TO_VARCHAR(COUNT(*)), 13 FROM {{ current_schema }}.demographic WHERE HISPANIC = 'N' UNION
    SELECT '', 'Y (Yes)',            TO_VARCHAR(COUNT(*)), 14 FROM {{ current_schema }}.demographic WHERE HISPANIC = 'Y' UNION
    SELECT '', 'Missing or Refused', TO_VARCHAR(COUNT(*)), 15 FROM {{ current_schema }}.demographic WHERE HISPANIC = 'R' OR HISPANIC IS NULL UNION
    SELECT 'Sex', '', '', 16 UNION
    SELECT '', 'F (Female)',          TO_VARCHAR(COUNT(*)), 17 FROM {{ current_schema }}.demographic WHERE SEX = 'F' UNION
    SELECT '', 'M (Male)',            TO_VARCHAR(COUNT(*)), 18 FROM {{ current_schema }}.demographic WHERE SEX = 'M' UNION
    SELECT '', 'Missing or Ambiguous',TO_VARCHAR(COUNT(*)), 19 FROM {{ current_schema }}.demographic WHERE SEX IS NULL UNION
    SELECT 'Race', '', '', 20 UNION
    SELECT '', 'White',              TO_VARCHAR(COUNT(*)), 21 FROM {{ current_schema }}.demographic WHERE RACE = '05' UNION
    SELECT '', 'Non-White',          TO_VARCHAR(COUNT(*)), 22 FROM {{ current_schema }}.demographic WHERE RACE IN ('01','02','03','04','06') UNION
    SELECT '', 'Missing or Refused', TO_VARCHAR(COUNT(*)), 23 FROM {{ current_schema }}.demographic WHERE RACE IN ('07','NI','UN','OT') OR RACE IS NULL UNION
    SELECT 'Race among patients with at least 1 encounter after December 2011', '', '', 24 UNION
    SELECT '', 'White',              TO_VARCHAR(COUNT(*)), 25 FROM {{ current_schema }}.demographic WHERE RACE = '05'                                    AND patid IN (SELECT DISTINCT patid FROM {{ current_schema }}.encounter WHERE ADMIT_DATE > '2011-12-01') UNION
    SELECT '', 'Non-White',          TO_VARCHAR(COUNT(*)), 26 FROM {{ current_schema }}.demographic WHERE RACE IN ('01','02','03','04','06')             AND patid IN (SELECT DISTINCT patid FROM {{ current_schema }}.encounter WHERE ADMIT_DATE > '2011-12-01') UNION
    SELECT '', 'Missing or Refused', TO_VARCHAR(COUNT(*)), 27 FROM {{ current_schema }}.demographic WHERE (RACE IN ('07','NI','UN','OT') OR RACE IS NULL) AND patid IN (SELECT DISTINCT patid FROM {{ current_schema }}.encounter WHERE ADMIT_DATE > '2011-12-01') UNION
    SELECT 'Gender Identity', '', '', 28 UNION
    SELECT '', 'GQ (Genderqueer/Non-Binary)',  TO_VARCHAR(COUNT(*)), 29 FROM {{ current_schema }}.demographic WHERE GENDER_IDENTITY = 'GQ' UNION
    SELECT '', 'M (Man)',                      TO_VARCHAR(COUNT(*)), 30 FROM {{ current_schema }}.demographic WHERE GENDER_IDENTITY = 'M'  UNION
    SELECT '', 'W (Woman)',                    TO_VARCHAR(COUNT(*)), 31 FROM {{ current_schema }}.demographic WHERE GENDER_IDENTITY = 'W'  UNION
    SELECT '', 'MU (Multiple gender categories), SE (Something else),TF (Transgender female/Trans woman/Male-to-female), or TM (Transgender male/Trans man/Female-to-male)',
               TO_VARCHAR(COUNT(*)), 32 FROM {{ current_schema }}.demographic WHERE GENDER_IDENTITY IN ('MU','SE','TF','TM') UNION
    SELECT '', 'Missing or Refused', TO_VARCHAR(COUNT(*)), 33 FROM {{ current_schema }}.demographic WHERE GENDER_IDENTITY IN ('DC','NI','UN','OT') OR GENDER_IDENTITY IS NULL UNION
    SELECT 'Sexual Orientation', '', '', 34 UNION
    SELECT '', 'Bisexual', TO_VARCHAR(COUNT(*)), 35 FROM {{ current_schema }}.demographic WHERE SEXUAL_ORIENTATION = 'BI' UNION
    SELECT '', 'Gay',      TO_VARCHAR(COUNT(*)), 36 FROM {{ current_schema }}.demographic WHERE SEXUAL_ORIENTATION = 'GA' UNION
    SELECT '', 'Lesbian',  TO_VARCHAR(COUNT(*)), 35 FROM {{ current_schema }}.demographic WHERE SEXUAL_ORIENTATION = 'LE' UNION
    SELECT '', 'Queer',    TO_VARCHAR(COUNT(*)), 36 FROM {{ current_schema }}.demographic WHERE SEXUAL_ORIENTATION = 'QU' UNION
    SELECT '', 'Straight', TO_VARCHAR(COUNT(*)), 35 FROM {{ current_schema }}.demographic WHERE SEXUAL_ORIENTATION = 'ST' UNION
    SELECT '', 'AS (Asexual), MU (Multiple sexual orientations),SE (Something else), QS (Questioning)',
               TO_VARCHAR(COUNT(*)), 36 FROM {{ current_schema }}.demographic WHERE SEXUAL_ORIENTATION IN ('AS','MU','SE','QS') UNION
    SELECT '', 'Missing or Refused', TO_VARCHAR(COUNT(*)), 37 FROM {{ current_schema }}.demographic WHERE SEXUAL_ORIENTATION IN ('DC','NI','UN','OT') OR SEXUAL_ORIENTATION IS NULL
) subquery, total, least_encounter
ORDER BY row_order
