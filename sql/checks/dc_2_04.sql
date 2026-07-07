-- DC 2.04 | Table IIID | Data Plausibility | Investigative
-- The average number of encounters per visit is > 2.0 for inpatient (IP), emergency department (ED),
-- or ED to inpatient (EI) encounters. A visit is defined as a unique combination of PATID, ENC_TYPE,
-- ADMIT_DATE, and PROVIDERID in the ENCOUNTER table
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH visits AS (
    SELECT PATID, ENC_TYPE, ADMIT_DATE, PROVIDERID, COUNT(*) AS ENC_COUNT
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ENC_TYPE IN ('IP','ED','EI')
      AND ADMIT_DATE >= TO_DATE('{{ start_date }}') AND ADMIT_DATE <= TO_DATE('{{ end_date }}')
    GROUP BY PATID, ENC_TYPE, ADMIT_DATE, PROVIDERID
),
avg_enc AS (SELECT ROUND(AVG(ENC_COUNT), 2) AS AVG_ENC_PER_VISIT FROM visits)
SELECT
    '2.04'                                                AS CHECK_NUM,
    'The average number of encounters per visit is > 2.0 for IP/ED/EI' AS DESCRIPTION,
    CASE WHEN AVG_ENC_PER_VISIT > 2.0 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM avg_enc
