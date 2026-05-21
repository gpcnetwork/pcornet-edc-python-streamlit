-- Table IVF. Data Latency and Completeness of Encounter, Diagnoses, and Procedures, Past 2 Years
-- This table includes ENCOUNTER, DIAGNOSIS, and PROCEDURES from the most recent 24 month period; month -0 is the month the data curation query was run.
-- Data completeness is determined by comparing the actual volume to the expected volume in each month. Expected volume is determined by taking the average volume
-- during the benchmark period of months -12 to month -23. Data completeness is reported as a percentage of the benchmark average. Temporal differences may be
-- affected by data availability, ETL processes, date shifting, secular trends, and/or changes in data provenance.
-- These data support Data Check 3.07 (encounters, diagnoses, or procedures in an ambulatory (AV), telehealth (TH), emergency department (ED), ED to inpatient (EI),
-- or inpatient (IP) setting are less than 75% complete two months prior to the current month). Data check exceptions occur if the month -2 result is <75% of the
-- benchmark average or 0 records. Data check exceptions are highlighted in blue. Data check exceptions and unexpected results (e.g. significant discrepancies in data
-- completeness between the tables) should be investigated and explained in the ETL ADD.

WITH months AS (
    SELECT DATE_TRUNC('month', DATEADD(month, -seq, TO_DATE('{{ report_month }}'))) AS MONTH_START
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS seq FROM TABLE(GENERATOR(ROWCOUNT => 24)))
),
enc_monthly AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY DATE_TRUNC('month', ADMIT_DATE)
),
dx_monthly AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY DATE_TRUNC('month', ADMIT_DATE)
),
px_monthly AS (
    SELECT DATE_TRUNC('month', PX_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.PROCEDURES
    WHERE ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY DATE_TRUNC('month', PX_DATE)
),
combined AS (
    SELECT 'ENCOUNTER' AS DOMAIN, m.MONTH_START, COALESCE(d.CNT, 0) AS RECORD_COUNT
    FROM months m LEFT JOIN enc_monthly d ON d.MONTH_START = m.MONTH_START
    UNION ALL
    SELECT 'DIAGNOSIS', m.MONTH_START, COALESCE(d.CNT, 0)
    FROM months m LEFT JOIN dx_monthly d ON d.MONTH_START = m.MONTH_START
    UNION ALL
    SELECT 'PROCEDURES', m.MONTH_START, COALESCE(d.CNT, 0)
    FROM months m LEFT JOIN px_monthly d ON d.MONTH_START = m.MONTH_START
),
with_avg AS (
    SELECT DOMAIN, MONTH_START, RECORD_COUNT,
           AVG(RECORD_COUNT) OVER (
               PARTITION BY DOMAIN
               ORDER BY MONTH_START
               ROWS BETWEEN 13 PRECEDING AND 2 PRECEDING
           ) AS PRIOR_YEAR_AVG
    FROM combined
)
SELECT DOMAIN, MONTH_START, RECORD_COUNT, ROUND(PRIOR_YEAR_AVG, 0) AS PRIOR_YEAR_AVG,
    CASE WHEN PRIOR_YEAR_AVG > 0
         THEN ROUND(100.0 * RECORD_COUNT / PRIOR_YEAR_AVG, 1)
         ELSE NULL END AS COMPLETENESS_PCT
FROM with_avg
ORDER BY DOMAIN, MONTH_START
