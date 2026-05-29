-- DC 3.07 | Table IVF | Data Completeness | Investigative
-- Encounters, diagnoses, or procedures in an ambulatory (AV), telehealth (TH), emergency department (ED),
-- ED to inpatient (EI), or inpatient (IP) setting are less than 75% complete two months prior to the
-- current month. Data completeness is calculated by comparing actual volume to the average volume
-- during the previous year
-- Parameters: {{ current_schema }}, {{ report_month }}
WITH months AS (
    SELECT DATE_TRUNC('month', DATEADD(month, -seq, TO_DATE('{{ report_month }}'))) AS MONTH_START
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS seq FROM TABLE(GENERATOR(ROWCOUNT => 24)))
),
enc_monthly AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY 1
),
dx_monthly AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY 1
),
px_monthly AS (
    SELECT DATE_TRUNC('month', PX_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.PROCEDURES
    WHERE ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY 1
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
               ROWS BETWEEN 14 PRECEDING AND 3 PRECEDING
           ) AS PRIOR_YEAR_AVG
    FROM combined
),
check_month AS (
    SELECT MAX(CASE
                   WHEN RECORD_COUNT = 0 THEN 1
                   WHEN PRIOR_YEAR_AVG > 0 AND RECORD_COUNT < 0.75 * PRIOR_YEAR_AVG THEN 1
                   ELSE 0
               END) AS HAS_EXCEPTION
    FROM with_avg
    WHERE MONTH_START = DATEADD(month, -2, TO_DATE('{{ report_month }}'))
)
SELECT
    '3.07'                                                                                          AS CHECK_NUM,
    'Encounters, diagnoses, or procedures < 75% complete two months prior to the current month'    AS DESCRIPTION,
    CASE WHEN HAS_EXCEPTION = 1 THEN 'Fail' ELSE 'Pass' END                                        AS STATUS
FROM check_month
