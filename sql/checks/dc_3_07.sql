-- DC 3.07: Encounters, diagnoses, or procedures in an ambulatory (AV), telehealth
-- (TH), emergency department (ED), ED to inpatient (EI), or inpatient (IP)
-- setting are less than 75% complete two months prior to the current month.
-- Data completeness is calculated by comparing actual volume to the average
-- volume during the previous year
-- Parameters: {{ current_schema }}, {{ cutoff_date }}, {{ report_month }}
WITH months AS (
    SELECT DATE_TRUNC('month', DATEADD(month, -seq, TO_DATE('{{ report_month }}'))) AS MONTH_START
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS seq FROM TABLE(GENERATOR(ROWCOUNT => 24)))
),
enc_monthly AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.ENCOUNTER
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY DATE_TRUNC('month', ADMIT_DATE)
),
combined AS (
    SELECT m.MONTH_START, COALESCE(e.CNT, 0) AS RECORD_COUNT
    FROM months m LEFT JOIN enc_monthly e ON e.MONTH_START = m.MONTH_START
),
with_avg AS (
    SELECT MONTH_START, RECORD_COUNT,
           AVG(RECORD_COUNT) OVER (ORDER BY MONTH_START ROWS BETWEEN 14 PRECEDING AND 3 PRECEDING) AS PRIOR_YEAR_AVG
    FROM combined
),
check_month AS (
    SELECT RECORD_COUNT, PRIOR_YEAR_AVG,
           CASE WHEN PRIOR_YEAR_AVG > 0 THEN ROUND(100.0 * RECORD_COUNT / PRIOR_YEAR_AVG, 1) ELSE NULL END AS COMPLETENESS_PCT
    FROM with_avg
    WHERE MONTH_START = DATEADD(month, -2, TO_DATE('{{ report_month }}'))
)
SELECT
    '3.07'                                                          AS CHECK_NUM,
    '< 75% completeness 2 months prior vs prior-year average'       AS DESCRIPTION,
    CASE WHEN COMPLETENESS_PCT IS NOT NULL AND COMPLETENESS_PCT < 75 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM check_month
