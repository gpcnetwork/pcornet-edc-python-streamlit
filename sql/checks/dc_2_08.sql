-- DC 2.08: The monthly volume of encounter, diagnosis, procedure, vital, prescribing,
-- medication administration, external medications, laboratory records, or
-- patient-reported outcomes is an outlier. Outliers are defined as months with
-- 0 records or a significant decrease compared to the average volume in the
-- previous 12 months. Encounters, diagnoses, and procedures are limited to
-- ambulatory (AV), telehealth (TH), emergency department (ED), ED to
-- inpatient (EI), or inpatient (IP) settings.
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH monthly AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.ENCOUNTER
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY DATE_TRUNC('month', ADMIT_DATE)
),
with_avg AS (
    SELECT MONTH_START, CNT,
           AVG(CNT) OVER (ORDER BY MONTH_START ROWS BETWEEN 13 PRECEDING AND 2 PRECEDING) AS PRIOR_12_AVG
    FROM monthly
),
outliers AS (
    SELECT COUNT(*) AS N
    FROM with_avg
    WHERE CNT = 0 OR (PRIOR_12_AVG > 0 AND CNT < 0.5 * PRIOR_12_AVG)
)
SELECT
    '2.08'                                                              AS CHECK_NUM,
    'Monthly volume = 0 or significant decrease vs prior 12-month avg'  AS DESCRIPTION,
    CASE WHEN N > 0 THEN 'Fail' ELSE 'Pass' END                         AS STATUS
FROM outliers
