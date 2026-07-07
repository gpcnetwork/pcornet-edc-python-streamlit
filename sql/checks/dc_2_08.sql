-- DC 2.08 | Table IIIG | Data Plausibility | Investigative
-- The monthly volume of encounter, diagnosis, procedure, vital, prescribing, medication administration,
-- external medications, laboratory records, or patient-reported outcomes is an outlier. Outliers are defined
-- as months with 0 records or a significant decrease compared to the average volume in the previous 12 months.
-- Encounters, diagnoses, and procedures are limited to AV, TH, ED, EI, or IP settings.
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH enc_monthly AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') AND ADMIT_DATE <= TO_DATE('{{ end_date }}') AND ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY 1
),
dx_monthly AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') AND ADMIT_DATE <= TO_DATE('{{ end_date }}') AND ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY 1
),
px_monthly AS (
    SELECT DATE_TRUNC('month', PX_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.PROCEDURES
    WHERE PX_DATE >= TO_DATE('{{ start_date }}') AND PX_DATE <= TO_DATE('{{ end_date }}') AND ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY 1
),
vital_monthly AS (
    SELECT DATE_TRUNC('month', MEASURE_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.VITAL
    WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}') AND MEASURE_DATE <= TO_DATE('{{ end_date }}')
    GROUP BY 1
),
rx_monthly AS (
    SELECT DATE_TRUNC('month', RX_ORDER_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.PRESCRIBING
    WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}') AND RX_ORDER_DATE <= TO_DATE('{{ end_date }}')
    GROUP BY 1
),
lab_monthly AS (
    SELECT DATE_TRUNC('month', RESULT_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE >= TO_DATE('{{ start_date }}') AND RESULT_DATE <= TO_DATE('{{ end_date }}')
    GROUP BY 1
),
medadmin_monthly AS (
    SELECT DATE_TRUNC('month', MEDADMIN_START_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.MED_ADMIN
    WHERE MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}') AND MEDADMIN_START_DATE <= TO_DATE('{{ end_date }}')
    GROUP BY 1
),
all_domains AS (
    SELECT 'ENCOUNTER'    AS DOMAIN, MONTH_START, CNT FROM enc_monthly     UNION ALL
    SELECT 'DIAGNOSIS',              MONTH_START, CNT FROM dx_monthly      UNION ALL
    SELECT 'PROCEDURES',             MONTH_START, CNT FROM px_monthly      UNION ALL
    SELECT 'VITAL',                  MONTH_START, CNT FROM vital_monthly   UNION ALL
    SELECT 'PRESCRIBING',            MONTH_START, CNT FROM rx_monthly      UNION ALL
    SELECT 'LAB_RESULT_CM',          MONTH_START, CNT FROM lab_monthly     UNION ALL
    SELECT 'MED_ADMIN',              MONTH_START, CNT FROM medadmin_monthly
),
with_avg AS (
    SELECT DOMAIN, MONTH_START, CNT,
           AVG(CNT) OVER (PARTITION BY DOMAIN ORDER BY MONTH_START ROWS BETWEEN 13 PRECEDING AND 2 PRECEDING) AS PRIOR_12_AVG
    FROM all_domains
),
outliers AS (
    SELECT COUNT(*) AS N
    FROM with_avg
    WHERE CNT = 0 OR (PRIOR_12_AVG > 0 AND CNT < 0.5 * PRIOR_12_AVG)
)
SELECT
    '2.08'                                                                      AS CHECK_NUM,
    'Monthly record volume is 0 or significant decrease vs prior 12-month avg'  AS DESCRIPTION,
    CASE WHEN N > 0 THEN 'Fail' ELSE 'Pass' END                                 AS STATUS
FROM outliers
