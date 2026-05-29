-- Table IIIG. Monthly Record Volume Outliers, Selected Domains
-- DC 2.08: months where RECORDS = 0 OR Difference Ratio ≤ -7.0.
-- ^ Evaluation Window: most recent 12 months are excluded (covered by DC 3.07/3.11); months
--   prior to the 5th percentile of the domain date field are excluded (early-period instability).
-- ^^ Difference Ratio = (volume in current month − avg volume in previous 12 months)
--    / standard deviation during the previous 12 months.
-- Also excluded: AVG_PREV_12MO < 500, STDDEV_PREV_12MO = 0, or fewer than 12 prior months.
-- Parameters: {{ current_schema }}, {{ start_date }}, {{ report_month }}

WITH
-- Monthly spine: all months from start_date through report_month
all_months AS (
    SELECT DATE_TRUNC('month', DATEADD(month, -seq, TO_DATE('{{ report_month }}'))) AS MONTH_START
    FROM (
        SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS seq
        FROM TABLE(GENERATOR(ROWCOUNT => 60))
    )
    WHERE DATE_TRUNC('month', DATEADD(month, -seq, TO_DATE('{{ report_month }}')))
          >= TO_DATE('{{ start_date }}')
),
eval_upper AS (
    SELECT DATEADD(month, -12, DATE_TRUNC('month', TO_DATE('{{ report_month }}'))) AS EVAL_END
),
-- 5th-percentile month per (table, enc_type) — lower bound of evaluation window
pctile AS (
    SELECT 'ENCOUNTER' AS TABLE_NAME, ENC_TYPE,
           DATE_TRUNC('month', DATEADD('day',
               PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY DATEDIFF('day', '1970-01-01'::DATE, ADMIT_DATE)),
               '1970-01-01'::DATE)) AS P05
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE IS NOT NULL
      AND ADMIT_DATE >= DATEADD(year, -20, TO_DATE('{{ report_month }}'))
      AND ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY ENC_TYPE
    UNION ALL
    SELECT 'DIAGNOSIS', ENC_TYPE,
           DATE_TRUNC('month', DATEADD('day',
               PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY DATEDIFF('day', '1970-01-01'::DATE, ADMIT_DATE)),
               '1970-01-01'::DATE))
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE IS NOT NULL
      AND ADMIT_DATE >= DATEADD(year, -20, TO_DATE('{{ report_month }}'))
      AND ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY ENC_TYPE
    UNION ALL
    SELECT 'PROCEDURES', ENC_TYPE,
           DATE_TRUNC('month', DATEADD('day',
               PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY DATEDIFF('day', '1970-01-01'::DATE, PX_DATE)),
               '1970-01-01'::DATE))
    FROM {{ current_schema }}.PROCEDURES
    WHERE PX_DATE IS NOT NULL
      AND PX_DATE >= DATEADD(year, -20, TO_DATE('{{ report_month }}'))
      AND ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY ENC_TYPE
    UNION ALL
    SELECT 'VITAL', '',
           DATE_TRUNC('month', DATEADD('day',
               PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY DATEDIFF('day', '1970-01-01'::DATE, MEASURE_DATE)),
               '1970-01-01'::DATE))
    FROM {{ current_schema }}.VITAL
    WHERE MEASURE_DATE IS NOT NULL
      AND MEASURE_DATE >= DATEADD(year, -20, TO_DATE('{{ report_month }}'))
    UNION ALL
    SELECT 'PRESCRIBING', '',
           DATE_TRUNC('month', DATEADD('day',
               PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY DATEDIFF('day', '1970-01-01'::DATE, RX_ORDER_DATE)),
               '1970-01-01'::DATE))
    FROM {{ current_schema }}.PRESCRIBING
    WHERE RX_ORDER_DATE IS NOT NULL
      AND RX_ORDER_DATE >= DATEADD(year, -20, TO_DATE('{{ report_month }}'))
    UNION ALL
    SELECT 'LAB_RESULT_CM', '',
           DATE_TRUNC('month', DATEADD('day',
               PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY DATEDIFF('day', '1970-01-01'::DATE, RESULT_DATE)),
               '1970-01-01'::DATE))
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE IS NOT NULL
      AND RESULT_DATE >= DATEADD(year, -20, TO_DATE('{{ report_month }}'))
    UNION ALL
    SELECT 'MED_ADMIN', '',
           DATE_TRUNC('month', DATEADD('day',
               PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY DATEDIFF('day', '1970-01-01'::DATE, MEDADMIN_START_DATE)),
               '1970-01-01'::DATE))
    FROM {{ current_schema }}.MED_ADMIN
    WHERE MEDADMIN_START_DATE IS NOT NULL
      AND MEDADMIN_START_DATE >= DATEADD(year, -20, TO_DATE('{{ report_month }}'))
),
-- Monthly counts per domain
enc_monthly AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS MONTH_START, ENC_TYPE, COUNT(*) AS CNT
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE IS NOT NULL
      AND ADMIT_DATE >= DATEADD(month, -72, TO_DATE('{{ report_month }}'))
      AND ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY 1, 2
),
diag_monthly AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS MONTH_START, ENC_TYPE, COUNT(*) AS CNT
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE IS NOT NULL
      AND ADMIT_DATE >= DATEADD(month, -72, TO_DATE('{{ report_month }}'))
      AND ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY 1, 2
),
px_monthly AS (
    SELECT DATE_TRUNC('month', PX_DATE) AS MONTH_START, ENC_TYPE, COUNT(*) AS CNT
    FROM {{ current_schema }}.PROCEDURES
    WHERE PX_DATE IS NOT NULL
      AND PX_DATE >= DATEADD(month, -72, TO_DATE('{{ report_month }}'))
      AND ENC_TYPE IN ('AV','TH','ED','EI','IP')
    GROUP BY 1, 2
),
vital_monthly AS (
    SELECT DATE_TRUNC('month', MEASURE_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.VITAL
    WHERE MEASURE_DATE IS NOT NULL
      AND MEASURE_DATE >= DATEADD(month, -72, TO_DATE('{{ report_month }}'))
    GROUP BY 1
),
rx_monthly AS (
    SELECT DATE_TRUNC('month', RX_ORDER_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.PRESCRIBING
    WHERE RX_ORDER_DATE IS NOT NULL
      AND RX_ORDER_DATE >= DATEADD(month, -72, TO_DATE('{{ report_month }}'))
    GROUP BY 1
),
lab_monthly AS (
    SELECT DATE_TRUNC('month', RESULT_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE IS NOT NULL
      AND RESULT_DATE >= DATEADD(month, -72, TO_DATE('{{ report_month }}'))
    GROUP BY 1
),
medadmin_monthly AS (
    SELECT DATE_TRUNC('month', MEDADMIN_START_DATE) AS MONTH_START, COUNT(*) AS CNT
    FROM {{ current_schema }}.MED_ADMIN
    WHERE MEDADMIN_START_DATE IS NOT NULL
      AND MEDADMIN_START_DATE >= DATEADD(month, -72, TO_DATE('{{ report_month }}'))
    GROUP BY 1
),
-- Full spine: all months × all (table, enc_type) combos, 0-filled
spine AS (
    SELECT t.MONTH_START, 'ENCOUNTER' AS TABLE_NAME, et.ENC_TYPE,
           COALESCE(e.CNT, 0) AS RECORDS
    FROM all_months t
    CROSS JOIN (SELECT 'AV' AS ENC_TYPE UNION ALL SELECT 'TH' UNION ALL SELECT 'ED'
                UNION ALL SELECT 'EI' UNION ALL SELECT 'IP') et
    LEFT JOIN enc_monthly e ON e.MONTH_START = t.MONTH_START AND e.ENC_TYPE = et.ENC_TYPE
    UNION ALL
    SELECT t.MONTH_START, 'DIAGNOSIS', et.ENC_TYPE, COALESCE(d.CNT, 0)
    FROM all_months t
    CROSS JOIN (SELECT 'AV' AS ENC_TYPE UNION ALL SELECT 'TH' UNION ALL SELECT 'ED'
                UNION ALL SELECT 'EI' UNION ALL SELECT 'IP') et
    LEFT JOIN diag_monthly d ON d.MONTH_START = t.MONTH_START AND d.ENC_TYPE = et.ENC_TYPE
    UNION ALL
    SELECT t.MONTH_START, 'PROCEDURES', et.ENC_TYPE, COALESCE(p.CNT, 0)
    FROM all_months t
    CROSS JOIN (SELECT 'AV' AS ENC_TYPE UNION ALL SELECT 'TH' UNION ALL SELECT 'ED'
                UNION ALL SELECT 'EI' UNION ALL SELECT 'IP') et
    LEFT JOIN px_monthly p ON p.MONTH_START = t.MONTH_START AND p.ENC_TYPE = et.ENC_TYPE
    UNION ALL
    SELECT t.MONTH_START, 'VITAL', '', COALESCE(v.CNT, 0)
    FROM all_months t LEFT JOIN vital_monthly v ON v.MONTH_START = t.MONTH_START
    UNION ALL
    SELECT t.MONTH_START, 'PRESCRIBING', '', COALESCE(r.CNT, 0)
    FROM all_months t LEFT JOIN rx_monthly r ON r.MONTH_START = t.MONTH_START
    UNION ALL
    SELECT t.MONTH_START, 'LAB_RESULT_CM', '', COALESCE(l.CNT, 0)
    FROM all_months t LEFT JOIN lab_monthly l ON l.MONTH_START = t.MONTH_START
    UNION ALL
    SELECT t.MONTH_START, 'MED_ADMIN', '', COALESCE(m.CNT, 0)
    FROM all_months t LEFT JOIN medadmin_monthly m ON m.MONTH_START = t.MONTH_START
),
-- Rolling 12-month prior stats per (table, enc_type)
with_stats AS (
    SELECT TABLE_NAME, ENC_TYPE, MONTH_START, RECORDS,
           AVG(RECORDS) OVER (
               PARTITION BY TABLE_NAME, ENC_TYPE ORDER BY MONTH_START
               ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
           ) AS AVG_12,
           STDDEV(RECORDS) OVER (
               PARTITION BY TABLE_NAME, ENC_TYPE ORDER BY MONTH_START
               ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
           ) AS STD_12,
           COUNT(*) OVER (
               PARTITION BY TABLE_NAME, ENC_TYPE ORDER BY MONTH_START
               ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
           ) AS N_PREV
    FROM spine
),
outliers AS (
    SELECT ws.TABLE_NAME,
           TO_VARCHAR(p.P05, 'YYYY-MM') || ' to ' || TO_VARCHAR(eu.EVAL_END, 'YYYY-MM') AS EVAL_WINDOW,
           ws.ENC_TYPE,
           ws.MONTH_START                                                AS EXCEPTION_MONTH,
           ws.RECORDS,
           ROUND(ws.AVG_12, 0)                                          AS AVG_PREV_12MO,
           ROUND(ws.STD_12, 2)                                          AS STDDEV_PREV_12MO,
           ROUND((ws.RECORDS - ws.AVG_12) / NULLIF(ws.STD_12, 0), 2)  AS DIFF_RATIO
    FROM with_stats ws
    JOIN pctile p ON p.TABLE_NAME = ws.TABLE_NAME AND p.ENC_TYPE = ws.ENC_TYPE
    CROSS JOIN eval_upper eu
    WHERE ws.N_PREV = 12
      AND ws.MONTH_START >= p.P05
      AND ws.MONTH_START <= eu.EVAL_END
      AND ws.AVG_12 >= 500
      AND ws.STD_12 > 0
      AND (ws.RECORDS = 0 OR (ws.RECORDS - ws.AVG_12) / NULLIF(ws.STD_12, 0) <= -7.0)
)
SELECT TABLE_NAME                                    AS "Table",
       EVAL_WINDOW                                    AS "Evaluation Window^",
       ENC_TYPE                                       AS "Encounter Type",
       TO_VARCHAR(EXCEPTION_MONTH, 'YYYY-MM')         AS "Exception Month",
       TO_VARCHAR(RECORDS)                            AS "Records",
       TO_VARCHAR(AVG_PREV_12MO)                      AS "Average Records in the Previous 12 Months",
       TO_VARCHAR(STDDEV_PREV_12MO)                   AS "Standard Deviation in the Previous 12 Months",
       TO_VARCHAR(DIFF_RATIO)                         AS "Difference Ratio^^"
FROM outliers
ORDER BY
    CASE "Table"
        WHEN 'ENCOUNTER'     THEN 1 WHEN 'DIAGNOSIS'     THEN 2
        WHEN 'PROCEDURES'    THEN 3 WHEN 'VITAL'         THEN 4
        WHEN 'PRESCRIBING'   THEN 5 WHEN 'LAB_RESULT_CM' THEN 6
        WHEN 'MED_ADMIN'     THEN 7 ELSE 8
    END,
    "Encounter Type", "Exception Month"
