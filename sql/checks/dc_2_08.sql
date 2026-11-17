-- DC 2.08 | Table IIIG | Data Plausibility | Investigative
-- The monthly volume of encounter, diagnosis, procedure, vital, prescribing, medication
-- administration, external medication, laboratory, or patient-reported outcome records is an
-- outlier: the month has 0 records, or sits more than 7 standard deviations BELOW the moving
-- average of the 12 preceding months.
--
-- SAS source of truth (v7.03): edc_report.sas 6481-6705 (Table IIIG, row=2.08):
--   6489-6490: &nback=12, &nsd=-7.
--   6532-6551: monthly RECORD_N series from ENC_L3_ENCTYPE_ADATE_YM / DIA_L3_ENCTYPE_ADATE_YM /
--              PRO_L3_ENCTYPE_ADATE_YM (one series PER ENC_TYPE, limited to AV, ED, IP, EI, TH;
--              month = ADMIT_DATE for all three, incl. PROCEDURES) and VIT_L3_MDATE_YM
--              (MEASURE_DATE), PRES_L3_ODATE_YM (RX_ORDER_DATE), LAB_L3_RDATE_YM (RESULT_DATE),
--              MEDADM_L3_SDATE_YM (MEDADMIN_START_DATE), EXTMED_L3_RDATE_YM (EXT_RECORD_DATE),
--              PROCM_L3_PDATE_YM (PRO_DATE).
--   6565-6568: PROC EXPAND movave/movstd 12 LAG 1, BY dc_name (and ENC_TYPE category for
--              ENCOUNTER/DIAGNOSIS/PROCEDURES): lag_avg / lag_std = mean / POPULATION std-dev
--              (PROC EXPAND MOVVAR divides by n) of the 12 OBSERVATIONS before the current row
--              (fewer at the start of a series; first row missing). No ID statement, so the
--              window is over consecutive observed rows, not calendar months.
--   6498-6514: p5_date per table = month of the P5 of the table's date field (XTBL_L3_DATES);
--              months <= P5 month are dropped.
--   6520-6529: month11_date = first of month 11 months before RESPONSE_DATE; months >=
--              month11_date are dropped (so only months at least 12 months old are evaluated).
--   6610-6611: also drop months with lag_avg < 500 (or missing) or lag_std <= 0 (or missing),
--              and Mar 2020 / Apr 2020.
--   6613-6617: dsd = (resultn - lag_avg) / lag_std; outlier month if resultn = 0 OR dsd < -7.
--   6653-6672: exception (row=2.08) if ANY outlier month remains.
-- data_curation_base.sas:
--   1529-1574: %n_pct_multilev uses PROC MEANS COMPLETETYPES/PRELOADFMT, so for ENCOUNTER /
--              DIAGNOSIS / PROCEDURES every ENC_TYPE gets a row (count may be 0) for EVERY month
--              observed anywhere in that table -> zero months ARE evaluated for those series.
--   2254-2257: %n_pct_noref (VITAL/PRESCRIBING/LAB/MED_ADMIN/EXTERNAL_MEDS/PRO_CM) emits only
--              OBSERVED months -> a month with no records simply does not exist in those series.
--   2704-2718: XTBL_L3_DATES P5 = PROC MEANS P5 over all non-missing dates of the loaded table.
-- data_curation_tables.sas: 10-yr lookback restriction at table load on the SAME date field
--   (1389 ENCOUNTER, 1684 DIAGNOSIS, 1643 PROCEDURES ADMIT_DATE; 1335 VITAL; 1532 PRESCRIBING;
--   1781 LAB_RESULT_CM; 1735 MED_ADMIN; 1496 EXTERNAL_MEDS; 1301 PRO_CM) = {{ start_date }}.
--   No upper date bound in SAS (future months exist but fall outside the evaluation window).
-- Assumptions (not derivable exactly): SAS anchors month11 on RESPONSE_DATE and the lookback on
--   the query run date; both are approximated by {{ end_date }} / {{ start_date }}. SAS P5 uses
--   PCTLDEF=5, approximated with PERCENTILE_DISC(0.05) (differs only when n*0.05 is an integer,
--   and then only within the same month boundary in practice).
-- Ground truth: C4UMO 2026-07-10 -> PRINT_IIIG has 0 outlier rows -> exception = No (Pass).
-- Parameters: {{ current_schema }}, {{ start_date }}, {{ end_date }}
WITH params AS (
    SELECT
        DATEADD(month, -11, DATE_TRUNC('month', TO_DATE('{{ end_date }}'))) AS month11_date
),
-- ================= ENC_TYPE-stratified domains (ADMIT_DATE for all three) =================
enc_rows AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS month_start, ENC_TYPE, ADMIT_DATE
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE IS NOT NULL
      AND ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
dia_rows AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS month_start, ENC_TYPE, ADMIT_DATE
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE IS NOT NULL
      AND ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
px_rows AS (
    SELECT DATE_TRUNC('month', ADMIT_DATE) AS month_start, ENC_TYPE, ADMIT_DATE
    FROM {{ current_schema }}.PROCEDURES
    WHERE ADMIT_DATE IS NOT NULL
      AND ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
enc_types AS (
    SELECT column1 AS category FROM VALUES ('AV'), ('ED'), ('EI'), ('IP'), ('TH')
),
-- spine = every month observed anywhere in the table (COMPLETETYPES semantics):
-- each ENC_TYPE gets a row for each such month, 0 when it has no records that month
enc_series AS (
    SELECT 'ENCOUNTER' AS table_name, 1 AS domain_ord, et.category,
           sp.month_start, COALESCE(c.cnt, 0) AS cnt
    FROM (SELECT DISTINCT month_start FROM enc_rows) sp
    CROSS JOIN enc_types et
    LEFT JOIN (
        SELECT month_start, ENC_TYPE AS category, COUNT(*) AS cnt
        FROM enc_rows
        WHERE ENC_TYPE IN ('AV', 'ED', 'EI', 'IP', 'TH')
        GROUP BY month_start, ENC_TYPE
    ) c ON c.month_start = sp.month_start AND c.category = et.category
),
dia_series AS (
    SELECT 'DIAGNOSIS' AS table_name, 2 AS domain_ord, et.category,
           sp.month_start, COALESCE(c.cnt, 0) AS cnt
    FROM (SELECT DISTINCT month_start FROM dia_rows) sp
    CROSS JOIN enc_types et
    LEFT JOIN (
        SELECT month_start, ENC_TYPE AS category, COUNT(*) AS cnt
        FROM dia_rows
        WHERE ENC_TYPE IN ('AV', 'ED', 'EI', 'IP', 'TH')
        GROUP BY month_start, ENC_TYPE
    ) c ON c.month_start = sp.month_start AND c.category = et.category
),
px_series AS (
    SELECT 'PROCEDURES' AS table_name, 3 AS domain_ord, et.category,
           sp.month_start, COALESCE(c.cnt, 0) AS cnt
    FROM (SELECT DISTINCT month_start FROM px_rows) sp
    CROSS JOIN enc_types et
    LEFT JOIN (
        SELECT month_start, ENC_TYPE AS category, COUNT(*) AS cnt
        FROM px_rows
        WHERE ENC_TYPE IN ('AV', 'ED', 'EI', 'IP', 'TH')
        GROUP BY month_start, ENC_TYPE
    ) c ON c.month_start = sp.month_start AND c.category = et.category
),
-- ================= single-series domains (observed months only, per %n_pct_noref) =========
vit_series AS (
    SELECT 'VITAL' AS table_name, 4 AS domain_ord, 'n/a' AS category,
           DATE_TRUNC('month', MEASURE_DATE) AS month_start, COUNT(*) AS cnt
    FROM {{ current_schema }}.VITAL
    WHERE MEASURE_DATE IS NOT NULL AND MEASURE_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY DATE_TRUNC('month', MEASURE_DATE)
),
pres_series AS (
    SELECT 'PRESCRIBING' AS table_name, 5 AS domain_ord, 'n/a' AS category,
           DATE_TRUNC('month', RX_ORDER_DATE) AS month_start, COUNT(*) AS cnt
    FROM {{ current_schema }}.PRESCRIBING
    WHERE RX_ORDER_DATE IS NOT NULL AND RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY DATE_TRUNC('month', RX_ORDER_DATE)
),
lab_series AS (
    SELECT 'LAB_RESULT_CM' AS table_name, 6 AS domain_ord, 'n/a' AS category,
           DATE_TRUNC('month', RESULT_DATE) AS month_start, COUNT(*) AS cnt
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE IS NOT NULL AND RESULT_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY DATE_TRUNC('month', RESULT_DATE)
),
medadm_series AS (
    SELECT 'MED_ADMIN' AS table_name, 7 AS domain_ord, 'n/a' AS category,
           DATE_TRUNC('month', MEDADMIN_START_DATE) AS month_start, COUNT(*) AS cnt
    FROM {{ current_schema }}.MED_ADMIN
    WHERE MEDADMIN_START_DATE IS NOT NULL AND MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY DATE_TRUNC('month', MEDADMIN_START_DATE)
),
extmed_series AS (
    SELECT 'EXTERNAL_MEDS' AS table_name, 8 AS domain_ord, 'n/a' AS category,
           DATE_TRUNC('month', EXT_RECORD_DATE) AS month_start, COUNT(*) AS cnt
    FROM {{ current_schema }}.EXTERNAL_MEDS
    WHERE EXT_RECORD_DATE IS NOT NULL AND EXT_RECORD_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY DATE_TRUNC('month', EXT_RECORD_DATE)
),
procm_series AS (
    SELECT 'PRO_CM' AS table_name, 9 AS domain_ord, 'n/a' AS category,
           DATE_TRUNC('month', PRO_DATE) AS month_start, COUNT(*) AS cnt
    FROM {{ current_schema }}.PRO_CM
    WHERE PRO_DATE IS NOT NULL AND PRO_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY DATE_TRUNC('month', PRO_DATE)
),
all_series AS (
    SELECT * FROM enc_series    UNION ALL
    SELECT * FROM dia_series    UNION ALL
    SELECT * FROM px_series     UNION ALL
    SELECT * FROM vit_series    UNION ALL
    SELECT * FROM pres_series   UNION ALL
    SELECT * FROM lab_series    UNION ALL
    SELECT * FROM medadm_series UNION ALL
    SELECT * FROM extmed_series UNION ALL
    SELECT * FROM procm_series
),
-- P5 of the table's date field (XTBL_L3_DATES), truncated to month; per TABLE, not per series
p5_dates AS (
    SELECT 'ENCOUNTER' AS table_name,
           DATE_TRUNC('month', PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY ADMIT_DATE)) AS p5_month
    FROM enc_rows
    UNION ALL
    SELECT 'DIAGNOSIS',
           DATE_TRUNC('month', PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY ADMIT_DATE))
    FROM dia_rows
    UNION ALL
    SELECT 'PROCEDURES',
           DATE_TRUNC('month', PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY ADMIT_DATE))
    FROM px_rows
    UNION ALL
    SELECT 'VITAL',
           DATE_TRUNC('month', PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY MEASURE_DATE))
    FROM {{ current_schema }}.VITAL
    WHERE MEASURE_DATE IS NOT NULL AND MEASURE_DATE >= TO_DATE('{{ start_date }}')
    UNION ALL
    SELECT 'PRESCRIBING',
           DATE_TRUNC('month', PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY RX_ORDER_DATE))
    FROM {{ current_schema }}.PRESCRIBING
    WHERE RX_ORDER_DATE IS NOT NULL AND RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
    UNION ALL
    SELECT 'LAB_RESULT_CM',
           DATE_TRUNC('month', PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY RESULT_DATE))
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE IS NOT NULL AND RESULT_DATE >= TO_DATE('{{ start_date }}')
    UNION ALL
    SELECT 'MED_ADMIN',
           DATE_TRUNC('month', PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY MEDADMIN_START_DATE))
    FROM {{ current_schema }}.MED_ADMIN
    WHERE MEDADMIN_START_DATE IS NOT NULL AND MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}')
    UNION ALL
    SELECT 'EXTERNAL_MEDS',
           DATE_TRUNC('month', PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY EXT_RECORD_DATE))
    FROM {{ current_schema }}.EXTERNAL_MEDS
    WHERE EXT_RECORD_DATE IS NOT NULL AND EXT_RECORD_DATE >= TO_DATE('{{ start_date }}')
    UNION ALL
    SELECT 'PRO_CM',
           DATE_TRUNC('month', PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY PRO_DATE))
    FROM {{ current_schema }}.PRO_CM
    WHERE PRO_DATE IS NOT NULL AND PRO_DATE >= TO_DATE('{{ start_date }}')
),
-- lagged moving stats over the 12 OBSERVATIONS before each row (PROC EXPAND movave/movstd 12
-- lag 1: shrinking window at series start, POPULATION std dev). Computed BEFORE any month
-- filtering, so excluded months (e.g. Mar/Apr 2020) still feed later windows, exactly as SAS.
stats AS (
    SELECT
        table_name, domain_ord, category, month_start, cnt,
        AVG(cnt) OVER (
            PARTITION BY table_name, category ORDER BY month_start
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS lag_avg,
        STDDEV_POP(cnt) OVER (
            PARTITION BY table_name, category ORDER BY month_start
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS lag_std
    FROM all_series
),
-- edc_report.sas 6610-6611 filters, then dsd (6614)
evaluated AS (
    SELECT
        s.table_name, s.domain_ord, s.category, s.month_start, s.cnt,
        s.lag_avg, s.lag_std,
        (s.cnt - s.lag_avg) / s.lag_std AS dsd
    FROM stats s
    LEFT JOIN p5_dates p ON p.table_name = s.table_name
    CROSS JOIN params
    WHERE s.lag_avg >= 500                                    -- drops NULL/missing too
      AND s.lag_std > 0                                       -- drops NULL/missing too
      AND (p.p5_month IS NULL OR s.month_start > p.p5_month)  -- keep months after P5 month
      AND s.month_start < params.month11_date                 -- months >= refresh month - 11 dropped
      AND s.month_start NOT IN (DATE '2020-03-01', DATE '2020-04-01')
),
outliers AS (
    SELECT * FROM evaluated WHERE cnt = 0 OR dsd < -7
),
summary AS (
    SELECT
        '2.08'                                                                 AS CHECK_NUM,
        'Monthly volume of encounter, diagnosis, procedure, vital, prescribing, med admin, external meds, lab, or PRO records is an outlier (0 records or >7 SD below the lagged 12-month moving average)' AS DESCRIPTION,
        CASE WHEN (SELECT COUNT(*) FROM outliers) > 0 THEN 'Fail' ELSE 'Pass' END AS STATUS,
        'SUMMARY'                                                              AS ROW_TYPE,
        CAST(NULL AS VARCHAR)                                                  AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                  AS EXC_FIELD,
        CAST(NULL AS VARCHAR)                                                  AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                   AS EXC_COUNT,
        0                                                                      AS ROW_ORDER
),
details AS (
    SELECT
        '2.08'                                                                 AS CHECK_NUM,
        'Monthly volume of encounter, diagnosis, procedure, vital, prescribing, med admin, external meds, lab, or PRO records is an outlier (0 records or >7 SD below the lagged 12-month moving average)' AS DESCRIPTION,
        'Fail'                                                                 AS STATUS,
        'DETAIL'                                                               AS ROW_TYPE,
        o.table_name                                                           AS EXC_TABLE,
        IFF(o.category = 'n/a', 'n/a', 'ENC_TYPE ' || o.category)              AS EXC_FIELD,
        TO_VARCHAR(o.month_start, 'YYYY-MM') || ': n=' || TO_VARCHAR(o.cnt) ||
            ', 12-mo lagged avg=' || TO_VARCHAR(ROUND(o.lag_avg, 1)) ||
            ', lagged std=' || TO_VARCHAR(ROUND(o.lag_std, 1)) ||
            ', std devs=' || TO_VARCHAR(ROUND(o.dsd, 2))                       AS EXC_DETAIL,
        o.cnt                                                                  AS EXC_COUNT,
        ROW_NUMBER() OVER (ORDER BY o.domain_ord, o.category, o.month_start)   AS ROW_ORDER
    FROM outliers o
)
SELECT CHECK_NUM, DESCRIPTION, STATUS, ROW_TYPE, EXC_TABLE, EXC_FIELD, EXC_DETAIL, EXC_COUNT
FROM (
    SELECT * FROM summary
    UNION ALL
    SELECT * FROM details
)
ORDER BY ROW_ORDER
