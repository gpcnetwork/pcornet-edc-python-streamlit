-- DC 3.11 | Table IVG | Data Completeness | Investigative
-- Vital, prescribing, or laboratory records are less than 75% complete three months prior to the
-- current month. Data completeness is calculated by comparing actual volume to the average volume
-- during the previous year.
--
-- SAS source of truth (v7.03): edc_report.sas 9217-9412 (Table IVG, row=3.11):
--   9224-9238: Month -0 = month of RESPONSE_DATE (refresh date, {{ end_date }} here); 24 monthly
--              buckets ord 0..23 anchored on the first of that month.
--   9249-9257: monthly RECORD_N from VIT_L3_MDATE_YM (MEASURE_DATE), PRES_L3_ODATE_YM
--              (RX_ORDER_DATE), LAB_L3_RDATE_YM (RESULT_DATE); NULL/invalid date rows excluded;
--              no ENC_TYPE restriction (data_curation_tables.sas 5530 / 6514 / 6421).
--   9274-9293: months with no data filled with 0; benchmark period = ord 12..23
--              (months -12 .. -23).
--   9332-9340: percent of benchmark = cnt / ROUND(benchmark_sum/12) * 100, computed only when
--              cnt>0 and benchmark_sum>0 (division by a 0 rounded average -> missing in SAS,
--              replicated with NULLIF).
--   9392-9402: exception when, at ord=3 (three months before the refresh month), ANY domain has
--              cnt=0 OR a non-missing percent < 75.0.
--   11340-11344: exception set to "n/a" only when the LAB, PRESCRIBING, and VITAL domains ALL
--              have no data (a single empty domain still evaluates and fails via cnt=0).
--   Lookback (data_curation_tables.sas 1335/1532/1781: date>=&lookback_dt or missing) never clips
--   the 24-month window, so no {{ start_date }} filter is needed.
-- Parameters: {{ current_schema }}, {{ end_date }}
WITH params AS (
    SELECT DATE_TRUNC('month', TO_DATE('{{ end_date }}')) AS month0
),
months AS (
    SELECT seq AS ord, DATEADD(month, -seq, p.month0) AS month_start
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS seq
          FROM TABLE(GENERATOR(ROWCOUNT => 24))), params p
),
vital_monthly AS (
    SELECT DATE_TRUNC('month', v.MEASURE_DATE) AS month_start, COUNT(*) AS cnt
    FROM {{ current_schema }}.VITAL v, params p
    WHERE v.MEASURE_DATE >= DATEADD(month, -23, p.month0)
      AND v.MEASURE_DATE <  DATEADD(month, 1, p.month0)
    GROUP BY 1
),
rx_monthly AS (
    SELECT DATE_TRUNC('month', r.RX_ORDER_DATE) AS month_start, COUNT(*) AS cnt
    FROM {{ current_schema }}.PRESCRIBING r, params p
    WHERE r.RX_ORDER_DATE >= DATEADD(month, -23, p.month0)
      AND r.RX_ORDER_DATE <  DATEADD(month, 1, p.month0)
    GROUP BY 1
),
lab_monthly AS (
    SELECT DATE_TRUNC('month', l.RESULT_DATE) AS month_start, COUNT(*) AS cnt
    FROM {{ current_schema }}.LAB_RESULT_CM l, params p
    WHERE l.RESULT_DATE >= DATEADD(month, -23, p.month0)
      AND l.RESULT_DATE <  DATEADD(month, 1, p.month0)
    GROUP BY 1
),
series AS (
    SELECT 'VITAL' AS domain, 1 AS domain_ord, 'MEASURE_DATE' AS date_field,
           m.ord, m.month_start, COALESCE(d.cnt, 0) AS cnt
    FROM months m LEFT JOIN vital_monthly d ON d.month_start = m.month_start
    UNION ALL
    SELECT 'PRESCRIBING', 2, 'RX_ORDER_DATE', m.ord, m.month_start, COALESCE(d.cnt, 0)
    FROM months m LEFT JOIN rx_monthly d ON d.month_start = m.month_start
    UNION ALL
    SELECT 'LAB_RESULT_CM', 3, 'RESULT_DATE', m.ord, m.month_start, COALESCE(d.cnt, 0)
    FROM months m LEFT JOIN lab_monthly d ON d.month_start = m.month_start
),
evaluated AS (
    -- test month = ord 3; benchmark = fixed window ord 12..23 (edc_report.sas 9289, 9395)
    SELECT domain, domain_ord, date_field,
           MAX(CASE WHEN ord = 3 THEN month_start END)       AS test_month,
           MAX(CASE WHEN ord = 3 THEN cnt END)               AS test_cnt,
           SUM(CASE WHEN ord BETWEEN 12 AND 23 THEN cnt END) AS bench_sum,
           ROUND(SUM(CASE WHEN ord BETWEEN 12 AND 23 THEN cnt END) / 12.0, 0) AS bench_avg
    FROM series
    GROUP BY domain, domain_ord, date_field
),
scored AS (
    SELECT e.*,
           CASE WHEN test_cnt > 0 AND bench_sum > 0
                THEN test_cnt / NULLIF(bench_avg, 0) * 100
           END AS pct_of_benchmark
    FROM evaluated e
),
exceptions AS (
    SELECT * FROM scored WHERE test_cnt = 0 OR pct_of_benchmark < 75.0
),
na_flag AS (
    -- edc_report.sas 11340-11344: n/a only when VITAL, PRESCRIBING, and LAB all have no data
    SELECT CASE WHEN (SELECT COUNT(*) FROM {{ current_schema }}.VITAL) = 0
                 AND (SELECT COUNT(*) FROM {{ current_schema }}.PRESCRIBING) = 0
                 AND (SELECT COUNT(*) FROM {{ current_schema }}.LAB_RESULT_CM) = 0
                THEN 1 ELSE 0 END AS is_na
),
summary AS (
    SELECT
        '3.11'                                                                                              AS CHECK_NUM,
        'Vital, prescribing, or laboratory records < 75% complete three months prior to the current month'  AS DESCRIPTION,
        CASE WHEN (SELECT is_na FROM na_flag) = 1 THEN 'n/a'
             WHEN (SELECT COUNT(*) FROM exceptions) > 0 THEN 'Fail'
             ELSE 'Pass' END                                                                                AS STATUS,
        'SUMMARY'                                                                                           AS ROW_TYPE,
        CAST(NULL AS VARCHAR)                                                                               AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                                               AS EXC_FIELD,
        CAST(NULL AS VARCHAR)                                                                               AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                                                AS EXC_COUNT,
        0                                                                                                   AS ROW_ORDER
),
details AS (
    SELECT
        '3.11'                                                                                              AS CHECK_NUM,
        'Vital, prescribing, or laboratory records < 75% complete three months prior to the current month'  AS DESCRIPTION,
        'Fail'                                                                                              AS STATUS,
        'DETAIL'                                                                                            AS ROW_TYPE,
        x.domain                                                                                            AS EXC_TABLE,
        x.date_field                                                                                        AS EXC_FIELD,
        'Month -3 (' || TO_VARCHAR(x.test_month, 'YYYY-MM') || '): n=' || TO_VARCHAR(x.test_cnt) ||
            ', benchmark avg (months -12..-23)=' || TO_VARCHAR(x.bench_avg) ||
            ', % of benchmark=' || COALESCE(TO_VARCHAR(ROUND(x.pct_of_benchmark, 1)), 'n/a')                AS EXC_DETAIL,
        x.test_cnt                                                                                          AS EXC_COUNT,
        x.domain_ord                                                                                        AS ROW_ORDER
    FROM exceptions x
    WHERE (SELECT is_na FROM na_flag) = 0
)
SELECT CHECK_NUM, DESCRIPTION, STATUS, ROW_TYPE, EXC_TABLE, EXC_FIELD, EXC_DETAIL, EXC_COUNT
FROM (
    SELECT * FROM summary
    UNION ALL
    SELECT * FROM details
)
ORDER BY ROW_ORDER
