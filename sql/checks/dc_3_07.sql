-- DC 3.07 | Table IVF | Data Completeness | Investigative
-- Encounters, diagnoses, or procedures in an ambulatory (AV), telehealth (TH), emergency department (ED),
-- ED to inpatient (EI), or inpatient (IP) setting are less than 75% complete two months prior to the
-- current month. Data completeness is calculated by comparing actual volume to the average volume
-- during the previous year.
--
-- SAS source of truth (v7.03): edc_report.sas 8886-9068 (Table IVF, row=3.07):
--   8893-8907: Month -0 = month of RESPONSE_DATE (refresh date, {{ end_date }} here); 24 monthly
--              buckets ord 0..23 anchored on the first of that month.
--   8918-8928: monthly RECORD_N from ENC_L3_ENCTYPE_ADATE_YM / DIA_L3_ENCTYPE_ADATE_YM /
--              PRO_L3_ENCTYPE_ADATE_YM, ENC_TYPE (category) limited to AV, ED, EI, IP, TH;
--              NULL/invalid date rows excluded. Month = ADMIT_DATE for ALL THREE domains,
--              including PROCEDURES (data_curation_tables.sas 3285: pro_l3_enctype_adate_ym is
--              ENC_TYPE x ADMIT_DATE_YM; 1694: _ym derived from admit_date).
--   8935-8954: months with no data filled with 0; benchmark period = ord 12..23
--              (months -12 .. -23).
--   8991-8997: percent of benchmark = cnt / ROUND(benchmark_sum/12) * 100, computed only when
--              cnt>0 and benchmark_sum>0 (division by a 0 rounded average -> missing in SAS,
--              replicated with NULLIF).
--   9048-9058: exception when, at ord=2 (two months before the refresh month), ANY domain has
--              cnt=0 OR a non-missing percent < 75.0.
--   edc_report.sas 11314-11354: no n/a condition exists for row 3.07.
--   Lookback (data_curation_tables.sas 1389/1653/1694: admit_date>=&lookback_dt or missing) never
--   clips the 24-month window, so no {{ start_date }} filter is needed.
-- Parameters: {{ current_schema }}, {{ end_date }}
WITH params AS (
    SELECT DATE_TRUNC('month', TO_DATE('{{ end_date }}')) AS month0
),
months AS (
    SELECT seq AS ord, DATEADD(month, -seq, p.month0) AS month_start
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS seq
          FROM TABLE(GENERATOR(ROWCOUNT => 24))), params p
),
enc_monthly AS (
    SELECT DATE_TRUNC('month', e.ADMIT_DATE) AS month_start, COUNT(*) AS cnt
    FROM {{ current_schema }}.ENCOUNTER e, params p
    WHERE e.ENC_TYPE IN ('AV','TH','ED','EI','IP')
      AND e.ADMIT_DATE >= DATEADD(month, -23, p.month0)
      AND e.ADMIT_DATE <  DATEADD(month, 1, p.month0)
    GROUP BY 1
),
dx_monthly AS (
    SELECT DATE_TRUNC('month', d.ADMIT_DATE) AS month_start, COUNT(*) AS cnt
    FROM {{ current_schema }}.DIAGNOSIS d, params p
    WHERE d.ENC_TYPE IN ('AV','TH','ED','EI','IP')
      AND d.ADMIT_DATE >= DATEADD(month, -23, p.month0)
      AND d.ADMIT_DATE <  DATEADD(month, 1, p.month0)
    GROUP BY 1
),
px_monthly AS (
    -- PRO_L3_ENCTYPE_ADATE_YM buckets by ADMIT_DATE, not PX_DATE
    SELECT DATE_TRUNC('month', x.ADMIT_DATE) AS month_start, COUNT(*) AS cnt
    FROM {{ current_schema }}.PROCEDURES x, params p
    WHERE x.ENC_TYPE IN ('AV','TH','ED','EI','IP')
      AND x.ADMIT_DATE >= DATEADD(month, -23, p.month0)
      AND x.ADMIT_DATE <  DATEADD(month, 1, p.month0)
    GROUP BY 1
),
series AS (
    SELECT 'ENCOUNTER' AS domain, 1 AS domain_ord, m.ord, m.month_start, COALESCE(d.cnt, 0) AS cnt
    FROM months m LEFT JOIN enc_monthly d ON d.month_start = m.month_start
    UNION ALL
    SELECT 'DIAGNOSIS', 2, m.ord, m.month_start, COALESCE(d.cnt, 0)
    FROM months m LEFT JOIN dx_monthly d ON d.month_start = m.month_start
    UNION ALL
    SELECT 'PROCEDURES', 3, m.ord, m.month_start, COALESCE(d.cnt, 0)
    FROM months m LEFT JOIN px_monthly d ON d.month_start = m.month_start
),
evaluated AS (
    -- test month = ord 2; benchmark = fixed window ord 12..23 (edc_report.sas 8950, 9051)
    SELECT domain, domain_ord,
           MAX(CASE WHEN ord = 2 THEN month_start END)       AS test_month,
           MAX(CASE WHEN ord = 2 THEN cnt END)               AS test_cnt,
           SUM(CASE WHEN ord BETWEEN 12 AND 23 THEN cnt END) AS bench_sum,
           ROUND(SUM(CASE WHEN ord BETWEEN 12 AND 23 THEN cnt END) / 12.0, 0) AS bench_avg
    FROM series
    GROUP BY domain, domain_ord
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
summary AS (
    SELECT
        '3.07'                                                                                       AS CHECK_NUM,
        'Encounters, diagnoses, or procedures < 75% complete two months prior to the current month'  AS DESCRIPTION,
        CASE WHEN (SELECT COUNT(*) FROM exceptions) > 0 THEN 'Fail' ELSE 'Pass' END                  AS STATUS,
        'SUMMARY'                                                                                    AS ROW_TYPE,
        CAST(NULL AS VARCHAR)                                                                        AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                                        AS EXC_FIELD,
        CAST(NULL AS VARCHAR)                                                                        AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                                         AS EXC_COUNT,
        0                                                                                            AS ROW_ORDER
),
details AS (
    SELECT
        '3.07'                                                                                       AS CHECK_NUM,
        'Encounters, diagnoses, or procedures < 75% complete two months prior to the current month'  AS DESCRIPTION,
        'Fail'                                                                                       AS STATUS,
        'DETAIL'                                                                                     AS ROW_TYPE,
        x.domain                                                                                     AS EXC_TABLE,
        'ADMIT_DATE'                                                                                 AS EXC_FIELD,
        'Month -2 (' || TO_VARCHAR(x.test_month, 'YYYY-MM') || '): n=' || TO_VARCHAR(x.test_cnt) ||
            ', benchmark avg (months -12..-23)=' || TO_VARCHAR(x.bench_avg) ||
            ', % of benchmark=' || COALESCE(TO_VARCHAR(ROUND(x.pct_of_benchmark, 1)), 'n/a')         AS EXC_DETAIL,
        x.test_cnt                                                                                   AS EXC_COUNT,
        x.domain_ord                                                                                 AS ROW_ORDER
    FROM exceptions x
)
SELECT CHECK_NUM, DESCRIPTION, STATUS, ROW_TYPE, EXC_TABLE, EXC_FIELD, EXC_DETAIL, EXC_COUNT
FROM (
    SELECT * FROM summary
    UNION ALL
    SELECT * FROM details
)
ORDER BY ROW_ORDER
