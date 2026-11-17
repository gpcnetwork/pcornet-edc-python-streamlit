-- DC 2.02 | Table IIIB | Data Plausibility | Investigative
-- More than 10% of records fall into the lowest or highest category of AGE (<0 or >89 yrs,
-- derived from BIRTH_DATE), HT (<21 or >76 inches), WT (<0 or >350 lbs),
-- DIASTOLIC (<40 or >120 mmHg), SYSTOLIC (<40 or >210 mmHg), or
-- DISPENSE_SUP (<1 or >90 days).
--
-- SAS source of truth (v7.03):
--   edc_report.sas 5935-6102 (Table IIIB):
--     5986-6007: numerators per field from DC query categories
--                (DEM_L3_AGEYRSDIST2 "<0 yrs"/">89 yrs"; VIT_L3_HT "<0","0-10","11-20" /
--                 "77-82","83-88","89-94",">=95"; VIT_L3_WT "<0"/">350";
--                 VIT_L3_DIASTOLIC "<40"/">120"; VIT_L3_SYSTOLIC "<40"/">210";
--                 DISP_L3_SUPDIST2 "<1 day"/">90 days").
--     6023-6031: denominator per field = sum of RECORD_N over ALL categories except
--                "NULL or missing" (gap/out-of-range values land in "Values outside of
--                CDM specifications" and still count) = count of NON-MISSING values.
--     6087-6088: LOW% and HIGH% computed SEPARATELY per field, formatted 16.1
--                (i.e. rounded to 1 decimal place).
--     6094-6098: exception (row=2.02) if ANY field's rounded LOW% > 10.00 OR HIGH% > 10.00.
--   data_curation_tables.sas:
--     4944-4985 (DEM_L3_AGEYRSDIST2): AGE = int(yrdif(BIRTH_DATE, min(earliest non-missing
--                DEATH_DATE per patient, today()))), i.e. completed years truncated toward
--                zero, as of the earlier of first death date and the run date
--                (= {{ end_date }} here). DEMOGRAPHIC/DEATH have NO lookback window.
--     5476-5486 (VIT_L3_HT):  grouped on round(HT,1)  -> low: round<21,  high: round>=77.
--     5652-5662 (VIT_L3_WT):  grouped on round(WT,1)  -> low: round<0,   high: round>350.
--     5403-5412 (VIT_L3_DIASTOLIC): raw value         -> low: <40,       high: >120.
--     5563-5572 (VIT_L3_SYSTOLIC):  raw value         -> low: <40,       high: >210.
--     3348-3357 (DISP_L3_SUPDIST2): raw value         -> low: <1,        high: >90.
--     1334-1335: VITAL restricted to MEASURE_DATE >= lookback date OR MEASURE_DATE missing.
--     1606-1607: DISPENSING restricted to DISPENSE_DATE >= lookback date OR missing.
--                (lookback = run date minus &lookback years, default 10 -> {{ start_date }};
--                 NO upper date bound in SAS.)
-- Parameters: {{ current_schema }}, {{ start_date }}, {{ end_date }}
WITH age_base AS (
    SELECT
        TRUNC(
            MONTHS_BETWEEN(
                LEAST(COALESCE(dd.FIRST_DEATH, TO_DATE('{{ end_date }}')), TO_DATE('{{ end_date }}')),
                d.BIRTH_DATE
            ) / 12,
            0
        ) AS AGE
    FROM {{ current_schema }}.DEMOGRAPHIC d
    LEFT JOIN (
        SELECT PATID, MIN(DEATH_DATE) AS FIRST_DEATH
        FROM {{ current_schema }}.DEATH
        WHERE DEATH_DATE IS NOT NULL
        GROUP BY PATID
    ) dd
        ON d.PATID = dd.PATID
    WHERE d.BIRTH_DATE IS NOT NULL
),
field_stats AS (
    SELECT 1                                AS ORD,
           'DEMOGRAPHIC'                    AS SRC_TABLE,
           'AGE (derived from BIRTH_DATE)'  AS FIELD,
           '<0 yrs.'                        AS LOW_LABEL,
           '>89 yrs.'                       AS HIGH_LABEL,
           COUNT(*)                         AS DENOM,
           COUNT_IF(AGE < 0)                AS LOW_N,
           COUNT_IF(AGE > 89)               AS HIGH_N
    FROM age_base
    UNION ALL
    SELECT 2, 'VITAL', 'HT', '<21 inches', '>76 inches',
           COUNT(*),
           COUNT_IF(ROUND(HT, 0) < 21),
           COUNT_IF(ROUND(HT, 0) >= 77)
    FROM {{ current_schema }}.VITAL
    WHERE HT IS NOT NULL
      AND (MEASURE_DATE IS NULL OR MEASURE_DATE >= TO_DATE('{{ start_date }}'))
    UNION ALL
    SELECT 3, 'VITAL', 'WT', '<0 lbs.', '>350 lbs.',
           COUNT(*),
           COUNT_IF(ROUND(WT, 0) < 0),
           COUNT_IF(ROUND(WT, 0) > 350)
    FROM {{ current_schema }}.VITAL
    WHERE WT IS NOT NULL
      AND (MEASURE_DATE IS NULL OR MEASURE_DATE >= TO_DATE('{{ start_date }}'))
    UNION ALL
    SELECT 4, 'VITAL', 'DIASTOLIC', '<40 mgHg', '>120 mgHg',
           COUNT(*),
           COUNT_IF(DIASTOLIC < 40),
           COUNT_IF(DIASTOLIC > 120)
    FROM {{ current_schema }}.VITAL
    WHERE DIASTOLIC IS NOT NULL
      AND (MEASURE_DATE IS NULL OR MEASURE_DATE >= TO_DATE('{{ start_date }}'))
    UNION ALL
    SELECT 5, 'VITAL', 'SYSTOLIC', '<40 mgHg', '>210 mgHg',
           COUNT(*),
           COUNT_IF(SYSTOLIC < 40),
           COUNT_IF(SYSTOLIC > 210)
    FROM {{ current_schema }}.VITAL
    WHERE SYSTOLIC IS NOT NULL
      AND (MEASURE_DATE IS NULL OR MEASURE_DATE >= TO_DATE('{{ start_date }}'))
    UNION ALL
    SELECT 6, 'DISPENSING', 'DISPENSE_SUP_GROUP', '<1 day', '>90 days',
           COUNT(*),
           COUNT_IF(DISPENSE_SUP < 1),
           COUNT_IF(DISPENSE_SUP > 90)
    FROM {{ current_schema }}.DISPENSING
    WHERE DISPENSE_SUP IS NOT NULL
      AND (DISPENSE_DATE IS NULL OR DISPENSE_DATE >= TO_DATE('{{ start_date }}'))
),
evaluated AS (
    -- SAS formats each pct with 16.1 (1 decimal) BEFORE comparing to 10.00
    SELECT
        ORD, SRC_TABLE, FIELD, LOW_LABEL, HIGH_LABEL, DENOM, LOW_N, HIGH_N,
        ROUND(100.0 * LOW_N  / NULLIF(DENOM, 0), 1) AS LOW_PCT,
        ROUND(100.0 * HIGH_N / NULLIF(DENOM, 0), 1) AS HIGH_PCT
    FROM field_stats
),
exceeded AS (
    SELECT
        e.ORD,
        e.SRC_TABLE,
        e.FIELD,
        b.BOUND,
        IFF(b.BOUND = 'LOW', e.LOW_LABEL, e.HIGH_LABEL) AS LABEL,
        IFF(b.BOUND = 'LOW', e.LOW_N,     e.HIGH_N)     AS N,
        IFF(b.BOUND = 'LOW', e.LOW_PCT,   e.HIGH_PCT)   AS PCT,
        e.DENOM
    FROM evaluated e
    CROSS JOIN (SELECT 'LOW' AS BOUND UNION ALL SELECT 'HIGH') b
    WHERE IFF(b.BOUND = 'LOW', e.LOW_PCT, e.HIGH_PCT) > 10.0
),
summary AS (
    SELECT
        '2.02'                                                                 AS CHECK_NUM,
        'More than 10% of records fall into the lowest or highest age, height, weight, diastolic, systolic, or dispensed days supply category' AS DESCRIPTION,
        CASE WHEN (SELECT COUNT(*) FROM exceeded) > 0 THEN 'Fail' ELSE 'Pass' END AS STATUS,
        'SUMMARY'                                                              AS ROW_TYPE,
        CAST(NULL AS VARCHAR)                                                  AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                  AS EXC_FIELD,
        CAST(NULL AS VARCHAR)                                                  AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                   AS EXC_COUNT,
        0                                                                      AS ROW_ORDER
),
details AS (
    SELECT
        '2.02'                                                                 AS CHECK_NUM,
        'More than 10% of records fall into the lowest or highest age, height, weight, diastolic, systolic, or dispensed days supply category' AS DESCRIPTION,
        'Fail'                                                                 AS STATUS,
        'DETAIL'                                                               AS ROW_TYPE,
        x.SRC_TABLE                                                            AS EXC_TABLE,
        x.FIELD                                                                AS EXC_FIELD,
        x.BOUND || ' (' || x.LABEL || '): ' || TO_VARCHAR(x.PCT) || '% of ' ||
            TO_VARCHAR(x.DENOM) || ' non-missing records'                      AS EXC_DETAIL,
        x.N                                                                    AS EXC_COUNT,
        ROW_NUMBER() OVER (ORDER BY x.ORD, x.BOUND DESC)                       AS ROW_ORDER
    FROM exceeded x
)
SELECT * FROM summary
UNION ALL
SELECT * FROM details
ORDER BY ROW_ORDER
