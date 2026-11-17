-- DC 2.03 | Table IIIC | Data Plausibility | Investigative
-- More than 5% of patients have illogical date relationships.
--
-- SAS source of truth (v7.03):
--   data_curation_tables.sas 1886-1960 (XTBL_L3_DATE_LOGIC %xmin): per patient, take the
--     MINIMUM of each date field; count patients with min(field) < BIRTH_DATE and patients
--     with min(field) > DEATH_DATE. Fields (ord): ENCOUNTER ADMIT_DATE(1)/DISCHARGE_DATE(2),
--     PROCEDURES PX_DATE(3), VITAL MEASURE_DATE(4), DISPENSING DISPENSE_DATE(5),
--     PRESCRIBING RX_START_DATE(6), LAB_RESULT_CM RESULT_DATE(7), DEATH DEATH_DATE(8, birth
--     comparison only), MED_ADMIN MEDADMIN_START_DATE(9), OBS_CLIN OBSCLIN_START_DATE(10),
--     OBS_GEN OBSGEN_START_DATE(11), IMMUNIZATION VX_RECORD_DATE(12), DIAGNOSIS DX_DATE(13).
--   Within-record comparisons (distinct patients): ADMIT_DATE > DISCHARGE_DATE;
--     PX_DATE more than 5 days before ADMIT_DATE / after DISCHARGE_DATE (ords 27/28);
--     DX_DATE more than 5 days before ADMIT_DATE / after DISCHARGE_DATE (ords 29/30);
--     stop date before start date in EXTERNAL_MEDS, MED_ADMIN, OBS_CLIN, OBS_GEN,
--     PAT_RELATIONSHIP.
--   edc_report.sas %print_iiic: denominator = ENC_L3_N distinct PATID; exception (row=2.03)
--     when ANY comparison's patient pct > 5.0.
--   Table loads: (load_date >= lookback OR NULL), no upper bound. DEATH_DATE per patient
--     approximated as the earliest non-missing DEATH_DATE.
-- Parameters: {{ current_schema }}, {{ start_date }}, {{ end_date }}
WITH birth AS (
    SELECT PATID, BIRTH_DATE FROM {{ current_schema }}.DEMOGRAPHIC
    WHERE PATID IS NOT NULL AND BIRTH_DATE IS NOT NULL
),
death AS (
    SELECT PATID, MIN(DEATH_DATE) AS DEATH_DATE FROM {{ current_schema }}.DEATH
    WHERE PATID IS NOT NULL AND DEATH_DATE IS NOT NULL
    GROUP BY PATID
),
{% set mins = [
    ('ENCOUNTER',     'ADMIT_DATE',          "ADMIT_DATE >= TO_DATE('" ~ start_date ~ "') OR ADMIT_DATE IS NULL"),
    ('ENCOUNTER',     'DISCHARGE_DATE',      "ADMIT_DATE >= TO_DATE('" ~ start_date ~ "') OR ADMIT_DATE IS NULL"),
    ('PROCEDURES',    'PX_DATE',             "ADMIT_DATE >= TO_DATE('" ~ start_date ~ "') OR ADMIT_DATE IS NULL"),
    ('VITAL',         'MEASURE_DATE',        "MEASURE_DATE >= TO_DATE('" ~ start_date ~ "') OR MEASURE_DATE IS NULL"),
    ('DISPENSING',    'DISPENSE_DATE',       "DISPENSE_DATE >= TO_DATE('" ~ start_date ~ "') OR DISPENSE_DATE IS NULL"),
    ('PRESCRIBING',   'RX_START_DATE',       "RX_ORDER_DATE >= TO_DATE('" ~ start_date ~ "') OR RX_ORDER_DATE IS NULL"),
    ('LAB_RESULT_CM', 'RESULT_DATE',         "RESULT_DATE >= TO_DATE('" ~ start_date ~ "') OR RESULT_DATE IS NULL"),
    ('DEATH',         'DEATH_DATE',          "1=1"),
    ('MED_ADMIN',     'MEDADMIN_START_DATE', "MEDADMIN_START_DATE >= TO_DATE('" ~ start_date ~ "') OR MEDADMIN_START_DATE IS NULL"),
    ('OBS_CLIN',      'OBSCLIN_START_DATE',  "OBSCLIN_START_DATE >= TO_DATE('" ~ start_date ~ "') OR OBSCLIN_START_DATE IS NULL"),
    ('OBS_GEN',       'OBSGEN_START_DATE',   "OBSGEN_START_DATE >= TO_DATE('" ~ start_date ~ "') OR OBSGEN_START_DATE IS NULL"),
    ('IMMUNIZATION',  'VX_RECORD_DATE',      "VX_RECORD_DATE >= TO_DATE('" ~ start_date ~ "') OR VX_RECORD_DATE IS NULL"),
    ('DIAGNOSIS',     'DX_DATE',             "ADMIT_DATE >= TO_DATE('" ~ start_date ~ "') OR ADMIT_DATE IS NULL")
] %}
min_dates AS (
    {%- for tbl, var, win in mins %}
    SELECT '{{ tbl }}' AS TBL, '{{ var }}' AS VAR, PATID, MIN({{ var }}) AS MIN_DT
    FROM {{ current_schema }}.{{ tbl }}
    WHERE PATID IS NOT NULL AND ({{ win }})
    GROUP BY PATID{{ " UNION ALL" if not loop.last }}
    {%- endfor %}
),
vs_birth_death AS (
    SELECT
        m.VAR || ' < BIRTH_DATE' AS COMPARISON,
        COUNT(DISTINCT CASE WHEN m.MIN_DT < b.BIRTH_DATE THEN m.PATID END) AS N
    FROM min_dates m JOIN birth b ON b.PATID = m.PATID
    WHERE m.MIN_DT IS NOT NULL
    GROUP BY m.TBL, m.VAR
    UNION ALL
    SELECT
        m.VAR || ' > DEATH_DATE',
        COUNT(DISTINCT CASE WHEN m.MIN_DT > d.DEATH_DATE THEN m.PATID END)
    FROM min_dates m JOIN death d ON d.PATID = m.PATID
    WHERE m.MIN_DT IS NOT NULL AND m.TBL <> 'DEATH'
    GROUP BY m.TBL, m.VAR
),
within AS (
    SELECT 'ADMIT_DATE > DISCHARGE_DATE' AS COMPARISON, COUNT(DISTINCT PATID) AS N
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE > DISCHARGE_DATE
      AND (ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL)
    UNION ALL
    SELECT 'PX_DATE more than 5 days prior to ADMIT_DATE', COUNT(DISTINCT PATID)
    FROM {{ current_schema }}.PROCEDURES
    WHERE PX_DATE < DATEADD(day, -5, ADMIT_DATE)
      AND (ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL)
    UNION ALL
    SELECT 'PX_DATE more than 5 days after DISCHARGE_DATE', COUNT(DISTINCT p.PATID)
    FROM {{ current_schema }}.PROCEDURES p
    JOIN {{ current_schema }}.ENCOUNTER e ON e.ENCOUNTERID = p.ENCOUNTERID
    WHERE p.PX_DATE > DATEADD(day, 5, e.DISCHARGE_DATE)
      AND (p.ADMIT_DATE >= TO_DATE('{{ start_date }}') OR p.ADMIT_DATE IS NULL)
    UNION ALL
    SELECT 'DX_DATE more than 5 days prior to ADMIT_DATE', COUNT(DISTINCT PATID)
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE DX_DATE < DATEADD(day, -5, ADMIT_DATE)
      AND (ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL)
    UNION ALL
    SELECT 'DX_DATE more than 5 days after DISCHARGE_DATE', COUNT(DISTINCT d.PATID)
    FROM {{ current_schema }}.DIAGNOSIS d
    JOIN {{ current_schema }}.ENCOUNTER e ON e.ENCOUNTERID = d.ENCOUNTERID
    WHERE d.DX_DATE > DATEADD(day, 5, e.DISCHARGE_DATE)
      AND (d.ADMIT_DATE >= TO_DATE('{{ start_date }}') OR d.ADMIT_DATE IS NULL)
    UNION ALL
    SELECT 'EXT_PAT_END_DATE < EXT_PAT_START_DATE', COUNT(DISTINCT PATID)
    FROM {{ current_schema }}.EXTERNAL_MEDS
    WHERE EXT_PAT_END_DATE < EXT_PAT_START_DATE
      AND (EXT_RECORD_DATE >= TO_DATE('{{ start_date }}') OR EXT_RECORD_DATE IS NULL)
    UNION ALL
    SELECT 'MEDADMIN_STOP_DATE < MEDADMIN_START_DATE', COUNT(DISTINCT PATID)
    FROM {{ current_schema }}.MED_ADMIN
    WHERE MEDADMIN_STOP_DATE < MEDADMIN_START_DATE
      AND (MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}') OR MEDADMIN_START_DATE IS NULL)
    UNION ALL
    SELECT 'OBSCLIN_STOP_DATE < OBSCLIN_START_DATE', COUNT(DISTINCT PATID)
    FROM {{ current_schema }}.OBS_CLIN
    WHERE OBSCLIN_STOP_DATE < OBSCLIN_START_DATE
      AND (OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}') OR OBSCLIN_START_DATE IS NULL)
    UNION ALL
    SELECT 'OBSGEN_STOP_DATE < OBSGEN_START_DATE', COUNT(DISTINCT PATID)
    FROM {{ current_schema }}.OBS_GEN
    WHERE OBSGEN_STOP_DATE < OBSGEN_START_DATE
      AND (OBSGEN_START_DATE >= TO_DATE('{{ start_date }}') OR OBSGEN_START_DATE IS NULL)
    UNION ALL
    SELECT 'RELATIONSHIP_END < RELATIONSHIP_START', COUNT(DISTINCT PATID_1)
    FROM {{ current_schema }}.PAT_RELATIONSHIP
    WHERE RELATIONSHIP_END < RELATIONSHIP_START
),
denom AS (
    SELECT COUNT(DISTINCT PATID) AS N FROM {{ current_schema }}.ENCOUNTER
    WHERE (ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL)
),
scored AS (
    SELECT COMPARISON, N, ROUND(100.0 * N / NULLIF((SELECT N FROM denom), 0), 2) AS PCT
    FROM (SELECT * FROM vs_birth_death UNION ALL SELECT * FROM within)
),
summary AS (
    SELECT
        '2.03'                                                                 AS CHECK_NUM,
        'More than 5% of patients have illogical date relationships'           AS DESCRIPTION,
        CASE WHEN MAX(PCT) > 5.0 THEN 'Fail' ELSE 'Pass' END                   AS STATUS,
        'SUMMARY'                                                              AS ROW_TYPE,
        ''                                                                     AS EXC_TABLE,
        ''                                                                     AS EXC_FIELD,
        'max pct of patients = ' || TO_VARCHAR(COALESCE(MAX(PCT), 0))          AS EXC_DETAIL,
        NULL                                                                   AS EXC_COUNT,
        0                                                                      AS ROW_ORDER
    FROM scored
),
details AS (
    SELECT
        '2.03', 'Illogical date relationship over threshold', 'Fail', 'DETAIL',
        '', COMPARISON,
        TO_VARCHAR(PCT) || '% of patients (' || TO_VARCHAR(N) || ')',
        N,
        ROW_NUMBER() OVER (ORDER BY PCT DESC)
    FROM scored
    WHERE PCT > 5.0
)
SELECT CHECK_NUM, DESCRIPTION, STATUS, ROW_TYPE, EXC_TABLE, EXC_FIELD, EXC_DETAIL, EXC_COUNT
FROM (
    SELECT * FROM summary
    UNION ALL
    SELECT * FROM details
)
ORDER BY ROW_ORDER
