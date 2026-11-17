-- DC 2.04 | Table IIID | Data Plausibility | Investigative
-- The average number of encounters per visit is > 2.0 for inpatient (IP), emergency department (ED),
-- or ED to inpatient (EI) encounters.
--
-- SAS source of truth (v7.03):
--   data_curation_tables.sas 4050-4065 (ENC_L3_ENCTYPE): per encounter type,
--     ELIG_RECORD_N   = encounters with PATID, ENC_TYPE, ADMIT_DATE, PROVIDERID and ENCOUNTERID
--                       all non-missing;
--     DISTINCT_VISIT_N = distinct PATID/ENC_TYPE/ADMIT_DATE/PROVIDERID combos among encounters
--                        with those four fields non-missing.
--   ENCOUNTER load (1389): ADMIT_DATE >= lookback OR ADMIT_DATE IS NULL — no upper bound.
--   edc_report.sas 6377-6420 (Table IIID): ratio = ELIG_RECORD_N / DISTINCT_VISIT_N per type;
--     exception (row=2.04) when the ratio > 2.0 for ED, EI, or IP (evaluated per type,
--     not pooled; the old SQL pooled the three types and ignored the known-PROVIDERID rule).
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH base AS (
    SELECT PATID, ENC_TYPE, ADMIT_DATE, PROVIDERID, ENCOUNTERID
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ENC_TYPE IN ('ED','EI','IP')
      AND (ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL)
),
per_type AS (
    SELECT
        ENC_TYPE,
        COUNT_IF(PATID IS NOT NULL AND ADMIT_DATE IS NOT NULL
                 AND NULLIF(TRIM(PROVIDERID), '') IS NOT NULL
                 AND NULLIF(TRIM(ENCOUNTERID), '') IS NOT NULL)                 AS ELIG_RECORD_N,
        COUNT(DISTINCT CASE WHEN PATID IS NOT NULL AND ADMIT_DATE IS NOT NULL
                             AND NULLIF(TRIM(PROVIDERID), '') IS NOT NULL
                            THEN PATID || '/' || ENC_TYPE || '/' || TO_VARCHAR(ADMIT_DATE) || '/' || PROVIDERID
                       END)                                                     AS DISTINCT_VISIT_N
    FROM base
    GROUP BY ENC_TYPE
),
scored AS (
    SELECT ENC_TYPE, ELIG_RECORD_N, DISTINCT_VISIT_N,
           CASE WHEN ELIG_RECORD_N > 0 AND DISTINCT_VISIT_N > 0
                THEN ELIG_RECORD_N / DISTINCT_VISIT_N END AS ENC_PER_VISIT
    FROM per_type
),
summary AS (
    SELECT
        '2.04'                                                                 AS CHECK_NUM,
        'The average number of encounters per visit is > 2.0 for IP, ED, or EI encounters' AS DESCRIPTION,
        CASE WHEN MAX(ENC_PER_VISIT) > 2.0 THEN 'Fail' ELSE 'Pass' END         AS STATUS,
        'SUMMARY'                                                              AS ROW_TYPE,
        ''                                                                     AS EXC_TABLE,
        ''                                                                     AS EXC_FIELD,
        'max encounters per visit = ' || TO_VARCHAR(COALESCE(ROUND(MAX(ENC_PER_VISIT), 2), 0)) AS EXC_DETAIL,
        NULL                                                                   AS EXC_COUNT,
        0                                                                      AS ROW_ORDER
    FROM scored
),
details AS (
    SELECT
        '2.04', 'Encounters per visit above threshold', 'Fail', 'DETAIL',
        'ENCOUNTER', ENC_TYPE,
        TO_VARCHAR(ROUND(ENC_PER_VISIT, 2)) || ' encounters per visit (' ||
            TO_VARCHAR(ELIG_RECORD_N) || ' eligible encounters / ' ||
            TO_VARCHAR(DISTINCT_VISIT_N) || ' visits)',
        ELIG_RECORD_N,
        ROW_NUMBER() OVER (ORDER BY ENC_PER_VISIT DESC)
    FROM scored
    WHERE ENC_PER_VISIT > 2.0
)
SELECT CHECK_NUM, DESCRIPTION, STATUS, ROW_TYPE, EXC_TABLE, EXC_FIELD, EXC_DETAIL, EXC_COUNT
FROM (
    SELECT * FROM summary
    UNION ALL
    SELECT * FROM details
)
ORDER BY ROW_ORDER
