-- DC 3.01 | Table IVA | Data Completeness | Investigative
-- The average number of diagnosis records with known diagnosis types per encounter is below
-- threshold [1.0 for AV, ED, EI, IP, or TH encounters].
--
-- SAS source of truth (v7.03): edc_report.sas Table IVA (%print_iva):
--   perc(enc_type) = DIAGNOSIS records with known DX_TYPE for that ENC_TYPE
--                    (DIA_L3_DXTYPE_ENCTYPE, DX_TYPE not NI/UN/OT/missing)
--                    / ENCOUNTER records of that ENC_TYPE (ENC_L3_ENCTYPE RECORD_N);
--   exception (row=3.01) when 0 < perc < 1.00 for any of AV, ED, EI, IP, TH
--   (per encounter type — not pooled; record-count ratio, not an ENCOUNTERID join).
--   Table loads restricted to (ADMIT_DATE >= lookback OR NULL), no upper bound.
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH enc AS (
    SELECT ENC_TYPE, COUNT(*) AS ENC_N
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ENC_TYPE IN ('AV','ED','EI','IP','TH')
      AND (ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL)
    GROUP BY ENC_TYPE
),
dx AS (
    SELECT ENC_TYPE, COUNT(*) AS DX_KNOWN_N
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ENC_TYPE IN ('AV','ED','EI','IP','TH')
      AND (ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL)
      AND NULLIF(TRIM(DX_TYPE), '') IS NOT NULL
      AND TRIM(DX_TYPE) NOT IN ('NI','UN','OT')
    GROUP BY ENC_TYPE
),
scored AS (
    SELECT e.ENC_TYPE, e.ENC_N, COALESCE(d.DX_KNOWN_N, 0) AS DX_KNOWN_N,
           CASE WHEN e.ENC_N > 0 THEN COALESCE(d.DX_KNOWN_N, 0) / e.ENC_N END AS PERC
    FROM enc e
    LEFT JOIN dx d ON d.ENC_TYPE = e.ENC_TYPE
),
summary AS (
    SELECT
        '3.01'                                                                 AS CHECK_NUM,
        'Average diagnoses with known DX_TYPE per encounter below 1.0 for AV/ED/EI/IP/TH' AS DESCRIPTION,
        CASE WHEN COUNT_IF(PERC > 0 AND PERC < 1.00) > 0 THEN 'Fail' ELSE 'Pass' END AS STATUS,
        'SUMMARY'                                                              AS ROW_TYPE,
        ''                                                                     AS EXC_TABLE,
        ''                                                                     AS EXC_FIELD,
        'min ratio = ' || TO_VARCHAR(COALESCE(ROUND(MIN(PERC), 2), 0))         AS EXC_DETAIL,
        NULL                                                                   AS EXC_COUNT,
        0                                                                      AS ROW_ORDER
    FROM scored
),
details AS (
    SELECT
        '3.01', 'Diagnoses per encounter below threshold', 'Fail', 'DETAIL',
        'DIAGNOSIS', ENC_TYPE,
        TO_VARCHAR(ROUND(PERC, 2)) || ' known-type diagnoses per encounter (' ||
            TO_VARCHAR(DX_KNOWN_N) || ' / ' || TO_VARCHAR(ENC_N) || ')',
        DX_KNOWN_N,
        ROW_NUMBER() OVER (ORDER BY PERC)
    FROM scored
    WHERE PERC > 0 AND PERC < 1.00
)
SELECT CHECK_NUM, DESCRIPTION, STATUS, ROW_TYPE, EXC_TABLE, EXC_FIELD, EXC_DETAIL, EXC_COUNT
FROM (
    SELECT * FROM summary
    UNION ALL
    SELECT * FROM details
)
ORDER BY ROW_ORDER
