-- DC 3.02 | Table IVB | Data Completeness | Investigative
-- The average number of procedure records with known procedure types per encounter is below
-- threshold [0.75 for AV, 0.75 for ED, 1.00 for EI, and 1.00 for IP encounters].
--
-- SAS source of truth (v7.03): edc_report.sas Table IVB (%print_ivb):
--   perc(enc_type) = PROCEDURES records with known PX_TYPE for that ENC_TYPE
--                    (PRO_L3_PXTYPE_ENCTYPE, PX_TYPE not NI/UN/OT/missing)
--                    / ENCOUNTER records of that ENC_TYPE (record-count ratio, not a join);
--   exception (row=3.02) when (AV/ED: 0 < perc < 0.75) or (EI/IP: 0 < perc < 1.00).
--   Table loads restricted to (ADMIT_DATE >= lookback OR NULL), no upper bound.
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH enc AS (
    SELECT ENC_TYPE, COUNT(*) AS ENC_N
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ENC_TYPE IN ('AV','ED','EI','IP')
      AND (ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL)
    GROUP BY ENC_TYPE
),
px AS (
    SELECT ENC_TYPE, COUNT(*) AS PX_KNOWN_N
    FROM {{ current_schema }}.PROCEDURES
    WHERE ENC_TYPE IN ('AV','ED','EI','IP')
      AND (ADMIT_DATE >= TO_DATE('{{ start_date }}') OR ADMIT_DATE IS NULL)
      AND NULLIF(TRIM(PX_TYPE), '') IS NOT NULL
      AND TRIM(PX_TYPE) NOT IN ('NI','UN','OT')
    GROUP BY ENC_TYPE
),
thresholds AS (
    SELECT v.ENC_TYPE, v.THRESH
    FROM (VALUES ('AV', 0.75), ('ED', 0.75), ('EI', 1.00), ('IP', 1.00)) v(ENC_TYPE, THRESH)
),
scored AS (
    SELECT e.ENC_TYPE, e.ENC_N, COALESCE(p.PX_KNOWN_N, 0) AS PX_KNOWN_N, t.THRESH,
           CASE WHEN e.ENC_N > 0 THEN COALESCE(p.PX_KNOWN_N, 0) / e.ENC_N END AS PERC
    FROM enc e
    JOIN thresholds t ON t.ENC_TYPE = e.ENC_TYPE
    LEFT JOIN px p ON p.ENC_TYPE = e.ENC_TYPE
),
summary AS (
    SELECT
        '3.02'                                                                 AS CHECK_NUM,
        'Average procedures with known PX_TYPE per encounter below threshold (AV/ED 0.75; EI/IP 1.00)' AS DESCRIPTION,
        CASE WHEN COUNT_IF(PERC > 0 AND PERC < THRESH) > 0 THEN 'Fail' ELSE 'Pass' END AS STATUS,
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
        '3.02', 'Procedures per encounter below threshold', 'Fail', 'DETAIL',
        'PROCEDURES', ENC_TYPE,
        TO_VARCHAR(ROUND(PERC, 2)) || ' known-type procedures per encounter (' ||
            TO_VARCHAR(PX_KNOWN_N) || ' / ' || TO_VARCHAR(ENC_N) || '; threshold ' ||
            TO_VARCHAR(THRESH) || ')',
        PX_KNOWN_N,
        ROW_NUMBER() OVER (ORDER BY PERC)
    FROM scored
    WHERE PERC > 0 AND PERC < THRESH
)
SELECT CHECK_NUM, DESCRIPTION, STATUS, ROW_TYPE, EXC_TABLE, EXC_FIELD, EXC_DETAIL, EXC_COUNT
FROM (
    SELECT * FROM summary
    UNION ALL
    SELECT * FROM details
)
ORDER BY ROW_ORDER
