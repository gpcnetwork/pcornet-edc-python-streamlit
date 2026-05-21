-- Table IE. Records Per Table By Encounter Type
-- Record counts by encounter type for ENCOUNTER, DIAGNOSIS, and PROCEDURES.
-- Supports DC 2.01, 2.02, 2.03.

WITH enc_total AS (
    SELECT COUNT(*) AS n FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
dx_total AS (
    SELECT COUNT(*) AS n FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
px_total AS (
    SELECT COUNT(*) AS n FROM {{ current_schema }}.PROCEDURES
    WHERE PX_DATE >= TO_DATE('{{ start_date }}')
),
enc_by_type AS (
    SELECT
        CASE WHEN ENC_TYPE IN ('AV','ED','EI','IC','IP','IS','OA','OS','TH') THEN ENC_TYPE
             ELSE 'Missing/NI/UN/OT' END AS ENC_TYPE_GRP,
        COUNT(*) AS ENC_N
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY 1
),
dx_by_type AS (
    SELECT
        CASE WHEN ENC_TYPE IN ('AV','ED','EI','IC','IP','IS','OA','OS','TH') THEN ENC_TYPE
             ELSE 'Missing/NI/UN/OT' END AS ENC_TYPE_GRP,
        COUNT(*) AS DX_N
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY 1
),
px_by_type AS (
    SELECT
        CASE WHEN ENC_TYPE IN ('AV','ED','EI','IC','IP','IS','OA','OS','TH') THEN ENC_TYPE
             ELSE 'Missing/NI/UN/OT' END AS ENC_TYPE_GRP,
        COUNT(*) AS PX_N
    FROM {{ current_schema }}.PROCEDURES
    WHERE PX_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY 1
),
all_types AS (
    SELECT 'AV' AS ENC_TYPE, 1 AS ROW_ORDER UNION ALL
    SELECT 'ED', 2 UNION ALL
    SELECT 'EI', 3 UNION ALL
    SELECT 'IC', 4 UNION ALL
    SELECT 'IP', 5 UNION ALL
    SELECT 'IS', 6 UNION ALL
    SELECT 'OA', 7 UNION ALL
    SELECT 'OS', 8 UNION ALL
    SELECT 'TH', 9 UNION ALL
    SELECT 'Missing/NI/UN/OT', 10
)
SELECT
    t.ENC_TYPE,
    TO_VARCHAR(COALESCE(e.ENC_N, 0))                                               AS "ENCOUNTER__N",
    TO_VARCHAR(ROUND(COALESCE(e.ENC_N, 0) * 100.0 / NULLIF(et.n, 0), 1)) || '%'  AS "ENCOUNTER__%",
    TO_VARCHAR(COALESCE(d.DX_N, 0))                                                AS "DIAGNOSIS__N",
    TO_VARCHAR(ROUND(COALESCE(d.DX_N, 0) * 100.0 / NULLIF(dt.n, 0), 1)) || '%'   AS "DIAGNOSIS__%",
    TO_VARCHAR(COALESCE(p.PX_N, 0))                                                AS "PROCEDURES__N",
    TO_VARCHAR(ROUND(COALESCE(p.PX_N, 0) * 100.0 / NULLIF(pt.n, 0), 1)) || '%'   AS "PROCEDURES__%"
FROM all_types t
LEFT JOIN enc_by_type e ON e.ENC_TYPE_GRP = t.ENC_TYPE
LEFT JOIN dx_by_type d ON d.ENC_TYPE_GRP = t.ENC_TYPE
LEFT JOIN px_by_type p ON p.ENC_TYPE_GRP = t.ENC_TYPE
CROSS JOIN enc_total et
CROSS JOIN dx_total dt
CROSS JOIN px_total pt

UNION ALL

SELECT
    'Total',
    TO_VARCHAR(et.n),
    '100%',
    TO_VARCHAR(dt.n),
    '100%',
    TO_VARCHAR(pt.n),
    '100%'
FROM enc_total et, dx_total dt, px_total pt

ORDER BY
    CASE ENC_TYPE
        WHEN 'AV' THEN 1 WHEN 'ED' THEN 2 WHEN 'EI' THEN 3 WHEN 'IC' THEN 4
        WHEN 'IP' THEN 5 WHEN 'IS' THEN 6 WHEN 'OA' THEN 7 WHEN 'OS' THEN 8
        WHEN 'TH' THEN 9 WHEN 'Missing/NI/UN/OT' THEN 10 ELSE 11 END
