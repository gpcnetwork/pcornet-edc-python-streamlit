-- DC 3.02 | Table IVB | Data Completeness | Investigative
-- The average number of procedure records with known procedure types per encounter is below threshold
-- [0.75 for AV encounters, 0.75 for ED encounters, 1.00 for EI encounters, and 1.00 for IP encounters]
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH px_per_enc AS (
    SELECT e.ENCOUNTERID, e.ENC_TYPE,
           COUNT_IF(p.PX_TYPE NOT IN ('NI','UN','OT') AND p.PX_TYPE IS NOT NULL) AS PX_COUNT
    FROM {{ current_schema }}.ENCOUNTER e
    LEFT JOIN {{ current_schema }}.PROCEDURES p ON p.ENCOUNTERID = e.ENCOUNTERID
    WHERE e.ENC_TYPE IN ('AV','ED','EI','IP')
      AND e.ADMIT_DATE >= TO_DATE('{{ start_date }}') AND e.ADMIT_DATE <= TO_DATE('{{ end_date }}')
    GROUP BY e.ENCOUNTERID, e.ENC_TYPE
),
avg_by_type AS (
    SELECT ENC_TYPE, ROUND(AVG(PX_COUNT), 2) AS AVG_PX FROM px_per_enc GROUP BY ENC_TYPE
),
thresholds AS (
    SELECT v.ENC_TYPE, v.THRESH FROM (VALUES ('AV',0.75),('ED',0.75),('EI',1.0),('IP',1.0)) v(ENC_TYPE, THRESH)
),
check_results AS (
    SELECT a.ENC_TYPE, a.AVG_PX, t.THRESH,
           CASE WHEN a.AVG_PX < t.THRESH THEN 1 ELSE 0 END AS IS_EXCEPTION
    FROM avg_by_type a JOIN thresholds t ON a.ENC_TYPE = t.ENC_TYPE
),
summary AS (SELECT MAX(IS_EXCEPTION) AS HAS_ANY_EXCEPTION FROM check_results)
SELECT
    '3.02'                                                                      AS CHECK_NUM,
    'Average number of procedures with known PX_TYPE per encounter is below threshold' AS DESCRIPTION,
    CASE WHEN HAS_ANY_EXCEPTION = 1 THEN 'Fail' ELSE 'Pass' END                 AS STATUS
FROM summary
