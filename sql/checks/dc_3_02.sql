-- DC 3.02: The average number of procedure records with known procedure types per
-- encounter is below threshold [0.75 for ambulatory (AV) encounters, 0.75 for
-- emergency department (ED) encounters, 1.00 for ED to inpatient (EI)
-- encounters, and 1.00 for inpatient (IP) encounters].
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH px_per_enc AS (
    SELECT e.ENCOUNTERID, e.ENC_TYPE,
           COUNT_IF(p.PX_TYPE NOT IN ('NI','UN','OT') AND p.PX_TYPE IS NOT NULL) AS PX_COUNT
    FROM {{ current_schema }}.ENCOUNTER e
    LEFT JOIN {{ current_schema }}.PROCEDURES p ON p.ENCOUNTERID = e.ENCOUNTERID
    WHERE e.ENC_TYPE IN ('AV','ED','EI','IP')
      {% if cutoff_date %}{% if cutoff_date %}AND e.ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
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
summary AS (SELECT MAX(IS_EXCEPTION) AS HAS_ANY_EXCEPTION, MIN(AVG_PX) AS WORST_AVG FROM check_results)
SELECT
    '3.02'                                                          AS CHECK_NUM,
    'Average PX per encounter < threshold by encounter type'        AS DESCRIPTION,
    CASE WHEN HAS_ANY_EXCEPTION = 1 THEN 'Fail' ELSE 'Pass' END     AS STATUS
FROM summary
