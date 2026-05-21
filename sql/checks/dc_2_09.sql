-- DC 2.09: Less than 80% of patients with a face-to-face encounter during the past 5
-- years have at least 1 face-to-face diagnosis and 1 vital measurement.
-- Face-to-face is defined as an encounter type of ambulatory visit (AV),
-- emergency department (ED), emergency department admit to inpatient
-- hospital stay (EI), inpatient hospital (IP), or observation stay (OS).
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH f2f_pats AS (
    SELECT DISTINCT PATID
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ENC_TYPE IN ('AV','ED','EI','IP','OS')
      {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
),
with_dx AS (SELECT DISTINCT PATID FROM {{ current_schema }}.DIAGNOSIS WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}),
with_vital AS (SELECT DISTINCT PATID FROM {{ current_schema }}.VITAL WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND MEASURE_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}),
counts AS (
    SELECT
        COUNT(*) AS TOTAL,
        COUNT_IF(EXISTS (SELECT 1 FROM with_dx d WHERE d.PATID = f.PATID)
              AND EXISTS (SELECT 1 FROM with_vital v WHERE v.PATID = f.PATID)) AS COMPLETE
    FROM f2f_pats f
)
SELECT
    '2.09'                                              AS CHECK_NUM,
    '< 80% F2F patients with DX + VITAL'                AS DESCRIPTION,
    CASE WHEN 100.0 * COMPLETE / NULLIF(TOTAL, 0) < 80 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM counts
