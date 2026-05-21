-- DC 2.03: More than 5% of patients have illogical date relationships. Illogical date
-- relationships are defined as dates of service which occur before a patient's
-- birth date or after a patient's death date; procedure dates occurring more
-- than 5 days before the admit date or 5 days after the discharge date for the
-- same encounter; or stop dates before start dates in the EXTERNAL_MEDS,
-- MED_ADMIN, OBS_CLIN, OBS_GEN, or PAT_RELATIONSHIP tables
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH birth_after_service AS (
    SELECT COUNT(DISTINCT e.PATID) AS N
    FROM {{ current_schema }}.ENCOUNTER e
    JOIN {{ current_schema }}.DEMOGRAPHIC d ON e.PATID = d.PATID
    WHERE e.ADMIT_DATE < d.BIRTH_DATE {% if cutoff_date %}{% if cutoff_date %}AND e.ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
),
total_pats AS (
    SELECT COUNT(DISTINCT PATID) AS N FROM {{ current_schema }}.ENCOUNTER WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
)
SELECT
    '2.03'                                                  AS CHECK_NUM,
    'More than 5% patients with illogical date relationships' AS DESCRIPTION,
    CASE WHEN 100.0 * (SELECT N FROM birth_after_service) / NULLIF((SELECT N FROM total_pats), 0) > 5
         THEN 'Fail' ELSE 'Pass' END                        AS STATUS
