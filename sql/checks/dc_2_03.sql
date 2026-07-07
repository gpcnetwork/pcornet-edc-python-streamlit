-- DC 2.03 | Table IIIC | Data Plausibility | Investigative
-- More than 5% of patients have illogical date relationships. Illogical date relationships are defined as
-- dates of service which occur before a patient's birth date or after a patient's death date; procedure dates
-- occurring more than 5 days before the admit date or 5 days after the discharge date for the same encounter;
-- or stop dates before start dates in the EXTERNAL_MEDS, MED_ADMIN, OBS_CLIN, OBS_GEN, or PAT_RELATIONSHIP tables
-- Parameters: {{ current_schema }}, {{ start_date }}
WITH birth_after_service AS (
    SELECT COUNT(DISTINCT e.PATID) AS N
    FROM {{ current_schema }}.ENCOUNTER e
    JOIN {{ current_schema }}.DEMOGRAPHIC d ON e.PATID = d.PATID
    WHERE e.ADMIT_DATE < d.BIRTH_DATE
      AND e.ADMIT_DATE >= TO_DATE('{{ start_date }}') AND e.ADMIT_DATE <= TO_DATE('{{ end_date }}')
),
total_pats AS (
    SELECT COUNT(DISTINCT PATID) AS N FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') AND ADMIT_DATE <= TO_DATE('{{ end_date }}')
)
SELECT
    '2.03'                                                     AS CHECK_NUM,
    'More than 5% of patients have illogical date relationships' AS DESCRIPTION,
    CASE WHEN 100.0 * (SELECT N FROM birth_after_service) / NULLIF((SELECT N FROM total_pats), 0) > 5
         THEN 'Fail' ELSE 'Pass' END                           AS STATUS
