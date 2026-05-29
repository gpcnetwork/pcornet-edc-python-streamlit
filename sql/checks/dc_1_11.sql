-- DC 1.11 | Table IIE | Data Model Conformance | Required
-- More than 5% of encounters are assigned to more than one patient. Evaluated per table across
-- CONDITION, DIAGNOSIS, ENCOUNTER, IMMUNIZATION, LAB_RESULT_CM, MED_ADMIN, OBS_CLIN, OBS_GEN,
-- PRESCRIBING, PROCEDURES, PRO_CM, VITAL — each filtered by its standard date column >= cutoff_date.
-- Fails if any table's multi-patient-encounter rate exceeds 5%.
-- Parameters: {{ current_schema }}, {{ cutoff_date }} (falls back to {{ end_date }})
WITH per_table AS (
{% set dc111_tables = [
    ('CONDITION', 'REPORT_DATE'),
    ('DIAGNOSIS', 'ADMIT_DATE'),
    ('ENCOUNTER', 'ADMIT_DATE'),
    ('IMMUNIZATION', 'VX_ADMIN_DATE'),
    ('LAB_RESULT_CM', 'RESULT_DATE'),
    ('MED_ADMIN', 'MEDADMIN_START_DATE'),
    ('OBS_CLIN', 'OBSCLIN_START_DATE'),
    ('OBS_GEN', 'OBSGEN_START_DATE'),
    ('PRESCRIBING', 'RX_ORDER_DATE'),
    ('PROCEDURES', 'PX_DATE'),
    ('PRO_CM', 'PRO_DATE'),
    ('VITAL', 'MEASURE_DATE'),
] %}
{% for tbl, dcol in dc111_tables %}
    {% if not loop.first %}UNION ALL {% endif %}SELECT
        (SELECT COUNT(*) FROM (
            SELECT ENCOUNTERID FROM {{ current_schema }}.{{ tbl }}
            WHERE ENCOUNTERID IS NOT NULL AND {{ dcol }} >= TO_DATE('{{ cutoff_date or end_date }}')
            GROUP BY ENCOUNTERID HAVING COUNT(DISTINCT PATID) > 1
        )) * 100.0
        / NULLIF((SELECT COUNT(DISTINCT ENCOUNTERID) FROM {{ current_schema }}.{{ tbl }}
                  WHERE ENCOUNTERID IS NOT NULL AND {{ dcol }} >= TO_DATE('{{ cutoff_date or end_date }}')), 0) AS PCT
{% endfor %}
)
SELECT
    '1.11'                                                            AS CHECK_NUM,
    'More than 5% of encounters are assigned to more than one patient' AS DESCRIPTION,
    CASE WHEN COALESCE(MAX(PCT), 0) > 5 THEN 'Fail' ELSE 'Pass' END   AS STATUS
FROM per_table
