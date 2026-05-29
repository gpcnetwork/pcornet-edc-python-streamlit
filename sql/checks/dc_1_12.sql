-- DC 1.12 | Table IIE | Data Model Conformance | Required
-- Tables contain orphan PROVIDERIDs (not present in the PROVIDER table). Evaluated across 8 tables,
-- each with its own provider-ID column. Fails if any orphan PROVIDERID is found anywhere.
-- Parameters: {{ current_schema }}
WITH all_orphans AS (
{% set dc112_tables = [
    ('DIAGNOSIS', 'PROVIDERID'),
    ('ENCOUNTER', 'PROVIDERID'),
    ('IMMUNIZATION', 'VX_PROVIDERID'),
    ('MED_ADMIN', 'MEDADMIN_PROVIDERID'),
    ('OBS_CLIN', 'OBSCLIN_PROVIDERID'),
    ('OBS_GEN', 'OBSGEN_PROVIDERID'),
    ('PRESCRIBING', 'RX_PROVIDERID'),
    ('PROCEDURES', 'PROVIDERID'),
] %}
{% for tbl, pcol in dc112_tables %}
    {% if not loop.first %}UNION ALL {% endif %}SELECT COUNT(DISTINCT t.{{ pcol }}) AS N
    FROM {{ current_schema }}.{{ tbl }} t
    WHERE t.{{ pcol }} IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM {{ current_schema }}.PROVIDER p WHERE p.PROVIDERID = t.{{ pcol }})
{% endfor %}
)
SELECT
    '1.12'                                                                AS CHECK_NUM,
    'Tables contain orphan PROVIDERIDs not present in the PROVIDER table' AS DESCRIPTION,
    CASE WHEN COALESCE(SUM(N), 0) > 0 THEN 'Fail' ELSE 'Pass' END         AS STATUS
FROM all_orphans
