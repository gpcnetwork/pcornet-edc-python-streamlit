-- DC 1.09 | Table IIE | Data Model Conformance | Required
-- Tables contain orphan ENCOUNTERIDs (not in the ENCOUNTER table) for more than 5% of records.
-- Evaluated per table across CONDITION, DIAGNOSIS, IMMUNIZATION, LAB_RESULT_CM, MED_ADMIN,
-- OBS_CLIN, OBS_GEN, PRESCRIBING, PROCEDURES, PRO_CM, VITAL. Fails if any table's orphan-rate > 5%.
-- Parameters: {{ current_schema }}
WITH per_table AS (
{% for t in ['CONDITION','DIAGNOSIS','IMMUNIZATION','LAB_RESULT_CM','MED_ADMIN','OBS_CLIN','OBS_GEN','PRESCRIBING','PROCEDURES','PRO_CM','VITAL'] %}
    {% if not loop.first %}UNION ALL {% endif %}SELECT
        (SELECT COUNT(DISTINCT t.ENCOUNTERID) FROM {{ current_schema }}.{{ t }} t
         WHERE t.ENCOUNTERID IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM {{ current_schema }}.ENCOUNTER e WHERE e.ENCOUNTERID = t.ENCOUNTERID))
        * 100.0
        / NULLIF((SELECT COUNT(DISTINCT ENCOUNTERID) FROM {{ current_schema }}.{{ t }}
                  WHERE ENCOUNTERID IS NOT NULL), 0) AS PCT
{% endfor %}
)
SELECT
    '1.09'                                                                                       AS CHECK_NUM,
    'Tables contain orphan ENCOUNTERIDs not present in the ENCOUNTER table for > 5% of records' AS DESCRIPTION,
    CASE WHEN COALESCE(MAX(PCT), 0) > 5 THEN 'Fail' ELSE 'Pass' END                              AS STATUS
FROM per_table
