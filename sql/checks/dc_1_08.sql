-- DC 1.08 | Table IIE | Data Model Conformance | Required
-- Tables contain orphan PATIDs (PATIDs that are not present in the DEMOGRAPHIC table)
-- Fails if any of the 19 CDM tables (CONDITION, DIAGNOSIS, DEATH, DEATH_CAUSE, DISPENSING, ENCOUNTER,
-- ENROLLMENT, HASH_TOKEN, IMMUNIZATION, LAB_RESULT_CM, LDS_ADDRESS_HISTORY, MED_ADMIN, OBS_CLIN,
-- OBS_GEN, PCORNET_TRIAL, PRESCRIBING, PROCEDURES, PRO_CM, VITAL) has any PATID not in DEMOGRAPHIC.
-- Parameters: {{ current_schema }}
WITH all_orphans AS (
{% for t in ['CONDITION','DIAGNOSIS','DEATH','DEATH_CAUSE','DISPENSING','ENCOUNTER','ENROLLMENT','HASH_TOKEN','IMMUNIZATION','LAB_RESULT_CM','LDS_ADDRESS_HISTORY','MED_ADMIN','OBS_CLIN','OBS_GEN','PCORNET_TRIAL','PRESCRIBING','PROCEDURES','PRO_CM','VITAL'] %}
    {% if not loop.first %}UNION ALL {% endif %}SELECT COUNT(DISTINCT t.PATID) AS N FROM {{ current_schema }}.{{ t }} t
    WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)
{% endfor %}
)
SELECT
    '1.08'                                                                       AS CHECK_NUM,
    'Tables contain orphan PATIDs (PATIDs not present in the DEMOGRAPHIC table)' AS DESCRIPTION,
    CASE WHEN COALESCE(SUM(N), 0) > 0 THEN 'Fail' ELSE 'Pass' END                AS STATUS
FROM all_orphans
