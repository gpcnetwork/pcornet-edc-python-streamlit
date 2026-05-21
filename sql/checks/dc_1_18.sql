-- DC 1.18: Table refresh dates are not documented in the HARVEST table for populated tables
-- Parameters: {{ current_schema }}
WITH harvest AS (
    SELECT * FROM {{ current_schema }}.HARVEST LIMIT 1
),
checks AS (
    SELECT 'DEMOGRAPHIC'    AS T, REFRESH_DEMOGRAPHIC_DATE     AS D FROM harvest UNION ALL
    SELECT 'ENROLLMENT',         REFRESH_ENROLLMENT_DATE           FROM harvest UNION ALL
    SELECT 'ENCOUNTER',          REFRESH_ENCOUNTER_DATE            FROM harvest UNION ALL
    SELECT 'DIAGNOSIS',          REFRESH_DIAGNOSIS_DATE            FROM harvest UNION ALL
    SELECT 'PROCEDURES',         REFRESH_PROCEDURES_DATE           FROM harvest UNION ALL
    SELECT 'VITAL',              REFRESH_VITAL_DATE                FROM harvest UNION ALL
    SELECT 'PRESCRIBING',        REFRESH_PRESCRIBING_DATE          FROM harvest UNION ALL
    SELECT 'LAB_RESULT_CM',      REFRESH_LAB_RESULT_CM_DATE        FROM harvest UNION ALL
    SELECT 'DISPENSING',         REFRESH_DISPENSING_DATE           FROM harvest UNION ALL
    SELECT 'MED_ADMIN',          REFRESH_MED_ADMIN_DATE            FROM harvest UNION ALL
    SELECT 'IMMUNIZATION',       REFRESH_IMMUNIZATION_DATE         FROM harvest
),
missing AS (SELECT COUNT(*) AS N FROM checks WHERE D IS NULL)
SELECT
    '1.18'                                                  AS CHECK_NUM,
    'Table refresh dates are not documented in the HARVEST table for populated tables' AS DESCRIPTION,
    CASE WHEN N > 0 THEN 'Fail' ELSE 'Pass' END             AS STATUS
FROM missing
