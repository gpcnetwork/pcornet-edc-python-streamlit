-- DC 1.11: More than 5% of encounters are assigned to more than one patient
-- Parameters: {{ current_schema }}
WITH multi AS (
    SELECT
        COUNT(DISTINCT ENCOUNTERID) AS TOTAL_ENC,
        COUNT_IF(CNT > 1) AS MULTI_PATIENT_ENC
    FROM (
        SELECT ENCOUNTERID, COUNT(DISTINCT PATID) AS CNT
        FROM {{ current_schema }}.ENCOUNTER
        GROUP BY ENCOUNTERID
    )
)
SELECT
    '1.11'                                              AS CHECK_NUM,
    'More than 5% of encounters are assigned to more than one patient' AS DESCRIPTION,
    CASE WHEN 100.0 * MULTI_PATIENT_ENC / NULLIF(TOTAL_ENC, 0) > 5 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM multi
