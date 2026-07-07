-- DC 1.20 | Table IIG | Data Model Conformance | Investigative
-- More than 5% of LOINC records in the LAB_RESULT_CM, PRO_CM, and OBS_CLIN tables are panel codes based on the LOINC panel type
-- Parameters: {{ current_schema }}, {{ start_date }}, {{ loinc_ref_fqn }}
WITH panel_ref AS (
    {% if loinc_ref_fqn %}
    SELECT DISTINCT TRIM(UPPER(LOINC_NUM)) AS LOINC_NUM
    FROM {{ loinc_ref_fqn }}
    WHERE LOINC_NUM IS NOT NULL AND TRIM(LOINC_NUM) <> ''
      AND TRIM(PANEL_TYPE) = 'Panel'
    {% else %}
    SELECT NULL::VARCHAR AS LOINC_NUM WHERE 1=0
    {% endif %}
),
lab AS (
    SELECT COUNT(*) AS TOTAL,
           SUM(CASE WHEN p.LOINC_NUM IS NOT NULL THEN 1 ELSE 0 END) AS PANELS
    FROM {{ current_schema }}.LAB_RESULT_CM l
    LEFT JOIN panel_ref p ON TRIM(UPPER(l.LAB_LOINC)) = p.LOINC_NUM
    WHERE l.LAB_LOINC IS NOT NULL
      AND l.RESULT_DATE >= TO_DATE('{{ start_date }}') AND l.RESULT_DATE <= TO_DATE('{{ end_date }}')
),
obs AS (
    SELECT COUNT(*) AS TOTAL,
           SUM(CASE WHEN p.LOINC_NUM IS NOT NULL THEN 1 ELSE 0 END) AS PANELS
    FROM {{ current_schema }}.OBS_CLIN o
    LEFT JOIN panel_ref p ON TRIM(UPPER(o.OBSCLIN_CODE)) = p.LOINC_NUM
    WHERE o.OBSCLIN_CODE IS NOT NULL AND o.OBSCLIN_TYPE = 'LC'
      AND o.OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}') AND o.OBSCLIN_START_DATE <= TO_DATE('{{ end_date }}')
),
pro AS (
    SELECT COUNT(*) AS TOTAL,
           SUM(CASE WHEN p.LOINC_NUM IS NOT NULL THEN 1 ELSE 0 END) AS PANELS
    FROM {{ current_schema }}.PRO_CM pr
    LEFT JOIN panel_ref p ON TRIM(UPPER(pr.PRO_CODE)) = p.LOINC_NUM
    WHERE pr.PRO_CODE IS NOT NULL
)
SELECT
    '1.20'                                                                          AS CHECK_NUM,
    'More than 5% of LOINC records in LAB_RESULT_CM/PRO_CM/OBS_CLIN are panel codes' AS DESCRIPTION,
    CASE
        WHEN GREATEST(
            COALESCE(100.0 * lab.PANELS / NULLIF(lab.TOTAL, 0), 0),
            COALESCE(100.0 * obs.PANELS / NULLIF(obs.TOTAL, 0), 0),
            COALESCE(100.0 * pro.PANELS / NULLIF(pro.TOTAL, 0), 0)
        ) > 5 THEN 'Fail'
        ELSE 'Pass'
    END                                                                             AS STATUS
FROM lab, obs, pro
