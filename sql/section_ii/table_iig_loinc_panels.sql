-- Table IIG. LOINC Panel Codes
-- Exceptions to DC 1.20 (more than 5% of LOINC records are panel codes).
-- Exceptions highlighted in blue; must be explained in the ETL ADD.
-- Requires a LOINC reference table with LOINC_NUM and CLASS columns.

WITH panel_ref AS (
    {% if loinc_ref_fqn %}
    SELECT DISTINCT TRIM(UPPER(LOINC_NUM)) AS LOINC_NUM
    FROM {{ loinc_ref_fqn }}
    WHERE LOINC_NUM IS NOT NULL AND TRIM(LOINC_NUM) != ''
      AND UPPER(TRIM(CLASSTYPE)) = 'PANEL'
    {% else %}
    SELECT NULL::VARCHAR AS LOINC_NUM WHERE 1=0
    {% endif %}
),
lab_loinc AS (
    SELECT 'LAB_RESULT_CM' AS TABLE_NAME,
           'LAB_LOINC' AS FIELD_NAME,
           COUNT(*) AS TOTAL_LOINC_RECORDS,
           SUM(CASE WHEN p.LOINC_NUM IS NOT NULL THEN 1 ELSE 0 END) AS LOINC_PANEL_RECORDS,
           'LAB_L3_LOINC' AS SOURCE_TABLE
    FROM {{ current_schema }}.LAB_RESULT_CM l
    LEFT JOIN panel_ref p ON TRIM(UPPER(l.LAB_LOINC)) = p.LOINC_NUM
    WHERE l.LAB_LOINC IS NOT NULL
      AND l.RESULT_DATE >= TO_DATE('{{ start_date }}')
),
obs_clin_loinc AS (
    SELECT 'OBS_CLIN' AS TABLE_NAME,
           'OBSCLIN_CODE' AS FIELD_NAME,
           COUNT(*) AS TOTAL_LOINC_RECORDS,
           SUM(CASE WHEN p.LOINC_NUM IS NOT NULL THEN 1 ELSE 0 END) AS LOINC_PANEL_RECORDS,
           'OBSCLIN_L3_CODE' AS SOURCE_TABLE
    FROM {{ current_schema }}.OBS_CLIN o
    LEFT JOIN panel_ref p ON TRIM(UPPER(o.OBSCLIN_CODE)) = p.LOINC_NUM
    WHERE o.OBSCLIN_CODE IS NOT NULL AND o.OBSCLIN_TYPE = 'LC'
      AND o.OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}')
),
pro_cm_loinc AS (
    SELECT 'PRO_CM' AS TABLE_NAME,
           'PRO_CODE' AS FIELD_NAME,
           COUNT(*) AS TOTAL_LOINC_RECORDS,
           SUM(CASE WHEN p.LOINC_NUM IS NOT NULL THEN 1 ELSE 0 END) AS LOINC_PANEL_RECORDS,
           'PROM_L3_CODE' AS SOURCE_TABLE
    FROM {{ current_schema }}.PRO_CM pr
    LEFT JOIN panel_ref p ON TRIM(UPPER(pr.PRO_CODE)) = p.LOINC_NUM
    WHERE pr.PRO_CODE IS NOT NULL
      AND pr.PRO_DATE >= TO_DATE('{{ start_date }}')
)
SELECT TABLE_NAME, FIELD_NAME,
       TO_VARCHAR(LOINC_PANEL_RECORDS) AS LOINC_PANEL_RECORDS,
       TO_VARCHAR(TOTAL_LOINC_RECORDS) AS TOTAL_LOINC_RECORDS,
       TO_VARCHAR(ROUND(LOINC_PANEL_RECORDS * 100.0 / NULLIF(TOTAL_LOINC_RECORDS, 0), 1)) || '%' AS PCT_PANEL,
       SOURCE_TABLE
FROM (
    SELECT * FROM lab_loinc
    UNION ALL
    SELECT * FROM obs_clin_loinc
    UNION ALL
    SELECT * FROM pro_cm_loinc
)
ORDER BY TABLE_NAME
