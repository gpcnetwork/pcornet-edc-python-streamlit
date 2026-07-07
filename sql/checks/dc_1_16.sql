-- DC 1.16 | Table IIF | Data Model Conformance | Investigative
-- Laboratory results or clinical observations are recorded in the wrong table based on the LOINC classtype.
-- A LAB_RESULT_CM row is wrong-tabled if its LOINC has CLASSTYPE <> '1' (not a Laboratory LOINC).
-- An OBS_CLIN row with OBSCLIN_TYPE='LC' is wrong-tabled if its LOINC has CLASSTYPE = '1' (a Lab LOINC in OBS_CLIN).
-- Parameters: {{ current_schema }}, {{ start_date }}, {{ loinc_ref_fqn }}
WITH loinc_class AS (
    {% if loinc_ref_fqn %}
    SELECT DISTINCT TRIM(UPPER(LOINC_NUM)) AS LOINC_NUM,
                    TRIM(CLASSTYPE)        AS CLASSTYPE
    FROM {{ loinc_ref_fqn }}
    WHERE LOINC_NUM IS NOT NULL AND TRIM(LOINC_NUM) <> ''
    {% else %}
    SELECT NULL::VARCHAR AS LOINC_NUM, NULL::VARCHAR AS CLASSTYPE WHERE 1=0
    {% endif %}
),
lab_wrong AS (
    SELECT COUNT(*) AS N
    FROM {{ current_schema }}.LAB_RESULT_CM l
    JOIN loinc_class lc ON TRIM(UPPER(l.LAB_LOINC)) = lc.LOINC_NUM
    WHERE l.LAB_LOINC IS NOT NULL
      AND l.RESULT_DATE >= TO_DATE('{{ start_date }}') AND l.RESULT_DATE <= TO_DATE('{{ end_date }}')
      AND lc.CLASSTYPE <> '1'
),
obs_wrong AS (
    SELECT COUNT(*) AS N
    FROM {{ current_schema }}.OBS_CLIN o
    JOIN loinc_class lc ON TRIM(UPPER(o.OBSCLIN_CODE)) = lc.LOINC_NUM
    WHERE o.OBSCLIN_CODE IS NOT NULL
      AND o.OBSCLIN_TYPE = 'LC'
      AND o.OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}') AND o.OBSCLIN_START_DATE <= TO_DATE('{{ end_date }}')
      AND lc.CLASSTYPE = '1'
),
total AS (
    SELECT (SELECT N FROM lab_wrong) + (SELECT N FROM obs_wrong) AS TOTAL_MISCLASSIFIED
)
SELECT
    '1.16'                                                                                   AS CHECK_NUM,
    'Laboratory results or clinical observations recorded in the wrong table based on LOINC' AS DESCRIPTION,
    CASE WHEN TOTAL_MISCLASSIFIED > 0 THEN 'Fail' ELSE 'Pass' END                            AS STATUS
FROM total
