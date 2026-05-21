-- DC 1.13: More than 5% of CPT®/HCPCS, CVX, ICD, NDC, LOINC or RXNORM codes do not conform to the expected length or content based on terminology-specific heuristics
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH icd_codes AS (
    SELECT COUNT(*) AS TOTAL, COUNT_IF(NOT REGEXP_LIKE(DX, '^[A-Z][0-9A-Z]{1,6}(\\..*)?$')) AS BAD
    FROM {{ current_schema }}.DIAGNOSIS WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND DX IS NOT NULL AND DX_TYPE = '10'
),
ndc_codes AS (
    SELECT COUNT(*) AS TOTAL, COUNT_IF(NOT REGEXP_LIKE(NDC, '^[0-9]{9,11}$')) AS BAD
    FROM {{ current_schema }}.DISPENSING WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND DISPENSE_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND NDC IS NOT NULL
),
results AS (
    SELECT 'ICD-10' AS TERMINOLOGY, TOTAL, BAD FROM icd_codes
    UNION ALL SELECT 'NDC', TOTAL, BAD FROM ndc_codes
),
summary AS (
    SELECT MAX(ROUND(100.0 * BAD / NULLIF(TOTAL, 0), 2)) AS MAX_PCT_BAD FROM results
)
SELECT
    '1.13'                                             AS CHECK_NUM,
    'More than 5% of CPT®/HCPCS, CVX, ICD, NDC, LOINC or RXNORM codes do not conform to the expected length or content based on terminology-specific heuristics' AS DESCRIPTION,
    CASE WHEN MAX_PCT_BAD > 5 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM summary
