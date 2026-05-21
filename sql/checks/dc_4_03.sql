-- DC 4.03: More than a 5% decrease in the number of records or distinct codes for
-- CPT/HCPCS, CVX, ICD10, LOINC, NDC, or RXNORM codes between
-- the previous and current DataMart refresh 
-- Parameters: {{ current_schema }}, {{ last_schema }}, {{ cutoff_date }}
WITH crt AS (
    SELECT 'DIAGNOSIS-10' AS CODE_TYPE, COUNT(*) AS CURRENT_RECORD, COUNT(DISTINCT DX) AS CURRENT_DISTINCT
    FROM {{ current_schema }}.DIAGNOSIS WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND DX_TYPE = '10' AND DX IS NOT NULL UNION ALL
    SELECT 'PROCEDURES-CH', COUNT(*), COUNT(DISTINCT PX)
    FROM {{ current_schema }}.PROCEDURES WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND PX_TYPE = 'CH' AND PX IS NOT NULL UNION ALL
    SELECT 'DISPENSING-ND', COUNT(*), COUNT(DISTINCT NDC)
    FROM {{ current_schema }}.DISPENSING WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND DISPENSE_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND NDC IS NOT NULL UNION ALL
    SELECT 'PRESCRIBING-RX', COUNT(*), COUNT(DISTINCT RXNORM_CUI)
    FROM {{ current_schema }}.PRESCRIBING WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND RX_ORDER_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND RXNORM_CUI IS NOT NULL
),
old AS (
    SELECT 'DIAGNOSIS-10' AS CODE_TYPE, COUNT(*) AS PREVIOUS_RECORD, COUNT(DISTINCT DX) AS PREVIOUS_DISTINCT
    FROM {{ last_schema }}.DIAGNOSIS WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND DX_TYPE = '10' AND DX IS NOT NULL UNION ALL
    SELECT 'PROCEDURES-CH', COUNT(*), COUNT(DISTINCT PX)
    FROM {{ last_schema }}.PROCEDURES WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND PX_TYPE = 'CH' AND PX IS NOT NULL UNION ALL
    SELECT 'DISPENSING-ND', COUNT(*), COUNT(DISTINCT NDC)
    FROM {{ last_schema }}.DISPENSING WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND DISPENSE_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND NDC IS NOT NULL UNION ALL
    SELECT 'PRESCRIBING-RX', COUNT(*), COUNT(DISTINCT RXNORM_CUI)
    FROM {{ last_schema }}.PRESCRIBING WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND RX_ORDER_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} AND RXNORM_CUI IS NOT NULL
),
exceptions AS (
    SELECT COUNT(*) AS EXCEPTION_COUNT
    FROM crt JOIN old ON crt.CODE_TYPE = old.CODE_TYPE
    WHERE (old.PREVIOUS_RECORD > 0 AND ((crt.CURRENT_RECORD - old.PREVIOUS_RECORD) / old.PREVIOUS_RECORD::FLOAT) * 100 < -5)
       OR (old.PREVIOUS_DISTINCT > 0 AND ((crt.CURRENT_DISTINCT - old.PREVIOUS_DISTINCT) / old.PREVIOUS_DISTINCT::FLOAT) * 100 < -5)
)
SELECT
    '4.03'                                                                  AS CHECK_NUM,
    '> 5% decrease in records or distinct codes per terminology'            AS DESCRIPTION,
    CASE WHEN EXCEPTION_COUNT > 0 THEN 'Fail' ELSE 'Pass' END               AS STATUS
FROM exceptions
