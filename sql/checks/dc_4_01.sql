-- DC 4.01: More than a 5% decrease in the number of patients or records in a CDM
-- table between the previous and current DataMart refresh
-- Parameters: {{ current_schema }}, {{ last_schema }}, {{ cutoff_date }}
WITH crt AS (
    SELECT 'ENCOUNTER' AS TABLE_NAME, COUNT(*) AS CURRENT_RECORD, COUNT(DISTINCT PATID) AS CURRENT_PATIENTS
    FROM {{ current_schema }}.ENCOUNTER WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} UNION ALL
    SELECT 'DIAGNOSIS', COUNT(*), COUNT(DISTINCT PATID) FROM {{ current_schema }}.DIAGNOSIS WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} UNION ALL
    SELECT 'PROCEDURES', COUNT(*), COUNT(DISTINCT PATID) FROM {{ current_schema }}.PROCEDURES WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} UNION ALL
    SELECT 'DEMOGRAPHIC', COUNT(*), COUNT(DISTINCT PATID) FROM {{ current_schema }}.DEMOGRAPHIC
),
old AS (
    SELECT 'ENCOUNTER' AS TABLE_NAME, COUNT(*) AS PREVIOUS_RECORD, COUNT(DISTINCT PATID) AS PREVIOUS_PATIENTS
    FROM {{ last_schema }}.ENCOUNTER WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} UNION ALL
    SELECT 'DIAGNOSIS', COUNT(*), COUNT(DISTINCT PATID) FROM {{ last_schema }}.DIAGNOSIS WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} UNION ALL
    SELECT 'PROCEDURES', COUNT(*), COUNT(DISTINCT PATID) FROM {{ last_schema }}.PROCEDURES WHERE 1=1
 {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %} UNION ALL
    SELECT 'DEMOGRAPHIC', COUNT(*), COUNT(DISTINCT PATID) FROM {{ last_schema }}.DEMOGRAPHIC
),
exceptions AS (
    SELECT COUNT(*) AS EXCEPTION_COUNT
    FROM crt JOIN old ON crt.TABLE_NAME = old.TABLE_NAME
    WHERE (old.PREVIOUS_RECORD > 0 AND ((crt.CURRENT_RECORD - old.PREVIOUS_RECORD) / old.PREVIOUS_RECORD::FLOAT) * 100 < -5)
       OR (old.PREVIOUS_PATIENTS > 0 AND ((crt.CURRENT_PATIENTS - old.PREVIOUS_PATIENTS) / old.PREVIOUS_PATIENTS::FLOAT) * 100 < -5)
)
SELECT
    '4.01'                                                          AS CHECK_NUM,
    '> 5% decrease in patients or records per CDM table'            AS DESCRIPTION,
    CASE WHEN EXCEPTION_COUNT > 0 THEN 'Fail' ELSE 'Pass' END       AS STATUS
FROM exceptions
