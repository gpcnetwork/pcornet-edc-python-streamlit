-- DC 2.07: The average number of principal diagnoses per known DX_ORIGIN per
-- encounter is above threshold [2.0 for inpatient (IP) and ED to inpatient (EI)]
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH principal AS (
    SELECT ENCOUNTERID, COUNT(*) AS PDX_COUNT
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND ADMIT_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
      AND ENC_TYPE IN ('IP','EI')
      AND PDX = 'P'
      AND DX_ORIGIN NOT IN ('NI','UN','OT')
      AND DX_ORIGIN IS NOT NULL
    GROUP BY ENCOUNTERID
),
avg_pdx AS (SELECT ROUND(AVG(PDX_COUNT), 2) AS AVG_PDX FROM principal)
SELECT
    '2.07'                                              AS CHECK_NUM,
    'Average principal DX per encounter > 2.0 for IP/EI' AS DESCRIPTION,
    CASE WHEN AVG_PDX > 2.0 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM avg_pdx
