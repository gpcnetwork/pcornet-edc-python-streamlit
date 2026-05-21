-- DC 3.08: Less than 80% of prescribing orders are mapped to a RXCUI which fully
-- specifies the ingredient, strength and dose form (i.e. RXCUI codes that have
-- a Term Type of SCD, SBD,BPCK, or GPCK)
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH rx AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(RXNORM_CUI IS NOT NULL AND RXNORM_CUI NOT IN ('NI','UN','OT')) AS MAPPED
    FROM {{ current_schema }}.PRESCRIBING
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND RX_ORDER_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
)
SELECT
    '3.08'                                                              AS CHECK_NUM,
    '< 80% prescribing orders mapped to Tier 1 RXCUI'                  AS DESCRIPTION,
    CASE WHEN 100.0 * MAPPED / NULLIF(TOTAL, 0) < 80 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM rx
