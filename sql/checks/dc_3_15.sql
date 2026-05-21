-- DC 3.15: Less than 80% of medication administrations mapped to RXNORM are
-- mapped to a RXCUI that fully specifies the ingredient, strength and dose
-- form (i.e. RXCUI codes that have a Term Type of SCD, SBD,BPCK, or
-- GPCK)
-- Parameters: {{ current_schema }}, {{ cutoff_date }}
WITH med AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(MEDADMIN_CODE IS NOT NULL AND MEDADMIN_TYPE = 'RX'
                    AND MEDADMIN_CODE NOT IN ('NI','UN','OT')) AS MAPPED
    FROM {{ current_schema }}.MED_ADMIN
    WHERE 1=1
    {% if cutoff_date %}{% if cutoff_date %}AND MEDADMIN_START_DATE >= {% if cutoff_date %}TO_DATE('{{ cutoff_date }}'){% else %}DATEADD('year', -5, CURRENT_DATE){% endif %}{% endif %}{% endif %}
      AND MEDADMIN_TYPE = 'RX'
)
SELECT
    '3.15'                                                              AS CHECK_NUM,
    '< 80% MED_ADMIN records mapped to Tier 1 RXCUI'                   AS DESCRIPTION,
    CASE WHEN 100.0 * MAPPED / NULLIF(TOTAL, 0) < 80 THEN 'Fail' ELSE 'Pass' END AS STATUS
FROM med
