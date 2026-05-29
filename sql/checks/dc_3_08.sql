-- DC 3.08 | Table IVH | Data Completeness | Investigative
-- Less than 80% of prescribing orders are mapped to a RXCUI which fully specifies the ingredient,
-- strength and dose form (i.e. RXCUI codes that have a Term Type of SCD, SBD, BPCK, or GPCK)
-- Parameters: {{ current_schema }}, {{ rxnorm_ref_fqn }}, {{ start_date }}
WITH rxnorm_tier1 AS (
    SELECT TRIM(RXNORM_CUI)::VARCHAR AS rxcui_str
    FROM {{ rxnorm_ref_fqn }}
    WHERE RXNORM_CUI IS NOT NULL
      AND UPPER(TRIM(RXNORM_CUI_TIER)) = 'TIER 1'
),
rx AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(EXISTS (
               SELECT 1 FROM rxnorm_tier1 r WHERE r.rxcui_str = TRIM(p.RXNORM_CUI)::VARCHAR
           )) AS TIER1_MAPPED
    FROM {{ current_schema }}.PRESCRIBING p
    WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
)
SELECT
    '3.08'                                                                                              AS CHECK_NUM,
    'Less than 80% of prescribing orders mapped to a Tier 1 RXCUI (SCD, SBD, BPCK, or GPCK)'         AS DESCRIPTION,
    CASE WHEN 100.0 * TIER1_MAPPED / NULLIF(TOTAL, 0) < 80 THEN 'Fail' ELSE 'Pass' END                AS STATUS
FROM rx
