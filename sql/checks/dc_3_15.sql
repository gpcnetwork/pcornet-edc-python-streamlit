-- DC 3.15 | Table IVH | Data Completeness | Investigative
-- Less than 80% of medication administrations mapped to RXNORM are mapped to a RXCUI that fully
-- specifies the ingredient, strength and dose form (i.e. RXCUI codes that have a Term Type of
-- SCD, SBD, BPCK, or GPCK)
-- Parameters: {{ current_schema }}, {{ rxnorm_ref_fqn }}, {{ start_date }}
WITH rxnorm_tier1 AS (
    SELECT TRIM(RXNORM_CUI)::VARCHAR AS rxcui_str
    FROM {{ rxnorm_ref_fqn }}
    WHERE RXNORM_CUI IS NOT NULL
      AND UPPER(TRIM(RXNORM_CUI_TIER)) = 'TIER 1'
),
med AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(EXISTS (
               SELECT 1 FROM rxnorm_tier1 r WHERE r.rxcui_str = TRIM(m.MEDADMIN_CODE)::VARCHAR
           )) AS TIER1_MAPPED
    FROM {{ current_schema }}.MED_ADMIN m
    WHERE MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}')
      AND MEDADMIN_TYPE = 'RX'
)
SELECT
    '3.15'                                                                                                      AS CHECK_NUM,
    'Less than 80% of MED_ADMIN records mapped to a Tier 1 RXCUI (SCD, SBD, BPCK, or GPCK)'                   AS DESCRIPTION,
    CASE WHEN 100.0 * TIER1_MAPPED / NULLIF(TOTAL, 0) < 80 THEN 'Fail' ELSE 'Pass' END                        AS STATUS
FROM med
