-- DC 3.15 | Table IVH | Data Completeness | Investigative
-- Less than 80% of medication administrations mapped to RXNORM are mapped to a RXCUI that fully
-- specifies the ingredient, strength and dose form (i.e. RXCUI codes that have a Term Type of
-- SCD, SBD, BPCK, or GPCK).
--
-- SAS source of truth (v7.03):
--   data_curation_tables.sas 1734-1735: MED_ADMIN restricted at load to
--     medadmin_start_date >= &lookback_dt OR missing(medadmin_start_date) -- NO upper bound.
--   data_curation_tables.sas 6052-6138 (MEDADM_L3_RXCUI_TIER):
--     - 6076: input rows limited to medadmin_type='RX' AND medadmin_code not NULL/missing
--       (the 'NULL or missing' code category is excluded BEFORE the tier merge, so blank codes
--       are excluded from the denominator -- unlike 3.08)
--     - numerator  = records whose MEDADMIN_CODE matches rxnorm_cui_ref with
--       rxnorm_cui_tier='Tier 1' (SCD, SBD, BPCK, GPCK per edc_report.sas 9582-9590)
--     - denominator = all RX-typed records with a populated MEDADMIN_CODE (incl. unmatched codes)
--     - record_pct = put(num/denom*100, 6.1) -- rounded to 1 decimal
--   edc_report.sas 9666-9676: exception when ord=7 and (col5p < 80.0 OR numerator = 0).
--   edc_report.sas 11345-11349: status is n/a when there is no MED_ADMIN data at all.
-- Parameters: {{ current_schema }}, {{ rxnorm_ref_fqn }}, {{ start_date }}
WITH rxnorm_tier1 AS (
    SELECT TRIM(RXNORM_CUI)::VARCHAR AS rxcui_str
    FROM {{ rxnorm_ref_fqn }}
    WHERE RXNORM_CUI IS NOT NULL
      AND UPPER(TRIM(RXNORM_CUI_TIER)) = 'TIER 1'
),
tbl AS (
    SELECT COUNT(*) AS TABLE_ROWS FROM {{ current_schema }}.MED_ADMIN
),
med AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(EXISTS (
               SELECT 1 FROM rxnorm_tier1 r WHERE r.rxcui_str = TRIM(m.MEDADMIN_CODE)::VARCHAR
           )) AS TIER1_MAPPED
    FROM {{ current_schema }}.MED_ADMIN m
    WHERE (MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}') OR MEDADMIN_START_DATE IS NULL)
      AND MEDADMIN_TYPE = 'RX'
      AND MEDADMIN_CODE IS NOT NULL AND TRIM(MEDADMIN_CODE) <> ''
),
evaluated AS (
    SELECT t.TABLE_ROWS, med.TOTAL, med.TIER1_MAPPED,
           -- SAS record_pct = put(num/denom*100, 6.1); 0.0 when denom=0
           IFF(med.TOTAL > 0, ROUND(100.0 * med.TIER1_MAPPED / med.TOTAL, 1), 0) AS PCT,
           CASE
               WHEN t.TABLE_ROWS = 0 THEN 'n/a'
               WHEN IFF(med.TOTAL > 0, ROUND(100.0 * med.TIER1_MAPPED / med.TOTAL, 1), 0) < 80.0
                    OR med.TIER1_MAPPED = 0 THEN 'Fail'
               ELSE 'Pass'
           END AS STATUS
    FROM med CROSS JOIN tbl t
)
SELECT
    '3.15'                                                                                      AS CHECK_NUM,
    'Less than 80% of MED_ADMIN RXNORM records mapped to a Tier 1 RXCUI (SCD, SBD, BPCK, or GPCK)' AS DESCRIPTION,
    STATUS,
    'SUMMARY'                                                                                   AS ROW_TYPE,
    CAST(NULL AS VARCHAR)                                                                       AS EXC_TABLE,
    CAST(NULL AS VARCHAR)                                                                       AS EXC_FIELD,
    CAST(NULL AS VARCHAR)                                                                       AS EXC_DETAIL,
    CAST(NULL AS NUMBER)                                                                        AS EXC_COUNT,
    0                                                                                           AS ROW_ORDER
FROM evaluated
UNION ALL
SELECT
    '3.15',
    'Less than 80% of MED_ADMIN RXNORM records mapped to a Tier 1 RXCUI (SCD, SBD, BPCK, or GPCK)',
    STATUS,
    'DETAIL',
    'MED_ADMIN',
    'MEDADMIN_CODE',
    'Tier 1 mapped ' || TO_VARCHAR(PCT::DECIMAL(10,1)) || '% ('
        || TO_VARCHAR(TIER1_MAPPED) || '/' || TO_VARCHAR(TOTAL) || ')',
    TIER1_MAPPED,
    1
FROM evaluated
ORDER BY ROW_ORDER
