-- DC 3.08 | Table IVH | Data Completeness | Investigative
-- Less than 80% of prescribing orders are mapped to a RXCUI which fully specifies the ingredient,
-- strength and dose form (i.e. RXCUI codes that have a Term Type of SCD, SBD, BPCK, or GPCK).
--
-- SAS source of truth (v7.03):
--   data_curation_tables.sas 1531-1532: PRESCRIBING restricted at load to
--     rx_order_date >= &lookback_dt OR missing(rx_order_date) -- NO upper bound.
--   data_curation_tables.sas 6549-6626 (PRES_L3_RXCUI_TIER):
--     - numerator  = records whose RXNORM_CUI matches rxnorm_cui_ref with rxnorm_cui_tier='Tier 1'
--       (Tier 1 = term types SCD, SBD, BPCK, GPCK per edc_report.sas 9582-9590)
--     - denominator = ALL PRESCRIBING records (grand total across tiers incl. NULL/unmatched CUIs)
--     - record_pct = put(num/denom*100, 6.2) -- rounded to 2 decimals
--   edc_report.sas 9666-9676: exception when ord=1 and (col5p < 80.0 OR numerator = 0).
--   edc_report.sas 11330-11334: status is n/a when there is no PRESCRIBING data at all.
-- Parameters: {{ current_schema }}, {{ rxnorm_ref_fqn }}, {{ start_date }}
WITH rxnorm_tier1 AS (
    SELECT TRIM(RXNORM_CUI)::VARCHAR AS rxcui_str
    FROM {{ rxnorm_ref_fqn }}
    WHERE RXNORM_CUI IS NOT NULL
      AND UPPER(TRIM(RXNORM_CUI_TIER)) = 'TIER 1'
),
tbl AS (
    SELECT COUNT(*) AS TABLE_ROWS FROM {{ current_schema }}.PRESCRIBING
),
rx AS (
    SELECT COUNT(*) AS TOTAL,
           COUNT_IF(EXISTS (
               SELECT 1 FROM rxnorm_tier1 r WHERE r.rxcui_str = TRIM(p.RXNORM_CUI)::VARCHAR
           )) AS TIER1_MAPPED
    FROM {{ current_schema }}.PRESCRIBING p
    WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}') OR RX_ORDER_DATE IS NULL
),
evaluated AS (
    SELECT t.TABLE_ROWS, rx.TOTAL, rx.TIER1_MAPPED,
           -- SAS record_pct = put(num/denom*100, 6.2); 0.00 when denom=0
           IFF(rx.TOTAL > 0, ROUND(100.0 * rx.TIER1_MAPPED / rx.TOTAL, 2), 0) AS PCT,
           CASE
               WHEN t.TABLE_ROWS = 0 THEN 'n/a'
               WHEN IFF(rx.TOTAL > 0, ROUND(100.0 * rx.TIER1_MAPPED / rx.TOTAL, 2), 0) < 80.0
                    OR rx.TIER1_MAPPED = 0 THEN 'Fail'
               ELSE 'Pass'
           END AS STATUS
    FROM rx CROSS JOIN tbl t
)
SELECT
    '3.08'                                                                                      AS CHECK_NUM,
    'Less than 80% of prescribing orders mapped to a Tier 1 RXCUI (SCD, SBD, BPCK, or GPCK)'    AS DESCRIPTION,
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
    '3.08',
    'Less than 80% of prescribing orders mapped to a Tier 1 RXCUI (SCD, SBD, BPCK, or GPCK)',
    STATUS,
    'DETAIL',
    'PRESCRIBING',
    'RXNORM_CUI',
    'Tier 1 mapped ' || TO_VARCHAR(PCT::DECIMAL(10,2)) || '% ('
        || TO_VARCHAR(TIER1_MAPPED) || '/' || TO_VARCHAR(TOTAL) || ')',
    TIER1_MAPPED,
    1
FROM evaluated
ORDER BY ROW_ORDER
