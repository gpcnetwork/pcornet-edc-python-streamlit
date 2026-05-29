-- Table IVH. RXNORM Term Type Mapping
-- This table shows the number of records in the PRESCRIBING and MED_ADMIN tables by RXNORM Term Type tier. Guidance on mapping prescribing orders to
-- RXNORM is provided in the CDM specifications. These data support Data Check 3.08 (less than 80% of prescribing orders are mapped to a RXCUI which fully
-- specifies the ingredient, strength and dose form) and Data Check 3.15 (less than 80% of medication administrations mapped to RXNORM are mapped to a RXCUI
-- that fully specifies the ingredient, strength and dose form). Exceptions highlighted in blue.
--
WITH rxnorm_ref AS (
    SELECT TRIM(r.RXNORM_CUI)::VARCHAR            AS rxcui_str,
           TRY_TO_NUMBER(TRIM(r.RXNORM_CUI))      AS rxcui_num,
           UPPER(TRIM(r.RXNORM_CUI_TTY))::VARCHAR AS tty_norm,
           UPPER(TRIM(r.RXNORM_CUI_TIER))::VARCHAR AS tier_norm
    FROM {{ rxnorm_ref_fqn }} r
    WHERE r.RXNORM_CUI IS NOT NULL
),
tty_by_tier AS (
    SELECT tier_norm,
           LISTAGG(tty_norm, ', ') WITHIN GROUP (ORDER BY tty_norm) AS term_types
    FROM (
        SELECT DISTINCT tier_norm, tty_norm
        FROM rxnorm_ref
        WHERE UPPER(tier_norm) IN ('TIER 1', 'TIER 2', 'TIER 3', 'TIER 4')
    )
    GROUP BY tier_norm
),
-- PRESCRIBING ----------------------------------------------------------------
pres_raw AS (
    SELECT TRIM(p.RXNORM_CUI)::VARCHAR AS rxcui_str
    FROM {{ current_schema }}.PRESCRIBING p
    WHERE p.RX_ORDER_DATE BETWEEN TO_DATE('{{ start_date }}') AND TO_DATE('{{ end_date }}')
),
pres_tag AS (
    SELECT
        IFF(UPPER(r.tier_norm) = 'TIER 1', 1, 0)                                          AS is_t1,
        IFF(UPPER(r.tier_norm) = 'TIER 1' AND r.tty_norm IN ('SBD', 'BPCK'), 1, 0)       AS is_t1_brand,
        IFF(UPPER(r.tier_norm) = 'TIER 2', 1, 0)                                          AS is_t2,
        IFF(UPPER(r.tier_norm) = 'TIER 3', 1, 0)                                          AS is_t3,
        IFF(UPPER(r.tier_norm) = 'TIER 4', 1, 0)                                          AS is_t4,
        IFF(p.rxcui_str IS NULL OR r.rxcui_str IS NULL, 1, 0)                             AS is_unknown
    FROM pres_raw p
    LEFT JOIN rxnorm_ref r ON r.rxcui_str = p.rxcui_str
),
pres_agg AS (
    SELECT
        COUNT(*)                                                                           AS denom,
        SUM(is_t1)                                                                         AS n_t1,
        SUM(is_t1_brand)                                                                   AS n_t1_brand,
        SUM(is_t2)                                                                         AS n_t2,
        SUM(is_t3)                                                                         AS n_t3,
        SUM(is_t4)                                                                         AS n_t4,
        SUM(is_unknown)                                                                    AS n_unknown,
        IFF(COUNT(*) > 0, ROUND(100.0 * SUM(is_t1)       / COUNT(*), 2), NULL)            AS pct_t1,
        IFF(COUNT(*) > 0, ROUND(100.0 * SUM(is_t2)       / COUNT(*), 2), NULL)            AS pct_t2,
        IFF(COUNT(*) > 0, ROUND(100.0 * SUM(is_t3)       / COUNT(*), 2), NULL)            AS pct_t3,
        IFF(COUNT(*) > 0, ROUND(100.0 * SUM(is_t4)       / COUNT(*), 2), NULL)            AS pct_t4,
        IFF(COUNT(*) > 0, ROUND(100.0 * SUM(is_unknown)  / COUNT(*), 2), NULL)            AS pct_unknown,
        IFF(SUM(is_t1)  > 0, ROUND(100.0 * SUM(is_t1_brand) / SUM(is_t1), 2), NULL)      AS pct_t1_brand
    FROM pres_tag
),
-- MED_ADMIN ------------------------------------------------------------------
med_raw AS (
    SELECT
        TRIM(m.MEDADMIN_CODE)::VARCHAR           AS rxcui_str,
        TRY_TO_NUMBER(TRIM(m.MEDADMIN_CODE))     AS rxcui_num
    FROM {{ current_schema }}.MED_ADMIN m
    WHERE m.MEDADMIN_START_DATE IS NOT NULL
      AND m.MEDADMIN_START_DATE BETWEEN TO_DATE('{{ start_date }}') AND TO_DATE('{{ end_date }}')
),
-- Restrict denominator to RxNorm-like codes (numeric, not NI/UN/OT/blank)
med AS (
    SELECT rxcui_str, rxcui_num
    FROM med_raw
    WHERE rxcui_num IS NOT NULL
      AND UPPER(rxcui_str) NOT IN ('NI', 'UN', 'OT', '')
),
med_tag AS (
    SELECT
        IFF(UPPER(r.tier_norm) = 'TIER 1', 1, 0)   AS is_t1,
        IFF(UPPER(r.tier_norm) = 'TIER 2', 1, 0)   AS is_t2,
        IFF(UPPER(r.tier_norm) = 'TIER 3', 1, 0)   AS is_t3,
        IFF(UPPER(r.tier_norm) = 'TIER 4', 1, 0)   AS is_t4,
        IFF(r.rxcui_num IS NULL, 1, 0)              AS is_unknown
    FROM med m
    LEFT JOIN rxnorm_ref r ON r.rxcui_num = m.rxcui_num
),
med_agg AS (
    SELECT
        COUNT(*)                                                                        AS denom,
        SUM(is_t1)                                                                      AS n_t1,
        SUM(is_t2)                                                                      AS n_t2,
        SUM(is_t3)                                                                      AS n_t3,
        SUM(is_t4)                                                                      AS n_t4,
        SUM(is_unknown)                                                                 AS n_unknown,
        IFF(COUNT(*) > 0, ROUND(100.0 * SUM(is_t1)      / COUNT(*), 2), NULL)          AS pct_t1,
        IFF(COUNT(*) > 0, ROUND(100.0 * SUM(is_t2)      / COUNT(*), 2), NULL)          AS pct_t2,
        IFF(COUNT(*) > 0, ROUND(100.0 * SUM(is_t3)      / COUNT(*), 2), NULL)          AS pct_t3,
        IFF(COUNT(*) > 0, ROUND(100.0 * SUM(is_t4)      / COUNT(*), 2), NULL)          AS pct_t4,
        IFF(COUNT(*) > 0, ROUND(100.0 * SUM(is_unknown) / COUNT(*), 2), NULL)          AS pct_unknown
    FROM med_tag
),
-- Final rows -----------------------------------------------------------------
all_rows AS (
    -- PRESCRIBING: Tier 1 (includes brand + non-brand)
    SELECT 'PRESCRIBING' AS TABLE_NAME,
           'Tier 1'      AS TIER,
           'RXNORM_CUI encodes ingredient(s), strength and dose form' AS TIER_DESCRIPTION,
           COALESCE((SELECT term_types FROM tty_by_tier WHERE UPPER(tier_norm) = 'TIER 1'),
                    'SCD, SBD, BPCK, GPCK') AS TERM_TYPES,
           n_t1 AS NUMERATOR, pct_t1 AS PCT, 1 AS SORT_ORDER
    FROM pres_agg
    UNION ALL
    -- PRESCRIBING: Tier 1 brand subset (denominator = Tier 1 count)
    SELECT 'PRESCRIBING', 'Tier 1 brand (subset)',
           'RXNORM_CUI encodes ingredient(s), strength, dose form and brand',
           'SBD, BPCK',
           n_t1_brand, pct_t1_brand, 2
    FROM pres_agg
    UNION ALL
    -- PRESCRIBING: Tier 2
    SELECT 'PRESCRIBING', 'Tier 2',
           'RXNORM_CUI encodes ingredient(s) and potentially strength or dose form. Can still represent medications with multiple ingredients with a single RXCUI.',
           COALESCE((SELECT term_types FROM tty_by_tier WHERE UPPER(tier_norm) = 'TIER 2'),
                    'SBDF, SBDFP, SCDF, SCDFP, SBDG, SCDG, SCDGP, BN, MIN'),
           n_t2, pct_t2, 3
    FROM pres_agg
    UNION ALL
    -- PRESCRIBING: Tier 3
    SELECT 'PRESCRIBING', 'Tier 3',
           'Requires more than one RXNORM_CUI to represent medications with multiple ingredients.',
           COALESCE((SELECT term_types FROM tty_by_tier WHERE UPPER(tier_norm) = 'TIER 3'),
                    'IN, PIN, SCDC'),
           n_t3, pct_t3, 4
    FROM pres_agg
    UNION ALL
    -- PRESCRIBING: Tier 4
    SELECT 'PRESCRIBING', 'Tier 4',
           'RXNORM_CUI does not encode any ingredient information.',
           COALESCE((SELECT term_types FROM tty_by_tier WHERE UPPER(tier_norm) = 'TIER 4'),
                    'DF, DFG'),
           n_t4, pct_t4, 5
    FROM pres_agg
    UNION ALL
    -- PRESCRIBING: Unknown
    SELECT 'PRESCRIBING', 'Unknown',
           'RXNORM_CUI was not populated or could not be matched to the reference table',
           'n/a',
           n_unknown, pct_unknown, 6
    FROM pres_agg
    UNION ALL
    -- MED_ADMIN: Tier 1
    SELECT 'MED_ADMIN', 'Tier 1',
           'MEDADMIN_CODE encodes ingredient(s), strength and dose form',
           COALESCE((SELECT term_types FROM tty_by_tier WHERE UPPER(tier_norm) = 'TIER 1'),
                    'SCD, SBD, BPCK, GPCK'),
           n_t1, pct_t1, 7
    FROM med_agg
    UNION ALL
    -- MED_ADMIN: Tier 2
    SELECT 'MED_ADMIN', 'Tier 2',
           'MEDADMIN_CODE encodes ingredient(s) and potentially strength or dose form. Can still represent medications with multiple ingredients with a single RXCUI.',
           COALESCE((SELECT term_types FROM tty_by_tier WHERE UPPER(tier_norm) = 'TIER 2'),
                    'SBDF, SBDFP, SCDF, SCDFP, SBDG, SCDG, SCDGP, BN, MIN'),
           n_t2, pct_t2, 8
    FROM med_agg
    UNION ALL
    -- MED_ADMIN: Tier 3
    SELECT 'MED_ADMIN', 'Tier 3',
           'Requires more than one MEDADMIN_CODE to represent medications with multiple ingredients.',
           COALESCE((SELECT term_types FROM tty_by_tier WHERE UPPER(tier_norm) = 'TIER 3'),
                    'IN, PIN, SCDC'),
           n_t3, pct_t3, 9
    FROM med_agg
    UNION ALL
    -- MED_ADMIN: Tier 4
    SELECT 'MED_ADMIN', 'Tier 4',
           'MEDADMIN_CODE does not encode any ingredient information.',
           COALESCE((SELECT term_types FROM tty_by_tier WHERE UPPER(tier_norm) = 'TIER 4'),
                    'DF, DFG'),
           n_t4, pct_t4, 10
    FROM med_agg
    UNION ALL
    -- MED_ADMIN: Unknown
    SELECT 'MED_ADMIN', 'Unknown',
           'MEDADMIN_CODE was not populated or could not be matched to the reference table',
           'n/a',
           n_unknown, pct_unknown, 11
    FROM med_agg
)
SELECT TABLE_NAME, TIER, TIER_DESCRIPTION, TERM_TYPES, NUMERATOR, PCT
FROM all_rows
ORDER BY SORT_ORDER
