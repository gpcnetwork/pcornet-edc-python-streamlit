-- Table IVH. RXNORM Term Type Mapping
-- RXNORM mapping completeness for PRESCRIBING and MED_ADMIN. Supports DC 3.08, 3.15.
-- DC 3.08: < 80% of prescribing orders mapped to RXCUI. DC 3.15: < 80% for MED_ADMIN.
-- Exceptions highlighted in blue. Full tier analysis requires an external RXNORM reference table.

WITH pres_stats AS (
    SELECT
        COUNT(*) AS TOTAL_RECORDS,
        SUM(CASE WHEN RXNORM_CUI IS NOT NULL THEN 1 ELSE 0 END) AS MAPPED_RECORDS,
        SUM(CASE WHEN RXNORM_CUI IS NULL THEN 1 ELSE 0 END) AS UNMAPPED_RECORDS
    FROM {{ current_schema }}.PRESCRIBING
    WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
),
medadmin_stats AS (
    SELECT
        COUNT(*) AS TOTAL_RECORDS,
        SUM(CASE WHEN MEDADMIN_CODE IS NOT NULL AND MEDADMIN_TYPE = 'RX' THEN 1 ELSE 0 END) AS MAPPED_RECORDS,
        SUM(CASE WHEN (MEDADMIN_CODE IS NULL OR MEDADMIN_TYPE != 'RX') THEN 1 ELSE 0 END) AS UNMAPPED_RECORDS
    FROM {{ current_schema }}.MED_ADMIN
    WHERE MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}')
)
SELECT TABLE_NAME, TERM_TYPE,
       TO_VARCHAR(RECORDS) AS RECORDS,
       TO_VARCHAR(ROUND(RECORDS * 100.0 / NULLIF(TOTAL_RECORDS, 0), 1)) || '%' AS PCT_OF_TOTAL,
       SOURCE_TABLE
FROM (
    SELECT 'PRESCRIBING' AS TABLE_NAME, 'Total records' AS TERM_TYPE,
           TOTAL_RECORDS AS RECORDS, TOTAL_RECORDS,
           'PRES_L3_RXCUI' AS SOURCE_TABLE, 1 AS ROW_ORDER
    FROM pres_stats
    UNION ALL
    SELECT 'PRESCRIBING', 'Mapped to RXNORM (RXNORM_CUI not null)',
           MAPPED_RECORDS, TOTAL_RECORDS,
           'PRES_L3_RXCUI', 2
    FROM pres_stats
    UNION ALL
    SELECT 'PRESCRIBING', 'Not mapped (RXNORM_CUI null)',
           UNMAPPED_RECORDS, TOTAL_RECORDS,
           'PRES_L3_RXCUI', 3
    FROM pres_stats
    UNION ALL
    SELECT 'MED_ADMIN', 'Total records',
           TOTAL_RECORDS, TOTAL_RECORDS,
           'MEDA_L3_N', 4
    FROM medadmin_stats
    UNION ALL
    SELECT 'MED_ADMIN', 'Mapped to RXNORM (MEDADMIN_TYPE=RX, MEDADMIN_CODE not null)',
           MAPPED_RECORDS, TOTAL_RECORDS,
           'MEDA_L3_N', 5
    FROM medadmin_stats
    UNION ALL
    SELECT 'MED_ADMIN', 'Not mapped to RXNORM',
           UNMAPPED_RECORDS, TOTAL_RECORDS,
           'MEDA_L3_N', 6
    FROM medadmin_stats
) sub
ORDER BY ROW_ORDER
