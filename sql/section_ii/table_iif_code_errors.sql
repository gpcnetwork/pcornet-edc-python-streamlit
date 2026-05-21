-- Table IIF. Potential Code Errors
-- Exceptions to DC 1.13 (> 5% of ICD/CPT/LOINC/RXCUI/NDC codes do not conform to expected format)
-- and DC 1.16 (lab results or clinical observations recorded in wrong table).
-- DC 1.13 exceptions highlighted in red; DC 1.16 exceptions highlighted in blue.

SELECT TABLE_NAME, CODE_TYPE, DISTINCT_CODES, RECORDS, RECORDS_WITH_ERRORS, PCT_ERRORS,
       RECORDS_WRONG_TABLE, PCT_WRONG_TABLE, SOURCE_TABLE
FROM (
    -- DC 1.13: ICD-10-CM in DIAGNOSIS
    SELECT 'DIAGNOSIS' AS TABLE_NAME, 'ICD-10-CM' AS CODE_TYPE,
           TO_VARCHAR(COUNT(DISTINCT DX)) AS DISTINCT_CODES,
           TO_VARCHAR(COUNT(*)) AS RECORDS,
           TO_VARCHAR(SUM(CASE WHEN NOT REGEXP_LIKE(TRIM(DX), '^[A-Z][0-9A-Z]{2,6}$') THEN 1 ELSE 0 END)) AS RECORDS_WITH_ERRORS,
           TO_VARCHAR(ROUND(SUM(CASE WHEN NOT REGEXP_LIKE(TRIM(DX), '^[A-Z][0-9A-Z]{2,6}$') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1)) || '%' AS PCT_ERRORS,
           '' AS RECORDS_WRONG_TABLE,
           '' AS PCT_WRONG_TABLE,
           'DIA_L3_DXTYPE' AS SOURCE_TABLE,
           1 AS ROW_ORDER
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE DX_TYPE = '10' AND ADMIT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- DC 1.13: ICD-9-CM in DIAGNOSIS
    SELECT 'DIAGNOSIS', 'ICD-9-CM',
           TO_VARCHAR(COUNT(DISTINCT DX)),
           TO_VARCHAR(COUNT(*)),
           TO_VARCHAR(SUM(CASE WHEN NOT REGEXP_LIKE(TRIM(DX), '^[0-9VEve][0-9]{2,4}$') THEN 1 ELSE 0 END)),
           TO_VARCHAR(ROUND(SUM(CASE WHEN NOT REGEXP_LIKE(TRIM(DX), '^[0-9VEve][0-9]{2,4}$') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1)) || '%',
           '', '',
           'DIA_L3_DXTYPE',
           2
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE DX_TYPE = '09' AND ADMIT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- DC 1.13: CPT/HCPCS in PROCEDURES
    SELECT 'PROCEDURES', 'CPT/HCPCS',
           TO_VARCHAR(COUNT(DISTINCT PX)),
           TO_VARCHAR(COUNT(*)),
           TO_VARCHAR(SUM(CASE WHEN NOT REGEXP_LIKE(TRIM(PX), '^[0-9A-Za-z]{5}$') THEN 1 ELSE 0 END)),
           TO_VARCHAR(ROUND(SUM(CASE WHEN NOT REGEXP_LIKE(TRIM(PX), '^[0-9A-Za-z]{5}$') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1)) || '%',
           '', '',
           'PRO_L3_PXTYPE',
           3
    FROM {{ current_schema }}.PROCEDURES
    WHERE PX_TYPE IN ('C2','C3','C4','H3') AND PX_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- DC 1.13: ICD-10-PCS in PROCEDURES
    SELECT 'PROCEDURES', 'ICD-10-PCS',
           TO_VARCHAR(COUNT(DISTINCT PX)),
           TO_VARCHAR(COUNT(*)),
           TO_VARCHAR(SUM(CASE WHEN NOT REGEXP_LIKE(TRIM(PX), '^[0-9A-Za-z]{7}$') THEN 1 ELSE 0 END)),
           TO_VARCHAR(ROUND(SUM(CASE WHEN NOT REGEXP_LIKE(TRIM(PX), '^[0-9A-Za-z]{7}$') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1)) || '%',
           '', '',
           'PRO_L3_PXTYPE',
           4
    FROM {{ current_schema }}.PROCEDURES
    WHERE PX_TYPE = '10' AND PX_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- DC 1.13: LOINC in LAB_RESULT_CM
    SELECT 'LAB_RESULT_CM', 'LOINC',
           TO_VARCHAR(COUNT(DISTINCT LAB_LOINC)),
           TO_VARCHAR(COUNT(*)),
           TO_VARCHAR(SUM(CASE WHEN NOT REGEXP_LIKE(TRIM(LAB_LOINC), '^[0-9]{1,5}-[0-9]$') THEN 1 ELSE 0 END)),
           TO_VARCHAR(ROUND(SUM(CASE WHEN NOT REGEXP_LIKE(TRIM(LAB_LOINC), '^[0-9]{1,5}-[0-9]$') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1)) || '%',
           '', '',
           'LAB_L3_LOINC',
           5
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE LAB_LOINC IS NOT NULL AND RESULT_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- DC 1.13: LOINC in OBS_CLIN
    SELECT 'OBS_CLIN', 'LOINC',
           TO_VARCHAR(COUNT(DISTINCT OBSCLIN_CODE)),
           TO_VARCHAR(COUNT(*)),
           TO_VARCHAR(SUM(CASE WHEN NOT REGEXP_LIKE(TRIM(OBSCLIN_CODE), '^[0-9]{1,5}-[0-9]$') THEN 1 ELSE 0 END)),
           TO_VARCHAR(ROUND(SUM(CASE WHEN NOT REGEXP_LIKE(TRIM(OBSCLIN_CODE), '^[0-9]{1,5}-[0-9]$') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1)) || '%',
           '', '',
           'OBSCLIN_L3_CODE',
           6
    FROM {{ current_schema }}.OBS_CLIN
    WHERE OBSCLIN_TYPE = 'LC' AND OBSCLIN_CODE IS NOT NULL
      AND OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- DC 1.13: NDC in DISPENSING
    SELECT 'DISPENSING', 'NDC',
           TO_VARCHAR(COUNT(DISTINCT NDC)),
           TO_VARCHAR(COUNT(*)),
           TO_VARCHAR(SUM(CASE WHEN NOT REGEXP_LIKE(TRIM(NDC), '^[0-9]{11}$') THEN 1 ELSE 0 END)),
           TO_VARCHAR(ROUND(SUM(CASE WHEN NOT REGEXP_LIKE(TRIM(NDC), '^[0-9]{11}$') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1)) || '%',
           '', '',
           'DISP_L3_NDC',
           7
    FROM {{ current_schema }}.DISPENSING
    WHERE NDC IS NOT NULL AND DISPENSE_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- DC 1.13: RXNORM in PRESCRIBING
    SELECT 'PRESCRIBING', 'RXNORM',
           TO_VARCHAR(COUNT(DISTINCT RXNORM_CUI)),
           TO_VARCHAR(COUNT(*)),
           TO_VARCHAR(SUM(CASE WHEN NOT REGEXP_LIKE(TRIM(RXNORM_CUI::VARCHAR), '^[0-9]+$') THEN 1 ELSE 0 END)),
           TO_VARCHAR(ROUND(SUM(CASE WHEN NOT REGEXP_LIKE(TRIM(RXNORM_CUI::VARCHAR), '^[0-9]+$') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1)) || '%',
           '', '',
           'PRES_L3_RXCUI',
           8
    FROM {{ current_schema }}.PRESCRIBING
    WHERE RXNORM_CUI IS NOT NULL AND RX_ORDER_DATE >= TO_DATE('{{ start_date }}')

    UNION ALL
    -- DC 1.16: OBS_CLIN records with LOINC codes that also appear in LAB_RESULT_CM (wrong table)
    SELECT 'OBS_CLIN', 'LOINC (DC 1.16)',
           TO_VARCHAR(COUNT(DISTINCT OBSCLIN_CODE)),
           TO_VARCHAR(COUNT(*)),
           '', '',
           TO_VARCHAR(SUM(CASE WHEN EXISTS (
               SELECT 1 FROM {{ current_schema }}.LAB_RESULT_CM l
               WHERE l.LAB_LOINC = o.OBSCLIN_CODE
           ) THEN 1 ELSE 0 END)),
           TO_VARCHAR(ROUND(SUM(CASE WHEN EXISTS (
               SELECT 1 FROM {{ current_schema }}.LAB_RESULT_CM l
               WHERE l.LAB_LOINC = o.OBSCLIN_CODE
           ) THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1)) || '%',
           'OBSCLIN_L3_CODE; LAB_L3_LOINC',
           9
    FROM {{ current_schema }}.OBS_CLIN o
    WHERE o.OBSCLIN_TYPE = 'LC' AND o.OBSCLIN_CODE IS NOT NULL
      AND o.OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}')

) sub
ORDER BY ROW_ORDER
