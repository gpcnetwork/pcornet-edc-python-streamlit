-- Table IIE. Orphan Records, Replication Errors, Encounter Duplication and Hash Token Duplication
-- Exceptions to DC 1.08 (orphan PATIDs), 1.09 (orphan ENCOUNTERIDs > 5%), 1.10 (replication errors),
-- 1.11 (encounters assigned to > 1 patient > 5%), 1.12 (orphan PROVIDERIDs),
-- 1.14 (patients missing from HASH_TOKEN), 1.19 (hash tokens assigned to multiple patients > 10%).
-- DC 1.14 and 1.19 exceptions highlighted in blue; all others in red.

SELECT DATA_CHECK, DATA_CHECK_DESCRIPTION, EXCEPTION, TABLE_NAME, FIELD_NAME,
       TO_VARCHAR(COUNT_VAL) AS COUNT_VAL,
       TO_VARCHAR(ROUND(CASE WHEN DENOMINATOR > 0 THEN 100.0 * COUNT_VAL / DENOMINATOR ELSE 0 END, 2)) || '%' AS PCT,
       SOURCE_TABLES
FROM (
    -- DC 1.08: Orphan PATIDs in DIAGNOSIS
    SELECT '1.08' AS DATA_CHECK,
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS DATA_CHECK_DESCRIPTION,
           CASE WHEN COUNT(*) = 0 THEN 'None' ELSE 'Orphan PATIDs found' END AS EXCEPTION,
           'DIAGNOSIS' AS TABLE_NAME, 'PATID' AS FIELD_NAME,
           COUNT(*) AS COUNT_VAL,
           (SELECT COUNT(*) FROM {{ current_schema }}.DIAGNOSIS WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')) AS DENOMINATOR,
           'DIA_L3_N; DEM_L3_N' AS SOURCE_TABLES
    FROM {{ current_schema }}.DIAGNOSIS d
    WHERE d.ADMIT_DATE >= TO_DATE('{{ start_date }}')
      AND NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = d.PATID)

    UNION ALL
    SELECT '1.08',
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC',
           CASE WHEN COUNT(*) = 0 THEN 'None' ELSE 'Orphan PATIDs found' END,
           'ENCOUNTER', 'PATID',
           COUNT(*),
           (SELECT COUNT(*) FROM {{ current_schema }}.ENCOUNTER WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')),
           'ENC_L3_N; DEM_L3_N'
    FROM {{ current_schema }}.ENCOUNTER e
    WHERE e.ADMIT_DATE >= TO_DATE('{{ start_date }}')
      AND NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = e.PATID)

    UNION ALL
    SELECT '1.08',
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC',
           CASE WHEN COUNT(*) = 0 THEN 'None' ELSE 'Orphan PATIDs found' END,
           'VITAL', 'PATID',
           COUNT(*),
           (SELECT COUNT(*) FROM {{ current_schema }}.VITAL WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}')),
           'VIT_L3_N; DEM_L3_N'
    FROM {{ current_schema }}.VITAL v
    WHERE v.MEASURE_DATE >= TO_DATE('{{ start_date }}')
      AND NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = v.PATID)

    UNION ALL
    -- DC 1.09: Orphan ENCOUNTERIDs in DIAGNOSIS
    SELECT '1.09',
           'Tables contain orphan ENCOUNTERIDs not in ENCOUNTER (threshold: > 5%)',
           CASE WHEN COUNT(*) = 0 THEN 'None' ELSE 'Orphan ENCOUNTERIDs found' END,
           'DIAGNOSIS', 'ENCOUNTERID',
           COUNT(*),
           (SELECT COUNT(*) FROM {{ current_schema }}.DIAGNOSIS WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') AND ENCOUNTERID IS NOT NULL),
           'DIA_L3_N; ENC_L3_N'
    FROM {{ current_schema }}.DIAGNOSIS d
    WHERE d.ADMIT_DATE >= TO_DATE('{{ start_date }}')
      AND d.ENCOUNTERID IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM {{ current_schema }}.ENCOUNTER e WHERE e.ENCOUNTERID = d.ENCOUNTERID)

    UNION ALL
    SELECT '1.09',
           'Tables contain orphan ENCOUNTERIDs not in ENCOUNTER (threshold: > 5%)',
           CASE WHEN COUNT(*) = 0 THEN 'None' ELSE 'Orphan ENCOUNTERIDs found' END,
           'PROCEDURES', 'ENCOUNTERID',
           COUNT(*),
           (SELECT COUNT(*) FROM {{ current_schema }}.PROCEDURES WHERE PX_DATE >= TO_DATE('{{ start_date }}') AND ENCOUNTERID IS NOT NULL),
           'PRO_L3_N; ENC_L3_N'
    FROM {{ current_schema }}.PROCEDURES p
    WHERE p.PX_DATE >= TO_DATE('{{ start_date }}')
      AND p.ENCOUNTERID IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM {{ current_schema }}.ENCOUNTER e WHERE e.ENCOUNTERID = p.ENCOUNTERID)

    UNION ALL
    -- DC 1.10: Replication errors — ENC_TYPE or ADMIT_DATE mismatch
    SELECT '1.10',
           'ENCOUNTERIDs in DIAGNOSIS where ENC_TYPE or ADMIT_DATE does not match ENCOUNTER',
           CASE WHEN COUNT(*) = 0 THEN 'None' ELSE 'Replication errors found' END,
           'DIAGNOSIS', 'ENCOUNTERID, ENC_TYPE, ADMIT_DATE',
           COUNT(*),
           (SELECT COUNT(*) FROM {{ current_schema }}.DIAGNOSIS WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') AND ENCOUNTERID IS NOT NULL),
           'DIA_L3_N; ENC_L3_N'
    FROM {{ current_schema }}.DIAGNOSIS d
    JOIN {{ current_schema }}.ENCOUNTER e ON e.ENCOUNTERID = d.ENCOUNTERID
    WHERE d.ADMIT_DATE >= TO_DATE('{{ start_date }}')
      AND (d.ENC_TYPE != e.ENC_TYPE OR d.ADMIT_DATE::DATE != e.ADMIT_DATE::DATE)

    UNION ALL
    -- DC 1.11: ENCOUNTERIDs assigned to more than one PATID
    SELECT '1.11',
           'ENCOUNTERIDs assigned to more than one PATID (threshold: > 5%)',
           CASE WHEN COUNT(*) = 0 THEN 'None' ELSE 'Encounters assigned to multiple patients found' END,
           'ENCOUNTER', 'ENCOUNTERID',
           COUNT(*),
           (SELECT COUNT(DISTINCT ENCOUNTERID) FROM {{ current_schema }}.ENCOUNTER WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')),
           'ENC_L3_N'
    FROM (
        SELECT ENCOUNTERID FROM {{ current_schema }}.ENCOUNTER
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
        GROUP BY ENCOUNTERID HAVING COUNT(DISTINCT PATID) > 1
    ) multi

    UNION ALL
    -- DC 1.12: Orphan PROVIDERIDs in ENCOUNTER
    SELECT '1.12',
           'Tables contain orphan PROVIDERIDs not in PROVIDER',
           CASE WHEN COUNT(*) = 0 THEN 'None' ELSE 'Orphan PROVIDERIDs found' END,
           'ENCOUNTER', 'PROVIDERID',
           COUNT(*),
           (SELECT COUNT(*) FROM {{ current_schema }}.ENCOUNTER WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') AND PROVIDERID IS NOT NULL),
           'ENC_L3_N; PROV_L3_N'
    FROM {{ current_schema }}.ENCOUNTER e
    WHERE e.ADMIT_DATE >= TO_DATE('{{ start_date }}')
      AND e.PROVIDERID IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM {{ current_schema }}.PROVIDER p WHERE p.PROVIDERID = e.PROVIDERID)

    UNION ALL
    -- DC 1.14: Patients missing from HASH_TOKEN
    SELECT '1.14',
           'Patients in DEMOGRAPHIC are missing from HASH_TOKEN',
           CASE WHEN COUNT(*) = 0 THEN 'None' ELSE 'Patients missing from HASH_TOKEN (explain in ETL ADD)' END,
           'HASH_TOKEN', 'PATID',
           COUNT(*),
           (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.DEMOGRAPHIC),
           'DEM_L3_N; HTOK_L3_N'
    FROM {{ current_schema }}.DEMOGRAPHIC dem
    WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.HASH_TOKEN ht WHERE ht.PATID = dem.PATID)

    UNION ALL
    -- DC 1.19: Hash tokens assigned to multiple patients
    SELECT '1.19',
           'Hash tokens assigned to more than one PATID (threshold: > 10%)',
           CASE WHEN COUNT(*) = 0 THEN 'None' ELSE 'Hash tokens assigned to multiple patients (explain in ETL ADD)' END,
           'HASH_TOKEN', 'TOKEN_ENCRYPTION_KEY',
           COUNT(*),
           (SELECT COUNT(*) FROM {{ current_schema }}.HASH_TOKEN),
           'HTOK_L3_N'
    FROM (
        SELECT TOKEN_ENCRYPTION_KEY FROM {{ current_schema }}.HASH_TOKEN
        GROUP BY TOKEN_ENCRYPTION_KEY HAVING COUNT(DISTINCT PATID) > 1
    ) dup

) sub
ORDER BY DATA_CHECK, TABLE_NAME
