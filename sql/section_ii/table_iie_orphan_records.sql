-- Table IIE. Orphan Records, Replication Errors, Encounter Duplication and Hash Token Duplication
-- Exceptions to DC 1.08 (orphan PATIDs), 1.09 (orphan ENCOUNTERIDs > 5%), 1.10 (replication errors),
-- 1.11 (encounters assigned to more than one PATID > 5%), 1.12 (orphan PROVIDERIDs),
-- 1.14 (patients missing from HASH_TOKEN), 1.19 (hash tokens assigned to multiple PATIDs > 10%).
-- DC 1.14 and 1.19 exceptions highlighted in blue; all others in red.
-- Parameters: {{ current_schema }}, {{ cutoff_date }} (falls back to {{ end_date }})

WITH
-- ============ DC 1.08: Orphan PATIDs (19 tables) ============
orphan_patid_condition AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'CONDITION' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.CONDITION t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.CONDITION) AS denom
    )
),
orphan_patid_diagnosis AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'DIAGNOSIS' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.DIAGNOSIS t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.DIAGNOSIS) AS denom
    )
),
orphan_patid_death AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'DEATH' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.DEATH t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.DEATH) AS denom
    )
),
orphan_patid_death_cause AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'DEATH_CAUSE' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.DEATH_CAUSE t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.DEATH_CAUSE) AS denom
    )
),
orphan_patid_dispensing AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'DISPENSING' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.DISPENSING t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.DISPENSING) AS denom
    )
),
orphan_patid_encounter AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'ENCOUNTER' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.ENCOUNTER t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.ENCOUNTER) AS denom
    )
),
orphan_patid_enrollment AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'ENROLLMENT' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.ENROLLMENT t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.ENROLLMENT) AS denom
    )
),
orphan_patid_hash_token AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'HASH_TOKEN' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.HASH_TOKEN t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.HASH_TOKEN) AS denom
    )
),
orphan_patid_immunization AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'IMMUNIZATION' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.IMMUNIZATION t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.IMMUNIZATION) AS denom
    )
),
orphan_patid_lab_result_cm AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'LAB_RESULT_CM' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.LAB_RESULT_CM t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.LAB_RESULT_CM) AS denom
    )
),
orphan_patid_lds_address_history AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'LDS_ADDRESS_HISTORY' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.LDS_ADDRESS_HISTORY t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.LDS_ADDRESS_HISTORY) AS denom
    )
),
orphan_patid_med_admin AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'MED_ADMIN' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.MED_ADMIN t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.MED_ADMIN) AS denom
    )
),
orphan_patid_obs_clin AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'OBS_CLIN' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.OBS_CLIN t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.OBS_CLIN) AS denom
    )
),
orphan_patid_obs_gen AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'OBS_GEN' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.OBS_GEN t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.OBS_GEN) AS denom
    )
),
orphan_patid_pcornet_trial AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'PCORNET_TRIAL' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.PCORNET_TRIAL t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.PCORNET_TRIAL) AS denom
    )
),
orphan_patid_prescribing AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'PRESCRIBING' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.PRESCRIBING t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.PRESCRIBING) AS denom
    )
),
orphan_patid_procedures AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'PROCEDURES' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.PROCEDURES t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.PROCEDURES) AS denom
    )
),
orphan_patid_pro_cm AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'PRO_CM' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.PRO_CM t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.PRO_CM) AS denom
    )
),
orphan_patid_vital AS (
    SELECT '1.08' AS "Data Check",
           'Tables contain orphan PATIDs not present in DEMOGRAPHIC' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PATID(s) found' END AS "Exception",
           'VITAL' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.PATID) FROM {{ current_schema }}.VITAL t
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.DEMOGRAPHIC dem WHERE dem.PATID = t.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.VITAL) AS denom
    )
),

-- ============ DC 1.09: Orphan ENCOUNTERIDs (11 tables, threshold > 5%) ============
{% set dc109_tables = ['CONDITION','DIAGNOSIS','IMMUNIZATION','LAB_RESULT_CM','MED_ADMIN','OBS_CLIN','OBS_GEN','PRESCRIBING','PROCEDURES','PRO_CM','VITAL'] %}
{% for t in dc109_tables %}
orphan_enc_{{ t.lower() }} AS (
    SELECT '1.09' AS "Data Check",
           'Tables contain orphan ENCOUNTERIDs not in ENCOUNTER (threshold > 5%)' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan ENCOUNTERID(s) found' END AS "Exception",
           '{{ t }}' AS "Table(s)", 'ENCOUNTERID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.ENCOUNTERID) FROM {{ current_schema }}.{{ t }} t
                WHERE t.ENCOUNTERID IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM {{ current_schema }}.ENCOUNTER e WHERE e.ENCOUNTERID = t.ENCOUNTERID)) AS cnt,
               (SELECT COUNT(DISTINCT ENCOUNTERID) FROM {{ current_schema }}.{{ t }} WHERE ENCOUNTERID IS NOT NULL) AS denom
    )
),
{% endfor %}

-- ============ DC 1.10: Replication errors (DIAGNOSIS, PROCEDURES) ============
{% for t in ['DIAGNOSIS','PROCEDURES'] %}
replication_{{ t.lower() }} AS (
    SELECT '1.10' AS "Data Check",
           'Replication errors in DIAGNOSIS or PROCEDURES vs ENCOUNTER (ENC_TYPE/ADMIT_DATE mismatch)' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Replication error(s) found' END AS "Exception",
           '{{ t }}' AS "Table(s)", 'ENCOUNTERID, ENC_TYPE, ADMIT_DATE' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(*) FROM {{ current_schema }}.{{ t }} d
                JOIN {{ current_schema }}.ENCOUNTER e ON d.ENCOUNTERID = e.ENCOUNTERID
                WHERE d.ENC_TYPE != e.ENC_TYPE OR d.ADMIT_DATE != e.ADMIT_DATE) AS cnt,
               (SELECT COUNT(*) FROM {{ current_schema }}.{{ t }}) AS denom
    )
),
{% endfor %}

-- ============ DC 1.11: Multi-PATID ENCOUNTERIDs (12 tables, threshold > 5%, cutoff_date filter) ============
{% set dc111_tables = [
    ('CONDITION', 'REPORT_DATE'),
    ('DIAGNOSIS', 'ADMIT_DATE'),
    ('ENCOUNTER', 'ADMIT_DATE'),
    ('IMMUNIZATION', 'VX_ADMIN_DATE'),
    ('LAB_RESULT_CM', 'RESULT_DATE'),
    ('MED_ADMIN', 'MEDADMIN_START_DATE'),
    ('OBS_CLIN', 'OBSCLIN_START_DATE'),
    ('OBS_GEN', 'OBSGEN_START_DATE'),
    ('PRESCRIBING', 'RX_ORDER_DATE'),
    ('PROCEDURES', 'PX_DATE'),
    ('PRO_CM', 'PRO_DATE'),
    ('VITAL', 'MEASURE_DATE'),
] %}
{% for tbl, dcol in dc111_tables %}
multi_pat_enc_{{ tbl.lower() }} AS (
    SELECT '1.11' AS "Data Check",
           'ENCOUNTERIDs assigned to more than one PATID (threshold > 5%)' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Encounters with multiple PATIDs found' END AS "Exception",
           '{{ tbl }}' AS "Table(s)", 'ENCOUNTERID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(*) FROM (
                    SELECT ENCOUNTERID FROM {{ current_schema }}.{{ tbl }}
                    WHERE ENCOUNTERID IS NOT NULL AND {{ dcol }} >= TO_DATE('{{ cutoff_date or end_date }}')
                    GROUP BY ENCOUNTERID HAVING COUNT(DISTINCT PATID) > 1
                )) AS cnt,
               (SELECT COUNT(DISTINCT ENCOUNTERID) FROM {{ current_schema }}.{{ tbl }}
                WHERE ENCOUNTERID IS NOT NULL AND {{ dcol }} >= TO_DATE('{{ cutoff_date or end_date }}')) AS denom
    )
),
{% endfor %}

-- ============ DC 1.12: Orphan PROVIDERIDs (8 tables, table-specific provider column) ============
{% set dc112_tables = [
    ('DIAGNOSIS', 'PROVIDERID'),
    ('ENCOUNTER', 'PROVIDERID'),
    ('IMMUNIZATION', 'VX_PROVIDERID'),
    ('MED_ADMIN', 'MEDADMIN_PROVIDERID'),
    ('OBS_CLIN', 'OBSCLIN_PROVIDERID'),
    ('OBS_GEN', 'OBSGEN_PROVIDERID'),
    ('PRESCRIBING', 'RX_PROVIDERID'),
    ('PROCEDURES', 'PROVIDERID'),
] %}
{% for tbl, pcol in dc112_tables %}
orphan_provider_{{ tbl.lower() }} AS (
    SELECT '1.12' AS "Data Check",
           'Tables contain orphan PROVIDERIDs not in PROVIDER' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Orphan PROVIDERID(s) found' END AS "Exception",
           '{{ tbl }}' AS "Table(s)", '{{ pcol }}' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT t.{{ pcol }}) FROM {{ current_schema }}.{{ tbl }} t
                WHERE t.{{ pcol }} IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM {{ current_schema }}.PROVIDER p WHERE p.PROVIDERID = t.{{ pcol }})) AS cnt,
               (SELECT COUNT(DISTINCT {{ pcol }}) FROM {{ current_schema }}.{{ tbl }} WHERE {{ pcol }} IS NOT NULL) AS denom
    )
),
{% endfor %}

-- ============ DC 1.14: Patients missing from HASH_TOKEN ============
missing_hash_token AS (
    SELECT '1.14' AS "Data Check",
           'Patients in DEMOGRAPHIC are missing from HASH_TOKEN' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Missing PATID(s) (explain in ETL ADD)' END AS "Exception",
           'HASH_TOKEN' AS "Table(s)", 'PATID' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(DISTINCT dem.PATID) FROM {{ current_schema }}.DEMOGRAPHIC dem
                WHERE NOT EXISTS (SELECT 1 FROM {{ current_schema }}.HASH_TOKEN ht WHERE ht.PATID = dem.PATID)) AS cnt,
               (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.DEMOGRAPHIC) AS denom
    )
),

-- ============ DC 1.19: Hash tokens assigned to multiple PATIDs (threshold > 10%) ============
multi_pat_hash_token AS (
    SELECT '1.19' AS "Data Check",
           'Hash tokens assigned to more than one PATID (threshold > 10%)' AS "Data Check Description",
           CASE WHEN cnt = 0 THEN 'None' ELSE 'Hash tokens with multiple PATIDs (explain in ETL ADD)' END AS "Exception",
           'HASH_TOKEN' AS "Table(s)", 'TOKEN_ENCRYPTION_KEY' AS "Field(s)",
           TO_VARCHAR(cnt) AS "Count",
           TO_VARCHAR(ROUND(100.0 * cnt / NULLIF(denom, 0), 2)) || '%' AS "%"
    FROM (
        SELECT (SELECT COUNT(*) FROM (
                    SELECT TOKEN_ENCRYPTION_KEY FROM {{ current_schema }}.HASH_TOKEN
                    WHERE TOKEN_ENCRYPTION_KEY IS NOT NULL
                    GROUP BY TOKEN_ENCRYPTION_KEY HAVING COUNT(DISTINCT PATID) > 1
                )) AS cnt,
               (SELECT COUNT(DISTINCT TOKEN_ENCRYPTION_KEY) FROM {{ current_schema }}.HASH_TOKEN
                WHERE TOKEN_ENCRYPTION_KEY IS NOT NULL) AS denom
    )
)
SELECT "Data Check", "Data Check Description", "Exception", "Table(s)", "Field(s)", "Count", "%"
FROM (
    SELECT * FROM orphan_patid_condition
    UNION ALL SELECT * FROM orphan_patid_diagnosis
    UNION ALL SELECT * FROM orphan_patid_death
    UNION ALL SELECT * FROM orphan_patid_death_cause
    UNION ALL SELECT * FROM orphan_patid_dispensing
    UNION ALL SELECT * FROM orphan_patid_encounter
    UNION ALL SELECT * FROM orphan_patid_enrollment
    UNION ALL SELECT * FROM orphan_patid_hash_token
    UNION ALL SELECT * FROM orphan_patid_immunization
    UNION ALL SELECT * FROM orphan_patid_lab_result_cm
    UNION ALL SELECT * FROM orphan_patid_lds_address_history
    UNION ALL SELECT * FROM orphan_patid_med_admin
    UNION ALL SELECT * FROM orphan_patid_obs_clin
    UNION ALL SELECT * FROM orphan_patid_obs_gen
    UNION ALL SELECT * FROM orphan_patid_pcornet_trial
    UNION ALL SELECT * FROM orphan_patid_prescribing
    UNION ALL SELECT * FROM orphan_patid_procedures
    UNION ALL SELECT * FROM orphan_patid_pro_cm
    UNION ALL SELECT * FROM orphan_patid_vital
{% for t in dc109_tables %}
    UNION ALL SELECT * FROM orphan_enc_{{ t.lower() }}
{% endfor %}
{% for t in ['DIAGNOSIS','PROCEDURES'] %}
    UNION ALL SELECT * FROM replication_{{ t.lower() }}
{% endfor %}
{% for tbl, _ in dc111_tables %}
    UNION ALL SELECT * FROM multi_pat_enc_{{ tbl.lower() }}
{% endfor %}
{% for tbl, _ in dc112_tables %}
    UNION ALL SELECT * FROM orphan_provider_{{ tbl.lower() }}
{% endfor %}
    UNION ALL SELECT * FROM missing_hash_token
    UNION ALL SELECT * FROM multi_pat_hash_token
)
ORDER BY "Data Check", "Table(s)"
