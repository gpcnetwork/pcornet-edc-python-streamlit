-- Table IIIC. Illogical Dates
-- Patients with date relationships that are clinically implausible. Supports DC 2.03 (> 5%).
-- Exceptions highlighted in blue and should be investigated and explained in the ETL ADD.
-- Parameters: {{ current_schema }}, {{ start_date }}

WITH enc_patients AS (
    SELECT COUNT(DISTINCT PATID) AS TOTAL FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') AND ADMIT_DATE <= TO_DATE('{{ end_date }}')
),
birth_death AS (
    SELECT dem.PATID, dem.BIRTH_DATE, d.DEATH_DATE
    FROM {{ current_schema }}.DEMOGRAPHIC dem
    LEFT JOIN {{ current_schema }}.DEATH d ON d.PATID = dem.PATID
)
SELECT DATE_COMPARISON,
       TO_VARCHAR(PATIENTS) AS "Patients",
       TO_VARCHAR(ROUND(PATIENTS * 100.0 / NULLIF(ep.TOTAL, 0), 1)) || '%'
           AS "Percentage of total patients in the ENCOUNTER table"
FROM enc_patients ep,
(
    -- ── Group 1: < BIRTH_DATE ──────────────────────────────────────────────────
    SELECT 'ADMIT_DATE < BIRTH_DATE' AS DATE_COMPARISON,
           COUNT(DISTINCT e.PATID) AS PATIENTS,
           1 AS ROW_ORDER
    FROM {{ current_schema }}.ENCOUNTER e
    JOIN birth_death bd ON bd.PATID = e.PATID
    WHERE e.ADMIT_DATE >= TO_DATE('{{ start_date }}') AND e.ADMIT_DATE <= TO_DATE('{{ end_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND e.ADMIT_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'DISCHARGE_DATE < BIRTH_DATE',
           COUNT(DISTINCT e.PATID),
           2
    FROM {{ current_schema }}.ENCOUNTER e
    JOIN birth_death bd ON bd.PATID = e.PATID
    WHERE e.ADMIT_DATE >= TO_DATE('{{ start_date }}') AND e.ADMIT_DATE <= TO_DATE('{{ end_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND e.DISCHARGE_DATE IS NOT NULL AND e.DISCHARGE_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'PX_DATE < BIRTH_DATE',
           COUNT(DISTINCT p.PATID),
           3
    FROM {{ current_schema }}.PROCEDURES p
    JOIN birth_death bd ON bd.PATID = p.PATID
    WHERE p.PX_DATE >= TO_DATE('{{ start_date }}') AND p.PX_DATE <= TO_DATE('{{ end_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND p.PX_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'DX_DATE < BIRTH_DATE',
           COUNT(DISTINCT d.PATID),
           4
    FROM {{ current_schema }}.DIAGNOSIS d
    JOIN birth_death bd ON bd.PATID = d.PATID
    WHERE d.ADMIT_DATE >= TO_DATE('{{ start_date }}') AND d.ADMIT_DATE <= TO_DATE('{{ end_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND d.ADMIT_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'MEASURE_DATE < BIRTH_DATE',
           COUNT(DISTINCT v.PATID),
           5
    FROM {{ current_schema }}.VITAL v
    JOIN birth_death bd ON bd.PATID = v.PATID
    WHERE v.MEASURE_DATE >= TO_DATE('{{ start_date }}') AND v.MEASURE_DATE <= TO_DATE('{{ end_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND v.MEASURE_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'DISPENSE_DATE < BIRTH_DATE',
           COUNT(DISTINCT d.PATID),
           6
    FROM {{ current_schema }}.DISPENSING d
    JOIN birth_death bd ON bd.PATID = d.PATID
    WHERE d.DISPENSE_DATE >= TO_DATE('{{ start_date }}') AND d.DISPENSE_DATE <= TO_DATE('{{ end_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND d.DISPENSE_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'RX_START_DATE < BIRTH_DATE',
           COUNT(DISTINCT p.PATID),
           7
    FROM {{ current_schema }}.PRESCRIBING p
    JOIN birth_death bd ON bd.PATID = p.PATID
    WHERE p.RX_START_DATE >= TO_DATE('{{ start_date }}') AND p.RX_START_DATE <= TO_DATE('{{ end_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND p.RX_START_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'RESULT_DATE < BIRTH_DATE',
           COUNT(DISTINCT l.PATID),
           8
    FROM {{ current_schema }}.LAB_RESULT_CM l
    JOIN birth_death bd ON bd.PATID = l.PATID
    WHERE l.RESULT_DATE >= TO_DATE('{{ start_date }}') AND l.RESULT_DATE <= TO_DATE('{{ end_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND l.RESULT_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'DEATH_DATE < BIRTH_DATE',
           COUNT(DISTINCT PATID),
           9
    FROM birth_death
    WHERE DEATH_DATE IS NOT NULL AND BIRTH_DATE IS NOT NULL
      AND DEATH_DATE < BIRTH_DATE

    UNION ALL
    SELECT 'MEDADMIN_START_DATE < BIRTH_DATE',
           COUNT(DISTINCT m.PATID),
           10
    FROM {{ current_schema }}.MED_ADMIN m
    JOIN birth_death bd ON bd.PATID = m.PATID
    WHERE m.MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}') AND m.MEDADMIN_START_DATE <= TO_DATE('{{ end_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND m.MEDADMIN_START_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'OBSCLIN_START_DATE < BIRTH_DATE',
           COUNT(DISTINCT o.PATID),
           11
    FROM {{ current_schema }}.OBS_CLIN o
    JOIN birth_death bd ON bd.PATID = o.PATID
    WHERE o.OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}') AND o.OBSCLIN_START_DATE <= TO_DATE('{{ end_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND o.OBSCLIN_START_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'OBSGEN_START_DATE < BIRTH_DATE',
           COUNT(DISTINCT o.PATID),
           12
    FROM {{ current_schema }}.OBS_GEN o
    JOIN birth_death bd ON bd.PATID = o.PATID
    WHERE o.OBSGEN_START_DATE >= TO_DATE('{{ start_date }}') AND o.OBSGEN_START_DATE <= TO_DATE('{{ end_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND o.OBSGEN_START_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'VX_RECORD_DATE < BIRTH_DATE',
           COUNT(DISTINCT i.PATID),
           13
    FROM {{ current_schema }}.IMMUNIZATION i
    JOIN birth_death bd ON bd.PATID = i.PATID
    WHERE bd.BIRTH_DATE IS NOT NULL
      AND i.VX_RECORD_DATE < bd.BIRTH_DATE

    -- ── Group 2: > DEATH_DATE ─────────────────────────────────────────────────
    UNION ALL
    SELECT 'ADMIT_DATE > DEATH_DATE',
           COUNT(DISTINCT e.PATID),
           14
    FROM {{ current_schema }}.ENCOUNTER e
    JOIN birth_death bd ON bd.PATID = e.PATID
    WHERE e.ADMIT_DATE >= TO_DATE('{{ start_date }}') AND e.ADMIT_DATE <= TO_DATE('{{ end_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND e.ADMIT_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'DISCHARGE_DATE > DEATH_DATE',
           COUNT(DISTINCT e.PATID),
           15
    FROM {{ current_schema }}.ENCOUNTER e
    JOIN birth_death bd ON bd.PATID = e.PATID
    WHERE e.ADMIT_DATE >= TO_DATE('{{ start_date }}') AND e.ADMIT_DATE <= TO_DATE('{{ end_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND e.DISCHARGE_DATE IS NOT NULL AND e.DISCHARGE_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'PX_DATE > DEATH_DATE',
           COUNT(DISTINCT p.PATID),
           16
    FROM {{ current_schema }}.PROCEDURES p
    JOIN birth_death bd ON bd.PATID = p.PATID
    WHERE p.PX_DATE >= TO_DATE('{{ start_date }}') AND p.PX_DATE <= TO_DATE('{{ end_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND p.PX_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'DX_DATE > DEATH_DATE',
           COUNT(DISTINCT d.PATID),
           17
    FROM {{ current_schema }}.DIAGNOSIS d
    JOIN birth_death bd ON bd.PATID = d.PATID
    WHERE d.ADMIT_DATE >= TO_DATE('{{ start_date }}') AND d.ADMIT_DATE <= TO_DATE('{{ end_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND d.ADMIT_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'MEASURE_DATE > DEATH_DATE',
           COUNT(DISTINCT v.PATID),
           18
    FROM {{ current_schema }}.VITAL v
    JOIN birth_death bd ON bd.PATID = v.PATID
    WHERE v.MEASURE_DATE >= TO_DATE('{{ start_date }}') AND v.MEASURE_DATE <= TO_DATE('{{ end_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND v.MEASURE_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'DISPENSE_DATE > DEATH_DATE',
           COUNT(DISTINCT d.PATID),
           19
    FROM {{ current_schema }}.DISPENSING d
    JOIN birth_death bd ON bd.PATID = d.PATID
    WHERE d.DISPENSE_DATE >= TO_DATE('{{ start_date }}') AND d.DISPENSE_DATE <= TO_DATE('{{ end_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND d.DISPENSE_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'RX_START_DATE > DEATH_DATE',
           COUNT(DISTINCT p.PATID),
           20
    FROM {{ current_schema }}.PRESCRIBING p
    JOIN birth_death bd ON bd.PATID = p.PATID
    WHERE p.RX_START_DATE >= TO_DATE('{{ start_date }}') AND p.RX_START_DATE <= TO_DATE('{{ end_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND p.RX_START_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'RESULT_DATE > DEATH_DATE',
           COUNT(DISTINCT l.PATID),
           21
    FROM {{ current_schema }}.LAB_RESULT_CM l
    JOIN birth_death bd ON bd.PATID = l.PATID
    WHERE l.RESULT_DATE >= TO_DATE('{{ start_date }}') AND l.RESULT_DATE <= TO_DATE('{{ end_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND l.RESULT_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'MEDADMIN_START_DATE > DEATH_DATE',
           COUNT(DISTINCT m.PATID),
           22
    FROM {{ current_schema }}.MED_ADMIN m
    JOIN birth_death bd ON bd.PATID = m.PATID
    WHERE m.MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}') AND m.MEDADMIN_START_DATE <= TO_DATE('{{ end_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND m.MEDADMIN_START_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'OBSCLIN_START_DATE > DEATH_DATE',
           COUNT(DISTINCT o.PATID),
           23
    FROM {{ current_schema }}.OBS_CLIN o
    JOIN birth_death bd ON bd.PATID = o.PATID
    WHERE o.OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}') AND o.OBSCLIN_START_DATE <= TO_DATE('{{ end_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND o.OBSCLIN_START_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'OBSGEN_START_DATE > DEATH_DATE',
           COUNT(DISTINCT o.PATID),
           24
    FROM {{ current_schema }}.OBS_GEN o
    JOIN birth_death bd ON bd.PATID = o.PATID
    WHERE o.OBSGEN_START_DATE >= TO_DATE('{{ start_date }}') AND o.OBSGEN_START_DATE <= TO_DATE('{{ end_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND o.OBSGEN_START_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'VX_RECORD_DATE > DEATH_DATE',
           COUNT(DISTINCT i.PATID),
           25
    FROM {{ current_schema }}.IMMUNIZATION i
    JOIN birth_death bd ON bd.PATID = i.PATID
    WHERE bd.DEATH_DATE IS NOT NULL
      AND i.VX_RECORD_DATE > bd.DEATH_DATE

    -- ── Group 3: Logical date comparisons ────────────────────────────────────
    UNION ALL
    SELECT 'ADMIT_DATE > DISCHARGE_DATE',
           COUNT(DISTINCT PATID),
           26
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') AND ADMIT_DATE <= TO_DATE('{{ end_date }}') AND DISCHARGE_DATE IS NOT NULL
      AND DISCHARGE_DATE < ADMIT_DATE

    UNION ALL
    SELECT 'PX_DATE is More Than 5 Days Prior To The ADMIT_DATE',
           COUNT(DISTINCT p.PATID),
           27
    FROM {{ current_schema }}.PROCEDURES p
    JOIN {{ current_schema }}.ENCOUNTER e ON e.ENCOUNTERID = p.ENCOUNTERID
    WHERE p.PX_DATE >= TO_DATE('{{ start_date }}') AND p.PX_DATE <= TO_DATE('{{ end_date }}') AND e.ADMIT_DATE IS NOT NULL
      AND p.PX_DATE < DATEADD(day, -5, e.ADMIT_DATE)

    UNION ALL
    SELECT 'PX_DATE is More Than 5 Days After The DISCHARGE_DATE',
           COUNT(DISTINCT p.PATID),
           28
    FROM {{ current_schema }}.PROCEDURES p
    JOIN {{ current_schema }}.ENCOUNTER e ON e.ENCOUNTERID = p.ENCOUNTERID
    WHERE p.PX_DATE >= TO_DATE('{{ start_date }}') AND p.PX_DATE <= TO_DATE('{{ end_date }}') AND e.DISCHARGE_DATE IS NOT NULL
      AND p.PX_DATE > DATEADD(day, 5, e.DISCHARGE_DATE)

    UNION ALL
    SELECT 'DX_DATE is More Than 5 Days Prior To The ADMIT_DATE',
           COUNT(DISTINCT d.PATID),
           29
    FROM {{ current_schema }}.DIAGNOSIS d
    JOIN {{ current_schema }}.ENCOUNTER e ON e.ENCOUNTERID = d.ENCOUNTERID
    WHERE d.ADMIT_DATE >= TO_DATE('{{ start_date }}') AND d.ADMIT_DATE <= TO_DATE('{{ end_date }}') AND e.ADMIT_DATE IS NOT NULL
      AND d.ADMIT_DATE < DATEADD(day, -5, e.ADMIT_DATE)

    UNION ALL
    SELECT 'DX_DATE is More Than 5 Days After The DISCHARGE_DATE',
           COUNT(DISTINCT d.PATID),
           30
    FROM {{ current_schema }}.DIAGNOSIS d
    JOIN {{ current_schema }}.ENCOUNTER e ON e.ENCOUNTERID = d.ENCOUNTERID
    WHERE d.ADMIT_DATE >= TO_DATE('{{ start_date }}') AND d.ADMIT_DATE <= TO_DATE('{{ end_date }}') AND e.DISCHARGE_DATE IS NOT NULL
      AND d.ADMIT_DATE > DATEADD(day, 5, e.DISCHARGE_DATE)

    UNION ALL
    SELECT 'OBSCLIN_START_DATE > OBSCLIN_STOP_DATE',
           COUNT(DISTINCT PATID),
           31
    FROM {{ current_schema }}.OBS_CLIN
    WHERE OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}') AND OBSCLIN_START_DATE <= TO_DATE('{{ end_date }}') AND OBSCLIN_STOP_DATE IS NOT NULL
      AND OBSCLIN_START_DATE > OBSCLIN_STOP_DATE

    UNION ALL
    SELECT 'OBSGEN_START_DATE > OBSGEN_STOP_DATE',
           COUNT(DISTINCT PATID),
           32
    FROM {{ current_schema }}.OBS_GEN
    WHERE OBSGEN_START_DATE >= TO_DATE('{{ start_date }}') AND OBSGEN_START_DATE <= TO_DATE('{{ end_date }}') AND OBSGEN_STOP_DATE IS NOT NULL
      AND OBSGEN_START_DATE > OBSGEN_STOP_DATE

    UNION ALL
    SELECT 'RELATIONSHIP_START > RELATIONSHIP_END',
           COUNT(DISTINCT PATID_1),
           33
    FROM {{ current_schema }}.PAT_RELATIONSHIP
    WHERE RELATIONSHIP_START IS NOT NULL AND RELATIONSHIP_END IS NOT NULL
      AND RELATIONSHIP_START > RELATIONSHIP_END

    UNION ALL
    SELECT 'EXT_PAT_START_DATE > EXT_PAT_END_DATE',
           COUNT(DISTINCT PATID),
           34
    FROM {{ current_schema }}.EXTERNAL_MEDS
    WHERE EXT_PAT_START_DATE IS NOT NULL AND EXT_PAT_END_DATE IS NOT NULL
      AND EXT_PAT_START_DATE > EXT_PAT_END_DATE

    UNION ALL
    SELECT 'MEDADMIN_START_DATE > MEDADMIN_STOP_DATE',
           COUNT(DISTINCT PATID),
           35
    FROM {{ current_schema }}.MED_ADMIN
    WHERE MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}') AND MEDADMIN_START_DATE <= TO_DATE('{{ end_date }}') AND MEDADMIN_STOP_DATE IS NOT NULL
      AND MEDADMIN_START_DATE > MEDADMIN_STOP_DATE

) checks
ORDER BY ROW_ORDER
