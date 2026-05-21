-- Table IIIC. Illogical Dates
-- Patients with date relationships that are clinically implausible. Supports DC 2.03 (> 5%).
-- Exceptions highlighted in blue and should be investigated and explained in the ETL ADD.

WITH enc_patients AS (
    SELECT COUNT(DISTINCT PATID) AS TOTAL FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
birth_death AS (
    SELECT dem.PATID, dem.BIRTH_DATE, d.DEATH_DATE
    FROM {{ current_schema }}.DEMOGRAPHIC dem
    LEFT JOIN {{ current_schema }}.DEATH d ON d.PATID = dem.PATID
)
SELECT DATE_COMPARISON,
       TO_VARCHAR(PATIENTS) AS PATIENTS,
       TO_VARCHAR(ROUND(PATIENTS * 100.0 / NULLIF(ep.TOTAL, 0), 1)) || '%' AS PCT_OF_ENCOUNTER_PATIENTS,
       SOURCE_TABLES
FROM enc_patients ep,
(
    SELECT 'ENCOUNTER: Admit date before birth date' AS DATE_COMPARISON,
           COUNT(DISTINCT e.PATID) AS PATIENTS,
           'ENC_L3_N; DEM_L3_N' AS SOURCE_TABLES, 1 AS ROW_ORDER
    FROM {{ current_schema }}.ENCOUNTER e
    JOIN birth_death bd ON bd.PATID = e.PATID
    WHERE e.ADMIT_DATE >= TO_DATE('{{ start_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND e.ADMIT_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'ENCOUNTER: Admit date after death date',
           COUNT(DISTINCT e.PATID),
           'ENC_L3_N; DEATH_L3_N', 2
    FROM {{ current_schema }}.ENCOUNTER e
    JOIN birth_death bd ON bd.PATID = e.PATID
    WHERE e.ADMIT_DATE >= TO_DATE('{{ start_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND e.ADMIT_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'ENCOUNTER: Discharge date before admit date',
           COUNT(DISTINCT PATID),
           'ENC_L3_N', 3
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}') AND DISCHARGE_DATE IS NOT NULL
      AND DISCHARGE_DATE < ADMIT_DATE

    UNION ALL
    SELECT 'VITAL: Measure date before birth date',
           COUNT(DISTINCT v.PATID),
           'VIT_L3_N; DEM_L3_N', 4
    FROM {{ current_schema }}.VITAL v
    JOIN birth_death bd ON bd.PATID = v.PATID
    WHERE v.MEASURE_DATE >= TO_DATE('{{ start_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND v.MEASURE_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'VITAL: Measure date after death date',
           COUNT(DISTINCT v.PATID),
           'VIT_L3_N; DEATH_L3_N', 5
    FROM {{ current_schema }}.VITAL v
    JOIN birth_death bd ON bd.PATID = v.PATID
    WHERE v.MEASURE_DATE >= TO_DATE('{{ start_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND v.MEASURE_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'LAB_RESULT_CM: Result date before birth date',
           COUNT(DISTINCT l.PATID),
           'LAB_L3_N; DEM_L3_N', 6
    FROM {{ current_schema }}.LAB_RESULT_CM l
    JOIN birth_death bd ON bd.PATID = l.PATID
    WHERE l.RESULT_DATE >= TO_DATE('{{ start_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND l.RESULT_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'LAB_RESULT_CM: Result date after death date',
           COUNT(DISTINCT l.PATID),
           'LAB_L3_N; DEATH_L3_N', 7
    FROM {{ current_schema }}.LAB_RESULT_CM l
    JOIN birth_death bd ON bd.PATID = l.PATID
    WHERE l.RESULT_DATE >= TO_DATE('{{ start_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND l.RESULT_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'LAB_RESULT_CM: Result date before lab order date',
           COUNT(DISTINCT PATID),
           'LAB_L3_N', 8
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE >= TO_DATE('{{ start_date }}') AND LAB_ORDER_DATE IS NOT NULL
      AND RESULT_DATE < LAB_ORDER_DATE

    UNION ALL
    SELECT 'PRESCRIBING: Rx order date before birth date',
           COUNT(DISTINCT p.PATID),
           'PRES_L3_N; DEM_L3_N', 9
    FROM {{ current_schema }}.PRESCRIBING p
    JOIN birth_death bd ON bd.PATID = p.PATID
    WHERE p.RX_ORDER_DATE >= TO_DATE('{{ start_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND p.RX_ORDER_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'PRESCRIBING: Rx order date after death date',
           COUNT(DISTINCT p.PATID),
           'PRES_L3_N; DEATH_L3_N', 10
    FROM {{ current_schema }}.PRESCRIBING p
    JOIN birth_death bd ON bd.PATID = p.PATID
    WHERE p.RX_ORDER_DATE >= TO_DATE('{{ start_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND p.RX_ORDER_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'DISPENSING: Dispense date before birth date',
           COUNT(DISTINCT d.PATID),
           'DISP_L3_N; DEM_L3_N', 11
    FROM {{ current_schema }}.DISPENSING d
    JOIN birth_death bd ON bd.PATID = d.PATID
    WHERE d.DISPENSE_DATE >= TO_DATE('{{ start_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND d.DISPENSE_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'DISPENSING: Dispense date after death date',
           COUNT(DISTINCT d.PATID),
           'DISP_L3_N; DEATH_L3_N', 12
    FROM {{ current_schema }}.DISPENSING d
    JOIN birth_death bd ON bd.PATID = d.PATID
    WHERE d.DISPENSE_DATE >= TO_DATE('{{ start_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND d.DISPENSE_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'PROCEDURES: Px date before birth date',
           COUNT(DISTINCT p.PATID),
           'PRO_L3_N; DEM_L3_N', 13
    FROM {{ current_schema }}.PROCEDURES p
    JOIN birth_death bd ON bd.PATID = p.PATID
    WHERE p.PX_DATE >= TO_DATE('{{ start_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND p.PX_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'PROCEDURES: Px date after death date',
           COUNT(DISTINCT p.PATID),
           'PRO_L3_N; DEATH_L3_N', 14
    FROM {{ current_schema }}.PROCEDURES p
    JOIN birth_death bd ON bd.PATID = p.PATID
    WHERE p.PX_DATE >= TO_DATE('{{ start_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND p.PX_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'PROCEDURES: Px date > 5 days before encounter admit or after discharge',
           COUNT(DISTINCT p.PATID),
           'PRO_L3_N; ENC_L3_N', 15
    FROM {{ current_schema }}.PROCEDURES p
    JOIN {{ current_schema }}.ENCOUNTER e ON e.ENCOUNTERID = p.ENCOUNTERID
    WHERE p.PX_DATE >= TO_DATE('{{ start_date }}') AND e.ADMIT_DATE IS NOT NULL
      AND e.DISCHARGE_DATE IS NOT NULL
      AND (p.PX_DATE < DATEADD(day, -5, e.ADMIT_DATE)
           OR p.PX_DATE > DATEADD(day, 5, e.DISCHARGE_DATE))

    UNION ALL
    SELECT 'MED_ADMIN: Start date before birth date',
           COUNT(DISTINCT m.PATID),
           'MEDA_L3_N; DEM_L3_N', 16
    FROM {{ current_schema }}.MED_ADMIN m
    JOIN birth_death bd ON bd.PATID = m.PATID
    WHERE m.MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND m.MEDADMIN_START_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'MED_ADMIN: Start date after death date',
           COUNT(DISTINCT m.PATID),
           'MEDA_L3_N; DEATH_L3_N', 17
    FROM {{ current_schema }}.MED_ADMIN m
    JOIN birth_death bd ON bd.PATID = m.PATID
    WHERE m.MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND m.MEDADMIN_START_DATE > bd.DEATH_DATE

    UNION ALL
    SELECT 'OBS_CLIN: Start date before birth date',
           COUNT(DISTINCT o.PATID),
           'OBSCLIN_L3_N; DEM_L3_N', 18
    FROM {{ current_schema }}.OBS_CLIN o
    JOIN birth_death bd ON bd.PATID = o.PATID
    WHERE o.OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND o.OBSCLIN_START_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'OBS_CLIN: Stop date before start date',
           COUNT(DISTINCT PATID),
           'OBSCLIN_L3_N', 19
    FROM {{ current_schema }}.OBS_CLIN
    WHERE OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}') AND OBSCLIN_STOP_DATE IS NOT NULL
      AND OBSCLIN_STOP_DATE < OBSCLIN_START_DATE

    UNION ALL
    SELECT 'OBS_GEN: Start date before birth date',
           COUNT(DISTINCT o.PATID),
           'OBSGEN_L3_N; DEM_L3_N', 20
    FROM {{ current_schema }}.OBS_GEN o
    JOIN birth_death bd ON bd.PATID = o.PATID
    WHERE o.OBSGEN_START_DATE >= TO_DATE('{{ start_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND o.OBSGEN_START_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'OBS_GEN: Stop date before start date',
           COUNT(DISTINCT PATID),
           'OBSGEN_L3_N', 21
    FROM {{ current_schema }}.OBS_GEN
    WHERE OBSGEN_START_DATE >= TO_DATE('{{ start_date }}') AND OBSGEN_STOP_DATE IS NOT NULL
      AND OBSGEN_STOP_DATE < OBSGEN_START_DATE

    UNION ALL
    SELECT 'CONDITION: Report date before birth date',
           COUNT(DISTINCT c.PATID),
           'COND_L3_N; DEM_L3_N', 22
    FROM {{ current_schema }}.CONDITION c
    JOIN birth_death bd ON bd.PATID = c.PATID
    WHERE c.REPORT_DATE >= TO_DATE('{{ start_date }}') AND bd.BIRTH_DATE IS NOT NULL
      AND c.REPORT_DATE < bd.BIRTH_DATE

    UNION ALL
    SELECT 'CONDITION: Report date after death date',
           COUNT(DISTINCT c.PATID),
           'COND_L3_N; DEATH_L3_N', 23
    FROM {{ current_schema }}.CONDITION c
    JOIN birth_death bd ON bd.PATID = c.PATID
    WHERE c.REPORT_DATE >= TO_DATE('{{ start_date }}') AND bd.DEATH_DATE IS NOT NULL
      AND c.REPORT_DATE > bd.DEATH_DATE

) checks
ORDER BY ROW_ORDER
