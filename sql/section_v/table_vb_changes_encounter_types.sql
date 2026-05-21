-- Table VB. Changes in Selected Encounter Types and Domains
-- This table shows changes in key DataMart attributes between the most recent approved DataMart refresh and the current DataMart refresh and supports Data Check
-- 4.02 [more than a 5% decrease in the number of patients or records for diagnosis, procedures, labs or prescriptions during an ambulatory (AV), telehealth (TH), other
-- ambulatory (OA), emergency department (ED), or inpatient (IP) encounter]. Data check exceptions are highlighted in blue and should be investigated and explained in
-- the ETL ADD.

WITH crt_diag AS (
    SELECT enc.ENC_TYPE, 'DIAGNOSIS' AS DOMAIN, COUNT(*) AS CURRENT_RECORD, COUNT(DISTINCT d.PATID) AS CURRENT_PATIENTS
    FROM {{ current_schema }}.DIAGNOSIS d
    JOIN {{ current_schema }}.ENCOUNTER enc ON d.ENCOUNTERID = enc.ENCOUNTERID
    {% if cutoff_date %}WHERE d.ADMIT_DATE >= '{{ cutoff_date }}'{% endif %}
    GROUP BY enc.ENC_TYPE
),
crt_proc AS (
    SELECT enc.ENC_TYPE, 'PROCEDURES' AS DOMAIN, COUNT(*) AS CURRENT_RECORD, COUNT(DISTINCT p.PATID) AS CURRENT_PATIENTS
    FROM {{ current_schema }}.PROCEDURES p
    JOIN {{ current_schema }}.ENCOUNTER enc ON p.ENCOUNTERID = enc.ENCOUNTERID
    {% if cutoff_date %}WHERE p.ADMIT_DATE >= '{{ cutoff_date }}'{% endif %}
    GROUP BY enc.ENC_TYPE
),
crt_lab AS (
    SELECT enc.ENC_TYPE, 'LAB_RESULT_CM' AS DOMAIN, COUNT(*) AS CURRENT_RECORD, COUNT(DISTINCT l.PATID) AS CURRENT_PATIENTS
    FROM {{ current_schema }}.LAB_RESULT_CM l
    JOIN {{ current_schema }}.ENCOUNTER enc ON l.ENCOUNTERID = enc.ENCOUNTERID
    {% if cutoff_date %}WHERE l.RESULT_DATE >= '{{ cutoff_date }}'{% endif %}
    GROUP BY enc.ENC_TYPE
),
crt_rx AS (
    SELECT enc.ENC_TYPE, 'PRESCRIBING' AS DOMAIN, COUNT(*) AS CURRENT_RECORD, COUNT(DISTINCT rx.PATID) AS CURRENT_PATIENTS
    FROM {{ current_schema }}.PRESCRIBING rx
    JOIN {{ current_schema }}.ENCOUNTER enc ON rx.ENCOUNTERID = enc.ENCOUNTERID
    {% if cutoff_date %}WHERE rx.RX_ORDER_DATE >= '{{ cutoff_date }}'{% endif %}
    GROUP BY enc.ENC_TYPE
),
old_diag AS (
    SELECT enc.ENC_TYPE, 'DIAGNOSIS' AS DOMAIN, COUNT(*) AS PREVIOUS_RECORD, COUNT(DISTINCT d.PATID) AS PREVIOUS_PATIENTS
    FROM {{ last_schema }}.DIAGNOSIS d
    JOIN {{ last_schema }}.ENCOUNTER enc ON d.ENCOUNTERID = enc.ENCOUNTERID
    {% if cutoff_date %}WHERE d.ADMIT_DATE >= '{{ cutoff_date }}'{% endif %}
    GROUP BY enc.ENC_TYPE
),
old_proc AS (
    SELECT enc.ENC_TYPE, 'PROCEDURES' AS DOMAIN, COUNT(*), COUNT(DISTINCT p.PATID)
    FROM {{ last_schema }}.PROCEDURES p
    JOIN {{ last_schema }}.ENCOUNTER enc ON p.ENCOUNTERID = enc.ENCOUNTERID
    {% if cutoff_date %}WHERE p.ADMIT_DATE >= '{{ cutoff_date }}'{% endif %}
    GROUP BY enc.ENC_TYPE
),
old_lab AS (
    SELECT enc.ENC_TYPE, 'LAB_RESULT_CM' AS DOMAIN, COUNT(*), COUNT(DISTINCT l.PATID)
    FROM {{ last_schema }}.LAB_RESULT_CM l
    JOIN {{ last_schema }}.ENCOUNTER enc ON l.ENCOUNTERID = enc.ENCOUNTERID
    {% if cutoff_date %}WHERE l.RESULT_DATE >= '{{ cutoff_date }}'{% endif %}
    GROUP BY enc.ENC_TYPE
),
old_rx AS (
    SELECT enc.ENC_TYPE, 'PRESCRIBING' AS DOMAIN, COUNT(*), COUNT(DISTINCT rx.PATID)
    FROM {{ last_schema }}.PRESCRIBING rx
    JOIN {{ last_schema }}.ENCOUNTER enc ON rx.ENCOUNTERID = enc.ENCOUNTERID
    {% if cutoff_date %}WHERE rx.RX_ORDER_DATE >= '{{ cutoff_date }}'{% endif %}
    GROUP BY enc.ENC_TYPE
),
crt_all AS (
    SELECT * FROM crt_diag UNION ALL SELECT * FROM crt_proc
    UNION ALL SELECT * FROM crt_lab  UNION ALL SELECT * FROM crt_rx
),
old_all AS (
    SELECT * FROM old_diag UNION ALL SELECT * FROM old_proc
    UNION ALL SELECT * FROM old_lab  UNION ALL SELECT * FROM old_rx
),
detail AS (
    SELECT
        CASE c.DOMAIN
            WHEN 'DIAGNOSIS'     THEN 'Diagnosis'
            WHEN 'PROCEDURES'    THEN 'Procedures'
            WHEN 'LAB_RESULT_CM' THEN 'Labs'
            WHEN 'PRESCRIBING'   THEN 'Prescriptions'
        END                                                                                                    AS DOMAIN_LABEL,
        c.ENC_TYPE,
        COALESCE(o.PREVIOUS_RECORD, 0)                                                                        AS PREV_RECORDS,
        c.CURRENT_RECORD                                                                                       AS CURR_RECORDS,
        CASE WHEN COALESCE(o.PREVIOUS_RECORD, 0) = 0 THEN NULL
             ELSE ROUND(((c.CURRENT_RECORD - o.PREVIOUS_RECORD) / o.PREVIOUS_RECORD::FLOAT) * 100, 1)
        END                                                                                                    AS PCT_CHANGE_RECORDS,
        COALESCE(o.PREVIOUS_PATIENTS, 0)                                                                      AS PREV_PATIENTS,
        c.CURRENT_PATIENTS                                                                                     AS CURR_PATIENTS,
        CASE WHEN COALESCE(o.PREVIOUS_PATIENTS, 0) = 0 THEN NULL
             ELSE ROUND(((c.CURRENT_PATIENTS - o.PREVIOUS_PATIENTS) / o.PREVIOUS_PATIENTS::FLOAT) * 100, 1)
        END                                                                                                    AS PCT_CHANGE_PATIENTS,
        CASE c.ENC_TYPE || '|' || c.DOMAIN
            WHEN 'AV|DIAGNOSIS'      THEN 11  WHEN 'AV|PROCEDURES'     THEN 12
            WHEN 'AV|LAB_RESULT_CM'  THEN 13  WHEN 'AV|PRESCRIBING'    THEN 14
            WHEN 'ED|DIAGNOSIS'      THEN 21  WHEN 'ED|PROCEDURES'     THEN 22
            WHEN 'ED|LAB_RESULT_CM'  THEN 23  WHEN 'ED|PRESCRIBING'    THEN 24
            WHEN 'IP|DIAGNOSIS'      THEN 31  WHEN 'IP|PROCEDURES'     THEN 32
            WHEN 'IP|LAB_RESULT_CM'  THEN 33  WHEN 'IP|PRESCRIBING'    THEN 34
            WHEN 'OA|DIAGNOSIS'      THEN 41  WHEN 'OA|PROCEDURES'     THEN 42
            WHEN 'OA|LAB_RESULT_CM'  THEN 43  WHEN 'OA|PRESCRIBING'    THEN 44
            WHEN 'TH|DIAGNOSIS'      THEN 51  WHEN 'TH|PROCEDURES'     THEN 52
            WHEN 'TH|LAB_RESULT_CM'  THEN 53  WHEN 'TH|PRESCRIBING'    THEN 54
            ELSE 99
        END AS ROW_ORDER
    FROM crt_all c
    LEFT JOIN old_all o ON o.ENC_TYPE = c.ENC_TYPE AND o.DOMAIN = c.DOMAIN
    WHERE c.ENC_TYPE IN ('AV','TH','OA','ED','IP')
)
SELECT
    CASE WHEN DOMAIN_LABEL = '' THEN ENC_TYPE ELSE DOMAIN_LABEL END  AS "Encounter Type",
    COALESCE(TO_VARCHAR(PREV_RECORDS), '')                            AS "RECORDS__Previous Refresh",
    COALESCE(TO_VARCHAR(CURR_RECORDS), '')                            AS "RECORDS__Current Refresh",
    PCT_CHANGE_RECORDS                                                 AS "RECORDS__PCT_CHANGE",
    COALESCE(TO_VARCHAR(PREV_PATIENTS), '')                           AS "PATIENTS__Previous Refresh",
    COALESCE(TO_VARCHAR(CURR_PATIENTS), '')                           AS "PATIENTS__Current Refresh",
    PCT_CHANGE_PATIENTS                                                AS "PATIENTS__PCT_CHANGE"
FROM (
    SELECT DOMAIN_LABEL, ENC_TYPE, PREV_RECORDS, CURR_RECORDS, PCT_CHANGE_RECORDS,
           PREV_PATIENTS, CURR_PATIENTS, PCT_CHANGE_PATIENTS, ROW_ORDER
    FROM detail
    UNION ALL
    SELECT '', 'Ambulatory Visit (AV)',     NULL, NULL, NULL, NULL, NULL, NULL, 10
    UNION ALL
    SELECT '', 'Emergency Department (ED)', NULL, NULL, NULL, NULL, NULL, NULL, 20
    UNION ALL
    SELECT '', 'Inpatient (IP)',            NULL, NULL, NULL, NULL, NULL, NULL, 30
    UNION ALL
    SELECT '', 'Other Ambulatory (OA)',     NULL, NULL, NULL, NULL, NULL, NULL, 40
    UNION ALL
    SELECT '', 'Telehealth (TH)',           NULL, NULL, NULL, NULL, NULL, NULL, 50
) combined
ORDER BY ROW_ORDER
