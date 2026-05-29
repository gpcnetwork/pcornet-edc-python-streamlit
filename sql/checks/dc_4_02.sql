-- DC 4.02 | Table VB | Data Persistence | Investigative
-- More than a 5% decrease in the number of patients or records for diagnosis, procedures, labs or
-- prescriptions during an ambulatory (AV), telehealth (TH), other ambulatory (OA), emergency
-- department (ED), or inpatient (IP) encounter between the previous and current DataMart refresh
-- Parameters: {{ current_schema }}, {{ last_schema }}, {{ start_date }}
WITH crt AS (
    SELECT enc.ENC_TYPE, 'DIAGNOSIS' AS DOMAIN, COUNT(*) AS CURRENT_RECORD, COUNT(DISTINCT d.PATID) AS CURRENT_PATIENTS
    FROM {{ current_schema }}.DIAGNOSIS d
    JOIN {{ current_schema }}.ENCOUNTER enc ON d.ENCOUNTERID = enc.ENCOUNTERID
    WHERE d.ADMIT_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY enc.ENC_TYPE
    UNION ALL
    SELECT enc.ENC_TYPE, 'PROCEDURES', COUNT(*), COUNT(DISTINCT p.PATID)
    FROM {{ current_schema }}.PROCEDURES p
    JOIN {{ current_schema }}.ENCOUNTER enc ON p.ENCOUNTERID = enc.ENCOUNTERID
    WHERE p.ADMIT_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY enc.ENC_TYPE
    UNION ALL
    SELECT enc.ENC_TYPE, 'LAB_RESULT_CM', COUNT(*), COUNT(DISTINCT l.PATID)
    FROM {{ current_schema }}.LAB_RESULT_CM l
    JOIN {{ current_schema }}.ENCOUNTER enc ON l.ENCOUNTERID = enc.ENCOUNTERID
    WHERE l.RESULT_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY enc.ENC_TYPE
    UNION ALL
    SELECT enc.ENC_TYPE, 'PRESCRIBING', COUNT(*), COUNT(DISTINCT rx.PATID)
    FROM {{ current_schema }}.PRESCRIBING rx
    JOIN {{ current_schema }}.ENCOUNTER enc ON rx.ENCOUNTERID = enc.ENCOUNTERID
    WHERE rx.RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY enc.ENC_TYPE
),
old AS (
    SELECT enc.ENC_TYPE, 'DIAGNOSIS' AS DOMAIN, COUNT(*) AS PREVIOUS_RECORD, COUNT(DISTINCT d.PATID) AS PREVIOUS_PATIENTS
    FROM {{ last_schema }}.DIAGNOSIS d
    JOIN {{ last_schema }}.ENCOUNTER enc ON d.ENCOUNTERID = enc.ENCOUNTERID
    WHERE d.ADMIT_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY enc.ENC_TYPE
    UNION ALL
    SELECT enc.ENC_TYPE, 'PROCEDURES', COUNT(*), COUNT(DISTINCT p.PATID)
    FROM {{ last_schema }}.PROCEDURES p
    JOIN {{ last_schema }}.ENCOUNTER enc ON p.ENCOUNTERID = enc.ENCOUNTERID
    WHERE p.ADMIT_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY enc.ENC_TYPE
    UNION ALL
    SELECT enc.ENC_TYPE, 'LAB_RESULT_CM', COUNT(*), COUNT(DISTINCT l.PATID)
    FROM {{ last_schema }}.LAB_RESULT_CM l
    JOIN {{ last_schema }}.ENCOUNTER enc ON l.ENCOUNTERID = enc.ENCOUNTERID
    WHERE l.RESULT_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY enc.ENC_TYPE
    UNION ALL
    SELECT enc.ENC_TYPE, 'PRESCRIBING', COUNT(*), COUNT(DISTINCT rx.PATID)
    FROM {{ last_schema }}.PRESCRIBING rx
    JOIN {{ last_schema }}.ENCOUNTER enc ON rx.ENCOUNTERID = enc.ENCOUNTERID
    WHERE rx.RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
    GROUP BY enc.ENC_TYPE
),
exceptions AS (
    SELECT COUNT(*) AS EXCEPTION_COUNT
    FROM crt JOIN old ON crt.ENC_TYPE = old.ENC_TYPE AND crt.DOMAIN = old.DOMAIN
    WHERE crt.ENC_TYPE IN ('AV','TH','OA','ED','IP')
      AND ((old.PREVIOUS_RECORD > 0 AND ((crt.CURRENT_RECORD - old.PREVIOUS_RECORD) / old.PREVIOUS_RECORD::FLOAT) * 100 < -5)
        OR (old.PREVIOUS_PATIENTS > 0 AND ((crt.CURRENT_PATIENTS - old.PREVIOUS_PATIENTS) / old.PREVIOUS_PATIENTS::FLOAT) * 100 < -5))
)
SELECT
    '4.02'                                                                                      AS CHECK_NUM,
    'More than 5% decrease in records or patients for diagnosis/procedures/labs/Rx by encounter type' AS DESCRIPTION,
    CASE WHEN EXCEPTION_COUNT > 0 THEN 'Fail' ELSE 'Pass' END                                  AS STATUS
FROM exceptions
