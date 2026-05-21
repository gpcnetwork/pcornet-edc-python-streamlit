-- DC 4.02: More than a 5% decrease in the number of patients or records for diagnosis,
-- procedures, labs or prescriptions during an ambulatory (AV), telehealth
-- (TH), other ambulatory (OA), emergency department (ED), or inpatient (IP)
-- encounter between the previous and current DataMart refresh
-- Parameters: {{ current_schema }}, {{ last_schema }}, {{ cutoff_date }}
WITH crt AS (
    SELECT enc.ENC_TYPE, 'DIAGNOSIS' AS DOMAIN, COUNT(*) AS CURRENT_RECORD, COUNT(DISTINCT d.PATID) AS CURRENT_PATIENTS
    FROM {{ current_schema }}.DIAGNOSIS d
    JOIN {{ current_schema }}.ENCOUNTER enc ON d.ENCOUNTERID = enc.ENCOUNTERID
    WHERE 1=1 {% if cutoff_date %}AND d.ADMIT_DATE >= TO_DATE('{{ cutoff_date }}'){% endif %}
    GROUP BY enc.ENC_TYPE
    UNION ALL
    SELECT enc.ENC_TYPE, 'PROCEDURES', COUNT(*), COUNT(DISTINCT p.PATID)
    FROM {{ current_schema }}.PROCEDURES p
    JOIN {{ current_schema }}.ENCOUNTER enc ON p.ENCOUNTERID = enc.ENCOUNTERID
    WHERE 1=1 {% if cutoff_date %}AND p.ADMIT_DATE >= TO_DATE('{{ cutoff_date }}'){% endif %}
    GROUP BY enc.ENC_TYPE
    UNION ALL
    SELECT enc.ENC_TYPE, 'PRESCRIBING', COUNT(*), COUNT(DISTINCT rx.PATID)
    FROM {{ current_schema }}.PRESCRIBING rx
    JOIN {{ current_schema }}.ENCOUNTER enc ON rx.ENCOUNTERID = enc.ENCOUNTERID
    WHERE 1=1 {% if cutoff_date %}AND rx.RX_ORDER_DATE >= TO_DATE('{{ cutoff_date }}'){% endif %}
    GROUP BY enc.ENC_TYPE
),
old AS (
    SELECT enc.ENC_TYPE, 'DIAGNOSIS' AS DOMAIN, COUNT(*) AS PREVIOUS_RECORD, COUNT(DISTINCT d.PATID) AS PREVIOUS_PATIENTS
    FROM {{ last_schema }}.DIAGNOSIS d
    JOIN {{ last_schema }}.ENCOUNTER enc ON d.ENCOUNTERID = enc.ENCOUNTERID
    WHERE 1=1 {% if cutoff_date %}AND d.ADMIT_DATE >= TO_DATE('{{ cutoff_date }}'){% endif %}
    GROUP BY enc.ENC_TYPE
    UNION ALL
    SELECT enc.ENC_TYPE, 'PROCEDURES', COUNT(*), COUNT(DISTINCT p.PATID)
    FROM {{ last_schema }}.PROCEDURES p
    JOIN {{ last_schema }}.ENCOUNTER enc ON p.ENCOUNTERID = enc.ENCOUNTERID
    WHERE 1=1 {% if cutoff_date %}AND p.ADMIT_DATE >= TO_DATE('{{ cutoff_date }}'){% endif %}
    GROUP BY enc.ENC_TYPE
    UNION ALL
    SELECT enc.ENC_TYPE, 'PRESCRIBING', COUNT(*), COUNT(DISTINCT rx.PATID)
    FROM {{ last_schema }}.PRESCRIBING rx
    JOIN {{ last_schema }}.ENCOUNTER enc ON rx.ENCOUNTERID = enc.ENCOUNTERID
    WHERE 1=1 {% if cutoff_date %}AND rx.RX_ORDER_DATE >= TO_DATE('{{ cutoff_date }}'){% endif %}
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
    '4.02'                                                              AS CHECK_NUM,
    '> 5% decrease in domain records by encounter type'                 AS DESCRIPTION,
    CASE WHEN EXCEPTION_COUNT > 0 THEN 'Fail' ELSE 'Pass' END           AS STATUS
FROM exceptions
