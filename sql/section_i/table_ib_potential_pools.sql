-- Table IB. Potential Pools of Patients
-- Illustrates patient counts meeting different inclusion criteria.
-- Supports DC 2.09, 3.04, 3.05.

WITH total_patients AS (
    SELECT COUNT(DISTINCT patid) AS n FROM {{ current_schema }}.demographic
),
enc_patient_pool_5 AS (
    SELECT DISTINCT patid FROM {{ current_schema }}.encounter
    WHERE admit_date >= '{{ filter_date }}' AND admit_date <= '{{ end_date }}' AND ENC_TYPE IN ('EI','ED','AV','IP','OS')
),
total_enc_patients AS (
    SELECT COUNT(DISTINCT patid) AS enc_n FROM enc_patient_pool_5
),
enc_patient_pool_1 AS (
    SELECT DISTINCT patid FROM {{ current_schema }}.encounter
    WHERE admit_date >= '{{ year_1 }}' AND admit_date <= '{{ end_date }}' AND ENC_TYPE IN ('EI','ED','AV','IP','OS')
),
diagnosis_patient_pool_5 AS (
    SELECT DISTINCT patid FROM {{ current_schema }}.DIAGNOSIS
    WHERE enc_type IN ('EI','ED','AV','IP','OS') AND ADMIT_DATE >= '{{ filter_date }}' AND ADMIT_DATE <= '{{ end_date }}'
),
procedures_patient_pool_5 AS (
    SELECT DISTINCT patid FROM {{ current_schema }}.procedures WHERE ADMIT_DATE >= '{{ filter_date }}' AND ADMIT_DATE <= '{{ end_date }}'
),
diagnosis_vital_patient_pool_5 AS (
    SELECT DISTINCT patid FROM diagnosis_patient_pool_5
    INTERSECT
    SELECT DISTINCT patid FROM {{ current_schema }}.vital WHERE MEASURE_DATE >= '{{ filter_date }}' AND MEASURE_DATE <= '{{ end_date }}'
),
prescribing_or_med_admin_patient_pool_5 AS (
    SELECT DISTINCT patid FROM {{ current_schema }}.prescribing WHERE RX_ORDER_DATE >= '{{ filter_date }}' AND RX_ORDER_DATE <= '{{ end_date }}'
    UNION
    SELECT DISTINCT patid FROM {{ current_schema }}.med_admin WHERE MEDADMIN_START_DATE >= '{{ filter_date }}' AND MEDADMIN_START_DATE <= '{{ end_date }}'
),
diagnosis_vital_and_prescribing_or_med_admin_patient_pool_5 AS (
    SELECT DISTINCT patid FROM diagnosis_vital_patient_pool_5
    INTERSECT
    SELECT DISTINCT patid FROM prescribing_or_med_admin_patient_pool_5
),
diagnosis_vital_and_prescribing_or_med_admin_lab_result_cm_patient_pool_5 AS (
    SELECT DISTINCT patid FROM diagnosis_vital_and_prescribing_or_med_admin_patient_pool_5
    INTERSECT
    SELECT DISTINCT patid FROM {{ current_schema }}.lab_result_cm WHERE RESULT_DATE >= '{{ filter_date }}' AND RESULT_DATE <= '{{ end_date }}'
),
enc_diagnosis_patient_pool_5 AS (
    SELECT DISTINCT patid FROM enc_patient_pool_5
    INTERSECT
    SELECT DISTINCT patid FROM diagnosis_patient_pool_5
),
enc_procedures_patient_pool_5 AS (
    SELECT DISTINCT patid FROM enc_patient_pool_5
    INTERSECT
    SELECT DISTINCT patid FROM procedures_patient_pool_5
)
SELECT Metric, Metric_Description, Result,
    CASE
        WHEN row_order IN (2, 3)
            THEN TO_VARCHAR(ROUND((Result::FLOAT / n) * 100, 1)) || '%'
        WHEN row_order IN (1)
            THEN ''
        ELSE
            TO_VARCHAR(ROUND((Result::FLOAT / enc_n) * 100, 1)) || '%'
    END AS percentage
FROM (
    SELECT 'All patients' AS Metric,
           'Number of unique patients in the DEMOGRAPHIC table' AS Metric_Description,
           TO_VARCHAR(COUNT(DISTINCT patid)) AS Result, 1 AS row_order
    FROM {{ current_schema }}.demographic
    UNION
    SELECT 'Potential pool of patients for observational studies',
           'Number of unique patients with at least 1 face-to-face (ED, EI, IP, OS, or AV) encounter within the past 5 years',
           TO_VARCHAR(COUNT(patid)), 2
    FROM enc_patient_pool_5
    UNION
    SELECT 'Potential pool of patients for trials',
           'Number of unique patients with at least 1 face-to-face (ED, EI, IP, OS, or AV) encounter within the past 1 year',
           TO_VARCHAR(COUNT(patid)), 3
    FROM enc_patient_pool_1
    UNION
    SELECT 'Potential pool of patients for studies requiring data on diagnoses, vital measures and (a) medications or (b) medications and lab results',
           'Number of unique patients with at least 1 DIAGNOSIS record in a face-to-face setting and at least 1 VITAL record within the past 5 years',
           TO_VARCHAR(COUNT(patid)), 4
    FROM diagnosis_vital_patient_pool_5
    UNION
    SELECT '',
           'Number of unique patients with at least 1 DIAGNOSIS record in a face-to-face setting, at least 1 VITAL record, and at least 1 PRESCRIBING or MED_ADMIN record within the past 5 years',
           TO_VARCHAR(COUNT(patid)), 5
    FROM diagnosis_vital_and_prescribing_or_med_admin_patient_pool_5
    UNION
    SELECT '',
           'Number of unique patients with at least 1 DIAGNOSIS record in a face-to-face setting, at least 1 VITAL record, at least 1 PRESCRIBING or MED_ADMIN record, and at least 1 LAB_RESULT_CM record within the past 5 years',
           TO_VARCHAR(COUNT(patid)), 6
    FROM diagnosis_vital_and_prescribing_or_med_admin_lab_result_cm_patient_pool_5
    UNION
    SELECT 'Patients with diagnosis data',
           'Percentage of patients with encounters who have at least 1 diagnosis',
           TO_VARCHAR(COUNT(patid)), 7
    FROM enc_diagnosis_patient_pool_5
    UNION
    SELECT 'Patients with procedure data',
           'Percentage of patients with encounters who have at least 1 procedure',
           TO_VARCHAR(COUNT(patid)), 8
    FROM enc_procedures_patient_pool_5
) subquery, total_patients, total_enc_patients
ORDER BY row_order
