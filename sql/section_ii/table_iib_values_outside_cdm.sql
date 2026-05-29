-- Table IIB. Values Outside of CDM Specifications
-- Fields with pre-defined value sets containing non-conforming values. Supports DC 1.06.
-- Exceptions highlighted in red and must be corrected.
-- Parameters: {{ current_schema }}, {{ start_date }}

WITH violations AS (
    SELECT TABLE_NAME, FIELD_NAME, TO_VARCHAR(RECORDS_OUTSIDE_SPEC) AS RECORDS_OUTSIDE_SPEC
    FROM (
        SELECT 'DEMOGRAPHIC' AS TABLE_NAME, 'SEX' AS FIELD_NAME,
               COUNT(*) AS RECORDS_OUTSIDE_SPEC
        FROM {{ current_schema }}.DEMOGRAPHIC
        WHERE SEX NOT IN ('A','F','M','OT','NI','UN') AND SEX IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'DEMOGRAPHIC', 'HISPANIC',
               COUNT(*)
        FROM {{ current_schema }}.DEMOGRAPHIC
        WHERE HISPANIC NOT IN ('Y','N','R','NI','UN','OT') AND HISPANIC IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'DEMOGRAPHIC', 'RACE',
               COUNT(*)
        FROM {{ current_schema }}.DEMOGRAPHIC
        WHERE RACE NOT IN ('01','02','03','04','05','06','07','NI','UN','OT') AND RACE IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'DEMOGRAPHIC', 'GENDER_IDENTITY',
               COUNT(*)
        FROM {{ current_schema }}.DEMOGRAPHIC
        WHERE GENDER_IDENTITY NOT IN ('DC','GQ','M','MU','SE','TF','TM','W','NI','UN','OT') AND GENDER_IDENTITY IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'DEMOGRAPHIC', 'SEXUAL_ORIENTATION',
               COUNT(*)
        FROM {{ current_schema }}.DEMOGRAPHIC
        WHERE SEXUAL_ORIENTATION NOT IN ('AS','BI','DC','GA','LE','MU','NI','OT','QS','QU','SE','ST','UN') AND SEXUAL_ORIENTATION IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'ENCOUNTER', 'ENC_TYPE',
               COUNT(*)
        FROM {{ current_schema }}.ENCOUNTER
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
          AND ENC_TYPE NOT IN ('AV','ED','EI','IC','IP','IS','NI','NN','OA','OS','TH','UN','OT') AND ENC_TYPE IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'ENCOUNTER', 'DISCHARGE_STATUS',
               COUNT(*)
        FROM {{ current_schema }}.ENCOUNTER
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
          AND DISCHARGE_STATUS NOT IN ('AF','AL','AM','AW','EX','HH','HS','IP','MA','MF','NH','OT','RH','SH','SN','NI','UN') AND DISCHARGE_STATUS IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'ENROLLMENT', 'ENR_BASIS',
               COUNT(*)
        FROM {{ current_schema }}.ENROLLMENT
        WHERE ENR_BASIS NOT IN ('D','G','I','A','NI','UN','OT') AND ENR_BASIS IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'DIAGNOSIS', 'DX_TYPE',
               COUNT(*)
        FROM {{ current_schema }}.DIAGNOSIS
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
          AND DX_TYPE NOT IN ('09','10','11','SM','NI','UN','OT') AND DX_TYPE IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'DIAGNOSIS', 'PDX',
               COUNT(*)
        FROM {{ current_schema }}.DIAGNOSIS
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
          AND PDX NOT IN ('P','S','X','NI','UN','OT') AND PDX IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'DIAGNOSIS', 'DX_ORIGIN',
               COUNT(*)
        FROM {{ current_schema }}.DIAGNOSIS
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
          AND DX_ORIGIN NOT IN ('OD','BI','CL','DR','NI','UN','OT') AND DX_ORIGIN IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'PROCEDURES', 'PX_TYPE',
               COUNT(*)
        FROM {{ current_schema }}.PROCEDURES
        WHERE PX_DATE >= TO_DATE('{{ start_date }}')
          AND PX_TYPE NOT IN ('09','10','11','C2','C3','C4','H3','LC','ND','OT','RE','NI','UN') AND PX_TYPE IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'PROCEDURES', 'PPX',
               COUNT(*)
        FROM {{ current_schema }}.PROCEDURES
        WHERE PX_DATE >= TO_DATE('{{ start_date }}')
          AND PPX NOT IN ('P','S','X','NI','UN','OT') AND PPX IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'VITAL', 'VITAL_SOURCE',
               COUNT(*)
        FROM {{ current_schema }}.VITAL
        WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}')
          AND VITAL_SOURCE NOT IN ('HC','HD','PR','RD','NI','UN','OT') AND VITAL_SOURCE IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'VITAL', 'SMOKING',
               COUNT(*)
        FROM {{ current_schema }}.VITAL
        WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}')
          AND SMOKING NOT IN ('01','02','03','04','05','06','07','08','NI','UN','OT') AND SMOKING IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'PRESCRIBING', 'RX_BASIS',
               COUNT(*)
        FROM {{ current_schema }}.PRESCRIBING
        WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
          AND RX_BASIS NOT IN ('01','02','NI','UN','OT') AND RX_BASIS IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'LAB_RESULT_CM', 'RESULT_LOC',
               COUNT(*)
        FROM {{ current_schema }}.LAB_RESULT_CM
        WHERE RESULT_DATE >= TO_DATE('{{ start_date }}')
          AND RESULT_LOC NOT IN ('L','P','NI','UN','OT') AND RESULT_LOC IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'CONDITION', 'CONDITION_TYPE',
               COUNT(*)
        FROM {{ current_schema }}.CONDITION
        WHERE REPORT_DATE >= TO_DATE('{{ start_date }}')
          AND CONDITION_TYPE NOT IN ('09','10','SM','HP','AG','NI','UN','OT') AND CONDITION_TYPE IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'CONDITION', 'CONDITION_STATUS',
               COUNT(*)
        FROM {{ current_schema }}.CONDITION
        WHERE REPORT_DATE >= TO_DATE('{{ start_date }}')
          AND CONDITION_STATUS NOT IN ('AC','IN','RS','NI','UN','OT') AND CONDITION_STATUS IS NOT NULL
        HAVING COUNT(*) > 0
    ) sub
)
SELECT TABLE_NAME, FIELD_NAME, RECORDS_OUTSIDE_SPEC
FROM violations
ORDER BY TABLE_NAME, FIELD_NAME
