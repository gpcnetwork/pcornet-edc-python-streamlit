-- Table IIB. Values Outside of CDM Specifications
-- Fields with pre-defined value sets containing non-conforming values. Supports DC 1.06.
-- Exceptions highlighted in red and must be corrected.
-- Returns 'All fields conform to specifications' row when no violations are found.

WITH violations AS (
    SELECT TABLE_NAME, FIELD_NAME, TO_VARCHAR(RECORDS_OUTSIDE_SPEC) AS RECORDS_OUTSIDE_SPEC, SOURCE_TABLE
    FROM (
        SELECT 'DEMOGRAPHIC' AS TABLE_NAME, 'SEX' AS FIELD_NAME,
               COUNT(*) AS RECORDS_OUTSIDE_SPEC, 'DEM_L3_SEXDIST' AS SOURCE_TABLE
        FROM {{ current_schema }}.DEMOGRAPHIC
        WHERE SEX NOT IN ('A','F','M','OT','NI','UN') AND SEX IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'DEMOGRAPHIC', 'HISPANIC',
               COUNT(*), 'DEM_L3_HISPDIST'
        FROM {{ current_schema }}.DEMOGRAPHIC
        WHERE HISPANIC NOT IN ('Y','N','R','NI','UN','OT') AND HISPANIC IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'DEMOGRAPHIC', 'RACE',
               COUNT(*), 'DEM_L3_RACEDIST'
        FROM {{ current_schema }}.DEMOGRAPHIC
        WHERE RACE NOT IN ('01','02','03','04','05','06','07','NI','UN','OT') AND RACE IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'DEMOGRAPHIC', 'GENDER_IDENTITY',
               COUNT(*), 'DEM_L3_GENDERDIST'
        FROM {{ current_schema }}.DEMOGRAPHIC
        WHERE GENDER_IDENTITY NOT IN ('DC','GQ','M','MU','SE','TF','TM','W','NI','UN','OT') AND GENDER_IDENTITY IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'DEMOGRAPHIC', 'SEXUAL_ORIENTATION',
               COUNT(*), 'DEM_L3_ORIENTDIST'
        FROM {{ current_schema }}.DEMOGRAPHIC
        WHERE SEXUAL_ORIENTATION NOT IN ('AS','BI','DC','GA','LE','MU','NI','OT','QS','QU','SE','ST','UN') AND SEXUAL_ORIENTATION IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'ENCOUNTER', 'ENC_TYPE',
               COUNT(*), 'ENC_L3_ENCTYPE'
        FROM {{ current_schema }}.ENCOUNTER
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
          AND ENC_TYPE NOT IN ('AV','ED','EI','IC','IP','IS','NI','NN','OA','OS','TH','UN','OT') AND ENC_TYPE IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'ENCOUNTER', 'DISCHARGE_STATUS',
               COUNT(*), 'ENC_L3_DISSTAT'
        FROM {{ current_schema }}.ENCOUNTER
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
          AND DISCHARGE_STATUS NOT IN ('AF','AL','AM','AW','EX','HH','HS','IP','MA','MF','NH','OT','RH','SH','SN','NI','UN') AND DISCHARGE_STATUS IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'ENROLLMENT', 'ENR_BASIS',
               COUNT(*), 'ENR_L3_BASIS'
        FROM {{ current_schema }}.ENROLLMENT
        WHERE ENR_BASIS NOT IN ('D','G','I','A','NI','UN','OT') AND ENR_BASIS IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'DIAGNOSIS', 'DX_TYPE',
               COUNT(*), 'DIA_L3_DXTYPE'
        FROM {{ current_schema }}.DIAGNOSIS
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
          AND DX_TYPE NOT IN ('09','10','11','SM','NI','UN','OT') AND DX_TYPE IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'DIAGNOSIS', 'PDX',
               COUNT(*), 'DIA_L3_PDX'
        FROM {{ current_schema }}.DIAGNOSIS
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
          AND PDX NOT IN ('P','S','X','NI','UN','OT') AND PDX IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'DIAGNOSIS', 'DX_ORIGIN',
               COUNT(*), 'DIA_L3_ORIGIN'
        FROM {{ current_schema }}.DIAGNOSIS
        WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
          AND DX_ORIGIN NOT IN ('OD','BI','CL','DR','NI','UN','OT') AND DX_ORIGIN IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'PROCEDURES', 'PX_TYPE',
               COUNT(*), 'PRO_L3_PXTYPE'
        FROM {{ current_schema }}.PROCEDURES
        WHERE PX_DATE >= TO_DATE('{{ start_date }}')
          AND PX_TYPE NOT IN ('09','10','11','C2','C3','C4','H3','LC','ND','OT','RE','NI','UN') AND PX_TYPE IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'PROCEDURES', 'PPX',
               COUNT(*), 'PRO_L3_PPX'
        FROM {{ current_schema }}.PROCEDURES
        WHERE PX_DATE >= TO_DATE('{{ start_date }}')
          AND PPX NOT IN ('P','S','X','NI','UN','OT') AND PPX IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'VITAL', 'VITAL_SOURCE',
               COUNT(*), 'VIT_L3_SOURCE'
        FROM {{ current_schema }}.VITAL
        WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}')
          AND VITAL_SOURCE NOT IN ('HC','HD','PR','RD','NI','UN','OT') AND VITAL_SOURCE IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'VITAL', 'SMOKING',
               COUNT(*), 'VIT_L3_SMOKING'
        FROM {{ current_schema }}.VITAL
        WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}')
          AND SMOKING NOT IN ('01','02','03','04','05','06','07','08','NI','UN','OT') AND SMOKING IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'PRESCRIBING', 'RX_BASIS',
               COUNT(*), 'PRES_L3_BASIS'
        FROM {{ current_schema }}.PRESCRIBING
        WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}')
          AND RX_BASIS NOT IN ('01','02','NI','UN','OT') AND RX_BASIS IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'LAB_RESULT_CM', 'RESULT_LOC',
               COUNT(*), 'LAB_L3_LOC'
        FROM {{ current_schema }}.LAB_RESULT_CM
        WHERE RESULT_DATE >= TO_DATE('{{ start_date }}')
          AND RESULT_LOC NOT IN ('L','P','NI','UN','OT') AND RESULT_LOC IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'CONDITION', 'CONDITION_TYPE',
               COUNT(*), 'COND_L3_TYPE'
        FROM {{ current_schema }}.CONDITION
        WHERE REPORT_DATE >= TO_DATE('{{ start_date }}')
          AND CONDITION_TYPE NOT IN ('09','10','SM','HP','AG','NI','UN','OT') AND CONDITION_TYPE IS NOT NULL
        HAVING COUNT(*) > 0

        UNION ALL
        SELECT 'CONDITION', 'CONDITION_STATUS',
               COUNT(*), 'COND_L3_STATUS'
        FROM {{ current_schema }}.CONDITION
        WHERE REPORT_DATE >= TO_DATE('{{ start_date }}')
          AND CONDITION_STATUS NOT IN ('AC','IN','RS','NI','UN','OT') AND CONDITION_STATUS IS NOT NULL
        HAVING COUNT(*) > 0
    ) sub
)
SELECT TABLE_NAME, FIELD_NAME, RECORDS_OUTSIDE_SPEC, SOURCE_TABLE
FROM violations
UNION ALL
SELECT 'All fields conform to specifications', '', '', ''
WHERE NOT EXISTS (SELECT 1 FROM violations)
ORDER BY TABLE_NAME, FIELD_NAME
