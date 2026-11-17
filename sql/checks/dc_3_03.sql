-- DC 3.03 | Table IVC | Data Completeness | Investigative
-- More than 10% of records have missing or unknown values for required-table fields.
--
-- SAS source of truth: edc_report.sas 7612-8037 (Table IVC, row=3.03):
--   * Numerators  (7787-7827): records whose field value is "NULL or missing", NI, UN or OT
--       - categorical fields from the *_L3_* distributions (category in NULL/NI/UN/OT, RECORD_N)
--       - ENCOUNTERID from DIA_L3_N / PRO_L3_N (statistic NULL_N -> NULL only)
--       - date fields from XTBL_L3_DATES (statistic NMISS -> NULL only)
--       - IP/EI-constrained fields from ENC_L3_ENCTYPE_* crosstabs
--         (category in IP/EI, cross_category in NULL/NI/UN/OT)
--   * Denominators (7752-7781): total table record count (*_L3_N ALL_N + NULL_N of PATID),
--       or total IP/EI encounter records for the ENC_TYPE-constrained fields.
--   * Exception fields (7893-7895): thresholdflag=1 only for BIRTH_DATE, SEX, DX_ORIGIN,
--       DX_SOURCE, DIAGNOSIS.ENCOUNTERID, DISCHARGE_DATE (IP/EI), DISCHARGE_DISPOSITION (IP/EI),
--       PROCEDURES.ENCOUNTERID, PX_DATE, PX_SOURCE when num>0, denom>0 and perc >= 10.00.
--   * Exception row (7913-7932): row=3.03, edc_table_s_="Table IVC".
-- The underlying DC L3 aggregates are FULL-TABLE (no date window), so no
-- {{ start_date }} / {{ end_date }} filtering is applied here.
-- Parameters: {{ current_schema }}
WITH demographic AS (
    SELECT COUNT(*) AS DENOM,
           COUNT_IF(BIRTH_DATE IS NULL)                                        AS BAD_BIRTH_DATE,
           COUNT_IF(SEX IS NULL OR TRIM(SEX) = '' OR SEX IN ('NI','UN','OT'))  AS BAD_SEX
    FROM {{ current_schema }}.DEMOGRAPHIC
),
diagnosis AS (
    SELECT COUNT(*) AS DENOM,
           COUNT_IF(DX_ORIGIN IS NULL OR TRIM(DX_ORIGIN) = '' OR DX_ORIGIN IN ('NI','UN','OT')) AS BAD_DX_ORIGIN,
           COUNT_IF(DX_SOURCE IS NULL OR TRIM(DX_SOURCE) = '' OR DX_SOURCE IN ('NI','UN','OT')) AS BAD_DX_SOURCE,
           COUNT_IF(ENCOUNTERID IS NULL OR TRIM(ENCOUNTERID) = '')                              AS BAD_ENCOUNTERID
    FROM {{ current_schema }}.DIAGNOSIS
),
encounter_ipei AS (
    -- SAS denominator = ENC_L3_ENCTYPE_* RECORD_N for category IP / EI (edc_report.sas 7757-7760)
    SELECT COUNT(*) AS DENOM,
           COUNT_IF(DISCHARGE_DATE IS NULL)                                    AS BAD_DISCHARGE_DATE,
           COUNT_IF(DISCHARGE_DISPOSITION IS NULL OR TRIM(DISCHARGE_DISPOSITION) = ''
                    OR DISCHARGE_DISPOSITION IN ('NI','UN','OT'))              AS BAD_DISCHARGE_DISPOSITION
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ENC_TYPE IN ('IP','EI')
),
procedures AS (
    SELECT COUNT(*) AS DENOM,
           COUNT_IF(ENCOUNTERID IS NULL OR TRIM(ENCOUNTERID) = '')                              AS BAD_ENCOUNTERID,
           COUNT_IF(PX_DATE IS NULL)                                                            AS BAD_PX_DATE,
           COUNT_IF(PX_SOURCE IS NULL OR TRIM(PX_SOURCE) = '' OR PX_SOURCE IN ('NI','UN','OT')) AS BAD_PX_SOURCE
    FROM {{ current_schema }}.PROCEDURES
),
fields AS (
    SELECT 'DEMOGRAPHIC' AS EXC_TABLE, 'BIRTH_DATE' AS EXC_FIELD, BAD_BIRTH_DATE AS NUM, DENOM FROM demographic
    UNION ALL SELECT 'DEMOGRAPHIC', 'SEX',                          BAD_SEX,                   DENOM FROM demographic
    UNION ALL SELECT 'DIAGNOSIS',   'DX_ORIGIN',                    BAD_DX_ORIGIN,             DENOM FROM diagnosis
    UNION ALL SELECT 'DIAGNOSIS',   'DX_SOURCE',                    BAD_DX_SOURCE,             DENOM FROM diagnosis
    UNION ALL SELECT 'DIAGNOSIS',   'ENCOUNTERID',                  BAD_ENCOUNTERID,           DENOM FROM diagnosis
    UNION ALL SELECT 'ENCOUNTER',   'DISCHARGE_DATE (IP/EI)',       BAD_DISCHARGE_DATE,        DENOM FROM encounter_ipei
    UNION ALL SELECT 'ENCOUNTER',   'DISCHARGE_DISPOSITION (IP/EI)',BAD_DISCHARGE_DISPOSITION, DENOM FROM encounter_ipei
    UNION ALL SELECT 'PROCEDURES',  'ENCOUNTERID',                  BAD_ENCOUNTERID,           DENOM FROM procedures
    UNION ALL SELECT 'PROCEDURES',  'PX_DATE',                      BAD_PX_DATE,               DENOM FROM procedures
    UNION ALL SELECT 'PROCEDURES',  'PX_SOURCE',                    BAD_PX_SOURCE,             DENOM FROM procedures
),
evaluated AS (
    SELECT EXC_TABLE, EXC_FIELD, NUM, DENOM,
           CASE WHEN NUM > 0 AND DENOM > 0 THEN 100.0 * NUM / DENOM END AS PCT,
           -- SAS edc_report.sas 7892-7895: perc only computed when num>0 and denom>0;
           -- thresholdflag=1 when perc >= 10.00
           CASE WHEN NUM > 0 AND DENOM > 0 AND 100.0 * NUM / DENOM >= 10.0 THEN 1 ELSE 0 END AS FLAG
    FROM fields
),
summary AS (
    SELECT
        '3.03'                                                                 AS CHECK_NUM,
        'Table IVC: more than 10% of records have missing or unknown values for required-table fields' AS DESCRIPTION,
        CASE WHEN (SELECT MAX(FLAG) FROM evaluated) = 1 THEN 'Fail' ELSE 'Pass' END AS STATUS,
        'SUMMARY'                                                              AS ROW_TYPE,
        CAST(NULL AS VARCHAR)                                                  AS EXC_TABLE,
        CAST(NULL AS VARCHAR)                                                  AS EXC_FIELD,
        CAST(NULL AS VARCHAR)                                                  AS EXC_DETAIL,
        CAST(NULL AS NUMBER)                                                   AS EXC_COUNT,
        0                                                                      AS ROW_ORDER
),
details AS (
    SELECT
        '3.03'                                                                 AS CHECK_NUM,
        'Table IVC: more than 10% of records have missing or unknown values for required-table fields' AS DESCRIPTION,
        CASE WHEN FLAG = 1 THEN 'Fail' ELSE 'Pass' END                         AS STATUS,
        'DETAIL'                                                               AS ROW_TYPE,
        e.EXC_TABLE                                                            AS EXC_TABLE,
        e.EXC_FIELD                                                            AS EXC_FIELD,
        'missing/unknown ' || COALESCE(TO_VARCHAR(ROUND(e.PCT, 1)::DECIMAL(10,1)), '0.0') || '% ('
            || TO_VARCHAR(e.NUM) || '/' || TO_VARCHAR(e.DENOM) || ')'          AS EXC_DETAIL,
        e.NUM                                                                  AS EXC_COUNT,
        ROW_NUMBER() OVER (ORDER BY e.EXC_TABLE, e.EXC_FIELD)                  AS ROW_ORDER
    FROM evaluated e
)
SELECT * FROM summary
UNION ALL
SELECT * FROM details
ORDER BY ROW_ORDER
