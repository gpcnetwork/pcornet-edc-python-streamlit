-- Table IVC. Missing or Unknown Values, Required Tables
-- Fields in DEMOGRAPHIC, DIAGNOSIS, ENCOUNTER, ENROLLMENT, and PROCEDURES where the field is
-- not required to be populated. Supports DC 3.03 (> 10% missing/NI/UN/OT for required fields).
-- Exceptions highlighted in blue and should be investigated and explained in the ETL ADD.

WITH demo AS (
    SELECT
        COUNT(*)                                                                                                         AS total,
        SUM(CASE WHEN BIOBANK_FLAG          IS NULL OR BIOBANK_FLAG          IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS biobank_flag,
        SUM(CASE WHEN BIRTH_DATE            IS NULL                                              THEN 1 ELSE 0 END)     AS birth_date,
        SUM(CASE WHEN BIRTH_TIME            IS NULL                                              THEN 1 ELSE 0 END)     AS birth_time,
        SUM(CASE WHEN GENDER_IDENTITY       IS NULL OR GENDER_IDENTITY       IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS gender_identity,
        SUM(CASE WHEN HISPANIC              IS NULL OR HISPANIC              IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS hispanic,
        SUM(CASE WHEN PAT_PREF_LANGUAGE_SPOKEN IS NULL OR PAT_PREF_LANGUAGE_SPOKEN IN ('NI','UN','OT') THEN 1 ELSE 0 END) AS pat_pref_lang,
        SUM(CASE WHEN RACE                  IS NULL OR RACE                  IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS race,
        SUM(CASE WHEN RACE_ETH_AI_AN        IS NULL OR RACE_ETH_AI_AN        IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS race_eth_ai_an,
        SUM(CASE WHEN RACE_ETH_ASIAN        IS NULL OR RACE_ETH_ASIAN        IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS race_eth_asian,
        SUM(CASE WHEN RACE_ETH_BLACK        IS NULL OR RACE_ETH_BLACK        IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS race_eth_black,
        SUM(CASE WHEN RACE_ETH_HISPANIC     IS NULL OR RACE_ETH_HISPANIC     IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS race_eth_hispanic,
        SUM(CASE WHEN RACE_ETH_ME_NA        IS NULL OR RACE_ETH_ME_NA        IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS race_eth_me_na,
        SUM(CASE WHEN RACE_ETH_MISSING      IS NULL OR RACE_ETH_MISSING      IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS race_eth_missing,
        SUM(CASE WHEN RACE_ETH_NH_PI        IS NULL OR RACE_ETH_NH_PI        IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS race_eth_nh_pi,
        SUM(CASE WHEN RACE_ETH_WHITE        IS NULL OR RACE_ETH_WHITE        IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS race_eth_white,
        SUM(CASE WHEN SEX                   IS NULL OR SEX                   IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS sex,
        SUM(CASE WHEN SEXUAL_ORIENTATION    IS NULL OR SEXUAL_ORIENTATION    IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS sexual_orientation
    FROM {{ current_schema }}.DEMOGRAPHIC
),
dx AS (
    SELECT
        COUNT(*)                                                                                                         AS total,
        SUM(CASE WHEN ADMIT_DATE   IS NULL                                              THEN 1 ELSE 0 END)               AS admit_date,
        SUM(CASE WHEN DX_DATE      IS NULL                                              THEN 1 ELSE 0 END)               AS dx_date,
        SUM(CASE WHEN DX_ORIGIN    IS NULL OR DX_ORIGIN    IN ('NI','UN','OT')          THEN 1 ELSE 0 END)               AS dx_origin,
        SUM(CASE WHEN DX_POA       IS NULL OR DX_POA       IN ('NI','UN','OT')          THEN 1 ELSE 0 END)               AS dx_poa,
        SUM(CASE WHEN DX_SOURCE    IS NULL OR DX_SOURCE    IN ('NI','UN','OT')          THEN 1 ELSE 0 END)               AS dx_source,
        SUM(CASE WHEN DX_TYPE      IS NULL OR DX_TYPE      IN ('NI','UN','OT')          THEN 1 ELSE 0 END)               AS dx_type,
        SUM(CASE WHEN ENCOUNTERID  IS NULL                                              THEN 1 ELSE 0 END)               AS encounterid,
        SUM(CASE WHEN ENC_TYPE     IS NULL OR ENC_TYPE     IN ('NI','UN','OT')          THEN 1 ELSE 0 END)               AS enc_type,
        SUM(CASE WHEN PROVIDERID   IS NULL                                              THEN 1 ELSE 0 END)               AS providerid
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
dx_ip_ei AS (
    SELECT
        COUNT(*)                                                                                                         AS total,
        SUM(CASE WHEN PDX IS NULL OR PDX IN ('NI','UN','OT')                            THEN 1 ELSE 0 END)               AS pdx
    FROM {{ current_schema }}.DIAGNOSIS
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
      AND ENC_TYPE IN ('IP', 'EI')
),
enc AS (
    SELECT
        COUNT(*)                                                                                                         AS total,
        SUM(CASE WHEN ADMIT_TIME            IS NULL                                              THEN 1 ELSE 0 END)     AS admit_time,
        SUM(CASE WHEN DISCHARGE_TIME        IS NULL                                              THEN 1 ELSE 0 END)     AS discharge_time,
        SUM(CASE WHEN DRG_TYPE             IS NULL OR DRG_TYPE             IN ('NI','UN','OT')   THEN 1 ELSE 0 END)     AS drg_type,
        SUM(CASE WHEN ENC_TYPE             IS NULL OR ENC_TYPE             IN ('NI','UN','OT')   THEN 1 ELSE 0 END)     AS enc_type,
        SUM(CASE WHEN FACILITYID           IS NULL                                               THEN 1 ELSE 0 END)     AS facilityid,
        SUM(CASE WHEN FACILITY_LOCATION    IS NULL OR FACILITY_LOCATION    IN ('NI','UN','OT')   THEN 1 ELSE 0 END)     AS facility_location,
        SUM(CASE WHEN FACILITY_TYPE        IS NULL OR FACILITY_TYPE        IN ('NI','UN','OT')   THEN 1 ELSE 0 END)     AS facility_type,
        SUM(CASE WHEN PAYER_TYPE_PRIMARY   IS NULL OR PAYER_TYPE_PRIMARY   IN ('NI','UN','OT')   THEN 1 ELSE 0 END)     AS payer_type_primary,
        SUM(CASE WHEN PAYER_TYPE_SECONDARY IS NULL OR PAYER_TYPE_SECONDARY IN ('NI','UN','OT')   THEN 1 ELSE 0 END)     AS payer_type_secondary,
        SUM(CASE WHEN PROVIDERID           IS NULL                                               THEN 1 ELSE 0 END)     AS providerid
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
),
enc_ip_ei AS (
    SELECT
        COUNT(*)                                                                                                         AS total,
        SUM(CASE WHEN ADMITTING_SOURCE      IS NULL OR ADMITTING_SOURCE      IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS admitting_source,
        SUM(CASE WHEN DISCHARGE_DATE        IS NULL                                              THEN 1 ELSE 0 END)     AS discharge_date,
        SUM(CASE WHEN DISCHARGE_DISPOSITION IS NULL OR DISCHARGE_DISPOSITION IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS discharge_disposition,
        SUM(CASE WHEN DISCHARGE_STATUS      IS NULL OR DISCHARGE_STATUS      IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS discharge_status,
        SUM(CASE WHEN DRG                   IS NULL OR DRG                   IN ('NI','UN','OT') THEN 1 ELSE 0 END)     AS drg
    FROM {{ current_schema }}.ENCOUNTER
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
      AND ENC_TYPE IN ('IP', 'EI')
),
enr AS (
    SELECT
        COUNT(*)                                                                                                         AS total,
        SUM(CASE WHEN CHART       IS NULL OR CHART       IN ('NI','UN','OT')            THEN 1 ELSE 0 END)               AS chart,
        SUM(CASE WHEN ENR_END_DATE IS NULL                                              THEN 1 ELSE 0 END)               AS enr_end_date
    FROM {{ current_schema }}.ENROLLMENT
),
px AS (
    SELECT
        COUNT(*)                                                                                                         AS total,
        SUM(CASE WHEN ADMIT_DATE  IS NULL                                               THEN 1 ELSE 0 END)               AS admit_date,
        SUM(CASE WHEN ENCOUNTERID IS NULL                                               THEN 1 ELSE 0 END)               AS encounterid,
        SUM(CASE WHEN ENC_TYPE    IS NULL OR ENC_TYPE    IN ('NI','UN','OT')            THEN 1 ELSE 0 END)               AS enc_type,
        SUM(CASE WHEN PPX         IS NULL OR PPX         IN ('NI','UN','OT')            THEN 1 ELSE 0 END)               AS ppx,
        SUM(CASE WHEN PROVIDERID  IS NULL                                               THEN 1 ELSE 0 END)               AS providerid,
        SUM(CASE WHEN PX_DATE     IS NULL                                               THEN 1 ELSE 0 END)               AS px_date,
        SUM(CASE WHEN PX_SOURCE   IS NULL OR PX_SOURCE   IN ('NI','UN','OT')            THEN 1 ELSE 0 END)               AS px_source,
        SUM(CASE WHEN PX_TYPE     IS NULL OR PX_TYPE     IN ('NI','UN','OT')            THEN 1 ELSE 0 END)               AS px_type
    FROM {{ current_schema }}.PROCEDURES
    WHERE ADMIT_DATE >= TO_DATE('{{ start_date }}')
)
SELECT TABLE_NAME, FIELD_NAME, ENC_TYPE_CONSTRAINT,
       TO_VARCHAR(NUMERATOR)   AS NUMERATOR,
       TO_VARCHAR(DENOMINATOR) AS DENOMINATOR,
       CASE WHEN NUMERATOR = 0 OR DENOMINATOR IS NULL OR DENOMINATOR = 0 THEN NULL
            ELSE TO_VARCHAR(ROUND(100.0 * NUMERATOR / DENOMINATOR, 1)) || '%'
       END AS PCT
FROM (
    -- DEMOGRAPHIC (17 fields, no date filter — no date key in this table)
    SELECT 'DEMOGRAPHIC' AS TABLE_NAME, 'BIOBANK_FLAG'               AS FIELD_NAME, '' AS ENC_TYPE_CONSTRAINT, biobank_flag        AS NUMERATOR, total AS DENOMINATOR,  1 AS ROW_ORDER FROM demo
    UNION ALL SELECT 'DEMOGRAPHIC', 'BIRTH_DATE',               '', birth_date,          total,  2 FROM demo
    UNION ALL SELECT 'DEMOGRAPHIC', 'BIRTH_TIME',               '', birth_time,           total,  3 FROM demo
    UNION ALL SELECT 'DEMOGRAPHIC', 'GENDER_IDENTITY',          '', gender_identity,      total,  4 FROM demo
    UNION ALL SELECT 'DEMOGRAPHIC', 'HISPANIC',                 '', hispanic,             total,  5 FROM demo
    UNION ALL SELECT 'DEMOGRAPHIC', 'PAT_PREF_LANGUAGE_SPOKEN', '', pat_pref_lang,        total,  6 FROM demo
    UNION ALL SELECT 'DEMOGRAPHIC', 'RACE',                     '', race,                 total,  7 FROM demo
    UNION ALL SELECT 'DEMOGRAPHIC', 'RACE_ETH_AI_AN',           '', race_eth_ai_an,       total,  8 FROM demo
    UNION ALL SELECT 'DEMOGRAPHIC', 'RACE_ETH_ASIAN',           '', race_eth_asian,       total,  9 FROM demo
    UNION ALL SELECT 'DEMOGRAPHIC', 'RACE_ETH_BLACK',           '', race_eth_black,       total, 10 FROM demo
    UNION ALL SELECT 'DEMOGRAPHIC', 'RACE_ETH_HISPANIC',        '', race_eth_hispanic,    total, 11 FROM demo
    UNION ALL SELECT 'DEMOGRAPHIC', 'RACE_ETH_ME_NA',           '', race_eth_me_na,       total, 12 FROM demo
    UNION ALL SELECT 'DEMOGRAPHIC', 'RACE_ETH_MISSING',         '', race_eth_missing,     total, 13 FROM demo
    UNION ALL SELECT 'DEMOGRAPHIC', 'RACE_ETH_NH_PI',           '', race_eth_nh_pi,       total, 14 FROM demo
    UNION ALL SELECT 'DEMOGRAPHIC', 'RACE_ETH_WHITE',           '', race_eth_white,       total, 15 FROM demo
    UNION ALL SELECT 'DEMOGRAPHIC', 'SEX',                      '', sex,                  total, 16 FROM demo
    UNION ALL SELECT 'DEMOGRAPHIC', 'SEXUAL_ORIENTATION',       '', sexual_orientation,   total, 17 FROM demo
    -- DIAGNOSIS (10 rows — 9 all-records + 1 IP/EI)
    UNION ALL SELECT 'DIAGNOSIS', 'ADMIT_DATE',  '', admit_date,  total, 18 FROM dx
    UNION ALL SELECT 'DIAGNOSIS', 'DX_DATE',     '', dx_date,     total, 19 FROM dx
    UNION ALL SELECT 'DIAGNOSIS', 'DX_ORIGIN',   '', dx_origin,   total, 20 FROM dx
    UNION ALL SELECT 'DIAGNOSIS', 'DX_POA',      '', dx_poa,      total, 21 FROM dx
    UNION ALL SELECT 'DIAGNOSIS', 'DX_SOURCE',   '', dx_source,   total, 22 FROM dx
    UNION ALL SELECT 'DIAGNOSIS', 'DX_TYPE',     '', dx_type,     total, 23 FROM dx
    UNION ALL SELECT 'DIAGNOSIS', 'ENCOUNTERID', '', encounterid, total, 24 FROM dx
    UNION ALL SELECT 'DIAGNOSIS', 'ENC_TYPE',    '', enc_type,    total, 25 FROM dx
    UNION ALL SELECT 'DIAGNOSIS', 'PDX',         'IP or EI', pdx, total, 26 FROM dx_ip_ei
    UNION ALL SELECT 'DIAGNOSIS', 'PROVIDERID',  '', providerid,  total, 27 FROM dx
    -- ENCOUNTER (15 rows — 10 all-records + 5 IP/EI, ordered alphabetically)
    UNION ALL SELECT 'ENCOUNTER', 'ADMITTING_SOURCE',      'IP or EI', admitting_source,      total, 28 FROM enc_ip_ei
    UNION ALL SELECT 'ENCOUNTER', 'ADMIT_TIME',            '',          admit_time,            total, 29 FROM enc
    UNION ALL SELECT 'ENCOUNTER', 'DISCHARGE_DATE',        'IP or EI', discharge_date,        total, 30 FROM enc_ip_ei
    UNION ALL SELECT 'ENCOUNTER', 'DISCHARGE_DISPOSITION', 'IP or EI', discharge_disposition, total, 31 FROM enc_ip_ei
    UNION ALL SELECT 'ENCOUNTER', 'DISCHARGE_STATUS',      'IP or EI', discharge_status,      total, 32 FROM enc_ip_ei
    UNION ALL SELECT 'ENCOUNTER', 'DISCHARGE_TIME',        '',          discharge_time,        total, 33 FROM enc
    UNION ALL SELECT 'ENCOUNTER', 'DRG',                   'IP or EI', drg,                   total, 34 FROM enc_ip_ei
    UNION ALL SELECT 'ENCOUNTER', 'DRG_TYPE',              '',          drg_type,              total, 35 FROM enc
    UNION ALL SELECT 'ENCOUNTER', 'ENC_TYPE',              '',          enc_type,              total, 36 FROM enc
    UNION ALL SELECT 'ENCOUNTER', 'FACILITYID',            '',          facilityid,            total, 37 FROM enc
    UNION ALL SELECT 'ENCOUNTER', 'FACILITY_LOCATION',     '',          facility_location,     total, 38 FROM enc
    UNION ALL SELECT 'ENCOUNTER', 'FACILITY_TYPE',         '',          facility_type,         total, 39 FROM enc
    UNION ALL SELECT 'ENCOUNTER', 'PAYER_TYPE_PRIMARY',    '',          payer_type_primary,    total, 40 FROM enc
    UNION ALL SELECT 'ENCOUNTER', 'PAYER_TYPE_SECONDARY',  '',          payer_type_secondary,  total, 41 FROM enc
    UNION ALL SELECT 'ENCOUNTER', 'PROVIDERID',            '',          providerid,            total, 42 FROM enc
    -- ENROLLMENT (2 rows, no date filter)
    UNION ALL SELECT 'ENROLLMENT', 'CHART',        '', chart,        total, 43 FROM enr
    UNION ALL SELECT 'ENROLLMENT', 'ENR_END_DATE', '', enr_end_date, total, 44 FROM enr
    -- PROCEDURES (8 rows)
    UNION ALL SELECT 'PROCEDURES', 'ADMIT_DATE',  '', admit_date,  total, 45 FROM px
    UNION ALL SELECT 'PROCEDURES', 'ENCOUNTERID', '', encounterid, total, 46 FROM px
    UNION ALL SELECT 'PROCEDURES', 'ENC_TYPE',    '', enc_type,    total, 47 FROM px
    UNION ALL SELECT 'PROCEDURES', 'PPX',         '', ppx,         total, 48 FROM px
    UNION ALL SELECT 'PROCEDURES', 'PROVIDERID',  '', providerid,  total, 49 FROM px
    UNION ALL SELECT 'PROCEDURES', 'PX_DATE',     '', px_date,     total, 50 FROM px
    UNION ALL SELECT 'PROCEDURES', 'PX_SOURCE',   '', px_source,   total, 51 FROM px
    UNION ALL SELECT 'PROCEDURES', 'PX_TYPE',     '', px_type,     total, 52 FROM px
) sub
ORDER BY ROW_ORDER
