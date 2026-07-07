-- Table IVD. Missing or Unknown Values, Optional Tables
-- Fields in CONDITION, DEATH, DEATH_CAUSE, DISPENSING, EXTERNAL_MEDS, IMMUNIZATION, LAB_HISTORY,
-- LAB_RESULT_CM, LDS_ADDRESS_HISTORY, MED_ADMIN, OBS_CLIN, OBS_GEN, PAT_RELATIONSHIP, PRESCRIBING,
-- PRO_CM, PROVIDER, and VITAL with missing/unknown values.
-- Supports DC 3.03 (> 10% missing/NI/UN/OT for designated fields). Exceptions highlighted in blue.

WITH cond AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN CONDITION_SOURCE IS NULL OR CONDITION_SOURCE IN ('NI','UN','OT') THEN 1 ELSE 0 END)                       AS condition_source,
        SUM(CASE WHEN CONDITION_STATUS IS NULL OR CONDITION_STATUS IN ('NI','UN','OT') THEN 1 ELSE 0 END)                       AS condition_status,
        SUM(CASE WHEN CONDITION_TYPE   IS NULL OR CONDITION_TYPE   IN ('NI','UN','OT') THEN 1 ELSE 0 END)                       AS condition_type,
        SUM(CASE WHEN ENCOUNTERID      IS NULL                                         THEN 1 ELSE 0 END)                       AS encounterid,
        SUM(CASE WHEN ONSET_DATE       IS NULL                                         THEN 1 ELSE 0 END)                       AS onset_date,
        SUM(CASE WHEN REPORT_DATE      IS NULL                                         THEN 1 ELSE 0 END)                       AS report_date,
        SUM(CASE WHEN RESOLVE_DATE     IS NULL                                         THEN 1 ELSE 0 END)                       AS resolve_date
    FROM {{ current_schema }}.CONDITION
    WHERE REPORT_DATE >= TO_DATE('{{ start_date }}') AND REPORT_DATE <= TO_DATE('{{ end_date }}')
),
dth AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN DEATH_DATE             IS NULL                                         THEN 1 ELSE 0 END)                 AS death_date,
        SUM(CASE WHEN DEATH_DATE_IMPUTE      IS NULL OR DEATH_DATE_IMPUTE      IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS death_date_impute,
        SUM(CASE WHEN DEATH_MATCH_CONFIDENCE IS NULL OR DEATH_MATCH_CONFIDENCE IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS death_match_confidence,
        SUM(CASE WHEN DEATH_SOURCE           IS NULL OR DEATH_SOURCE           IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS death_source
    FROM {{ current_schema }}.DEATH
),
dthc AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN DEATH_CAUSE_CODE       IS NULL OR DEATH_CAUSE_CODE       IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS death_cause_code,
        SUM(CASE WHEN DEATH_CAUSE_CONFIDENCE IS NULL OR DEATH_CAUSE_CONFIDENCE IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS death_cause_confidence,
        SUM(CASE WHEN DEATH_CAUSE_SOURCE     IS NULL OR DEATH_CAUSE_SOURCE     IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS death_cause_source,
        SUM(CASE WHEN DEATH_CAUSE_TYPE       IS NULL OR DEATH_CAUSE_TYPE       IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS death_cause_type
    FROM {{ current_schema }}.DEATH_CAUSE
),
disp AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN DISPENSE_AMT           IS NULL                                         THEN 1 ELSE 0 END)                 AS dispense_amt,
        SUM(CASE WHEN DISPENSE_DOSE_DISP     IS NULL                                         THEN 1 ELSE 0 END)                 AS dispense_dose_disp,
        SUM(CASE WHEN DISPENSE_DOSE_DISP_UNIT IS NULL OR DISPENSE_DOSE_DISP_UNIT IN ('NI','UN','OT') THEN 1 ELSE 0 END)         AS dispense_dose_disp_unit,
        SUM(CASE WHEN DISPENSE_ROUTE         IS NULL OR DISPENSE_ROUTE         IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS dispense_route,
        SUM(CASE WHEN DISPENSE_SOURCE        IS NULL OR DISPENSE_SOURCE        IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS dispense_source,
        SUM(CASE WHEN DISPENSE_SUP           IS NULL                                         THEN 1 ELSE 0 END)                 AS dispense_sup,
        SUM(CASE WHEN PRESCRIBINGID          IS NULL                                         THEN 1 ELSE 0 END)                 AS prescribingid
    FROM {{ current_schema }}.DISPENSING
    WHERE DISPENSE_DATE >= TO_DATE('{{ start_date }}') AND DISPENSE_DATE <= TO_DATE('{{ end_date }}')
),
extmed AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN EXTMED_SOURCE          IS NULL OR EXTMED_SOURCE          IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS extmed_source,
        SUM(CASE WHEN EXT_BASIS              IS NULL OR EXT_BASIS              IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS ext_basis,
        SUM(CASE WHEN EXT_DOSE_FORM          IS NULL OR EXT_DOSE_FORM          IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS ext_dose_form,
        SUM(CASE WHEN EXT_DOSE_ORDERED_UNIT  IS NULL OR EXT_DOSE_ORDERED_UNIT  IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS ext_dose_ordered_unit,
        SUM(CASE WHEN EXT_RECORD_DATE        IS NULL                                         THEN 1 ELSE 0 END)                 AS ext_record_date,
        SUM(CASE WHEN EXT_ROUTE              IS NULL OR EXT_ROUTE              IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS ext_route,
        SUM(CASE WHEN RXNORM_CUI             IS NULL                                         THEN 1 ELSE 0 END)                 AS rxnorm_cui
    FROM {{ current_schema }}.EXTERNAL_MEDS
    WHERE EXT_RECORD_DATE >= TO_DATE('{{ start_date }}') AND EXT_RECORD_DATE <= TO_DATE('{{ end_date }}')
),
imm AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN ENCOUNTERID     IS NULL                                         THEN 1 ELSE 0 END)                       AS encounterid,
        SUM(CASE WHEN PROCEDURESID    IS NULL                                         THEN 1 ELSE 0 END)                       AS proceduresid,
        SUM(CASE WHEN VX_ADMIN_DATE   IS NULL                                         THEN 1 ELSE 0 END)                       AS vx_admin_date,
        SUM(CASE WHEN VX_BODY_SITE    IS NULL OR VX_BODY_SITE    IN ('NI','UN','OT')  THEN 1 ELSE 0 END)                       AS vx_body_site,
        SUM(CASE WHEN VX_CODE_TYPE    IS NULL OR VX_CODE_TYPE    IN ('NI','UN','OT')  THEN 1 ELSE 0 END)                       AS vx_code_type,
        SUM(CASE WHEN VX_DOSE         IS NULL                                         THEN 1 ELSE 0 END)                       AS vx_dose,
        SUM(CASE WHEN VX_DOSE_UNIT    IS NULL OR VX_DOSE_UNIT    IN ('NI','UN','OT')  THEN 1 ELSE 0 END)                       AS vx_dose_unit,
        SUM(CASE WHEN VX_EXP_DATE     IS NULL                                         THEN 1 ELSE 0 END)                       AS vx_exp_date,
        SUM(CASE WHEN VX_LOT_NUM      IS NULL                                         THEN 1 ELSE 0 END)                       AS vx_lot_num,
        SUM(CASE WHEN VX_MANUFACTURER IS NULL                                         THEN 1 ELSE 0 END)                       AS vx_manufacturer,
        SUM(CASE WHEN VX_PROVIDERID   IS NULL                                         THEN 1 ELSE 0 END)                       AS vx_providerid,
        SUM(CASE WHEN VX_RECORD_DATE  IS NULL                                         THEN 1 ELSE 0 END)                       AS vx_record_date,
        SUM(CASE WHEN VX_ROUTE        IS NULL OR VX_ROUTE        IN ('NI','UN','OT')  THEN 1 ELSE 0 END)                       AS vx_route,
        SUM(CASE WHEN VX_SOURCE       IS NULL OR VX_SOURCE       IN ('NI','UN','OT')  THEN 1 ELSE 0 END)                       AS vx_source,
        SUM(CASE WHEN VX_STATUS       IS NULL OR VX_STATUS       IN ('NI','UN','OT')  THEN 1 ELSE 0 END)                       AS vx_status,
        SUM(CASE WHEN VX_STATUS_REASON IS NULL OR VX_STATUS_REASON IN ('NI','UN','OT') THEN 1 ELSE 0 END)                      AS vx_status_reason
    FROM {{ current_schema }}.IMMUNIZATION
    WHERE VX_ADMIN_DATE >= TO_DATE('{{ start_date }}') AND VX_ADMIN_DATE <= TO_DATE('{{ end_date }}')
),
labhist AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN AGE_MAX_WKS          IS NULL                                         THEN 1 ELSE 0 END)                   AS age_max_wks,
        SUM(CASE WHEN AGE_MIN_WKS          IS NULL                                         THEN 1 ELSE 0 END)                   AS age_min_wks,
        SUM(CASE WHEN LAB_FACILITYID       IS NULL                                         THEN 1 ELSE 0 END)                   AS lab_facilityid,
        SUM(CASE WHEN NORM_MODIFIER_HIGH   IS NULL OR NORM_MODIFIER_HIGH   IN ('NI','UN','OT') THEN 1 ELSE 0 END)               AS norm_modifier_high,
        SUM(CASE WHEN NORM_MODIFIER_LOW    IS NULL OR NORM_MODIFIER_LOW    IN ('NI','UN','OT') THEN 1 ELSE 0 END)               AS norm_modifier_low,
        SUM(CASE WHEN PERIOD_END           IS NULL                                         THEN 1 ELSE 0 END)                   AS period_end,
        SUM(CASE WHEN PERIOD_START         IS NULL                                         THEN 1 ELSE 0 END)                   AS period_start,
        SUM(CASE WHEN RACE                 IS NULL OR RACE                 IN ('NI','UN','OT') THEN 1 ELSE 0 END)               AS race,
        SUM(CASE WHEN RESULT_UNIT          IS NULL                                         THEN 1 ELSE 0 END)                   AS result_unit,
        SUM(CASE WHEN SEX                  IS NULL OR SEX                  IN ('NI','UN','OT') THEN 1 ELSE 0 END)               AS sex
    FROM {{ current_schema }}.LAB_HISTORY
),
lab AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN ABN_IND            IS NULL OR ABN_IND            IN ('NI','UN','OT') THEN 1 ELSE 0 END)                   AS abn_ind,
        SUM(CASE WHEN ENCOUNTERID        IS NULL                                         THEN 1 ELSE 0 END)                     AS encounterid,
        SUM(CASE WHEN LAB_LOINC          IS NULL OR LAB_LOINC          IN ('NI','UN','OT') THEN 1 ELSE 0 END)                   AS lab_loinc,
        SUM(CASE WHEN LAB_LOINC_SOURCE   IS NULL OR LAB_LOINC_SOURCE   IN ('NI','UN','OT') THEN 1 ELSE 0 END)                   AS lab_loinc_source,
        SUM(CASE WHEN LAB_ORDER_DATE     IS NULL                                         THEN 1 ELSE 0 END)                     AS lab_order_date,
        SUM(CASE WHEN LAB_PX             IS NULL                                         THEN 1 ELSE 0 END)                     AS lab_px,
        SUM(CASE WHEN LAB_PX_TYPE        IS NULL OR LAB_PX_TYPE        IN ('NI','UN','OT') THEN 1 ELSE 0 END)                   AS lab_px_type,
        SUM(CASE WHEN LAB_RESULT_SOURCE  IS NULL OR LAB_RESULT_SOURCE  IN ('NI','UN','OT') THEN 1 ELSE 0 END)                   AS lab_result_source,
        SUM(CASE WHEN NORM_MODIFIER_HIGH IS NULL OR NORM_MODIFIER_HIGH IN ('NI','UN','OT') THEN 1 ELSE 0 END)                   AS norm_modifier_high,
        SUM(CASE WHEN NORM_MODIFIER_LOW  IS NULL OR NORM_MODIFIER_LOW  IN ('NI','UN','OT') THEN 1 ELSE 0 END)                   AS norm_modifier_low,
        SUM(CASE WHEN PRIORITY           IS NULL OR PRIORITY           IN ('NI','UN','OT') THEN 1 ELSE 0 END)                   AS priority,
        SUM(CASE WHEN RESULT_LOC         IS NULL OR RESULT_LOC         IN ('NI','UN','OT') THEN 1 ELSE 0 END)                   AS result_loc,
        SUM(CASE WHEN RESULT_MODIFIER    IS NULL OR RESULT_MODIFIER    IN ('NI','UN','OT') THEN 1 ELSE 0 END)                   AS result_modifier,
        SUM(CASE WHEN RESULT_NUM         IS NULL                                         THEN 1 ELSE 0 END)                     AS result_num,
        SUM(CASE WHEN RESULT_QUAL        IS NULL                                         THEN 1 ELSE 0 END)                     AS result_qual,
        SUM(CASE WHEN RESULT_SNOMED      IS NULL                                         THEN 1 ELSE 0 END)                     AS result_snomed,
        SUM(CASE WHEN RESULT_TIME        IS NULL                                         THEN 1 ELSE 0 END)                     AS result_time,
        SUM(CASE WHEN RESULT_UNIT        IS NULL                                         THEN 1 ELSE 0 END)                     AS result_unit,
        SUM(CASE WHEN SPECIMEN_DATE      IS NULL                                         THEN 1 ELSE 0 END)                     AS specimen_date,
        SUM(CASE WHEN SPECIMEN_SOURCE    IS NULL OR SPECIMEN_SOURCE    IN ('NI','UN','OT') THEN 1 ELSE 0 END)                   AS specimen_source,
        SUM(CASE WHEN SPECIMEN_TIME      IS NULL                                         THEN 1 ELSE 0 END)                     AS specimen_time
    FROM {{ current_schema }}.LAB_RESULT_CM
    WHERE RESULT_DATE >= TO_DATE('{{ start_date }}') AND RESULT_DATE <= TO_DATE('{{ end_date }}')
),
ldsadrs AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN ADDRESS_CITY         IS NULL                                         THEN 1 ELSE 0 END)                   AS address_city,
        SUM(CASE WHEN ADDRESS_COUNTY       IS NULL                                         THEN 1 ELSE 0 END)                   AS address_county,
        SUM(CASE WHEN ADDRESS_PERIOD_END   IS NULL                                         THEN 1 ELSE 0 END)                   AS address_period_end,
        SUM(CASE WHEN ADDRESS_PERIOD_START IS NULL                                         THEN 1 ELSE 0 END)                   AS address_period_start,
        SUM(CASE WHEN ADDRESS_STATE        IS NULL OR ADDRESS_STATE        IN ('NI','UN','OT') THEN 1 ELSE 0 END)               AS address_state,
        SUM(CASE WHEN ADDRESS_TYPE         IS NULL OR ADDRESS_TYPE         IN ('NI','UN','OT') THEN 1 ELSE 0 END)               AS address_type,
        SUM(CASE WHEN ADDRESS_USE          IS NULL OR ADDRESS_USE          IN ('NI','UN','OT') THEN 1 ELSE 0 END)               AS address_use,
        SUM(CASE WHEN ADDRESS_ZIP5         IS NULL                                         THEN 1 ELSE 0 END)                   AS address_zip5,
        SUM(CASE WHEN ADDRESS_ZIP9         IS NULL                                         THEN 1 ELSE 0 END)                   AS address_zip9,
        SUM(CASE WHEN COUNTY_FIPS          IS NULL                                         THEN 1 ELSE 0 END)                   AS county_fips,
        SUM(CASE WHEN CURRENT_ADDRESS_FLAG IS NULL OR CURRENT_ADDRESS_FLAG IN ('NI','UN','OT') THEN 1 ELSE 0 END)               AS current_address_flag,
        SUM(CASE WHEN RUCA_ZIP             IS NULL                                         THEN 1 ELSE 0 END)                   AS ruca_zip,
        SUM(CASE WHEN STATE_FIPS           IS NULL                                         THEN 1 ELSE 0 END)                   AS state_fips
    FROM {{ current_schema }}.LDS_ADDRESS_HISTORY
),
medadm AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN ENCOUNTERID           IS NULL                                         THEN 1 ELSE 0 END)                  AS encounterid,
        SUM(CASE WHEN MEDADMIN_CODE         IS NULL OR MEDADMIN_CODE         IN ('NI','UN','OT') THEN 1 ELSE 0 END)             AS medadmin_code,
        SUM(CASE WHEN MEDADMIN_DOSE_ADMIN   IS NULL                                         THEN 1 ELSE 0 END)                  AS medadmin_dose_admin,
        SUM(CASE WHEN MEDADMIN_DOSE_ADMIN_UNIT IS NULL OR MEDADMIN_DOSE_ADMIN_UNIT IN ('NI','UN','OT') THEN 1 ELSE 0 END)       AS medadmin_dose_admin_unit,
        SUM(CASE WHEN MEDADMIN_PROVIDERID   IS NULL                                         THEN 1 ELSE 0 END)                  AS medadmin_providerid,
        SUM(CASE WHEN MEDADMIN_ROUTE        IS NULL OR MEDADMIN_ROUTE        IN ('NI','UN','OT') THEN 1 ELSE 0 END)             AS medadmin_route,
        SUM(CASE WHEN MEDADMIN_SOURCE       IS NULL OR MEDADMIN_SOURCE       IN ('NI','UN','OT') THEN 1 ELSE 0 END)             AS medadmin_source,
        SUM(CASE WHEN MEDADMIN_START_TIME   IS NULL                                         THEN 1 ELSE 0 END)                  AS medadmin_start_time,
        SUM(CASE WHEN MEDADMIN_STOP_DATE    IS NULL                                         THEN 1 ELSE 0 END)                  AS medadmin_stop_date,
        SUM(CASE WHEN MEDADMIN_STOP_TIME    IS NULL                                         THEN 1 ELSE 0 END)                  AS medadmin_stop_time,
        SUM(CASE WHEN MEDADMIN_TYPE         IS NULL OR MEDADMIN_TYPE         IN ('NI','UN','OT') THEN 1 ELSE 0 END)             AS medadmin_type,
        SUM(CASE WHEN PRESCRIBINGID         IS NULL                                         THEN 1 ELSE 0 END)                  AS prescribingid
    FROM {{ current_schema }}.MED_ADMIN
    WHERE MEDADMIN_START_DATE >= TO_DATE('{{ start_date }}') AND MEDADMIN_START_DATE <= TO_DATE('{{ end_date }}')
),
obsclin AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN ENCOUNTERID            IS NULL                                         THEN 1 ELSE 0 END)                 AS encounterid,
        SUM(CASE WHEN OBSCLIN_ABN_IND        IS NULL OR OBSCLIN_ABN_IND        IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS obsclin_abn_ind,
        SUM(CASE WHEN OBSCLIN_CODE           IS NULL OR OBSCLIN_CODE           IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS obsclin_code,
        SUM(CASE WHEN OBSCLIN_PROVIDERID     IS NULL                                         THEN 1 ELSE 0 END)                 AS obsclin_providerid,
        SUM(CASE WHEN OBSCLIN_RESULT_MODIFIER IS NULL OR OBSCLIN_RESULT_MODIFIER IN ('NI','UN','OT') THEN 1 ELSE 0 END)         AS obsclin_result_modifier,
        SUM(CASE WHEN OBSCLIN_RESULT_QUAL    IS NULL                                         THEN 1 ELSE 0 END)                 AS obsclin_result_qual,
        SUM(CASE WHEN OBSCLIN_RESULT_UNIT    IS NULL                                         THEN 1 ELSE 0 END)                 AS obsclin_result_unit,
        SUM(CASE WHEN OBSCLIN_SOURCE         IS NULL OR OBSCLIN_SOURCE         IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS obsclin_source,
        SUM(CASE WHEN OBSCLIN_START_TIME     IS NULL                                         THEN 1 ELSE 0 END)                 AS obsclin_start_time,
        SUM(CASE WHEN OBSCLIN_STOP_DATE      IS NULL                                         THEN 1 ELSE 0 END)                 AS obsclin_stop_date,
        SUM(CASE WHEN OBSCLIN_STOP_TIME      IS NULL                                         THEN 1 ELSE 0 END)                 AS obsclin_stop_time,
        SUM(CASE WHEN OBSCLIN_TYPE           IS NULL OR OBSCLIN_TYPE           IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS obsclin_type
    FROM {{ current_schema }}.OBS_CLIN
    WHERE OBSCLIN_START_DATE >= TO_DATE('{{ start_date }}') AND OBSCLIN_START_DATE <= TO_DATE('{{ end_date }}')
),
obsgen AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN ENCOUNTERID           IS NULL                                         THEN 1 ELSE 0 END)                  AS encounterid,
        SUM(CASE WHEN OBSGEN_ABN_IND        IS NULL OR OBSGEN_ABN_IND        IN ('NI','UN','OT') THEN 1 ELSE 0 END)             AS obsgen_abn_ind,
        SUM(CASE WHEN OBSGEN_CODE           IS NULL OR OBSGEN_CODE           IN ('NI','UN','OT') THEN 1 ELSE 0 END)             AS obsgen_code,
        SUM(CASE WHEN OBSGEN_PROVIDERID     IS NULL                                         THEN 1 ELSE 0 END)                  AS obsgen_providerid,
        SUM(CASE WHEN OBSGEN_RESULT_MODIFIER IS NULL OR OBSGEN_RESULT_MODIFIER IN ('NI','UN','OT') THEN 1 ELSE 0 END)           AS obsgen_result_modifier,
        SUM(CASE WHEN OBSGEN_RESULT_QUAL    IS NULL                                         THEN 1 ELSE 0 END)                  AS obsgen_result_qual,
        SUM(CASE WHEN OBSGEN_RESULT_UNIT    IS NULL                                         THEN 1 ELSE 0 END)                  AS obsgen_result_unit,
        SUM(CASE WHEN OBSGEN_SOURCE         IS NULL OR OBSGEN_SOURCE         IN ('NI','UN','OT') THEN 1 ELSE 0 END)             AS obsgen_source,
        SUM(CASE WHEN OBSGEN_START_TIME     IS NULL                                         THEN 1 ELSE 0 END)                  AS obsgen_start_time,
        SUM(CASE WHEN OBSGEN_STOP_DATE      IS NULL                                         THEN 1 ELSE 0 END)                  AS obsgen_stop_date,
        SUM(CASE WHEN OBSGEN_STOP_TIME      IS NULL                                         THEN 1 ELSE 0 END)                  AS obsgen_stop_time,
        SUM(CASE WHEN OBSGEN_TYPE           IS NULL OR OBSGEN_TYPE           IN ('NI','UN','OT') THEN 1 ELSE 0 END)             AS obsgen_type
    FROM {{ current_schema }}.OBS_GEN
    WHERE OBSGEN_START_DATE >= TO_DATE('{{ start_date }}') AND OBSGEN_START_DATE <= TO_DATE('{{ end_date }}')
),
patrel AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN RELATIONSHIP_END   IS NULL THEN 1 ELSE 0 END)                                                             AS relationship_end,
        SUM(CASE WHEN RELATIONSHIP_START IS NULL THEN 1 ELSE 0 END)                                                             AS relationship_start
    FROM {{ current_schema }}.PAT_RELATIONSHIP
),
pres AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN ENCOUNTERID             IS NULL                                         THEN 1 ELSE 0 END)                AS encounterid,
        SUM(CASE WHEN RXNORM_CUI              IS NULL                                         THEN 1 ELSE 0 END)                AS rxnorm_cui,
        SUM(CASE WHEN RX_BASIS               IS NULL OR RX_BASIS               IN ('NI','UN','OT') THEN 1 ELSE 0 END)          AS rx_basis,
        SUM(CASE WHEN RX_DAYS_SUPPLY         IS NULL                                         THEN 1 ELSE 0 END)                AS rx_days_supply,
        SUM(CASE WHEN RX_DISPENSE_AS_WRITTEN IS NULL OR RX_DISPENSE_AS_WRITTEN IN ('NI','UN','OT') THEN 1 ELSE 0 END)          AS rx_dispense_as_written,
        SUM(CASE WHEN RX_DOSE_FORM           IS NULL OR RX_DOSE_FORM           IN ('NI','UN','OT') THEN 1 ELSE 0 END)          AS rx_dose_form,
        SUM(CASE WHEN RX_DOSE_ORDERED        IS NULL                                         THEN 1 ELSE 0 END)                AS rx_dose_ordered,
        SUM(CASE WHEN RX_DOSE_ORDERED_UNIT   IS NULL OR RX_DOSE_ORDERED_UNIT   IN ('NI','UN','OT') THEN 1 ELSE 0 END)          AS rx_dose_ordered_unit,
        SUM(CASE WHEN RX_END_DATE            IS NULL                                         THEN 1 ELSE 0 END)                AS rx_end_date,
        SUM(CASE WHEN RX_FREQUENCY           IS NULL OR RX_FREQUENCY           IN ('NI','UN','OT') THEN 1 ELSE 0 END)          AS rx_frequency,
        SUM(CASE WHEN RX_ORDER_DATE          IS NULL                                         THEN 1 ELSE 0 END)                AS rx_order_date,
        SUM(CASE WHEN RX_ORDER_TIME          IS NULL                                         THEN 1 ELSE 0 END)                AS rx_order_time,
        SUM(CASE WHEN RX_PRN_FLAG            IS NULL OR RX_PRN_FLAG            IN ('NI','UN','OT') THEN 1 ELSE 0 END)          AS rx_prn_flag,
        SUM(CASE WHEN RX_PROVIDERID          IS NULL                                         THEN 1 ELSE 0 END)                AS rx_providerid,
        SUM(CASE WHEN RX_QUANTITY            IS NULL                                         THEN 1 ELSE 0 END)                AS rx_quantity,
        SUM(CASE WHEN RX_REFILLS             IS NULL                                         THEN 1 ELSE 0 END)                AS rx_refills,
        SUM(CASE WHEN RX_ROUTE               IS NULL OR RX_ROUTE               IN ('NI','UN','OT') THEN 1 ELSE 0 END)          AS rx_route,
        SUM(CASE WHEN RX_SOURCE              IS NULL OR RX_SOURCE              IN ('NI','UN','OT') THEN 1 ELSE 0 END)          AS rx_source,
        SUM(CASE WHEN RX_START_DATE          IS NULL                                         THEN 1 ELSE 0 END)                AS rx_start_date
    FROM {{ current_schema }}.PRESCRIBING
    WHERE RX_ORDER_DATE >= TO_DATE('{{ start_date }}') AND RX_ORDER_DATE <= TO_DATE('{{ end_date }}')
),
procm AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN ENCOUNTERID  IS NULL                                         THEN 1 ELSE 0 END)                           AS encounterid,
        SUM(CASE WHEN PRO_CAT      IS NULL OR PRO_CAT      IN ('NI','UN','OT')     THEN 1 ELSE 0 END)                           AS pro_cat,
        SUM(CASE WHEN PRO_CODE     IS NULL                                         THEN 1 ELSE 0 END)                           AS pro_code,
        SUM(CASE WHEN PRO_FULLNAME IS NULL                                         THEN 1 ELSE 0 END)                           AS pro_fullname,
        SUM(CASE WHEN PRO_METHOD   IS NULL OR PRO_METHOD   IN ('NI','UN','OT')     THEN 1 ELSE 0 END)                           AS pro_method,
        SUM(CASE WHEN PRO_MODE     IS NULL OR PRO_MODE     IN ('NI','UN','OT')     THEN 1 ELSE 0 END)                           AS pro_mode,
        SUM(CASE WHEN PRO_NAME     IS NULL                                         THEN 1 ELSE 0 END)                           AS pro_name,
        SUM(CASE WHEN PRO_SOURCE   IS NULL OR PRO_SOURCE   IN ('NI','UN','OT')     THEN 1 ELSE 0 END)                           AS pro_source,
        SUM(CASE WHEN PRO_TIME     IS NULL                                         THEN 1 ELSE 0 END)                           AS pro_time,
        SUM(CASE WHEN PRO_TYPE     IS NULL OR PRO_TYPE     IN ('NI','UN','OT')     THEN 1 ELSE 0 END)                           AS pro_type
    FROM {{ current_schema }}.PRO_CM
    WHERE PRO_DATE >= TO_DATE('{{ start_date }}') AND PRO_DATE <= TO_DATE('{{ end_date }}')
),
prov AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN PROVIDER_NPI                IS NULL                                         THEN 1 ELSE 0 END)            AS provider_npi,
        SUM(CASE WHEN PROVIDER_NPI_FLAG           IS NULL OR PROVIDER_NPI_FLAG           IN ('NI','UN','OT') THEN 1 ELSE 0 END) AS provider_npi_flag,
        SUM(CASE WHEN PROVIDER_SEX                IS NULL OR PROVIDER_SEX                IN ('NI','UN','OT') THEN 1 ELSE 0 END) AS provider_sex,
        SUM(CASE WHEN PROVIDER_SPECIALTY_PRIMARY  IS NULL OR PROVIDER_SPECIALTY_PRIMARY  IN ('NI','UN','OT') THEN 1 ELSE 0 END) AS provider_specialty_primary
    FROM {{ current_schema }}.PROVIDER
),
vit AS (
    SELECT
        COUNT(*)                                                                                                                 AS total,
        SUM(CASE WHEN ENCOUNTERID  IS NULL                                         THEN 1 ELSE 0 END)                           AS encounterid,
        SUM(CASE WHEN MEASURE_TIME IS NULL                                         THEN 1 ELSE 0 END)                           AS measure_time,
        SUM(CASE WHEN VITAL_SOURCE IS NULL OR VITAL_SOURCE IN ('NI','UN','OT')     THEN 1 ELSE 0 END)                           AS vital_source
    FROM {{ current_schema }}.VITAL
    WHERE MEASURE_DATE >= TO_DATE('{{ start_date }}') AND MEASURE_DATE <= TO_DATE('{{ end_date }}')
)
SELECT TABLE_NAME, FIELD_NAME,
       TO_VARCHAR(NUMERATOR)   AS NUMERATOR,
       TO_VARCHAR(DENOMINATOR) AS DENOMINATOR,
       CASE WHEN NUMERATOR = 0 OR DENOMINATOR IS NULL OR DENOMINATOR = 0 THEN NULL
            ELSE TO_VARCHAR(ROUND(100.0 * NUMERATOR / DENOMINATOR, 1)) || '%'
       END AS PCT
FROM (
    -- CONDITION (rows 1–7)
    SELECT 'CONDITION' AS TABLE_NAME, 'CONDITION_SOURCE' AS FIELD_NAME, condition_source AS NUMERATOR, total AS DENOMINATOR,  1 AS ROW_ORDER FROM cond
    UNION ALL SELECT 'CONDITION', 'CONDITION_STATUS',  condition_status,  total,  2 FROM cond
    UNION ALL SELECT 'CONDITION', 'CONDITION_TYPE',    condition_type,    total,  3 FROM cond
    UNION ALL SELECT 'CONDITION', 'ENCOUNTERID',       encounterid,       total,  4 FROM cond
    UNION ALL SELECT 'CONDITION', 'ONSET_DATE',        onset_date,        total,  5 FROM cond
    UNION ALL SELECT 'CONDITION', 'REPORT_DATE',       report_date,       total,  6 FROM cond
    UNION ALL SELECT 'CONDITION', 'RESOLVE_DATE',      resolve_date,      total,  7 FROM cond
    -- DEATH (rows 8–11)
    UNION ALL SELECT 'DEATH', 'DEATH_DATE',             death_date,             total,  8 FROM dth
    UNION ALL SELECT 'DEATH', 'DEATH_DATE_IMPUTE',      death_date_impute,      total,  9 FROM dth
    UNION ALL SELECT 'DEATH', 'DEATH_MATCH_CONFIDENCE', death_match_confidence, total, 10 FROM dth
    UNION ALL SELECT 'DEATH', 'DEATH_SOURCE',           death_source,           total, 11 FROM dth
    -- DEATH_CAUSE (rows 12–15)
    UNION ALL SELECT 'DEATH_CAUSE', 'DEATH_CAUSE_CODE',       death_cause_code,       total, 12 FROM dthc
    UNION ALL SELECT 'DEATH_CAUSE', 'DEATH_CAUSE_CONFIDENCE', death_cause_confidence, total, 13 FROM dthc
    UNION ALL SELECT 'DEATH_CAUSE', 'DEATH_CAUSE_SOURCE',     death_cause_source,     total, 14 FROM dthc
    UNION ALL SELECT 'DEATH_CAUSE', 'DEATH_CAUSE_TYPE',       death_cause_type,       total, 15 FROM dthc
    -- DISPENSING (rows 16–22)
    UNION ALL SELECT 'DISPENSING', 'DISPENSE_AMT',            dispense_amt,            total, 16 FROM disp
    UNION ALL SELECT 'DISPENSING', 'DISPENSE_DOSE_DISP',      dispense_dose_disp,      total, 17 FROM disp
    UNION ALL SELECT 'DISPENSING', 'DISPENSE_DOSE_DISP_UNIT', dispense_dose_disp_unit, total, 18 FROM disp
    UNION ALL SELECT 'DISPENSING', 'DISPENSE_ROUTE',          dispense_route,          total, 19 FROM disp
    UNION ALL SELECT 'DISPENSING', 'DISPENSE_SOURCE',         dispense_source,         total, 20 FROM disp
    UNION ALL SELECT 'DISPENSING', 'DISPENSE_SUP',            dispense_sup,            total, 21 FROM disp
    UNION ALL SELECT 'DISPENSING', 'PRESCRIBINGID',           prescribingid,           total, 22 FROM disp
    -- EXTERNAL_MEDS (rows 23–29)
    UNION ALL SELECT 'EXTERNAL_MEDS', 'EXTMED_SOURCE',         extmed_source,         total, 23 FROM extmed
    UNION ALL SELECT 'EXTERNAL_MEDS', 'EXT_BASIS',             ext_basis,             total, 24 FROM extmed
    UNION ALL SELECT 'EXTERNAL_MEDS', 'EXT_DOSE_FORM',         ext_dose_form,         total, 25 FROM extmed
    UNION ALL SELECT 'EXTERNAL_MEDS', 'EXT_DOSE_ORDERED_UNIT', ext_dose_ordered_unit, total, 26 FROM extmed
    UNION ALL SELECT 'EXTERNAL_MEDS', 'EXT_RECORD_DATE',       ext_record_date,       total, 27 FROM extmed
    UNION ALL SELECT 'EXTERNAL_MEDS', 'EXT_ROUTE',             ext_route,             total, 28 FROM extmed
    UNION ALL SELECT 'EXTERNAL_MEDS', 'RXNORM_CUI',            rxnorm_cui,            total, 29 FROM extmed
    -- IMMUNIZATION (rows 30–45)
    UNION ALL SELECT 'IMMUNIZATION', 'ENCOUNTERID',      encounterid,      total, 30 FROM imm
    UNION ALL SELECT 'IMMUNIZATION', 'PROCEDURESID',     proceduresid,     total, 31 FROM imm
    UNION ALL SELECT 'IMMUNIZATION', 'VX_ADMIN_DATE',    vx_admin_date,    total, 32 FROM imm
    UNION ALL SELECT 'IMMUNIZATION', 'VX_BODY_SITE',     vx_body_site,     total, 33 FROM imm
    UNION ALL SELECT 'IMMUNIZATION', 'VX_CODE_TYPE',     vx_code_type,     total, 34 FROM imm
    UNION ALL SELECT 'IMMUNIZATION', 'VX_DOSE',          vx_dose,          total, 35 FROM imm
    UNION ALL SELECT 'IMMUNIZATION', 'VX_DOSE_UNIT',     vx_dose_unit,     total, 36 FROM imm
    UNION ALL SELECT 'IMMUNIZATION', 'VX_EXP_DATE',      vx_exp_date,      total, 37 FROM imm
    UNION ALL SELECT 'IMMUNIZATION', 'VX_LOT_NUM',       vx_lot_num,       total, 38 FROM imm
    UNION ALL SELECT 'IMMUNIZATION', 'VX_MANUFACTURER',  vx_manufacturer,  total, 39 FROM imm
    UNION ALL SELECT 'IMMUNIZATION', 'VX_PROVIDERID',    vx_providerid,    total, 40 FROM imm
    UNION ALL SELECT 'IMMUNIZATION', 'VX_RECORD_DATE',   vx_record_date,   total, 41 FROM imm
    UNION ALL SELECT 'IMMUNIZATION', 'VX_ROUTE',         vx_route,         total, 42 FROM imm
    UNION ALL SELECT 'IMMUNIZATION', 'VX_SOURCE',        vx_source,        total, 43 FROM imm
    UNION ALL SELECT 'IMMUNIZATION', 'VX_STATUS',        vx_status,        total, 44 FROM imm
    UNION ALL SELECT 'IMMUNIZATION', 'VX_STATUS_REASON', vx_status_reason, total, 45 FROM imm
    -- LAB_HISTORY (rows 46–55)
    UNION ALL SELECT 'LAB_HISTORY', 'AGE_MAX_WKS',        age_max_wks,        total, 46 FROM labhist
    UNION ALL SELECT 'LAB_HISTORY', 'AGE_MIN_WKS',        age_min_wks,        total, 47 FROM labhist
    UNION ALL SELECT 'LAB_HISTORY', 'LAB_FACILITYID',     lab_facilityid,     total, 48 FROM labhist
    UNION ALL SELECT 'LAB_HISTORY', 'NORM_MODIFIER_HIGH', norm_modifier_high, total, 49 FROM labhist
    UNION ALL SELECT 'LAB_HISTORY', 'NORM_MODIFIER_LOW',  norm_modifier_low,  total, 50 FROM labhist
    UNION ALL SELECT 'LAB_HISTORY', 'PERIOD_END',         period_end,         total, 51 FROM labhist
    UNION ALL SELECT 'LAB_HISTORY', 'PERIOD_START',       period_start,       total, 52 FROM labhist
    UNION ALL SELECT 'LAB_HISTORY', 'RACE',               race,               total, 53 FROM labhist
    UNION ALL SELECT 'LAB_HISTORY', 'RESULT_UNIT',        result_unit,        total, 54 FROM labhist
    UNION ALL SELECT 'LAB_HISTORY', 'SEX',                sex,                total, 55 FROM labhist
    -- LAB_RESULT_CM (rows 56–76)
    UNION ALL SELECT 'LAB_RESULT_CM', 'ABN_IND',            abn_ind,            total, 56 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'ENCOUNTERID',        encounterid,        total, 57 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'LAB_LOINC',          lab_loinc,          total, 58 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'LAB_LOINC_SOURCE',   lab_loinc_source,   total, 59 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'LAB_ORDER_DATE',     lab_order_date,     total, 60 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'LAB_PX',             lab_px,             total, 61 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'LAB_PX_TYPE',        lab_px_type,        total, 62 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'LAB_RESULT_SOURCE',  lab_result_source,  total, 63 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'NORM_MODIFIER_HIGH', norm_modifier_high, total, 64 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'NORM_MODIFIER_LOW',  norm_modifier_low,  total, 65 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'PRIORITY',           priority,           total, 66 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'RESULT_LOC',         result_loc,         total, 67 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'RESULT_MODIFIER',    result_modifier,    total, 68 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'RESULT_NUM',         result_num,         total, 69 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'RESULT_QUAL',        result_qual,        total, 70 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'RESULT_SNOMED',      result_snomed,      total, 71 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'RESULT_TIME',        result_time,        total, 72 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'RESULT_UNIT',        result_unit,        total, 73 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'SPECIMEN_DATE',      specimen_date,      total, 74 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'SPECIMEN_SOURCE',    specimen_source,    total, 75 FROM lab
    UNION ALL SELECT 'LAB_RESULT_CM', 'SPECIMEN_TIME',      specimen_time,      total, 76 FROM lab
    -- LDS_ADDRESS_HISTORY (rows 77–89)
    UNION ALL SELECT 'LDS_ADDRESS_HISTORY', 'ADDRESS_CITY',         address_city,         total, 77 FROM ldsadrs
    UNION ALL SELECT 'LDS_ADDRESS_HISTORY', 'ADDRESS_COUNTY',       address_county,       total, 78 FROM ldsadrs
    UNION ALL SELECT 'LDS_ADDRESS_HISTORY', 'ADDRESS_PERIOD_END',   address_period_end,   total, 79 FROM ldsadrs
    UNION ALL SELECT 'LDS_ADDRESS_HISTORY', 'ADDRESS_PERIOD_START', address_period_start, total, 80 FROM ldsadrs
    UNION ALL SELECT 'LDS_ADDRESS_HISTORY', 'ADDRESS_STATE',        address_state,        total, 81 FROM ldsadrs
    UNION ALL SELECT 'LDS_ADDRESS_HISTORY', 'ADDRESS_TYPE',         address_type,         total, 82 FROM ldsadrs
    UNION ALL SELECT 'LDS_ADDRESS_HISTORY', 'ADDRESS_USE',          address_use,          total, 83 FROM ldsadrs
    UNION ALL SELECT 'LDS_ADDRESS_HISTORY', 'ADDRESS_ZIP5',         address_zip5,         total, 84 FROM ldsadrs
    UNION ALL SELECT 'LDS_ADDRESS_HISTORY', 'ADDRESS_ZIP9',         address_zip9,         total, 85 FROM ldsadrs
    UNION ALL SELECT 'LDS_ADDRESS_HISTORY', 'COUNTY_FIPS',          county_fips,          total, 86 FROM ldsadrs
    UNION ALL SELECT 'LDS_ADDRESS_HISTORY', 'CURRENT_ADDRESS_FLAG', current_address_flag, total, 87 FROM ldsadrs
    UNION ALL SELECT 'LDS_ADDRESS_HISTORY', 'RUCA_ZIP',             ruca_zip,             total, 88 FROM ldsadrs
    UNION ALL SELECT 'LDS_ADDRESS_HISTORY', 'STATE_FIPS',           state_fips,           total, 89 FROM ldsadrs
    -- MED_ADMIN (rows 90–101)
    UNION ALL SELECT 'MED_ADMIN', 'ENCOUNTERID',            encounterid,            total,  90 FROM medadm
    UNION ALL SELECT 'MED_ADMIN', 'MEDADMIN_CODE',          medadmin_code,          total,  91 FROM medadm
    UNION ALL SELECT 'MED_ADMIN', 'MEDADMIN_DOSE_ADMIN',    medadmin_dose_admin,    total,  92 FROM medadm
    UNION ALL SELECT 'MED_ADMIN', 'MEDADMIN_DOSE_ADMIN_UNIT', medadmin_dose_admin_unit, total, 93 FROM medadm
    UNION ALL SELECT 'MED_ADMIN', 'MEDADMIN_PROVIDERID',    medadmin_providerid,    total,  94 FROM medadm
    UNION ALL SELECT 'MED_ADMIN', 'MEDADMIN_ROUTE',         medadmin_route,         total,  95 FROM medadm
    UNION ALL SELECT 'MED_ADMIN', 'MEDADMIN_SOURCE',        medadmin_source,        total,  96 FROM medadm
    UNION ALL SELECT 'MED_ADMIN', 'MEDADMIN_START_TIME',    medadmin_start_time,    total,  97 FROM medadm
    UNION ALL SELECT 'MED_ADMIN', 'MEDADMIN_STOP_DATE',     medadmin_stop_date,     total,  98 FROM medadm
    UNION ALL SELECT 'MED_ADMIN', 'MEDADMIN_STOP_TIME',     medadmin_stop_time,     total,  99 FROM medadm
    UNION ALL SELECT 'MED_ADMIN', 'MEDADMIN_TYPE',          medadmin_type,          total, 100 FROM medadm
    UNION ALL SELECT 'MED_ADMIN', 'PRESCRIBINGID',          prescribingid,          total, 101 FROM medadm
    -- OBS_CLIN (rows 102–113)
    UNION ALL SELECT 'OBS_CLIN', 'ENCOUNTERID',             encounterid,             total, 102 FROM obsclin
    UNION ALL SELECT 'OBS_CLIN', 'OBSCLIN_ABN_IND',         obsclin_abn_ind,         total, 103 FROM obsclin
    UNION ALL SELECT 'OBS_CLIN', 'OBSCLIN_CODE',            obsclin_code,            total, 104 FROM obsclin
    UNION ALL SELECT 'OBS_CLIN', 'OBSCLIN_PROVIDERID',      obsclin_providerid,      total, 105 FROM obsclin
    UNION ALL SELECT 'OBS_CLIN', 'OBSCLIN_RESULT_MODIFIER', obsclin_result_modifier, total, 106 FROM obsclin
    UNION ALL SELECT 'OBS_CLIN', 'OBSCLIN_RESULT_QUAL',     obsclin_result_qual,     total, 107 FROM obsclin
    UNION ALL SELECT 'OBS_CLIN', 'OBSCLIN_RESULT_UNIT',     obsclin_result_unit,     total, 108 FROM obsclin
    UNION ALL SELECT 'OBS_CLIN', 'OBSCLIN_SOURCE',          obsclin_source,          total, 109 FROM obsclin
    UNION ALL SELECT 'OBS_CLIN', 'OBSCLIN_START_TIME',      obsclin_start_time,      total, 110 FROM obsclin
    UNION ALL SELECT 'OBS_CLIN', 'OBSCLIN_STOP_DATE',       obsclin_stop_date,       total, 111 FROM obsclin
    UNION ALL SELECT 'OBS_CLIN', 'OBSCLIN_STOP_TIME',       obsclin_stop_time,       total, 112 FROM obsclin
    UNION ALL SELECT 'OBS_CLIN', 'OBSCLIN_TYPE',            obsclin_type,            total, 113 FROM obsclin
    -- OBS_GEN (rows 114–125)
    UNION ALL SELECT 'OBS_GEN', 'ENCOUNTERID',            encounterid,            total, 114 FROM obsgen
    UNION ALL SELECT 'OBS_GEN', 'OBSGEN_ABN_IND',         obsgen_abn_ind,         total, 115 FROM obsgen
    UNION ALL SELECT 'OBS_GEN', 'OBSGEN_CODE',            obsgen_code,            total, 116 FROM obsgen
    UNION ALL SELECT 'OBS_GEN', 'OBSGEN_PROVIDERID',      obsgen_providerid,      total, 117 FROM obsgen
    UNION ALL SELECT 'OBS_GEN', 'OBSGEN_RESULT_MODIFIER', obsgen_result_modifier, total, 118 FROM obsgen
    UNION ALL SELECT 'OBS_GEN', 'OBSGEN_RESULT_QUAL',     obsgen_result_qual,     total, 119 FROM obsgen
    UNION ALL SELECT 'OBS_GEN', 'OBSGEN_RESULT_UNIT',     obsgen_result_unit,     total, 120 FROM obsgen
    UNION ALL SELECT 'OBS_GEN', 'OBSGEN_SOURCE',          obsgen_source,          total, 121 FROM obsgen
    UNION ALL SELECT 'OBS_GEN', 'OBSGEN_START_TIME',      obsgen_start_time,      total, 122 FROM obsgen
    UNION ALL SELECT 'OBS_GEN', 'OBSGEN_STOP_DATE',       obsgen_stop_date,       total, 123 FROM obsgen
    UNION ALL SELECT 'OBS_GEN', 'OBSGEN_STOP_TIME',       obsgen_stop_time,       total, 124 FROM obsgen
    UNION ALL SELECT 'OBS_GEN', 'OBSGEN_TYPE',            obsgen_type,            total, 125 FROM obsgen
    -- PAT_RELATIONSHIP (rows 126–127)
    UNION ALL SELECT 'PAT_RELATIONSHIP', 'RELATIONSHIP_END',   relationship_end,   total, 126 FROM patrel
    UNION ALL SELECT 'PAT_RELATIONSHIP', 'RELATIONSHIP_START', relationship_start, total, 127 FROM patrel
    -- PRESCRIBING (rows 128–146)
    UNION ALL SELECT 'PRESCRIBING', 'ENCOUNTERID',             encounterid,             total, 128 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RXNORM_CUI',              rxnorm_cui,              total, 129 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_BASIS',                rx_basis,                total, 130 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_DAYS_SUPPLY',          rx_days_supply,          total, 131 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_DISPENSE_AS_WRITTEN',  rx_dispense_as_written,  total, 132 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_DOSE_FORM',            rx_dose_form,            total, 133 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_DOSE_ORDERED',         rx_dose_ordered,         total, 134 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_DOSE_ORDERED_UNIT',    rx_dose_ordered_unit,    total, 135 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_END_DATE',             rx_end_date,             total, 136 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_FREQUENCY',            rx_frequency,            total, 137 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_ORDER_DATE',           rx_order_date,           total, 138 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_ORDER_TIME',           rx_order_time,           total, 139 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_PRN_FLAG',             rx_prn_flag,             total, 140 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_PROVIDERID',           rx_providerid,           total, 141 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_QUANTITY',             rx_quantity,             total, 142 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_REFILLS',              rx_refills,              total, 143 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_ROUTE',                rx_route,                total, 144 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_SOURCE',               rx_source,               total, 145 FROM pres
    UNION ALL SELECT 'PRESCRIBING', 'RX_START_DATE',           rx_start_date,           total, 146 FROM pres
    -- PRO_CM (rows 147–156)
    UNION ALL SELECT 'PRO_CM', 'ENCOUNTERID',  encounterid,  total, 147 FROM procm
    UNION ALL SELECT 'PRO_CM', 'PRO_CAT',      pro_cat,      total, 148 FROM procm
    UNION ALL SELECT 'PRO_CM', 'PRO_CODE',     pro_code,     total, 149 FROM procm
    UNION ALL SELECT 'PRO_CM', 'PRO_FULLNAME', pro_fullname, total, 150 FROM procm
    UNION ALL SELECT 'PRO_CM', 'PRO_METHOD',   pro_method,   total, 151 FROM procm
    UNION ALL SELECT 'PRO_CM', 'PRO_MODE',     pro_mode,     total, 152 FROM procm
    UNION ALL SELECT 'PRO_CM', 'PRO_NAME',     pro_name,     total, 153 FROM procm
    UNION ALL SELECT 'PRO_CM', 'PRO_SOURCE',   pro_source,   total, 154 FROM procm
    UNION ALL SELECT 'PRO_CM', 'PRO_TIME',     pro_time,     total, 155 FROM procm
    UNION ALL SELECT 'PRO_CM', 'PRO_TYPE',     pro_type,     total, 156 FROM procm
    -- PROVIDER (rows 157–160)
    UNION ALL SELECT 'PROVIDER', 'PROVIDER_NPI',               provider_npi,               total, 157 FROM prov
    UNION ALL SELECT 'PROVIDER', 'PROVIDER_NPI_FLAG',          provider_npi_flag,          total, 158 FROM prov
    UNION ALL SELECT 'PROVIDER', 'PROVIDER_SEX',               provider_sex,               total, 159 FROM prov
    UNION ALL SELECT 'PROVIDER', 'PROVIDER_SPECIALTY_PRIMARY', provider_specialty_primary, total, 160 FROM prov
    -- VITAL (rows 161–163)
    UNION ALL SELECT 'VITAL', 'ENCOUNTERID',  encounterid,  total, 161 FROM vit
    UNION ALL SELECT 'VITAL', 'MEASURE_TIME', measure_time, total, 162 FROM vit
    UNION ALL SELECT 'VITAL', 'VITAL_SOURCE', vital_source, total, 163 FROM vit
) sub
ORDER BY ROW_ORDER
