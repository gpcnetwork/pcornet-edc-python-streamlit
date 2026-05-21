-- Table IIA. Primary Key Errors
-- Required primary key definitions for all 25 CDM tables. Supports DC 1.05.
-- Exceptions (EXCEPTION_FLAG = Yes) are highlighted in red and must be corrected.

SELECT TABLE_NAME, CDM_PK_SPEC, EXCEPTION_FLAG, SOURCE_TABLE
FROM (
    SELECT 'DEMOGRAPHIC' AS TABLE_NAME,
           'PATID is unique' AS CDM_PK_SPEC,
           CASE WHEN (SELECT COUNT(DISTINCT PATID) FROM {{ current_schema }}.DEMOGRAPHIC) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.DEMOGRAPHIC)
                THEN 'No' ELSE 'Yes' END AS EXCEPTION_FLAG,
           'DEM_L3_N' AS SOURCE_TABLE, 1 AS ROW_ORDER
    UNION ALL
    SELECT 'ENROLLMENT',
           'PATID + ENR_START_DATE + ENR_BASIS are unique',
           CASE WHEN (SELECT COUNT(DISTINCT PATID || TO_VARCHAR(ENR_START_DATE) || ENR_BASIS) FROM {{ current_schema }}.ENROLLMENT) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.ENROLLMENT)
                THEN 'No' ELSE 'Yes' END,
           'ENR_L3_N', 2
    UNION ALL
    SELECT 'ENCOUNTER',
           'ENCOUNTERID is unique',
           CASE WHEN (SELECT COUNT(DISTINCT ENCOUNTERID) FROM {{ current_schema }}.ENCOUNTER) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.ENCOUNTER)
                THEN 'No' ELSE 'Yes' END,
           'ENC_L3_N', 3
    UNION ALL
    SELECT 'DIAGNOSIS',
           'DIAGNOSISID is unique',
           CASE WHEN (SELECT COUNT(DISTINCT DIAGNOSISID) FROM {{ current_schema }}.DIAGNOSIS) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.DIAGNOSIS)
                THEN 'No' ELSE 'Yes' END,
           'DIA_L3_N', 4
    UNION ALL
    SELECT 'PROCEDURES',
           'PROCEDURESID is unique',
           CASE WHEN (SELECT COUNT(DISTINCT PROCEDURESID) FROM {{ current_schema }}.PROCEDURES) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.PROCEDURES)
                THEN 'No' ELSE 'Yes' END,
           'PRO_L3_N', 5
    UNION ALL
    SELECT 'VITAL',
           'VITALID is unique',
           CASE WHEN (SELECT COUNT(DISTINCT VITALID) FROM {{ current_schema }}.VITAL) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.VITAL)
                THEN 'No' ELSE 'Yes' END,
           'VIT_L3_N', 6
    UNION ALL
    SELECT 'LAB_RESULT_CM',
           'LAB_RESULT_CM_ID is unique',
           CASE WHEN (SELECT COUNT(DISTINCT LAB_RESULT_CM_ID) FROM {{ current_schema }}.LAB_RESULT_CM) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.LAB_RESULT_CM)
                THEN 'No' ELSE 'Yes' END,
           'LAB_L3_N', 7
    UNION ALL
    SELECT 'PRESCRIBING',
           'PRESCRIBINGID is unique',
           CASE WHEN (SELECT COUNT(DISTINCT PRESCRIBINGID) FROM {{ current_schema }}.PRESCRIBING) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.PRESCRIBING)
                THEN 'No' ELSE 'Yes' END,
           'PRES_L3_N', 8
    UNION ALL
    SELECT 'DISPENSING',
           'DISPENSINGID is unique',
           CASE WHEN (SELECT COUNT(DISTINCT DISPENSINGID) FROM {{ current_schema }}.DISPENSING) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.DISPENSING)
                THEN 'No' ELSE 'Yes' END,
           'DISP_L3_N', 9
    UNION ALL
    SELECT 'DEATH',
           'PATID + DEATH_SOURCE are unique',
           CASE WHEN (SELECT COUNT(DISTINCT PATID || COALESCE(DEATH_SOURCE,'')) FROM {{ current_schema }}.DEATH) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.DEATH)
                THEN 'No' ELSE 'Yes' END,
           'DEATH_L3_N', 10
    UNION ALL
    SELECT 'DEATH_CAUSE',
           'PATID + DEATH_CAUSE + DEATH_CAUSE_CODE + DEATH_CAUSE_TYPE + DEATH_CAUSE_SOURCE are unique',
           CASE WHEN (SELECT COUNT(DISTINCT PATID || COALESCE(DEATH_CAUSE,'') || COALESCE(DEATH_CAUSE_CODE,'') || COALESCE(DEATH_CAUSE_TYPE,'') || COALESCE(DEATH_CAUSE_SOURCE,''))
                      FROM {{ current_schema }}.DEATH_CAUSE) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.DEATH_CAUSE)
                THEN 'No' ELSE 'Yes' END,
           'DEATC_L3_N', 11
    UNION ALL
    SELECT 'CONDITION',
           'CONDITIONID is unique',
           CASE WHEN (SELECT COUNT(DISTINCT CONDITIONID) FROM {{ current_schema }}.CONDITION) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.CONDITION)
                THEN 'No' ELSE 'Yes' END,
           'COND_L3_N', 12
    UNION ALL
    SELECT 'IMMUNIZATION',
           'IMMUNIZATIONID is unique',
           CASE WHEN (SELECT COUNT(DISTINCT IMMUNIZATIONID) FROM {{ current_schema }}.IMMUNIZATION) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.IMMUNIZATION)
                THEN 'No' ELSE 'Yes' END,
           'IMMUNE_L3_N', 13
    UNION ALL
    SELECT 'MED_ADMIN',
           'MEDADMINID is unique',
           CASE WHEN (SELECT COUNT(DISTINCT MEDADMINID) FROM {{ current_schema }}.MED_ADMIN) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.MED_ADMIN)
                THEN 'No' ELSE 'Yes' END,
           'MEDA_L3_N', 14
    UNION ALL
    SELECT 'OBS_CLIN',
           'OBSCLINID is unique',
           CASE WHEN (SELECT COUNT(DISTINCT OBSCLINID) FROM {{ current_schema }}.OBS_CLIN) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.OBS_CLIN)
                THEN 'No' ELSE 'Yes' END,
           'OBSCLIN_L3_N', 15
    UNION ALL
    SELECT 'OBS_GEN',
           'OBSGENID is unique',
           CASE WHEN (SELECT COUNT(DISTINCT OBSGENID) FROM {{ current_schema }}.OBS_GEN) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.OBS_GEN)
                THEN 'No' ELSE 'Yes' END,
           'OBSGEN_L3_N', 16
    UNION ALL
    SELECT 'HASH_TOKEN',
           'PATID + TOKEN_ENCRYPTION_KEY are unique',
           CASE WHEN (SELECT COUNT(DISTINCT PATID || COALESCE(TOKEN_ENCRYPTION_KEY,'')) FROM {{ current_schema }}.HASH_TOKEN) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.HASH_TOKEN)
                THEN 'No' ELSE 'Yes' END,
           'HTOK_L3_N', 17
    UNION ALL
    SELECT 'LDS_ADDRESS_HISTORY',
           'ADDRESSID is unique',
           CASE WHEN (SELECT COUNT(DISTINCT ADDRESSID) FROM {{ current_schema }}.LDS_ADDRESS_HISTORY) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.LDS_ADDRESS_HISTORY)
                THEN 'No' ELSE 'Yes' END,
           'LDSADD_L3_N', 18
    UNION ALL
    SELECT 'PRO_CM',
           'PRO_CM_ID is unique',
           CASE WHEN (SELECT COUNT(DISTINCT PRO_CM_ID) FROM {{ current_schema }}.PRO_CM) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.PRO_CM)
                THEN 'No' ELSE 'Yes' END,
           'PROM_L3_N', 19
    UNION ALL
    SELECT 'PROVIDER',
           'PROVIDERID is unique',
           CASE WHEN (SELECT COUNT(DISTINCT PROVIDERID) FROM {{ current_schema }}.PROVIDER) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.PROVIDER)
                THEN 'No' ELSE 'Yes' END,
           'PROV_L3_N', 20
    UNION ALL
    SELECT 'PCORNET_TRIAL',
           'PATID + TRIALID + PARTICIPANTID are unique',
           CASE WHEN (SELECT COUNT(DISTINCT PATID || COALESCE(TRIALID,'') || COALESCE(PARTICIPANTID,'')) FROM {{ current_schema }}.PCORNET_TRIAL) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.PCORNET_TRIAL)
                THEN 'No' ELSE 'Yes' END,
           'TRIAL_L3_N', 21
    UNION ALL
    SELECT 'EXTERNAL_MEDS',
           'PATID + EXTMEDID are unique',
           CASE WHEN (SELECT COUNT(DISTINCT PATID || COALESCE(EXTMEDID,'')) FROM {{ current_schema }}.EXTERNAL_MEDS) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.EXTERNAL_MEDS)
                THEN 'No' ELSE 'Yes' END,
           'EXTMED_L3_N', 22
    UNION ALL
    SELECT 'PAT_RELATIONSHIP',
           'PATID_1 + PATID_2 + RELATIONSHIP_TYPE are unique',
           CASE WHEN (SELECT COUNT(DISTINCT PATID_1 || COALESCE(PATID_2,'') || COALESCE(RELATIONSHIP_TYPE,'')) FROM {{ current_schema }}.PAT_RELATIONSHIP) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.PAT_RELATIONSHIP)
                THEN 'No' ELSE 'Yes' END,
           'PATREL_L3_N', 23
    UNION ALL
    SELECT 'LAB_HISTORY',
           'LABHISTORYID is unique',
           CASE WHEN (SELECT COUNT(DISTINCT LABHISTORYID) FROM {{ current_schema }}.LAB_HISTORY) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.LAB_HISTORY)
                THEN 'No' ELSE 'Yes' END,
           'LABH_L3_N', 24
    UNION ALL
    SELECT 'HARVEST',
           'NETWORKID + DATAMARTID are unique',
           CASE WHEN (SELECT COUNT(DISTINCT NETWORKID || COALESCE(DATAMARTID,'')) FROM {{ current_schema }}.HARVEST) =
                     (SELECT COUNT(*) FROM {{ current_schema }}.HARVEST)
                THEN 'No' ELSE 'Yes' END,
           'HARV_L3_N', 25
) sub
ORDER BY ROW_ORDER
