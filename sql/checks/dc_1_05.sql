-- DC 1.05: Primary key definition errors
-- Parameters: {{ current_schema }}
WITH pk_dups AS (
    SELECT COUNT(*) AS N FROM (SELECT PATID                                                                               FROM {{ current_schema }}.DEMOGRAPHIC        GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT PATID || ENR_START_DATE || ENR_BASIS                                                     FROM {{ current_schema }}.ENROLLMENT         GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT PATID || DEATH_SOURCE                                                                    FROM {{ current_schema }}.DEATH              GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT ENCOUNTERID                                                                              FROM {{ current_schema }}.ENCOUNTER          GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT DIAGNOSISID                                                                              FROM {{ current_schema }}.DIAGNOSIS          GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT PROCEDURESID                                                                             FROM {{ current_schema }}.PROCEDURES         GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT VITALID                                                                                  FROM {{ current_schema }}.VITAL              GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT PRESCRIBINGID                                                                            FROM {{ current_schema }}.PRESCRIBING        GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT DISPENSINGID                                                                             FROM {{ current_schema }}.DISPENSING         GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT LAB_RESULT_CM_ID                                                                         FROM {{ current_schema }}.LAB_RESULT_CM      GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT NETWORKID || DATAMARTID                                                                  FROM {{ current_schema }}.HARVEST            GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT CONDITIONID                                                                              FROM {{ current_schema }}.CONDITION          GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT PATID || DEATH_CAUSE || DEATH_CAUSE_CODE || DEATH_CAUSE_TYPE || DEATH_CAUSE_SOURCE       FROM {{ current_schema }}.DEATH_CAUSE        GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT PATID || TRIALID || PARTICIPANTID                                                        FROM {{ current_schema }}.PCORNET_TRIAL      GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT PRO_CM_ID                                                                                FROM {{ current_schema }}.PRO_CM             GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT PROVIDERID                                                                               FROM {{ current_schema }}.PROVIDER           GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT MEDADMINID                                                                               FROM {{ current_schema }}.MED_ADMIN          GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT OBSCLINID                                                                                FROM {{ current_schema }}.OBS_CLIN           GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT OBSGENID                                                                                 FROM {{ current_schema }}.OBS_GEN            GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT PATID || TOKEN_ENCRYPTION_KEY                                                            FROM {{ current_schema }}.HASH_TOKEN         GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT ADDRESSID                                                                                FROM {{ current_schema }}.LDS_ADDRESS_HISTORY GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT IMMUNIZATIONID                                                                           FROM {{ current_schema }}.IMMUNIZATION       GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT LABHISTORYID                                                                             FROM {{ current_schema }}.LAB_HISTORY        GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT PATID || EXTMEDID                                                                        FROM {{ current_schema }}.EXTERNAL_MEDS      GROUP BY 1 HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT PATID_1 || PATID_2 || RELATIONSHIP_TYPE                                                  FROM {{ current_schema }}.PAT_RELATIONSHIP   GROUP BY 1 HAVING COUNT(*) > 1)
)
SELECT
    '1.05'                                AS CHECK_NUM,
    'No primary key duplicates'           AS DESCRIPTION,
    CASE WHEN SUM(N) = 0 THEN 'Pass' ELSE 'Fail' END AS STATUS
FROM pk_dups
