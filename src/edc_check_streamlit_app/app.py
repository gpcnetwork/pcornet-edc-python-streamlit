# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import datetime,date
from dateutil.relativedelta import relativedelta
import pandas as pd
import altair as alt



st.set_page_config(layout="wide")

session = get_active_session()

st.title("CDM HEALTH")
st.write("")
st.write("")
st.write("")


def years_ago(number_of_years: int) -> str:
    today = date.today()
    try:
        target = today.replace(year=today.year - number_of_years)
    except ValueError:            # handles Feb 29 ↦ Feb 28
        target = today.replace(month=2, day=28, year=today.year - number_of_years)
    return target.strftime("%Y-%m-%d")
    
def generate_html_table(records, red_columns):
    tr = ''
    for row in records:
        # Initialize an empty string for each row
        row_html = '<tr>'
        for col in row._fields:  # Using _fields to get the column names
            # Determine if the column should be red
            color = 'black'
            if col in red_columns:
                if getattr(row, col) is not None and getattr(row, col) < -5:
                    color = 'red'
            
            # Add the column data to the row with the appropriate color
            row_html += f'<td style="border: 1px solid black; padding: 8px; text-align: center; color:{color};">{getattr(row, col)}</td>'
        
        # Close the row tag and add to the complete table rows
        row_html += '</tr>'
        tr += row_html

    # Generate the table headers based on the first record fields
    headers = ''.join([f'<th style="border: 1px solid black; padding: 8px; text-align: center; background-color: #4CAF50; color: white;width:450px;">{col}</th>' for col in records[0]._fields])

    # Combine the header and rows into the final HTML table (in a single line)
    html = f'''<table style="width:100%; border-collapse: collapse; border: 1px solid black;"><tr>{headers}</tr>{tr}</table>'''

    return html


def generate_generic_table(records, red_columns , ignore_colums , bold_columns , red_value):
    tr = ''
    for row in records:
        # Initialize an empty string for each row
        row_html = '<tr>'
        for col in row._fields:  # Using _fields to get the column names
            # Determine if the column should be red
            if col in ignore_colums:
                continue
            color = 'black'
            weight = 'normal'
            if col in red_columns:
                if getattr(row, col) is not None:
                    if ( isinstance(getattr(row, col), str) and getattr(row, col) == red_value ) or (not isinstance(getattr(row, col), str) and getattr(row, col) > red_value):
                        color = 'red'
            if col in bold_columns and getattr(row, col) is not None:
                weight = 'bold'
                
            # Add the column data to the row with the appropriate color
            row_html += f'<td style="border: 1px solid black; padding: 8px; text-align: center; color:{color};font-weight:{weight};">{getattr(row, col)}</td>'
        
        # Close the row tag and add to the complete table rows
        row_html += '</tr>'
        tr += row_html

    # Generate the table headers based on the first record fields
    headers = ''.join([f'<th style="border: 1px solid black; padding: 8px; text-align: center; background-color: #4CAF50; color: white;min-width:300px;">{col}</th>' for col in records[0]._fields if col not in ignore_colums])

    # Combine the header and rows into the final HTML table (in a single line)
    html = f'''<table style="width:95%; border-collapse: collapse; border: 1px solid black;margin:25px auto;"><tr>{headers}</tr>{tr}</table>'''

    return html



def get_case_sql_by_table(current_schema, table):
    case = ''
    
    table_conditions = {
        "DEMOGRAPHIC": "PATID",
        "ENROLLMENT": "PATID || ENR_START_DATE || ENR_BASIS",
        "DEATH": "PATID || DEATH_SOURCE",
        "ENCOUNTER": "ENCOUNTERID",
        "DIAGNOSIS": "DIAGNOSISID",
        "PROCEDURES": "PROCEDURESID",
        "VITAL": "VITALID",
        "PRESCRIBING": "PRESCRIBINGID",
        "DISPENSING": "DISPENSINGID",
        "LAB_RESULT_CM": "LAB_RESULT_CM_ID",
        "HARVEST": "NETWORKID || DATAMARTID",
        "CONDITION": "CONDITIONID",
        "DEATH_CAUSE": "PATID || DEATH_CAUSE || DEATH_CAUSE_CODE || DEATH_CAUSE_TYPE || DEATH_CAUSE_SOURCE",
        "PCORNET_TRIAL": "PATID || TRIALID || PARTICIPANTID",
        "PRO_CM": "PRO_CM_ID",
        "PROVIDER": "PROVIDERID",
        "MED_ADMIN": "MEDADMINID",
        "OBS_CLIN": "OBSCLINID",
        "OBS_GEN": "OBSGENID",
        "HASH_TOKEN": "PATID || TOKEN_ENCRYPTION_KEY",
        "LDS_ADDRESS_HISTORY": "ADDRESSID",
        "IMMUNIZATION": "IMMUNIZATIONID",
        "LAB_HISTORY": "LABHISTORYID"
    }

    if table in table_conditions:
        case = f"""
            CASE WHEN (SELECT COUNT(DISTINCT {table_conditions[table]}) FROM {current_schema}.{table}) = 
                      (SELECT COUNT(*) FROM {current_schema}.{table}) 
            THEN 'No' ELSE 'Yes' END
        """

    return case

def construct_primary_key_errors_table(current_schema):
    st.write("")
    st.write("")
    st.markdown("##### Table IIA. Primary Key Errors")
    st.write("This table shows the required primary key definitions and supports Data Check 1.05 (primary key definition errors). Data check exceptions are highlighted in red and must be corrected.")
    st.write("")
    sql = ""
    table_dict = {
        "DEMOGRAPHIC": "PATID is unique",
        "ENROLLMENT": "ENROLLID (concatenation of PATID + ENR_START_DATE + ENR_BASIS) is unique",
        "DEATH": "DEATHID (concatenation of PATID and DEATH_SOURCE) is unique",
        "ENCOUNTER": "ENCOUNTERID is unique",
        "DIAGNOSIS": "DIAGNOSISID is unique",
        "PROCEDURES": "PROCEDURESID is unique",
        "VITAL": "VITALID is unique",
        "PRESCRIBING": "PRESCRIBINGID is unique",
        "DISPENSING": "DISPENSINGID is unique",
        "LAB_RESULT_CM": "LAB_RESULT_CM_ID is unique",
        "HARVEST": "NETWORKID + DATAMARTID is unique",
        "CONDITION": "CONDITIONID is unique",
        "DEATH_CAUSE": "DEATHCID (concatenation of PATID + DEATH_CAUSE + DEATH_CAUSE_CODE + DEATH_CAUSE_TYPE + DEATH_CAUSE_SOURCE) is unique",
        "PCORNET_TRIAL": "TRIAL_KEY (concatenation of PATID + TRIALID + PARTICIPANTID) is unique",
        "PRO_CM": "PRO_CM_ID is unique",
        "PROVIDER": "PROVIDERID is unique",
        "MED_ADMIN": "MEDADMINID is unique",
        "OBS_CLIN": "OBSCLINID is unique",
        "OBS_GEN": "OBSGENID is unique",
        "HASH_TOKEN": "HASHID (concatenation of PATID + TOKEN_ENCRYPTION_KEY) is unique",
        "LDS_ADDRESS_HISTORY": "ADDRESSID is unique",
        "IMMUNIZATION": "IMMUNIZATIONID is unique",
        "LAB_HISTORY": "LABHISTORYID is unique"
    }

    i = 1
    for key in table_dict:
        sql+= f"""
             SELECT '{key}' as \"TABLE\" ,  '{table_dict[key]}' as \"CDM specifications for primary keys\" , { get_case_sql_by_table(current_schema,key)} as \"Exception to specifications\" , {i} AS row_order UNION """
        i+=1
    sql = sql.rstrip(" UNION ")+" order by row_order"
    # st.write(sql)
    records = session.sql(sql).collect()
    html = generate_generic_table(records, ['Exception to specifications'] , ['ROW_ORDER'] , ['TABLE'] , 'Yes')
    st.markdown(html, unsafe_allow_html=True)


def construct_orphan_record_errors_table(current_schema , cutoff_date):
    st.write("")
    st.write("")
    st.markdown("##### Table IIE. Orphan Records, Replication Errors, Encounter Duplication and Hash Token Duplication")
    st.write("This table illustrates exceptions to Data Checks 1.08 (tables contain orphan PATIDs), 1.09 (tables contain orphan ENCOUNTERIDs for more than 5% of records), 1.10 (replication errors between the ENCOUNTER, PROCEDURES and DIAGNOSIS tables), 1.11 (more than 5% of encounters are assigned to more than one patient), 1.12 (tables contain orphan PROVIDERIDs), 1.14 (patients are missing from HASH_TOKEN), and 1.19 (more than 10% of hash tokens are assigned to multiple patients). Orphan PATIDs are not present in the DEMOGRAPHIC table. Orphan ENCOUNTERIDs are not present in the ENCOUNTER table. Orphan PROVIDERIDs are not present in the PROVIDER table. Replication errors are ENCOUNTERIDs in the DIAGNOSIS or PROCEDURES table where the encounter type or admit date does not match the corresponding value in the ENCOUNTER table. Data check exceptions to 1.14 and 1.19 are highlighted in blue and must be explained in the ETL ADD; all other data check exceptions are highlighted in red and must be corrected.")
    st.write("")
    st.markdown("###### 1.08: Orphan PATID(S)")
    cdm_tables = [
        "CONDITION", "DIAGNOSIS", "DEATH", "DEATH_CAUSE", "DISPENSING",  
        "ENCOUNTER", "ENROLLMENT", "HASH_TOKEN", "IMMUNIZATION",  
        "LAB_RESULT_CM", "LDS_ADDRESS_HISTORY", "MED_ADMIN", "OBS_CLIN",  
        "OBS_GEN", "PCORNET_TRIAL", "PRESCRIBING", "PROCEDURES",  
        "PRO_CM",
        "VITAL"
    ]
    sql = ""
    for table in cdm_tables:
        sql += f"""
            SELECT '{table}' AS \"TABLE\", COUNT(DISTINCT PATID) AS \"Count\", ROUND((COUNT(DISTINCT PATID) * 100.0) / NULLIF((SELECT COUNT(DISTINCT PATID) FROM {current_schema}.{table}), 0),1) AS percentage FROM {current_schema}.{table}  WHERE {table}.PATID NOT IN (SELECT DISTINCT PATID FROM {current_schema}.DEMOGRAPHIC) UNION """
    sql = sql.rstrip(" UNION ") + " ORDER BY \"Count\" DESC"
    records = session.sql(sql).collect()
    html = generate_generic_table(records, ['Count'] , [] , ['TABLE'] , 0)
    st.markdown(html, unsafe_allow_html=True)
    st.write("")
    st.markdown("###### 1.09: Orphan ENCOUNTERIDs. Exception: Orphan ENCOUNTERID(S) for more than 5% of the records in the CONDITION, DIAGNOSIS,IMMUNIZATION, LAB_RESULT_CM,MED_ADMIN, OBS_CLIN, OBS_GEN,PRESCRIBING, PROCEDURES,PRO_CM, or VITAL table")
    
    enc_tables = [
        "CONDITION", "DIAGNOSIS", "IMMUNIZATION", "LAB_RESULT_CM", "MED_ADMIN",
        "OBS_CLIN", "OBS_GEN", "PRESCRIBING", "PROCEDURES", "PRO_CM", "VITAL"
    ]

    sql = ""
    for table in enc_tables:
        sql += f"""
            SELECT '{table}' AS \"TABLE\", COUNT(DISTINCT ENCOUNTERID) AS \"Count\", CASE WHEN (SELECT COUNT(DISTINCT ENCOUNTERID) FROM {current_schema}.{table}) = 0 THEN 0  ELSE  ROUND((COUNT(DISTINCT ENCOUNTERID) * 100.0 ) / NULLIF((SELECT COUNT(DISTINCT ENCOUNTERID) FROM {current_schema}.{table}), 0),2) END AS percentage FROM {current_schema}.{table}  WHERE {table}.ENCOUNTERID NOT IN (SELECT DISTINCT ENCOUNTERID FROM {current_schema}.ENCOUNTER) UNION """
    sql = sql.rstrip(" UNION ") + " ORDER BY \"Count\" DESC"
    records = session.sql(sql).collect()
    html = generate_generic_table(records, ['PERCENTAGE'] , [] , ['TABLE'] , 4.99)
    st.markdown(html, unsafe_allow_html=True)

    st.write("")
    st.markdown("###### 1.10: Replication errors. Exception: Replication error(s) in ENC_TYPE or ADMIT_DATE in the DIAGNOSIS or PROCEDURES table.")
    replication_tables = ['DIAGNOSIS','PROCEDURES']
    sql = ""
    for table in replication_tables:
        sql+= f"""
        SELECT '{table}' AS \"TABLE\", COUNT(*) AS \"Count\", LISTAGG(CASE WHEN d.enc_type != e.enc_type AND d.admit_date != e.admit_date THEN 'ENC_TYPE & ADMIT_DATE' WHEN d.enc_type != e.enc_type THEN 'ENC_TYPE' WHEN d.admit_date != e.admit_date THEN 'ADMIT_DATE'  END, ', ') AS Mismatch_Fields FROM {current_schema}.{table} d JOIN {current_schema}.encounter e ON d.encounterid = e.encounterid WHERE ( d.enc_type != e.enc_type OR d.admit_date != e.admit_date) UNION """
        
    sql = sql.rstrip(" UNION ")
    records = session.sql(sql).collect()
    html = generate_generic_table(records, ['Count'] , [] , ['TABLE'] , 0)
    st.markdown(html, unsafe_allow_html=True)   

    st.write("")
    st.markdown("###### 1.11: More than 5% of encounters are assigned to more than one patient. Exception: An ENCOUNTERID in the CONDITION, DIAGNOSIS,ENCOUNTER, IMMUNIZATION,LAB_RESULT_CM, MED_ADMIN,OBS_CLIN, OBS_GEN, PRESCRIBING,PROCEDURES, PRO_CM, or VITAL table is associated with more than 1 PATID in the same table.")
    enc_tables = [
        "CONDITION", "DIAGNOSIS", "ENCOUNTER", "IMMUNIZATION", "LAB_RESULT_CM", "MED_ADMIN",
        "OBS_CLIN", "OBS_GEN", "PRESCRIBING", "PROCEDURES", "PRO_CM", "VITAL"
    ]
    sql = ""
    for table in enc_tables:
        col = table_dict[table]
        sql+= f"""

        SELECT 
             '{table}' AS \"TABLE\",
              COUNT(*) AS \"Count\",
              CASE WHEN (SELECT COUNT(DISTINCT ENCOUNTERID) FROM {current_schema}.{table}) = 0 THEN 0  ELSE  ROUND((COUNT(DISTINCT ENCOUNTERID) * 100.0 ) / NULLIF((SELECT COUNT(DISTINCT ENCOUNTERID) FROM {current_schema}.{table}), 0),2) END AS percentage
            FROM (
              SELECT ENCOUNTERID
              FROM {current_schema}.{table}
              WHERE ENCOUNTERID IS NOT NULL and {col} >= '{str(cutoff_date)}'
              GROUP BY ENCOUNTERID
              HAVING COUNT(DISTINCT PATID) > 1
            ) AS sub UNION """
        
    sql = sql.rstrip(" UNION ") +' ORDER BY "Count" Desc'
    records = session.sql(sql).collect()
    html = generate_generic_table(records, ['PERCENTAGE'] , [] , ['TABLE'] , 4.99)
    st.markdown(html, unsafe_allow_html=True)   

    st.write("")
    st.markdown("###### 1.12: Orphan PROVIDERIDs. Exception: Orphan PROVIDERID(S) in the ENCOUNTER, DIAGNOSIS,IMMUNIZATION, MED_ADMIN,OBS_CLIN, OBS_GEN, PRESCRIBING or PROCEDURES table.")
    provider_tables = {
        "DIAGNOSIS": "PROVIDERID",
        "ENCOUNTER": "PROVIDERID",
        "IMMUNIZATION": "VX_PROVIDERID",
        "MED_ADMIN": "MEDADMIN_PROVIDERID",
        "OBS_CLIN": "OBSCLIN_PROVIDERID",
        "OBS_GEN": "OBSGEN_PROVIDERID",
        "PRESCRIBING": "RX_PROVIDERID",
        "PROCEDURES": "PROVIDERID"
    }

    sql = ""
    for table in provider_tables:
        provider_id = provider_tables[table]
        sql += f"""
            SELECT '{table}' AS \"TABLE\", COUNT(DISTINCT {provider_id}) AS \"Count\", ROUND((COUNT(DISTINCT {provider_id}) * 100.0) / NULLIF((SELECT COUNT(DISTINCT {provider_id}) FROM {current_schema}.{table}), 0),1) AS percentage FROM {current_schema}.{table}  WHERE {table}.{provider_id} is not null and {table}.{provider_id} NOT IN (SELECT DISTINCT PROVIDERID FROM {current_schema}.PROVIDER) 
            UNION """
    sql = sql.rstrip(" UNION ") + "\n ORDER BY \"Count\" DESC"
    records = session.sql(sql).collect()
    html = generate_generic_table(records, ['Count'] , [] , ['TABLE'] , 0)
    st.markdown(html, unsafe_allow_html=True)
    

def construct_potential_pools_of_patients(current_schema):
    filter_date = years_ago(5)
    year_1 = years_ago(1)
    sql = f"""
    
                with total_patients as
            (
               select count(distinct patid) as n from {current_schema}.demographic
            ),
            enc_patient_pool_5 as 
            (
              select distinct patid from {current_schema}.encounter where admit_date >= '{filter_date}' and ENC_TYPE in ('EI','ED','AV','IP','OS')
            ),
            total_enc_patients as
            (
              select count(distinct patid) as enc_n from enc_patient_pool_5
            ),
            enc_patient_pool_1 as
            (
              select distinct patid from {current_schema}.encounter where admit_date >= '{year_1}' and ENC_TYPE in ('EI','ED','AV','IP','OS')
            ),
            diagnosis_patient_pool_5 as 
            (
              select distinct patid from {current_schema}.DIAGNOSIS where enc_type in ('EI','ED','AV','IP','OS') and ADMIT_DATE >= '{filter_date}' 
            ),
            procedures_patient_pool_5 as
            (
             select distinct patid from {current_schema}.procedures where ADMIT_DATE >= '{filter_date}'
            ),
            diagnosis_vital_patient_pool_5 as
            (
             select distinct patid from diagnosis_patient_pool_5
               INTERSECT 
             select distinct patid from {current_schema}.vital where MEASURE_DATE >= '{filter_date}' 
            ),
            prescribing_or_med_admin_patient_pool_5 as 
            (
             select distinct patid from {current_schema}.prescribing where RX_ORDER_DATE >= '{filter_date}' 
              UNION   
             select distinct patid from {current_schema}.med_admin where MEDADMIN_START_DATE >= '{filter_date}'
            ),
            diagnosis_vital_and_prescribing_or_med_admin_patient_pool_5 as
            (
              select distinct patid from diagnosis_vital_patient_pool_5 
                INTERSECT 
              select distinct patid from prescribing_or_med_admin_patient_pool_5 
            ),
            diagnosis_vital_and_prescribing_or_med_admin_lab_result_cm_patient_pool_5 as 
            (
              select distinct patid from diagnosis_vital_and_prescribing_or_med_admin_patient_pool_5  
                INTERSECT 
              select distinct patid from {current_schema}.lab_result_cm where RESULT_DATE >= '{filter_date}'
            ),
            enc_diagnosis_patient_pool_5 as 
            (
              select distinct patid from enc_patient_pool_5 
                INTERSECT
              select distinct patid from diagnosis_patient_pool_5
            ),
            enc_procedures_patient_pool_5 as 
            (
              select distinct patid from enc_patient_pool_5 
                INTERSECT
              select distinct patid from procedures_patient_pool_5
            )
            
            select Metric , Metric_Description, Result, row_order,
            
                CASE 
                    WHEN row_order IN (2,3) 
                    THEN TO_VARCHAR(ROUND((Result::FLOAT / n) * 100, 1)) || '%'
            
                    WHEN row_order IN (1) 
                    THEN ''
                    
                    ELSE
                    TO_VARCHAR(ROUND((Result::FLOAT / enc_n ) * 100, 1)) || '%'
                END AS percentage
            
            
            from
            (
            select 'All patients' as Metric , 'Number of unique patients in the DEMOGRAPHIC table' as Metric_Description , TO_VARCHAR(count(distinct patid)) as Result  , 1 AS row_order from {current_schema}.demographic  
            UNION
            select 'Potential pool of patients for observational studies' as Metric , 'Number of unique patients with at least 1 face-to-face (ED, EI, IP, OS, or AV) encounter within the past 5 years' as Metric_Description , TO_VARCHAR(count(patid)) as Result, 2 as row_order from enc_patient_pool_5 
            UNION
            select 'Potential pool of patients for trials' as Metric, 'Number of unique patients with at least 1 face-to-face (ED, EI, IP, OS, or AV) encounter within the past 1 year' as Metric_Description ,TO_VARCHAR(count(patid)) as Result, 3 as row_order from enc_patient_pool_1
            UNION
            select 'Potential pool of patients for studies requiring data on diagnoses, vital measures and (a) medications or (b) medications and lab results' as Metric, 'Number of unique patients with at least 1 DIAGNOSIS record in a face-to-face setting and at least 1 VITAL record within the past 5 years' as Metric_Description, TO_VARCHAR(count(patid)) as Result, 4 as row_order from diagnosis_vital_patient_pool_5  
            UNION
            select '' as Metric , 'Number of unique patients with at least 1 DIAGNOSIS record in a face-to-face setting, at least 1 VITAL record, and at least 1 PRESCRIBING or MED_ADMIN record within the past 5 years' as Metric_Description , TO_VARCHAR(count(patid)) as Result , 5 as row_order from diagnosis_vital_and_prescribing_or_med_admin_patient_pool_5
            UNION
            select '' as Metric , 'Number of unique patients with at least 1 DIAGNOSIS record in a face-to-face setting, at least 1 VITAL record, at least 1 PRESCRIBING or MED_ADMIN record, and at least 1 LAB_RESULT_CM record within the past 5 years' as Metric_Description ,TO_VARCHAR(count(patid)) as Result , 6 as row_order from diagnosis_vital_and_prescribing_or_med_admin_lab_result_cm_patient_pool_5 
            UNION
            select 'Patients with diagnosis data' as Metric , 'Percentage of patients with encounters who have at least 1 diagnosis' as Metric_Description ,TO_VARCHAR(count(patid)) as Result , 7 as row_order from enc_diagnosis_patient_pool_5
            UNION
            select 'Patients with procedure data' as Metric , 'Percentage of patients with encounters who have at least 1 procedure' as Metric_Description ,TO_VARCHAR(count(patid)) as Result , 8 as row_order from enc_procedures_patient_pool_5
            ) subquery, total_patients , total_enc_patients order by row_order;
    """
    records = session.sql(sql).collect()
    html = generate_generic_table(records, [] , ['ROW_ORDER'] , [] , '')
    st.markdown(html, unsafe_allow_html=True)
    
    

def construct_demographic_descriptive_info(current_schema):
    sql = f"""
          WITH total AS (
                SELECT COUNT(*) AS total_patients FROM {current_schema}.demographic
            ) ,

            least_encounter AS (
                 select COUNT(distinct patid) as total_patients_with_enc from {current_schema}.encounter where ADMIT_DATE > '2011-12-01'
            )
            SELECT CATEGORY, GROUP_NAME, N, row_order, 
                   CASE 
                        WHEN N != '' AND CATEGORY NOT IN ('Patients') AND GROUP_NAME NOT IN ('Mean', 'Median') 
                             AND row_order NOT IN (25, 26, 27) 
                             AND total_patients != 0 
                        THEN TO_VARCHAR(ROUND((N::FLOAT / total_patients) * 100, 1)) || '%'
                        
                        WHEN row_order IN (25, 26, 27) 
                            AND total_patients_with_enc!=0 
                        THEN TO_VARCHAR(ROUND((N::FLOAT / total_patients_with_enc) * 100, 1)) || '%'
                        
                        ELSE '' 
                    END AS percentage

            FROM (
                SELECT 'Patients' as CATEGORY , '' as GROUP_NAME, TO_VARCHAR(COUNT(*)) AS N, 1 AS row_order FROM {current_schema}.demographic UNION 
                SELECT 'Age' as CATEGORY , '' as GROUP_NAME , '' as N , 2 AS row_order UNION 
                SELECT '' as CATEGORY , 'Mean' as GROUP_NAME , TO_VARCHAR(CAST(AVG(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE)) AS INT)) as N , 3 AS row_order FROM {current_schema}.demographic UNION 
                SELECT '' AS CATEGORY, 'Median' AS GROUP_NAME, TO_VARCHAR(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE)) AS INT)) AS N, 4 AS row_order FROM {current_schema}.demographic UNION 
                SELECT 'Age group' as CATEGORY, '' as GROUP_NAME , '' as N , 5 as row_order UNION 
                SELECT '' as CATEGORY , '0-4' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 6 as row_order FROM {current_schema}.demographic WHERE CAST(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE) AS INT) <=4 UNION  
                SELECT '' as CATEGORY , '5-14' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 7 as row_order FROM {current_schema}.demographic WHERE CAST(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE) AS INT) >4 AND CAST(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE) AS INT)<=14 UNION
                SELECT '' as CATEGORY , '15-21' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 8 as row_order FROM {current_schema}.demographic WHERE CAST(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE) AS INT) >14 AND CAST(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE) AS INT)<=21 UNION
                SELECT '' as CATEGORY , '22-64' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 9 as row_order FROM {current_schema}.demographic WHERE CAST(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE) AS INT) >21 AND CAST(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE) AS INT)<=64 UNION
                SELECT '' as CATEGORY , '65+' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 10 as row_order FROM {current_schema}.demographic WHERE CAST(DATEDIFF(YEAR, BIRTH_DATE, CURRENT_DATE) AS INT) >64 UNION
                SELECT '' as CATEGORY , 'Missing' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 11 as row_order FROM {current_schema}.demographic WHERE BIRTH_DATE IS NULL UNION 
                SELECT 'Hispanic' as CATEGORY, '' as GROUP_NAME , '' as N , 12 as row_order UNION 
                SELECT '' as CATEGORY , 'N (No)' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 13 as row_order FROM {current_schema}.demographic WHERE HISPANIC='N' UNION
                SELECT '' as CATEGORY , 'Y (Yes)' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 14 as row_order FROM {current_schema}.demographic WHERE HISPANIC='Y' UNION 
                SELECT '' as CATEGORY , 'Missing or Refused' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 15 as row_order FROM {current_schema}.demographic WHERE HISPANIC = 'R' OR HISPANIC IS NULL UNION
                SELECT 'Sex' as CATEGORY, '' as GROUP_NAME , '' as N , 16 as row_order UNION
                SELECT '' as CATEGORY , 'F (Female)' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 17 as row_order FROM {current_schema}.demographic WHERE SEX='F' UNION
                SELECT '' as CATEGORY , 'M (Male)' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 18 as row_order FROM {current_schema}.demographic WHERE SEX='M' UNION 
                SELECT '' as CATEGORY , 'Missing or Ambiguous' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 19 as row_order FROM {current_schema}.demographic WHERE SEX IS NULL UNION

                  SELECT 'Race' as CATEGORY, '' as GROUP_NAME , '' as N , 20 as row_order UNION
                  SELECT '' as CATEGORY , 'White' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 21 as row_order from {current_schema}.demographic where RACE ='05' UNION  
                  SELECT '' as CATEGORY , 'Non-White' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 22 as row_order from {current_schema}.demographic where RACE in ('01','02','03','04','06') UNION 
                  SELECT '' as CATEGORY , 'Missing or Refused' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 23 as row_order from {current_schema}.demographic where race in ('07' , 'NI' , 'UN' , 'OT') or race is null UNION  
        
                  SELECT 'Race among patients with at least 1 encounter after December 2011' as CATEGORY, '' as GROUP_NAME , '' as N , 24 as row_order UNION
                  SELECT '' as CATEGORY , 'White' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 25 as row_order from {current_schema}.demographic where RACE ='05' and patid in (select distinct patid from {current_schema}.encounter where ADMIT_DATE > '2011-12-01') UNION  
                  SELECT '' as CATEGORY , 'Non-White' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 26 as row_order from {current_schema}.demographic where RACE in ('01','02','03','04','06') and patid in (select distinct patid from {current_schema}.encounter where ADMIT_DATE > '2011-12-01') UNION 
                  SELECT '' as CATEGORY , 'Missing or Refused' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 27 as row_order from {current_schema}.demographic where ( race in ('07' , 'NI' , 'UN' , 'OT') or race is null ) and patid in (select distinct patid from {current_schema}.encounter where ADMIT_DATE > '2011-12-01') UNION 
        
                  SELECT 'Gender Identity' as CATEGORY, '' as GROUP_NAME , '' as N , 28 as row_order UNION
                  SELECT '' as CATEGORY , 'GQ (Genderqueer/Non-Binary)' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 29 as row_order from {current_schema}.demographic where GENDER_IDENTITY ='GQ' UNION  
                  SELECT '' as CATEGORY , 'M (Man)' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 30 as row_order from {current_schema}.demographic where GENDER_IDENTITY ='M' UNION 
                  SELECT '' as CATEGORY , 'W (Woman)' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 31 as row_order from {current_schema}.demographic where GENDER_IDENTITY ='W' UNION 
                  SELECT '' as CATEGORY , 'MU (Multiple gender categories), SE (Something else),TF (Transgender female/Trans woman/Male-to-female), or TM (Transgender male/Trans man/Female-to-male)' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 32 as row_order from {current_schema}.demographic where GENDER_IDENTITY in ('MU','SE','TF','TM') UNION 
                  SELECT '' as CATEGORY , 'Missing or Refused' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 33 as row_order from {current_schema}.demographic where GENDER_IDENTITY in ('DC' , 'NI' , 'UN' , 'OT') or GENDER_IDENTITY is null UNION 
        
                  SELECT 'Sexual Orientation' as CATEGORY, '' as GROUP_NAME , '' as N , 34 as row_order UNION
                  SELECT '' as CATEGORY , 'Bisexual' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 35 as row_order from {current_schema}.demographic where SEXUAL_ORIENTATION ='BI' UNION  
                  SELECT '' as CATEGORY , 'Gay' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 36 as row_order from {current_schema}.demographic where SEXUAL_ORIENTATION ='GA' UNION  
                  SELECT '' as CATEGORY , 'Lesbian' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 35 as row_order from {current_schema}.demographic where SEXUAL_ORIENTATION ='LE' UNION  
                  SELECT '' as CATEGORY , 'Queer' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 36 as row_order from {current_schema}.demographic where SEXUAL_ORIENTATION ='QU' UNION  
                  SELECT '' as CATEGORY , 'Straight' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 35 as row_order from {current_schema}.demographic where SEXUAL_ORIENTATION ='ST' UNION  
                  SELECT '' as CATEGORY , 'AS (Asexual), MU (Multiple sexual orientations),SE (Something else), QS (Questioning)' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 36 as row_order from {current_schema}.demographic where SEXUAL_ORIENTATION in ('AS','MU','SE','QS') UNION  
                  SELECT '' as CATEGORY , 'Missing or Refused' as GROUP_NAME, TO_VARCHAR(COUNT(*)) as N, 37 as row_order from {current_schema}.demographic where SEXUAL_ORIENTATION in ('DC' , 'NI' , 'UN' , 'OT') or SEXUAL_ORIENTATION is null 
                
            ) subquery, total,least_encounter order BY row_order
    """

    records = session.sql(sql).collect()
    html = generate_generic_table(records, [] , ['ROW_ORDER'] , ['CATEGORY'] , '')
    st.markdown(html, unsafe_allow_html=True)





    

table_dict = {
        'DEMOGRAPHIC': '',
        'ENROLLMENT': 'ENR_END_DATE',
        'ENCOUNTER': 'ADMIT_DATE',
        'DIAGNOSIS': 'ADMIT_DATE',
        'PROCEDURES': 'ADMIT_DATE',
        'VITAL': 'MEASURE_DATE',
        'DEATH': '',
        'PRESCRIBING': 'RX_ORDER_DATE',
        'DISPENSING': 'DISPENSE_DATE',
        'LAB_RESULT_CM': 'RESULT_DATE',
        'CONDITION': 'REPORT_DATE',
        'DEATH_CAUSE': '',
        'PRO_CM': 'PRO_DATE',
        'PROVIDER': '',
        'MED_ADMIN': 'MEDADMIN_START_DATE',
        'OBS_CLIN': 'OBSCLIN_START_DATE',
        'OBS_GEN': 'OBSGEN_START_DATE',
        'HASH_TOKEN': '',
        'IMMUNIZATION': 'VX_ADMIN_DATE',
        'LDS_ADDRESS_HISTORY': '',
        # 'LAB_HISTORY': 'PERIOD_START'
    }
sql = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA where SCHEMA_NAME like 'CDM%'"
schemas = session.sql(sql).collect()
schemas = [row.SCHEMA_NAME for row in schemas]



        

with st.expander("DESCRIPTIVE INFORMATION" , True):
    st.write("")
    col11, col12 , col13 , col14  = st.columns(4)
    
    
    enc_dict = {
        'AV' : 'Ambulatory_Visit',
        'ED' : 'Emergency_Department',
        'IP' : 'Inpatient',
        'OA' : 'Other_Ambulatory',
        'TH' : 'Telehealth_Encounters'
    }
    
    
    
    with col11:
        current_schema = st.selectbox(
                    "CURRENT CDM SCHEMA",
                     schemas , key="current_schema_1")
    
   
    
    with col14:
        st.write("")
        st.write("")
        generate_btn_1 = st.button("GENERATE" , key="generate_btn_1")


    if generate_btn_1:
        st.write("")
        st.write("")
        st.markdown("##### Table IA. Demographic Summary")
        st.write("This table contains general descriptive information about the patients in the DEMOGRAPHIC table. These patients may or may not be represented in other CDM tables.")
        st.write("")

        construct_demographic_descriptive_info(current_schema)
        st.write("")
        st.markdown("##### Table IB. Potential Pools of Patients")
        st.write("This table illustrates the number of patients meeting different inclusion criteria and supports Data Check 2.09 (Less than 80% of patients with a face-to-face encounter during the past 5 years have at least 1 face-to-face diagnosis and 1 vital measurement), Data Check 3.04 (less than 50% of patients with encounters have DIAGNOSIS records) and Data Check 3.05 (less than 50% of patients with encounters have PROCEDURES records). Data check exceptions to 3.04 and 3.05 are highlighted in red and must be corrected; data check exceptions to 2.09 are highlighted in blue and must be explained in the ETL ADD.")
        construct_potential_pools_of_patients(current_schema)
        st.write("")
        
        


with st.expander("REQUIRED CHECK DESCRIPTION" , True):
    st.write("")
    col11, col12 , col13 , col14  = st.columns(4)
    
    
    enc_dict = {
        'AV' : 'Ambulatory_Visit',
        'ED' : 'Emergency_Department',
        'IP' : 'Inpatient',
        'OA' : 'Other_Ambulatory',
        'TH' : 'Telehealth_Encounters'
    }
    
    
    
    with col11:
        current_schema = st.selectbox(
                    "CURRENT CDM SCHEMA",
                     schemas , key="current_schema_2")
    
    with col12:
        last_schema = st.selectbox(
                         "PREVIOUS CDM SCHEMA",
                          schemas , key="previous_schema_2")
    with col13:
        cutoff_date = st.date_input("CUTOFF DATE" , key="cuttof_date_2")
    
    with col14:
        st.write("")
        st.write("")
        generate_btn = st.button("GENERATE" , key="generate_btn_2")
    
    if generate_btn:
        cutoff_date = cutoff_date - relativedelta(years=10)
        

        construct_primary_key_errors_table(current_schema)
        construct_orphan_record_errors_table(current_schema , cutoff_date)
        



        
        
    
with st.expander("CDM DATA PERSISTENCE" , True):
    st.write("")
    col11, col12 , col13 , col14  = st.columns(4)
    
    
    enc_dict = {
        'AV' : 'Ambulatory_Visit',
        'ED' : 'Emergency_Department',
        'IP' : 'Inpatient',
        'OA' : 'Other_Ambulatory',
        'TH' : 'Telehealth_Encounters'
    }
    
    
    
    with col11:
        current_schema = st.selectbox(
                    "CURRENT CDM SCHEMA",
                     schemas)
    
    with col12:
        last_schema = st.selectbox(
                         "PREVIOUS CDM SCHEMA",
                          schemas)
    with col13:
        cutoff_date = st.date_input("CUTOFF DATE")
    
    with col14:
        st.write("")
        st.write("")
        generate_btn = st.button("GENERATE")
    
    if generate_btn:
        st.write("")
        st.write("")
        st.markdown("##### Table VA. Changes in Tables")
        st.write("This table shows changes in key DataMart attributes between the most recent approved DataMart refresh and the current DataMart refresh and supports Data Check 4.01 (more than a 5% decrease in the number of patients or records in a CDM table). Data check exceptions are highlighted in blue and should be investigated and explained in the ETL ADD")
    
        st.write("")
        sql = ''
        crt_sql = ''
        prev_sql = ''
        cutoff_date = cutoff_date - relativedelta(years=10)
        
        for table in table_dict:
            col = table_dict[table]
            if table == 'PROVIDER':
                crt_sql+= " select '" + table +"' as TABLE_NAME , count(*) as CURRENT_RECORD,0 as CURRENT_PATIENTS from "+ str(current_schema)+"."+table+" UNION"
                prev_sql+= " select '" + table +"' as TABLE_NAME , count(*) as PREVIOUS_RECORD, 0 as PREVIOUS_PATIENTS from "+ str(last_schema)+"."+table+" UNION"
            else:    
                if col!='':
                    crt_sql+= " select '" + table +"' as TABLE_NAME , count(*) as CURRENT_RECORD, count(distinct patid) as CURRENT_PATIENTS from "+ str(current_schema)+"."+table+" where "+col+" >= '"+ str(cutoff_date)+"' UNION"
                    prev_sql+= " select '" + table +"' as TABLE_NAME , count(*) as PREVIOUS_RECORD, count(distinct patid) as PREVIOUS_PATIENTS from "+ str(last_schema)+"."+table+" where "+col+" >= '"+ str(cutoff_date)+"' UNION"
                    
                else:
                    crt_sql+= " select '" + table +"' as TABLE_NAME , count(*) as CURRENT_RECORD,count(distinct patid) as CURRENT_PATIENTS from "+ str(current_schema)+"."+table+" UNION"
                    prev_sql+= " select '" + table +"' as TABLE_NAME , count(*) as PREVIOUS_RECORD, count(distinct patid) as PREVIOUS_PATIENTS from "+ str(last_schema)+"."+table+" UNION"
        crt_sql+= crt_sql.strip("UNION")+" "
        prev_sql+= prev_sql.strip("UNION")+" "
        sql = "with crt as ( " + crt_sql + "),old as (" + prev_sql + ") select crt.table_name , old.PREVIOUS_RECORD, crt.CURRENT_RECORD, CASE WHEN old.PREVIOUS_RECORD = 0 THEN NULL ELSE ROUND(((crt.CURRENT_RECORD - old.PREVIOUS_RECORD) / old.PREVIOUS_RECORD::FLOAT) * 100,1) END AS RECORD_CHANGE,old.PREVIOUS_PATIENTS,crt.CURRENT_PATIENTS,CASE WHEN old.PREVIOUS_PATIENTS = 0 THEN NULL ELSE ROUND(((crt.CURRENT_PATIENTS - old.PREVIOUS_PATIENTS) / old.PREVIOUS_PATIENTS::FLOAT) * 100,1) END AS PATIENT_CHANGE from crt , old where crt.table_name=old.table_name"

        result = session.sql(sql).collect()

        # tr = ''
        # for row in result:
        #     record_change_color = 'red' if row['RECORD_CHANGE'] is not None and row['RECORD_CHANGE'] < -5 else 'black'
        #     patient_change_color = 'red' if  row['PATIENT_CHANGE'] is not None   and row['PATIENT_CHANGE'] < -5 else 'black'
        #     tr += f'''<tr><td style="border: 1px solid black; padding: 8px; text-align: center;">{row['TABLE_NAME']}</td><td style="border: 1px solid black; padding: 8px; text-align: center;">{row['PREVIOUS_RECORD']}</td><td style="border: 1px solid black; padding: 8px; text-align: center;">{row['CURRENT_RECORD']}</td><td style="border: 1px solid black; padding: 8px; text-align: center; color:{record_change_color};">{row['RECORD_CHANGE']}</td><td style="border: 1px solid black; padding: 8px; text-align: center;">{row['PREVIOUS_PATIENTS']}</td><td style="border: 1px solid black; padding: 8px; text-align: center;">{row['CURRENT_PATIENTS']}</td><td style="border: 1px solid black; padding: 8px; text-align: center; color:{patient_change_color};">{row['PATIENT_CHANGE']}</td></tr>'''

        # html = f'''<table style="width:100%; border-collapse: collapse; border: 1px solid black;"><tr><th style="border: 1px solid black; padding: 8px; text-align: center; background-color: #4CAF50; color: white;">TABLE_NAME</th><th style="border: 1px solid black; padding: 8px; text-align: center; background-color: #4CAF50; color: white;">PREVIOUS_RECORD</th><th style="border: 1px solid black; padding: 8px; text-align: center; background-color: #4CAF50; color: white;">CURRENT_RECORD</th><th style="border: 1px solid black; padding: 8px; text-align: center; background-color: #4CAF50; color: white;">RECORD_CHANGE</th><th style="border: 1px solid black; padding: 8px; text-align: center; background-color: #4CAF50; color: white;">PREVIOUS_PATIENTS</th><th style="border: 1px solid black; padding: 8px; text-align: center; background-color: #4CAF50; color: white;">CURRENT_PATIENTS</th><th style="border: 1px solid black; padding: 8px; text-align: center; background-color: #4CAF50; color: white;">PATIENT_CHANGE</th></tr>{tr}</table>'''
        # st.markdown(html, unsafe_allow_html=True) 
        html = generate_html_table(result, ['RECORD_CHANGE','PATIENT_CHANGE'])
        st.markdown(html, unsafe_allow_html=True)  
        
               
            
       
        

        
        
        st.write("")
        st.write("")
        st.markdown("##### Table VB. Changes in Selected Encounter Types and Domains.")
        st.write("This table shows changes in key DataMart attributes between the most recent approved DataMart refresh and the current DataMart refresh and supports Data Check 4.02 [more than a 5% decrease in the number of patients or records for diagnosis, procedures, labs or prescriptions during an ambulatory (AV), telehealth (TH), other ambulatory (OA), emergency department (ED), or inpatient (IP) encounter]. Data check exceptions are highlighted in blue and should be investigated and explained in the ETL ADD.")
        st.write("")
        st.write("")
        st.write("")
    
    
        for enc_type in enc_dict:
            enc = enc_dict[enc_type]
            crt_sql = ''
            prev_sql = ''
            for table in ['DIAGNOSIS' , 'PROCEDURES' , 'LAB_RESULT_CM' , 'PRESCRIBING']:
                col = table_dict[table]
                if table in ['DIAGNOSIS' , 'PROCEDURES']:
                    crt_sql+= " select '" + table +"' as " + enc+" , count(*) as CURRENT_RECORD,count(distinct patid) as CURRENT_PATIENTS from "+ str(current_schema)+"."+table+" where "+col+" >= '"+ str(cutoff_date)+"' and ENC_TYPE = '" + enc_type + "' UNION"
                    prev_sql+= " select '" + table +"' as " + enc+" , count(*) as PREVIOUS_RECORD, count(distinct patid) as PREVIOUS_PATIENTS from "+ str(last_schema)+"."+table+" where "+col+" >= '"+ str(cutoff_date)+"' and ENC_TYPE = '" + enc_type + "' UNION"
                else:
                    crt_sql+= " select '" + table +"' as " + enc+" , count(*) as CURRENT_RECORD,count(distinct t.patid) as CURRENT_PATIENTS from "+ str(current_schema)+"."+table+" t join "+ str(current_schema)+".encounter e on t.ENCOUNTERID = e.ENCOUNTERID and t.patid = e.patid  where t."+col+" >= '"+ str(cutoff_date)+"' and e.ENC_TYPE = '" + enc_type + "' and e.ADMIT_DATE >= '" + str(cutoff_date) +"' UNION"
                    prev_sql+= " select '" + table +"' as " + enc+" , count(*) as PREVIOUS_RECORD, count(distinct t.patid) as PREVIOUS_PATIENTS from "+ str(last_schema)+"."+table+" t join "+ str(last_schema)+".encounter e on t.ENCOUNTERID = e.ENCOUNTERID and t.patid = e.patid  where t."+col+" >= '"+ str(cutoff_date)+"' and e.ENC_TYPE = '" + enc_type + "' and e.ADMIT_DATE >= '" + str(cutoff_date) +"' UNION"
                
            crt_sql+= crt_sql.strip("UNION")+" "
            prev_sql+= prev_sql.strip("UNION")+" "
            sql = "with crt as ( " + crt_sql + "),old as (" + prev_sql + ") select crt."+enc+" , old.PREVIOUS_RECORD, crt.CURRENT_RECORD, CASE WHEN old.PREVIOUS_RECORD = 0 THEN NULL ELSE ROUND(((crt.CURRENT_RECORD - old.PREVIOUS_RECORD) / old.PREVIOUS_RECORD::FLOAT) * 100,1) END AS RECORD_CHANGE,old.PREVIOUS_PATIENTS,crt.CURRENT_PATIENTS,CASE WHEN old.PREVIOUS_PATIENTS = 0 THEN NULL ELSE ROUND(((crt.CURRENT_PATIENTS - old.PREVIOUS_PATIENTS) / old.PREVIOUS_PATIENTS::FLOAT) * 100,1) END AS PATIENT_CHANGE from crt , old where crt."+enc+"=old."+enc
            # st.write(sql)
            # st.write(session.sql(sql).collect())
            # df = pd.DataFrame(session.sql(sql).collect())
            # st.dataframe(df, use_container_width=True)  # Adjust height as needed

            html = generate_html_table(session.sql(sql).collect(), ['RECORD_CHANGE','PATIENT_CHANGE'])
            st.markdown(html, unsafe_allow_html=True)  
            

            
            st.write("")
        st.write("")
    
    
        st.markdown("##### Table VC. Changes in Selected Code Types")
        st.write(f"""This table shows changes in key DataMart attributes between the most recent approved DataMart refresh and the current DataMart refresh and supports Data Check
                4.03 (more than a 5% decrease in the number of records or distinct codes for CPT/HCPCS, CVX, ICD10, NDC, or RXCUI codes). The data check is not applied to
                ICD9 codes because these codes will decrease between refreshes because of the 10 year lookback. Data check exceptions are highlighted in blue and should be
                investigated and explained in the ETL ADD.""")
        st.write("")
        st.write("")
    
        sql = f"""
                with crt as (
                select 'DIAGNOSIS' as TABLE_NAME , '09' as CODE , COUNT(*) as CURRENT_RECORD , count(distinct DX) as CURRENT_DISTINCT_CODES 
                from {current_schema}.diagnosis where ADMIT_DATE >= '{cutoff_date}' and DX_TYPE='09' and DX is not null UNION
                select 'DIAGNOSIS' as TABLE_NAME , '10' as CODE , COUNT(*) as CURRENT_RECORD , count(distinct DX) as CURRENT_DISTINCT_CODES 
                from {current_schema}.diagnosis where ADMIT_DATE >= '{cutoff_date}' and DX_TYPE='10' and DX is not null UNION
                select 'PROCEDURES' as TABLE_NAME, '09' as CODE , COUNT(*) as CURRENT_RECORD , count(distinct PX) as CURRENT_DISTINCT_CODES 
                from {current_schema}.procedures where ADMIT_DATE >= '{cutoff_date}' and px_type='09' and PX is not null UNION
                select 'PROCEDURES' as TABLE_NAME, '10' as CODE , COUNT(*) as CURRENT_RECORD , count(distinct PX) as CURRENT_DISTINCT_CODES 
                from {current_schema}.procedures where ADMIT_DATE >= '{cutoff_date}' and px_type='10' and PX is not null UNION
                select 'PROCEDURES' as TABLE_NAME, 'CH' as CODE , COUNT(*) as CURRENT_RECORD , count(distinct PX) as CURRENT_DISTINCT_CODES 
                from {current_schema}.procedures where ADMIT_DATE >= '{cutoff_date}' and px_type='CH' and PX is not null UNION
                select 'PROCEDURES' as TABLE_NAME, 'ND' as CODE , COUNT(*) as CURRENT_RECORD , count(distinct PX) as CURRENT_DISTINCT_CODES 
                from {current_schema}.procedures where ADMIT_DATE >= '{cutoff_date}' and px_type='ND' and PX is not null UNION
                select 'DISPENSING' as TABLE_NAME, 'ND' as CODE , COUNT(*) as CURRENT_RECORD , count(distinct NDC) as CURRENT_DISTINCT_CODES 
                from {current_schema}.dispensing where DISPENSE_DATE >= '{cutoff_date}' and NDC is not null UNION
                select 'IMMUNIZATION' as TABLE_NAME, 'CH' as CODE , COUNT(*) as CURRENT_RECORD , count(distinct VX_CODE) as CURRENT_DISTINCT_CODES 
                from {current_schema}.IMMUNIZATION where VX_ADMIN_DATE >= '{cutoff_date}' and VX_CODE_TYPE='CH' and VX_CODE is not null UNION
                select 'IMMUNIZATION' as TABLE_NAME, 'CX' as CODE , COUNT(*) as CURRENT_RECORD , count(distinct VX_CODE) as CURRENT_DISTINCT_CODES 
                from {current_schema}.IMMUNIZATION where VX_ADMIN_DATE >= '{cutoff_date}' and VX_CODE_TYPE='CX' and VX_CODE is not null UNION
                select 'IMMUNIZATION' as TABLE_NAME, 'ND' as CODE , COUNT(*) as CURRENT_RECORD , count(distinct VX_CODE) as CURRENT_DISTINCT_CODES 
                from {current_schema}.IMMUNIZATION where VX_ADMIN_DATE >= '{cutoff_date}' and VX_CODE_TYPE='ND' and VX_CODE is not null UNION
                select 'IMMUNIZATION' as TABLE_NAME, 'RX' as CODE , COUNT(*) as CURRENT_RECORD , count(distinct VX_CODE) as CURRENT_DISTINCT_CODES 
                from {current_schema}.IMMUNIZATION where VX_ADMIN_DATE >= '{cutoff_date}' and VX_CODE_TYPE='RX' and VX_CODE is not null UNION
                select 'MED_ADMIN' as TABLE_NAME, 'ND' as CODE , COUNT(*) as CURRENT_RECORD , count(distinct MEDADMIN_CODE) as CURRENT_DISTINCT_CODES 
                from {current_schema}.MED_ADMIN where MEDADMIN_START_DATE >= '{cutoff_date}' and MEDADMIN_TYPE='ND' and MEDADMIN_CODE is not null UNION
                select 'MED_ADMIN' as TABLE_NAME, 'RX' as CODE , COUNT(*) as CURRENT_RECORD , count(distinct MEDADMIN_CODE) as CURRENT_DISTINCT_CODES 
                from {current_schema}.MED_ADMIN where MEDADMIN_START_DATE >= '{cutoff_date}' and MEDADMIN_TYPE='RX' and MEDADMIN_CODE is not null UNION
                select 'PRESCRIBING' as TABLE_NAME, 'RX' as CODE , COUNT(*) as CURRENT_RECORD , count(distinct RXNORM_CUI) as CURRENT_DISTINCT_CODES 
                from {current_schema}.PRESCRIBING where RX_ORDER_DATE >= '{cutoff_date}' and RXNORM_CUI is not null
                ) ,
                old as (
                select 'DIAGNOSIS' as TABLE_NAME , '09' as CODE , COUNT(*) as PREVIOUS_RECORD , count(distinct DX) as PREVIOUS_DISTINCT_CODES 
                from {last_schema}.diagnosis where ADMIT_DATE >= '{cutoff_date}' and DX_TYPE='09' and DX is not null UNION
                select 'DIAGNOSIS' as TABLE_NAME , '10' as CODE , COUNT(*) as PREVIOUS_RECORD , count(distinct DX) as PREVIOUS_DISTINCT_CODES 
                from {last_schema}.diagnosis where ADMIT_DATE >= '{cutoff_date}' and DX_TYPE='10' and DX is not null UNION
                select 'PROCEDURES' as TABLE_NAME, '09' as CODE , COUNT(*) as PREVIOUS_RECORD , count(distinct PX) as PREVIOUS_DISTINCT_CODES 
                from {last_schema}.procedures where ADMIT_DATE >= '{cutoff_date}' and px_type='09' and PX is not null UNION
                select 'PROCEDURES' as TABLE_NAME, '10' as CODE , COUNT(*) as PREVIOUS_RECORD , count(distinct PX) as PREVIOUS_DISTINCT_CODES 
                from {last_schema}.procedures where ADMIT_DATE >= '{cutoff_date}' and px_type='10' and PX is not null UNION
                select 'PROCEDURES' as TABLE_NAME, 'CH' as CODE , COUNT(*) as PREVIOUS_RECORD , count(distinct PX) as PREVIOUS_DISTINCT_CODES 
                from {last_schema}.procedures where ADMIT_DATE >= '{cutoff_date}' and px_type='CH' and PX is not null UNION
                select 'PROCEDURES' as TABLE_NAME, 'ND' as CODE , COUNT(*) as PREVIOUS_RECORD , count(distinct PX) as PREVIOUS_DISTINCT_CODES 
                from {last_schema}.procedures where ADMIT_DATE >= '{cutoff_date}' and px_type='ND' and PX is not null UNION
                select 'DISPENSING' as TABLE_NAME, 'ND' as CODE , COUNT(*) as PREVIOUS_RECORD , count(distinct NDC) as PREVIOUS_DISTINCT_CODES 
                from {last_schema}.dispensing where DISPENSE_DATE >= '{cutoff_date}' and NDC is not null UNION
                select 'IMMUNIZATION' as TABLE_NAME, 'CH' as CODE , COUNT(*) as PREVIOUS_RECORD , count(distinct VX_CODE) as PREVIOUS_DISTINCT_CODES 
                from {last_schema}.IMMUNIZATION where VX_ADMIN_DATE >= '{cutoff_date}' and VX_CODE_TYPE='CH' and VX_CODE is not null UNION
                select 'IMMUNIZATION' as TABLE_NAME, 'CX' as CODE , COUNT(*) as PREVIOUS_RECORD , count(distinct VX_CODE) as PREVIOUS_DISTINCT_CODES 
                from {last_schema}.IMMUNIZATION where VX_ADMIN_DATE >= '{cutoff_date}' and VX_CODE_TYPE='CX' and VX_CODE is not null UNION
                select 'IMMUNIZATION' as TABLE_NAME, 'ND' as CODE , COUNT(*) as PREVIOUS_RECORD , count(distinct VX_CODE) as PREVIOUS_DISTINCT_CODES 
                from {last_schema}.IMMUNIZATION where VX_ADMIN_DATE >= '{cutoff_date}' and VX_CODE_TYPE='ND' and VX_CODE is not null UNION
                select 'IMMUNIZATION' as TABLE_NAME, 'RX' as CODE , COUNT(*) as PREVIOUS_RECORD , count(distinct VX_CODE) as PREVIOUS_DISTINCT_CODES 
                from {last_schema}.IMMUNIZATION where VX_ADMIN_DATE >= '{cutoff_date}' and VX_CODE_TYPE='RX' and VX_CODE is not null UNION
                select 'MED_ADMIN' as TABLE_NAME, 'ND' as CODE , COUNT(*) as PREVIOUS_RECORD , count(distinct MEDADMIN_CODE) as PREVIOUS_DISTINCT_CODES 
                from {last_schema}.MED_ADMIN where MEDADMIN_START_DATE >= '{cutoff_date}' and MEDADMIN_TYPE='ND' and MEDADMIN_CODE is not null UNION
                select 'MED_ADMIN' as TABLE_NAME, 'RX' as CODE , COUNT(*) as PREVIOUS_RECORD , count(distinct MEDADMIN_CODE) as PREVIOUS_DISTINCT_CODES 
                from {last_schema}.MED_ADMIN where MEDADMIN_START_DATE >= '{cutoff_date}' and MEDADMIN_TYPE='RX' and MEDADMIN_CODE is not null UNION
                select 'PRESCRIBING' as TABLE_NAME, 'RX' as CODE , COUNT(*) as PREVIOUS_RECORD , count(distinct RXNORM_CUI) as PREVIOUS_DISTINCT_CODES 
                from {last_schema}.PRESCRIBING where RX_ORDER_DATE >= '{cutoff_date}' and RXNORM_CUI is not null
                )
                select crt.TABLE_NAME ,
                       crt.CODE , 
                       old.PREVIOUS_RECORD , 
                       crt.CURRENT_RECORD , 
                       CASE 
                        WHEN old.PREVIOUS_RECORD = 0 
                        THEN NULL 
                        ELSE ROUND(((crt.CURRENT_RECORD - old.PREVIOUS_RECORD) / old.PREVIOUS_RECORD::FLOAT) * 100,1) 
                       END AS RECORD_CHANGE,
                       old.PREVIOUS_DISTINCT_CODES,
                       crt.CURRENT_DISTINCT_CODES,
                       CASE 
                        WHEN old.PREVIOUS_DISTINCT_CODES = 0 
                         THEN NULL 
                        ELSE ROUND(((crt.CURRENT_DISTINCT_CODES - old.PREVIOUS_DISTINCT_CODES) / old.PREVIOUS_DISTINCT_CODES::FLOAT) * 100,1) 
                       END AS DISTINCT_CODES_CHANGE
                from crt,old where crt.TABLE_NAME = old.TABLE_NAME and crt.CODE = old.CODE order by crt.TABLE_NAME, crt.CODE;
                """
        # df = pd.DataFrame(session.sql(sql).collect())
        # st.dataframe(df, use_container_width=True , height=570)  # Adjust height as needed

        html = generate_html_table(session.sql(sql).collect(), ['RECORD_CHANGE','DISTINCT_CODES_CHANGE'])
        st.markdown(html, unsafe_allow_html=True)  
        
        st.write("")


with st.expander("CDM TABLE TREND ANALYSIS" , True):
    st.write("")
    col11, col12 , col13 , col14,col15   = st.columns(5)
    with col11:
        exploration_schema = st.selectbox(
                    "SELECT CDM SCHEMA",
                     schemas)

    with col12:
        tables = [table for table in table_dict if table_dict[table]!='']
        filter_table = st.selectbox(
                    "SELECT CDM TABLE",
                     tables)

    with col13:
        
        end_date = datetime.today()
        start_date = end_date - relativedelta(years=10)
        
        # Create a date range picker
        date_range = st.date_input(
            "SELECT A DATE RANGE",
            value=(start_date, end_date)
        )

    with col14:
        group_by = st.selectbox(
                    "DISTRIBUTION BY",
                     ['YEARLY','MONTHLY','DAILY'])
    
    with col15:
        st.write("")
        st.write("")
        explore_btn = st.button("EXPLORE")


    if explore_btn:
        st.write("")
        filter_column = table_dict[filter_table]
        start_date, end_date = date_range

        if group_by=='MONTHLY':
            sql = "SELECT TO_CHAR("+filter_column+",'YYYY-MM') AS DATE, COUNT(*) CT FROM "+exploration_schema+"."+filter_table+" WHERE "+filter_column +">='"+str(start_date)+"' and "+filter_column+"<='"+str(end_date)+"' GROUP BY TO_CHAR("+filter_column+",'YYYY-MM') ORDER BY DATE"
            pt_sql = "SELECT TO_CHAR("+filter_column+",'YYYY-MM') AS DATE, COUNT(distinct patid) CT FROM "+exploration_schema+"."+filter_table+" WHERE "+filter_column +">='"+str(start_date)+"' and "+filter_column+"<='"+str(end_date)+"' GROUP BY TO_CHAR("+filter_column+",'YYYY-MM') ORDER BY DATE"
        elif group_by== 'YEARLY':
            sql = "SELECT TO_CHAR("+filter_column+",'YYYY') AS DATE, COUNT(*) CT FROM "+exploration_schema+"."+filter_table+" WHERE "+filter_column +">='"+str(start_date)+"' and "+filter_column+"<='"+str(end_date)+"' GROUP BY TO_CHAR("+filter_column+",'YYYY') ORDER BY DATE"
            pt_sql = "SELECT TO_CHAR("+filter_column+",'YYYY') AS DATE, COUNT(distinct patid) CT FROM "+exploration_schema+"."+filter_table+" WHERE "+filter_column +">='"+str(start_date)+"' and "+filter_column+"<='"+str(end_date)+"' GROUP BY TO_CHAR("+filter_column+",'YYYY') ORDER BY DATE"
        else:
            sql = "SELECT "+filter_column+" AS DATE, COUNT(*) CT FROM "+exploration_schema+"."+filter_table+" WHERE "+filter_column +">='"+str(start_date)+"' and "+filter_column+"<='"+str(end_date)+"' GROUP BY "+filter_column+" ORDER BY DATE"
            pt_sql = "SELECT "+filter_column+" AS DATE, COUNT(distinct patid) CT FROM "+exploration_schema+"."+filter_table+" WHERE "+filter_column +">='"+str(start_date)+"' and "+filter_column+"<='"+str(end_date)+"' GROUP BY "+filter_column+" ORDER BY DATE"
        result =session.sql(sql).collect()
        # st.write(result)

        df = pd.DataFrame(result)
        
        # Convert DATE to datetime
        df['DATE'] = pd.to_datetime(df['DATE'])
        df.set_index('DATE', inplace=True)
        
        st.write("")
        st.markdown("#### RECORD DISTRIBUTION BY "+filter_column)
        st.bar_chart(df)


        result =session.sql(pt_sql).collect()
        # st.write(result)

        df = pd.DataFrame(result)
        
        # Convert DATE to datetime
        df['DATE'] = pd.to_datetime(df['DATE'])
        df.set_index('DATE', inplace=True)
        
        st.write("")
        st.write("")
        st.markdown("#### PATIENT DISTRIBUTION BY "+filter_column)
        st.bar_chart(df)

        
        
       
        
        
        
        

    

    
            
            
        
    
