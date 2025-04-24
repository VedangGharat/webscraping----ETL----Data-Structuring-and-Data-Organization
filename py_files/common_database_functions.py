
import pysolr
import mysql.connector
from mysql.connector import Error
import os
import sys
import re
import nltk
import time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime
from itertools import zip_longest
#importing connections here
import sys
from main import connections
mysqlcredentials,solr_connection,cred_json= connections()
import openai
import json
import dateparser
import pytz
import random
import requests
UTC = pytz.utc
from spacy.matcher import PhraseMatcher
import en_core_web_sm
from nltk.corpus import stopwords
stop = stopwords.words('english')
import logging
import logging_config

logger = logging.getLogger()



# Function to generate a random datetime for today's date within given IST hours
def get_random_datetime_ist(hours_start, hours_end):
    # Get today's date
    today = datetime.now(pytz.timezone('Asia/Kolkata')).date()
    
    # Generate a random time within the specified hours
    random_hour = random.randint(hours_start, hours_end - 1)
    random_minute = random.randint(0, 59)
    random_second = random.randint(0, 59)
    
    # Create a datetime object with today's date and the random time
    random_time_ist = datetime(year=today.year, month=today.month, day=today.day,
                               hour=random_hour, minute=random_minute, second=random_second)
    
    random_time_ist = pytz.timezone('Asia/Kolkata').localize(random_time_ist)
    # Convert the IST time to UTC
    random_time_utc = random_time_ist.astimezone(pytz.utc)
    
    return random_time_utc

def insert_contact_jobdescription(values, id):
    url = "https://apps.jobsnprofiles.com/jobs/insert_contact_jobdescription"
    contactname = values['contactemail'].split("@")[0].strip().title()
    data = {
        "contact_name": contactname,
        "contact_email": values['contactemail'],
        "contact_phone": values['contactno'],
        "designation": "Recruiter",
        "city": values['city'],
        "state": values['statename'],
        "country": "US",
        "linkedin_url": "0",
        "company_id": values['companyid'],
        "job_id": id
        }
    response = requests.post(url, json=data)
    return response

def get_todays_joblinks():
    mysql_connection = mysql.connector.connect(**mysqlcredentials)
    cursor = mysql_connection.cursor()

    get_jobs_query = "select joblink,id from jobsnprofiles_2022.jnp_jobs where joblink != '' and date(created) =curdate()"
    cursor.execute(get_jobs_query)
    get_today_jobs_t =cursor.fetchall()
    get_today_jobs = {element[0]:element[1] for element in get_today_jobs_t}
    
    if (mysql_connection.is_connected()):
        mysql_connection.close()
        cursor.close()
        print("MySQL mysql_connection is closed")

    return get_today_jobs

def insertMysql_live(df_jobs):
    get_today_jobs = get_todays_joblinks()
    '''Insert the scraped data into Databse after preprocessing.'''
    
    df_jobs['jobtypeid'] = df_jobs['jobtypeid'].astype('str')
    df_jobs['stateid'] = df_jobs['stateid'].astype('str')
    df_jobs['country'] = '233'
    df_jobs['salaryrangetype'] = df_jobs['salaryrangetype'].astype('str')
    df_jobs['cityid'] = df_jobs['cityid'].astype('str')
    df_jobs['workpermitid'] = df_jobs['workpermitid'].astype('str')
    df_jobs['zipcodeid'] = df_jobs['zipcode'].astype('str')
    df_jobs['longitude'] = df_jobs['longitude'].astype('str')
    df_jobs['latitude'] = df_jobs['latitude'].astype('str')
    df_jobs['day_ordering'] = df_jobs['day_ordering'].astype('str')
    df_jobs['id'] = 0
    df_jobs['id'] = df_jobs['id'].astype('Int64')
    if not df_jobs.empty:
        try:
            mysql_connection = mysql.connector.connect(**mysqlcredentials)
            cursor = mysql_connection.cursor()

            for keys,values in df_jobs.iterrows(): 
                if values['joblink'] not in get_today_jobs.keys():
                    if values['process_type'] == 'jnp':
                        current_time_ist = datetime.now(pytz.timezone('Asia/Kolkata')).time()

                        random_timestamp_utc = get_random_datetime_ist(8, current_time_ist.hour)
                        created = random_timestamp_utc.strftime('%Y-%m-%d %H:%M:%S') 
                        modified = random_timestamp_utc.strftime('%Y-%m-%d %H:%M:%S') 
                    else:
                        created = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
                        modified = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
                    try:
                        mySql_insert_query = "INSERT INTO jobsnprofiles_2022.jnp_jobs(uid, companyid,  title,  alias, jobtype, qualifications, prefferdskills,prefferd_skillnames, country, state, created,modified, city,zipcode, workpermit, jobid,longitude, latitude, joblink, jobapplylink, salaryrangefrom,salaryrangetype,jobcategory,requiredtravel,posted_flag,source,ai_skills,technologies_used,solr_search_string,contactemail,is_remote,source_name,day_ordering,company_name,industry_type,cityname,statename) "\
                            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"                                                                                                                            

                        records = (values['uid'], values['companyid'], values['title'], values['alias'], values['jobtypeid'], values['qualifications'], values['prefferdskills'],values['prefferd_skillnames'], values['country'], values['stateid'], created, modified, values['cityid'],values['zipcodeid'],values['workpermitid'], values['jobid'],values['longitude'], values['latitude'], values['joblink'], values['jobapplylink'],values['salaryrangefrom'],values['salaryrangetype'],values['jobcategory'],values['requiredtravel'],values['posted_flag'],values['source'],values['ai_skills'],values['technologies_used'],values['solr_search_string'],values['contactemail'],values['is_remote'],values['source_name'],values['day_ordering'],values['company_name'],values['industry_type'],values['city_name'],values['state_name'])           
                        cursor.execute(mySql_insert_query, records)
                        mysql_connection.commit()
                        insert_id =  cursor.lastrowid
                        df_jobs.at[keys,'id'] = int(insert_id)
                        df_jobs.at[keys,'created'] = created #pd.to_datetime(created)
                        df_jobs.at[keys,'modified'] = created#pd.to_datetime(created)
                        print('inserted id ---',insert_id)
                        mysql_summary_insert = "INSERT INTO jobsnprofiles_2022.jnp_job_description(job_id,description) VALUES(%s,%s)" 
                        summary_records =  (insert_id,values['job_description'])           
                        cursor.execute(mysql_summary_insert, summary_records)
                        mysql_connection.commit()
                        # try:
                        #     if values['contactemail'] != "" and values['contactemail'] != None:
                        #         res = insert_contact_jobdescription(values, insert_id)
                        # except Exception as e:
                        #     pass
                    except Exception as e:
                        print('error-----',str(e))
                        # Delete the current row from the dataframe
                        df_jobs.drop(keys, inplace=True)
                        mysql_connection = mysql.connector.connect(**mysqlcredentials)
                        cursor = mysql_connection.cursor()
                else:
                    print('job already present')
                    df_jobs.drop(keys, inplace=True)
            cursor.close()
            return df_jobs
        except mysql.connector.Error as error:
            logger.error("An error occurred--", exc_info=True)
            print("Failed to insert record into table: {}".format(error))
            return df_jobs

        finally:
            if (mysql_connection.is_connected()):
                mysql_connection.close()
                cursor.close()
                print("MySQL mysql_connection is closed")
                
    elif df_jobs.empty:
        return df_jobs

def pre_data_insert(df_jobs):
    df_jobs['jobtypeid'] = df_jobs['jobtypeid'].astype('str')
    df_jobs['stateid'] = df_jobs['stateid'].astype('str')
    df_jobs['country'] = '233'
    df_jobs['salaryrangetype'] = df_jobs['salaryrangetype'].astype('str')
    df_jobs['cityid'] = df_jobs['cityid'].astype('str')
    df_jobs['workpermitid'] = df_jobs['workpermitid'].astype('str')
    df_jobs['zipcodeid'] = df_jobs['zipcode'].astype('str')
    df_jobs['longitude'] = df_jobs['longitude'].astype('str')
    df_jobs['latitude'] = df_jobs['latitude'].astype('str')
    df_jobs['jobs_order'] =df_jobs['jobs_order'].astype('str')
    df_jobs['id'] = 0
    df_jobs['id'] = df_jobs['id'].astype('Int64')
    if not df_jobs.empty:
        try:
            mysql_connection = mysql.connector.connect(**mysqlcredentials)
            cursor = mysql_connection.cursor()

            for keys,values in df_jobs.iterrows():
                current_time = datetime.now().time()

                random_timestamp_utc = get_random_datetime_ist(6, current_time.hour)
                #print("Random Timestamp (UTC) for today's date between 6AM-6PM IST:", random_timestamp_utc.strftime('%Y-%m-%d %H:%M:%S'))
                created = random_timestamp_utc.strftime('%Y-%m-%d %H:%M:%S') #datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
                modified = random_timestamp_utc.strftime('%Y-%m-%d %H:%M:%S') #datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
                try:          
                    mySql_insert_query = "INSERT INTO jnp_reports_db.jnp_jobs_pre_insert(uid,companyid,company_name,company_logo,title,alias,jobcategory,jobtypeid,jobtype_name,salaryrangefrom,salaryrangeto,salaryrangetype,is_remote,country,stateid,cityid,zipcode,city_name,state_name,shortregion,longitude,latitude,contactname,contactphone,contactemail,created,modified,status,workpermitid,workpermit_name,requiredtravel,jobid,map_enable,joblink,jobapplylink,posted_flag,source,source_name,prefferdskills,prefferd_skillnames,ai_skills,technologies_used,solr_search_string,day_ordering,industry_type,workmode,job_description,qualifications) "\
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"                                                                                                                            
                    records = (values['uid'], values['companyid'], values['companyname'],values['logo_url'], values['title'], values['alias'],values['jobcategory'], values['jobtypeid'],values['jobtypename'],values['salaryrangefrom'],values['salaryrangeto'],values['salaryrangetype'],values['is_remote'],  values['country'], values['stateid'], values['cityid'],values['zipcode'],values['cityname'],values['statename'],values['shortregion'],values['longitude'], values['latitude'],values['contactname'],values['contactphone'],values['contactemail'],created, modified,values['status'],values['workpermitid'],values['workpermitname'],values['requiredtravel'],values['jobid'],values['mapenable'],values['joblink'],values['jobapplylink'],values['posted_flag'],values['source'],values['source_name'],values['preferredskills'],values['preferredskillsName'],values['ai_skills'],values['technologies_used'],values['solr_search_str'],values['jobs_order'],values['industry_type'],values['workmode'],values['description'],values['qualifications'])
                               
                    cursor.execute(mySql_insert_query, records)
                    mysql_connection.commit()
                    insert_id =  cursor.lastrowid
                    print('pre inserted id ---',insert_id)
                    
                except Exception as e:
                    print('error-----',str(e))
                    logger.error("An error occurred--", exc_info=True)

                    # Delete the current row from the dataframe
                    #df_jobs.drop(keys, inplace=True)
            
            cursor.close()
            return df_jobs
        except mysql.connector.Error as error:
            logger.error("An error occurred--", exc_info=True)

            print("Failed to insert record into table: {}".format(error))
            return df_jobs

        finally:
            if (mysql_connection.is_connected()):
                mysql_connection.close()
                cursor.close()
                print("MySQL mysql_connection is closed")
                
    elif df_jobs.empty:
        return df_jobs
    

def get_client_companies():
    client_comp_conn = mysql.connector.connect(**mysqlcredentials)
    cursor = client_comp_conn.cursor()
    client_comp_query = "select client_name,domain_name,database_name,created,result from jnp_reports_db.client_companies_python"
    cursor.execute(client_comp_query)
    client_Comp_Data = cursor.fetchall()
    print("Total number of rows in Companies Table is: ", cursor.rowcount)
    solrClient_comp = {element[0].lower():[element[1],element[2],element[3],element[4]] for element in client_Comp_Data if element[0]}

    if (client_comp_conn.is_connected()):
        client_comp_conn.close()
        cursor.close()
        print("MySQL connection is Closed")

    return solrClient_comp


def insert_client_comp(companyname,source_name):
    client_comp_conn = mysql.connector.connect(**mysqlcredentials)
    cursor = client_comp_conn.cursor()
    insert_client = "Insert into jnp_reports_db.client_companies_python (client_name,source) values (%s,%s)"
    vals = (companyname,source_name)
    cursor.execute(insert_client,vals)
    client_comp_conn.commit()

    if (client_comp_conn.is_connected()):
        client_comp_conn.close()
        cursor.close()
        print("MySQL connection is Closed")

def get_predata_jobs():
    predata_conn = mysql.connector.connect(**mysqlcredentials)
    cursor = predata_conn.cursor()
    jobs_get_query = "select * from jnp_reports_db.jnp_jobs_pre_insert"
    cursor.execute(jobs_get_query)
    pre_jobsdata = cursor.fetchall()
    print("Total number of rows in Companies Table is: ", cursor.rowcount)

    if (predata_conn.is_connected()):
        predata_conn.close()
        cursor.close()
        print("MySQL connection is Closed")
    
    return pre_jobsdata


def insert_report(companyname,jobs_count):
    report_db_conn = mysql.connector.connect(**mysqlcredentials)
    cursor = report_db_conn.cursor()

    #check column exist or not
    check_column_query = "SHOW COLUMNS FROM jnp_reports_db.jobs_report1 LIKE %s"
    cursor.execute(check_column_query, (companyname,))
    existing_column = cursor.fetchone()

    if existing_column:
        print('')
    else:
        add_column_query = f"ALTER TABLE jnp_reports_db.jobs_report1 ADD COLUMN {companyname} INT(11) NULL DEFAULT 0;"
        # Replace 'DATATYPE' with the actual data type for your column
        cursor.execute(add_column_query)
        print('column created---',companyname)

    # Get the current UTC time with timezone localization
    current_utc_time = pd.to_datetime('now', utc=True)

    # Convert to Asia/Kolkata timezone
    indian_time = current_utc_time.tz_convert('Asia/Kolkata')

    # Format the Indian time to a string: YYYY-MM-DD
    today_date = indian_time.strftime('%Y-%m-%d')
    last_modified = indian_time.strftime('%Y-%m-%d %H:%M:%S')
    search_date = "select scraped_date from jnp_reports_db.jobs_report1 where scraped_date = '{}'".format(today_date)
    cursor.execute(search_date)
    get_date = cursor.fetchall()
    if len(get_date) != 0:
        if companyname == 'Dice' or companyname == 'linkedin':
            update_report = "update jnp_reports_db.jobs_report1 set {} = {} + %s, last_modified = %s where scraped_date = %s".format(companyname,companyname)
        else:
            update_report = "update jnp_reports_db.jobs_report1 set {} =%s ,last_modified = %s where scraped_date = %s".format(companyname)
        report_vals = (jobs_count,last_modified, today_date)
        cursor.execute(update_report,report_vals)
        report_db_conn.commit()
        print('jobs count updated ---',companyname)
    else:             
        create_date = "INSERT INTO jnp_reports_db.jobs_report1(scraped_date,{}) VALUES(%s,%s)".format(companyname)
        report_vals = (today_date,jobs_count)
        cursor.execute(create_date,report_vals)
        report_db_conn.commit()
        print('jobs count created')

    if (report_db_conn.is_connected()):
        report_db_conn.close()
        cursor.close()
        #print("MySQL connection is Closed")


def getMySqlData():
    '''Fetch data from Database and represnt in Id"s.'''
    try:
        mysql_connection = mysql.connector.connect(**mysqlcredentials)
        cursor = mysql_connection.cursor()
        sqlCitiesQuery = "select citi.id,citi.name,citi.state_code,citi.stateid,state.name,state.id,citi.countryid from jobsnprofiles_2022.jnp_cities as citi left join jobsnprofiles_2022.jnp_states as state on citi.stateid = state.id where citi.countryid = 233"
        cursor.execute(sqlCitiesQuery)
        sqlCityData = cursor.fetchall()
        print("Total number of rows in City table is: ", cursor.rowcount)
        solrCity = {element[0]: [element[1].lower(),element[2],element[3],element[4].lower(),element[5],element[6]] for element in sqlCityData}
        solrcitysearch = {element[1].lower(): element[0] for element in sqlCityData}
        solrCountryId = {element[1]:element[5] for element in sqlCityData if element[1]}
                         
        sqlStatesQuery = "select id,shortRegion,name from jnp_states where countryid = 233"
        cursor.execute(sqlStatesQuery)
        sqlStateData = cursor.fetchall()
        print("Total number of rows in States table is: ", cursor.rowcount)
        solrShortregion = {element[1].lower():[element[0],element[2]] for element in sqlStateData if element[0]}
        solrStateName = {element[2].lower():[element[0], element[1]]  for element in sqlStateData if element[0]}
        
        sqlCompaniesQuery = "select id,jnp_uid,name,logo_url,logofilename from jnp_visa_companies.jnp_companies"
        cursor.execute(sqlCompaniesQuery)
        sqlCompanyData = cursor.fetchall()
        print("Total number of rows in Companies Table is: ", cursor.rowcount)
        solrCompany = {int(element[0]):[element[1],element[2],element[3],element[4]] for element in sqlCompanyData if element[0]}

        sqlJobtypeQuery = "select id,title from jnp_job_types"
        cursor.execute(sqlJobtypeQuery)
        sqlJobtypeData = cursor.fetchall()
        print("Total number of rows in Job Type Table is: ", cursor.rowcount)
        solrJobtypeData = {element[1].lower(): str(element[0]) for element in sqlJobtypeData if element[1]}
        
        sqlsalaryrangetypeQuery = "select id,title from jnp_salaryrangetypes"
        cursor.execute(sqlsalaryrangetypeQuery)
        sqlsalaryrangetypeData = cursor.fetchall()
        print("Total number of rows in salaryrangetype table is: ", cursor.rowcount)
        solrSalaryRangetype = {element[1].lower():element[0] for element in sqlsalaryrangetypeData if element[1]}
        
        sqlSkillQuery = "select * from jnp_skills"
        cursor.execute(sqlSkillQuery)
        Skils = cursor.fetchall()
        print("Total number of rows in skills is: ", cursor.rowcount)
        jobskill_dict = dict()
        for row in Skils:
            jobskill_dict[row[1].lower()] = row[0]
        
        sqlWorkpermitQuery = "select id,type_name from jnp_workpermit"
        cursor.execute(sqlWorkpermitQuery)
        workpermits = cursor.fetchall()
        print("Total number of rows in workpermit is: ", cursor.rowcount)
        solrworkpermitData = {element[1]: str(element[0]) for element in workpermits}

        sqlzipcodeQuery = "select city,state,zipcode,latitude,longitude from jnp_zipcode_county"
        cursor = mysql_connection.cursor()
        cursor.execute(sqlzipcodeQuery)
        sqlzipcodeData = cursor.fetchall()
        solrzipcodematch = {element[2]:[element[3],element[4]] for element in sqlzipcodeData if element[2]}
        print("Total number of rows in zipcode_county table is: ", cursor.rowcount)
    except Error as e:
        print("Error reading data from MySQL table", e)
    finally:
        if (mysql_connection.is_connected()):
            mysql_connection.close()
            cursor.close()
            print("MySQL mysql_connection is Closed")
            return solrCity,solrcitysearch, solrShortregion, solrStateName, solrCountryId, solrCompany,solrSalaryRangetype,jobskill_dict,solrJobtypeData, solrworkpermitData,sqlzipcodeData,solrzipcodematch
        

def get_union_of_companies():

    try:
        mysql_connection = mysql.connector.connect(**mysqlcredentials)
        cursor = mysql_connection.cursor()
        sqlCompaniesQuery = "SELECT name,jnp_uid,id,logo_url,logofilename,domain_name FROM jnp_visa_companies.jnp_companies UNION ALL SELECT ac.name,jc.jnp_uid,ac.companyid,jc.logo_url,jc.logofilename, jc.domain_name FROM jnp_visa_companies.jnp_alternative_companies ac LEFT JOIN jnp_visa_companies.jnp_companies jc ON ac.companyid = jc.id;"
        cursor.execute(sqlCompaniesQuery)
        sqlCompanyData = cursor.fetchall()
        print("Total number of rows in Companies Table is: ", cursor.rowcount)
        solrCompany_union = {element[0].lower():[element[1],element[2],element[3],element[4]] for element in sqlCompanyData if element[0]}

    except Error as e:
        print("Error reading data from MySQL table", e)
    finally:
        if (mysql_connection.is_connected()):
            mysql_connection.close()
            cursor.close()
            print("MySQL mysql_connection is Closed")
            
        return solrCompany_union
    

def get_duplicate_joblinks(cursor,table_name):
    query = f"SELECT joblink ,count(*) as count_num FROM jnp_reports_db.{table_name} where joblink != '' group by joblink having count_num>1 order by count_num desc"
    
    cursor.execute(query)
    return cursor.fetchall()

def del_duplicates_in_predata(table_name):
    conn = mysql.connector.connect(**mysqlcredentials)
    cursor = conn.cursor()

    try:
        duplicates = get_duplicate_joblinks(cursor,table_name)
        print('no of duplicates found in pre_data table ----',len(duplicates))
        for joblink, _ in duplicates:
            # Get all IDs for the given joblink
            cursor.execute(f"SELECT id FROM jnp_reports_db.{table_name} WHERE joblink = '{joblink}'")
            ids = [row[0] for row in cursor.fetchall()]

            # Keep the first ID and delete the rest
            if len(ids) > 1:
                ids_to_delete = ids[1:]
                format_strings = ','.join(['%s'] * len(ids_to_delete))
                if len(ids_to_delete) ==1:
                    query = f"DELETE FROM jnp_reports_db.{table_name} WHERE id = {ids_to_delete[0]}"
                    cursor.execute(query)
                    # Commit the changes
                    conn.commit()
                else:
                    query = f"DELETE FROM jnp_reports_db.{table_name} WHERE id IN {tuple(ids_to_delete)}"
                    cursor.execute(query)
                    # Commit the changes
                    conn.commit()
        
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def get_prejobdata(table_name):
    predata_conn = mysql.connector.connect(**mysqlcredentials)
    query_to_read = f"SELECT * FROM jnp_reports_db.{table_name} where date(created) = curdate() ORDER BY RAND();"

    df = pd.read_sql(query_to_read, predata_conn)

    predata_conn.close()

    return df

def turncate_data(table_name):
    try:
        conn = mysql.connector.connect(**mysqlcredentials)
        cursor = conn.cursor()
        truncate_query = f"TRUNCATE TABLE jnp_reports_db.{table_name}"

        # Executing the truncate query
        cursor.execute(truncate_query)
        conn.commit()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Closing the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def move_zero_sites():

    # List of company names
    companies = [
        "SoftPath", "SGA", "Compunnel", "Presidio", "SNI", "Michael_Page", "Judge",
        "Mitchellmartin", "TechUSA", "Pridestaff", "IDR_Inc", "Nesco_resource", 
        "CSI", "Hightechpros", "Flexton", "Bluestonestaffing", "OXFORD", "Brilliant", 
        "EDIStaff", "Cbsinfosys", "Accrue_partners", "ITech", "TriCom_Technical_Services", 
        "Motion_Recruitment", "The_Reserves_Network", "CSS_Tec", "Onward_Search", 
        "Incendia_partners", "Experis", "Ilabour360", "Burtch_works", "CDW", 
        "Addison_Group", "IronSystem", "Nextracker", "Sligosoft", "Hydrogengroup", 
        "Lytx", "kaseya", "FastSwitch", "MackinTalent", "DFin", "Thousandeyes", 
        "tandym", "BakerTilly", "KestaIT", "Artmacsoft", "Beaconhill", "Mondo", 
        "ReqRoute", "CTP_Consulting", "Verisign", "Aldebaran", "Nesco_resources", 
        "Brio_group", "Williams", "Ayalocums", "sbhfashion", "ManpowerSandiego", 
        "Ilocatum", "Pendaaiken", "Tristaff", "MasonFrank", "Rockwood", "AdvancedTech", 
        "AMNhealthcare", "24seventalent", "Talener", "LongView", "Gainor", "NoorStaffing"
    ]

    try:
        conn = mysql.connector.connect(**mysqlcredentials)
        cursor = conn.cursor()
        # Base SQL query template
        #sql_template = "ALTER TABLE `jnp_reports_db`.`jobs_report1` CHANGE COLUMN `{}` `{}` INT(11) NULL DEFAULT 0 AFTER `Vaco`;"

        # Loop over the company names and generate the SQL queries
        for company in companies:
            sql_query = f"ALTER TABLE `jnp_reports_db`.`jobs_report1` CHANGE COLUMN `{company}` `{company}` INT(11) NULL DEFAULT 0 AFTER `Vaco`;"            
            cursor.execute(sql_query)
            print(sql_query)
    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Closing the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()