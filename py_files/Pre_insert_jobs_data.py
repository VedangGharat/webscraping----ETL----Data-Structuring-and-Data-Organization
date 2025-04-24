
import mysql.connector
from datetime import date, timedelta, datetime
#importing connections here
import sys
from main import connections
mysqlcredentials,solr_connection,cred_json= connections()
import pytz
import random
UTC = pytz.utc
import logging
import logging_config

logger = logging.getLogger()


def pre_data_insert(df_jobs):
    try:
        # Join lists as strings
        join_cols = ['jobtypeid', 'jobtypename', 'workpermitid', 'workpermitname', 
                    'preferredskills', 'preferredskillsName', 'ai_skills', 
                    'technologies_used']
        for col in join_cols:
            df_jobs[f'{col}_mysql'] = df_jobs[col].apply(lambda x: ','.join(map(str, x)))

        # Convert several columns to string type at once
        cols_to_str = ['stateid', 'salaryrangetype', 'cityid', 'zipcode', 'longitude', 'latitude', 'jobs_order']
        df_jobs[cols_to_str] = df_jobs[cols_to_str].astype(str)

        # Assign constant values
        df_jobs['country'] = '233'
        df_jobs['process_type'] = 'jnp'

        # Initialize 'id' column with 0 and set its type to Int64
        df_jobs['id'] = 0
        df_jobs['id'] = df_jobs['id'].astype('Int64')
        df_jobs['process_type'] = 'jnp'
        if not df_jobs.empty:
            try:
                mysql_connection = mysql.connector.connect(**mysqlcredentials)
                cursor = mysql_connection.cursor()

                for keys,values in df_jobs.iterrows():
                    created = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
                    modified = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
                    try:          
                        mySql_insert_query = "INSERT INTO jnp_reports_db.jnp_jobs_pre_insert(uid,companyid,company_name,title,alias,jobcategory,jobtypeid,jobtype_name,salaryrangefrom,salaryrangetype,is_remote,country,stateid,cityid,zipcode,city_name,state_name,shortregion,longitude,latitude,contactname,contactphone,contactemail,created,modified,status,workpermitid,workpermit_name,requiredtravel,jobid,map_enable,joblink,jobapplylink,posted_flag,source,source_name,prefferdskills,prefferd_skillnames,ai_skills,technologies_used,solr_search_string,day_ordering,industry_type,workmode,job_description,qualifications,process_type) "\
                            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"                                                                                                                            
                        records = (
                        values['uid'], values['companyid'], values['companyname'], 
                        values['title'], values['alias'], values['jobcategory'], 
                        values['jobtypeid_mysql'], values['jobtypename_mysql'], 
                        values['salaryrangefrom'], values['salaryrangetype'], 
                        values['is_remote'], values['country'], values['stateid'], 
                        values['cityid'], values['zipcode'], values['cityname'], 
                        values['statename'], values['shortregion'], values['longitude'], 
                        values['latitude'], values['contactname'], values['contactphone'], 
                        values['contactemail'], created, modified, values['status'], 
                        values['workpermitid_mysql'], values['workpermitname_mysql'], 
                        values['required_travel'], values['jobid'], values['mapenable'], 
                        values['joblink'], values['jobapplylink'], values['posted_flag'], 
                        values['source'], values['source_name'], values['preferredskills_mysql'], 
                        values['preferredskillsName_mysql'], values['ai_skills_mysql'], 
                        values['technologies_used_mysql'], values['solr_search_str'], 
                        values['jobs_order'], values['industry_type'], values['workmode'], 
                        values['description'], values['qualifications'], values['process_type']
                    )
                                
                        cursor.execute(mySql_insert_query, records)
                        mysql_connection.commit()
                        insert_id =  cursor.lastrowid
                        print('pre inserted id ---',insert_id)
                        
                    except Exception as e:
                        print('error-----',str(e))
                        logger.error("An error occurred--", exc_info=True)

                        ##Delete the current row from the dataframe
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
    except Exception as e:
        print('excption at ---',e)

def pre_data_insert_bench(df_jobs):
    
    # Join lists as strings
    join_cols = ['jobtypeid', 'jobtypename', 'workpermitid', 
                 'preferredskills', 'preferredskillsName', 'ai_skills', 
                 'technologies_used','workpermitname']
    for col in join_cols:
        df_jobs[f'{col}_mysql'] = df_jobs[col].apply(lambda x: ','.join(map(str, x)))


    # Convert several columns to string type at once
    cols_to_str = ['stateid', 'salaryrangetype', 'cityid', 'zipcode', 'longitude', 'latitude', 'jobs_order']
    df_jobs[cols_to_str] = df_jobs[cols_to_str].astype(str)

    # Assign constant values
    df_jobs['country'] = '233'
    df_jobs['process_type'] = 'jnp'

    # Initialize 'id' column with 0 and set its type to Int64
    df_jobs['id'] = 0
    df_jobs['id'] = df_jobs['id'].astype('Int64')
    df_jobs['process_type'] = 'jnp'
    if not df_jobs.empty:
        try:
            mysql_connection = mysql.connector.connect(**mysqlcredentials)
            cursor = mysql_connection.cursor()

            for keys,values in df_jobs.iterrows():
                created = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
                modified = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
                try:          
                    mySql_insert_query = "INSERT INTO jnp_reports_db.jnp_jobs_pre_insert_bench(uid,companyid,company_name,title,alias,jobcategory,jobtypeid,jobtype_name,salaryrangefrom,salaryrangetype,is_remote,country,stateid,cityid,zipcode,city_name,state_name,shortregion,longitude,latitude,contactname,contactphone,contactemail,created,modified,status,workpermitid,workpermit_name,requiredtravel,jobid,map_enable,joblink,jobapplylink,posted_flag,source,source_name,prefferdskills,prefferd_skillnames,ai_skills,technologies_used,solr_search_string,day_ordering,industry_type,workmode,job_description,qualifications,process_type) "\
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"                                                                                                                            
                    records = (
                    values['uid'], values['companyid'], values['companyname'], 
                    values['title'], values['alias'], values['jobcategory'], 
                    values['jobtypeid_mysql'], values['jobtypename_mysql'], 
                    values['salaryrangefrom'], values['salaryrangetype'], 
                    values['is_remote'], values['country'], values['stateid'], 
                    values['cityid'], values['zipcode'], values['cityname'], 
                    values['statename'], values['shortregion'], values['longitude'], 
                    values['latitude'], values['contactname'], values['contactphone'], 
                    values['contactemail'], created, modified, values['status'], 
                    values['workpermitid_mysql'], values['workpermitname_mysql'], 
                    values['required_travel'], values['jobid'], values['mapenable'], 
                    values['joblink'], values['jobapplylink'], values['posted_flag'], 
                    values['source'], values['source_name'], values['preferredskills_mysql'], 
                    values['preferredskillsName_mysql'], values['ai_skills_mysql'], 
                    values['technologies_used_mysql'], values['solr_search_str'], 
                    values['jobs_order'], values['industry_type'], values['workmode'], 
                    values['description'], values['qualifications'], values['process_type']
                )
                               
                    cursor.execute(mySql_insert_query, records)
                    mysql_connection.commit()
                    insert_id =  cursor.lastrowid
                    print('bench pre insert id ---',insert_id)
                    
                except Exception as e:
                    print('error-----',str(e))
                    logger.error("An error occurred--", exc_info=True)

                    ##Delete the current row from the dataframe
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
    