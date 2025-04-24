
import mysql.connector
from main import connections
mysqlcredentials,solr_connection,cred_json= connections()


def get_jobids(companyid):
    mysql_connection = mysql.connector.connect(**mysqlcredentials)
    cursor = mysql_connection.cursor()
    mysql_fetch = "SELECT jobid from jobsnprofiles_2022.jnp_jobs where companyid=%s;" 
    vals = (companyid,)
    cursor.execute(mysql_fetch,vals)
    jobids =  cursor.fetchall()

    if (mysql_connection.is_connected()):
        mysql_connection.close()
        cursor.close()
        print("MySQL mysql_connection is closed")
    return jobids

def get_joblink(companyid):
    mysql_connection = mysql.connector.connect(**mysqlcredentials)
    cursor = mysql_connection.cursor()
    mysql_fetch = "SELECT joblink,jobid from jobsnprofiles_2022.jnp_jobs where companyid=%s and created > now() - INTERVAL 10 day;" 
    vals = (companyid,)
    cursor.execute(mysql_fetch,vals)
    joblinks_fetch =  cursor.fetchall()
    joblinks = {element[0]:element[1] for element in joblinks_fetch}
    if (mysql_connection.is_connected()):
        mysql_connection.close()
        cursor.close()
        print("MySQL mysql_connection is closed")
    return joblinks

def get_predata_joblink():
    mysql_connection = mysql.connector.connect(**mysqlcredentials)
    cursor = mysql_connection.cursor()
    mysql_fetch = "SELECT joblink,jobid from jnp_reports_db.jnp_jobs_pre_insert"
    cursor.execute(mysql_fetch)
    joblinks_fetch =  cursor.fetchall()
    joblinks_predata = {element[0]:element[1] for element in joblinks_fetch}
    if (mysql_connection.is_connected()):
        mysql_connection.close()
        cursor.close()
        print("MySQL mysql_connection is closed")
    return joblinks_predata

def get_client_jobid(source_name):
    mysql_connection = mysql.connector.connect(**mysqlcredentials)
    cursor = mysql_connection.cursor()
    mysql_fetch = f"SELECT joblink,id FROM jobsnprofiles_2022.jnp_jobs where source_name ='{source_name}' and joblink != '' and joblink is not null and created > now() - INTERVAL 10 day;" 
    cursor.execute(mysql_fetch)
    jobids_data =  cursor.fetchall()
    client_joblinks = {element[0]: element[1] for element in jobids_data}

    if (mysql_connection.is_connected()):
        mysql_connection.close()
        cursor.close()
        print("MySQL mysql_connection is closed")
    return client_joblinks