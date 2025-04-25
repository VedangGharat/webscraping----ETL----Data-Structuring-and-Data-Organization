import os
import sys
import mysql.connector
from mysql.connector import Error
import pysolr
import json

def get_secret_keys():
    mysql_secret_keys_connection = mysql.connector.connect(

    )
    cursor = mysql_secret_keys_connection.cursor()
    get_cred = "SELECT id,name,data,status,source FROM pyth_db_cred.jnp_credentials"
    cursor.execute(get_cred)
    get_cred_json = {element[0]:[element[1],element[2],element[3],element[4]] for element in cursor.fetchall()}
    cred_json = {"open_ai_keys":[],
                 "mysql_cred":"",
                 "solr_jobs" :"",
                 "apollo_keys":[],
                 "solr_cred":""
                }
    
    for element,vals in get_cred_json.items():
        if 'mysql_prod' in vals[0]:
            cred_json['mysql_cred'] = json.loads(vals[1])
        elif 'solr_jobs' in vals[0]:
            cred_json['solr_jobs'] = vals[1]
        elif 'openai' in vals[0] and 'scraping' in vals[3] and vals[2] == 1:
            cred_json['open_ai_keys'].append(vals[1])
        elif 'apollo' in vals[0]:
            cred_json['apollo_keys'].append(vals[1])
        elif 'solr_account' in vals[0]:
            cred_json['solr_cred'] = json.loads(vals[1])

    if (mysql_secret_keys_connection.is_connected()):
        mysql_secret_keys_connection.close()
        cursor.close()

    return cred_json

def connections():
    cred_json = get_secret_keys()
    mysqlconnection = {'host' : cred_json['mysql_cred']['ip'],'username' : cred_json['mysql_cred']['user'],'password' : cred_json['mysql_cred']['password'],'database' : 'jobsnprofiles_2022'}

    solr_connection = pysolr.Solr(cred_json['solr_jobs'], auth=(cred_json['solr_cred']['user'],cred_json['solr_cred']['password']), always_commit=True) 
    
    
    return mysqlconnection,solr_connection,cred_json

def paths_to_folders():
    csv_jobs_path = os.path.join(os.path.dirname((os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))),'jobs_scraped_csvs')
    os.makedirs(csv_jobs_path,exist_ok=True)
    log_file_path = os.path.join(os.path.dirname((os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))),'Job_logfiles')
    os.makedirs(log_file_path,exist_ok=True)
    sites_status_json = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),'regular_scheduled_sites.json')
    path_to_inputfiles = os.path.join(os.path.dirname(os.path.abspath(__file__)))

    return csv_jobs_path,log_file_path,sites_status_json,path_to_inputfiles
