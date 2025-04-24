import os
import json
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
from main import connections,paths_to_folders
mysqlcredentials,solr_connection,cred_json= connections()
csv_jobs_path,log_file_path,sites_status_json,path_to_inputfiles = paths_to_folders()
import openai
import json
import dateparser
from dateutil.parser import parse
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
import traceback

def read_schedulejson():
    with open(sites_status_json, 'r') as file:
        scheduled_data = json.load(file)
        scheduled_sites = []
        for sites,status  in scheduled_data['schedules'].items():
            if status[1] == 'done' and status[2] == None:
                scheduled_sites.append(sites)
                if len(scheduled_sites) >10:
                    break
    return scheduled_sites,scheduled_data

def update_task(schedules_list):
    found_active = False
    for schedule in schedules_list:
        if schedule['task'] == 'active':
            schedule['task'] = 'completed'
            found_active = True
        elif schedule['task'] == 'next' and found_active:
            schedule['task'] = 'active'
            break

    updated_json = json.dumps(schedules_list, indent=4)
    with open(sites_status_json, 'w') as file:
        file.write(updated_json)

def update_schedule(scheduled_sites,scheduled_data):
    # Update the schedules with "done"
    for site in scheduled_sites:
        if site in scheduled_data['schedules']:
            scheduled_data['schedules'][site][2] = "done"

    # Convert updated dictionary to JSON string
    updated_json = json.dumps(scheduled_data, indent=4)

    # Write the updated JSON to a file
    with open(sites_status_json, 'w') as file:
        file.write(updated_json)

def update_site_runstatus(site_name,val):
    # Read the JSON data from the file
    with open(sites_status_json, 'r') as file:
        data = json.load(file)
    
    # Access the schedules dictionary
    schedules = data['schedules']
    
    # Update the schedule for the given site name
    if site_name in schedules.keys():
        schedules[site_name][val] = "done"
    else:
        print(f"Site name '{site_name}' not found in schedules.")
        return
    
    # Write the updated JSON data back to the file
    with open(sites_status_json, 'w') as file:
        json.dump(data, file, indent=4)
    
    print(f"Schedule for '{site_name}' updated successfully.")

def read_key_skills():
    '''File path that consist of skill list'''
    filepath = os.path.join(path_to_inputfiles,"tech_skills_new.txt")
    dfSkills = pd.read_csv(filepath,header=None)
    dfSkillSet = dfSkills.drop_duplicates()
    skill_list_IT_final = dfSkillSet[0].tolist()
    skill_list_IT_final = [skill.lower() for skill in skill_list_IT_final]
    nlp = en_core_web_sm.load()
    stats_to_nlp_IT = [nlp(text_It) for text_It in skill_list_IT_final]
    matcher = PhraseMatcher(nlp.vocab)
    matcher.add('skill_list_IT', None, *stats_to_nlp_IT)
    return matcher,nlp


def get_key_skills(text_data,matcher,nlp):
    '''Take the textdata then parse the skills '''
    doc = nlp(text_data)
    keywords_IT = []
    matches = matcher(doc)
    for match_id, start, end in matches:
        rule_id = nlp.vocab.strings[match_id]
        span = doc[start: end]  # get the matched slice of the doc
        keywords_IT.append((rule_id, span.text))

    candiSkillsIt = []
    for skil_IT in keywords_IT:
        skil_IT = skil_IT[1]
        if skil_IT not in candiSkillsIt:
            candiSkillsIt.append(skil_IT)

    tech_skills = set(candiSkillsIt)
    return tech_skills


def extract_job(jobtext,jobtitle):
    OPENAI_API_KEY = random.choice(cred_json['open_ai_keys'])
    prompt ='''
   Context : "This is the job description, think step by step and fill fields as 'null' string if no exact data is found
   Step 1: Extract the exact sections of "job_role"
   Step 2: Extract the exact sections of "skills" in shortforms which are IT related.
   Step 3: Extract the exact sections of "Technologies used"
   Step 4: Extract the exact sections of "jobcategory" by categorizing job title, identify job category as IT or NON-IT only based on given job data. Please ensure that no other information.
   step 5: Generate a list of only 10 related job roles(not more than 10)for the jobtitle-'''+jobtitle+''' and make a string with 'OR' separated and send as solr_search_string.
   Step 6: Extract the exact sections of "solr_search_string"
   Step 7: Combine all the information generated from Steps 1-4 in the mentioned Example
   Step 8: Make sure that 'null' is a string and make neccessary changes
   Step 9: After filling out the information make sure that the data is a complete python dictionary"

   Example: "
   {
      "job_role":"" ,
      "skills":[] ,
      "technologies_used":[] ,
      "jobcategory":"",
      "solr_searchstr":""      
   }
   "
   
   HTML Data: 
   "''' +jobtext+ '''
   "
   '''
    converstaion =[]
    converstaion.append({"role": "user", "content":prompt })
    try:
        # SET GPT-3 API KEY FROM THE ENVIRONMENT VAIRABLE
        openai.api_key = OPENAI_API_KEY
        
        openai_response =openai.ChatCompletion.create( 
            model='gpt-3.5-turbo-0125',
            messages= converstaion,
            temperature=0,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0
        )
        tokens_used = json.loads(str(openai_response['usage']))
        response_text = openai_response['choices'][0]['message']['content']
        try:
            # TEXT TO JSON
            response_fields = json.loads(str(response_text))
        except Exception as e:
            logger.error("An error occurred--", exc_info=True)
            print('Error parsing--------',e)
            response_fields = ''
    except Exception as e:
        logger.error("An error occurred--", exc_info=True)
        print("Error creating-----",e)
        response_fields = ''
    
    return response_fields


 
def get_location(city,state,solrCity,solrcitysearch,solrStateName,solrShortregion,sqlzipcodeData):
    if city.lower() in solrcitysearch.keys():
        city_val = True
    else:
        cityid = ''
        city_val = False
    #check whether city is state
    if city_val == False:
        if len(city) >2:
            if city.lower() in solrStateName.keys():
                state = city
                city = ''
            else:
                state = ''
        
        elif len(city)==2:
            if city.lower() in solrShortregion.keys():
                state = city
                city = ''
            else:
                state = ''

        elif city == '':
            pass
    #get statedetails first:
    if len(state)==2:
        if state.lower() in solrShortregion.keys():
            s_statename = solrShortregion[state.lower()][1]
            s_shortregion = state
            s_stateid = solrShortregion[state.lower()][0]
        else:
            s_statename = ''
            s_shortregion =''
            s_stateid = 0
    elif len(state)>2: 
        if state.lower() in solrStateName.keys():
            s_statename = state
            s_shortregion = solrStateName[state.lower()][1]
            s_stateid = solrStateName[state.lower()][0]
        else:
            s_statename = ''
            s_shortregion =''
            s_stateid = 0
    else:
        s_statename = ''
        s_shortregion =''
        s_stateid = 0

    #get locations with both city and state match
    if city != '':
        for each_city,city_val in solrCity.items():
            if city.lower() == city_val[0].lower():
                cityid = each_city
                if (state != '') and (state.upper() == city_val[1] or state.lower() == city_val[3]):
                    #print('matched location----',city_val)
                    statename = city_val[3]
                    shortregion = city_val[1]
                    stateid = city_val[2]
                    is_remote = 0
                    break
                
                elif state == '':
                    statename = city_val[3]
                    shortregion = city_val[1]
                    stateid = city_val[2]
                    is_remote = 0
                    break
                else:
                    statename = s_statename
                    shortregion = s_shortregion
                    stateid = s_stateid
                    is_remote = 0
            else:
                cityid = 0
                statename = s_statename
                shortregion = s_shortregion
                stateid = s_stateid
                is_remote = 0

        if cityid == 0:
            for each_city,city_val in solrCity.items():
                if city.lower() in city_val[0].lower():
                    cityid = each_city
                    if (state != '') and (state.upper() == city_val[1] or state.lower() == city_val[3]):
                        #print('matched location----',city_val)
                        statename = city_val[3]
                        shortregion = city_val[1]
                        stateid = city_val[2]
                        is_remote = 0
                        break
                    
                    elif state == '':
                        statename = city_val[3]
                        shortregion = city_val[1]
                        stateid = city_val[2]
                        is_remote = 0
                        break
                    else:
                        statename = s_statename
                        shortregion = s_shortregion
                        stateid = s_stateid
                        is_remote = 0
                else:
                    cityid = 0
                    statename = s_statename
                    shortregion = s_shortregion
                    stateid = s_stateid
                    is_remote = 0

    elif city == '' and state != '':
        city = ''
        cityid = 0
        statename = s_statename
        shortregion = s_shortregion
        stateid = s_stateid
        is_remote = 0

    elif city == '' and state == '':
        cityid = 0
        city = ''
        statename = ''
        shortregion = ''
        stateid = 0
        is_remote = 1

    else:
        cityid = 0
        statename = s_statename
        shortregion = s_shortregion
        stateid = s_stateid
        is_remote = 0        

    zipcode_lst=[]   
    if city != '' and statename !='':
        for elements in sqlzipcodeData:
            if city.title() == elements[0] and statename.title() == elements[1]:
                zipcode_lst.append(elements)
        if len(zipcode_lst)==0 and (city == '' and statename !=''):
            for elements in sqlzipcodeData:
                if statename.title() == elements[1]:
                    zipcode_lst.append(elements)
                    
        if len(zipcode_lst) >0:
            randm_values = random.choices(zipcode_lst,k=1)[0] 
            zipcode =  randm_values[2]
            latitude = str(randm_values[3])
            longitude = str(randm_values[4])
            #print(randm_values)
        else:
            zipcode = ''
            latitude = ''
            longitude = ''     
    else:
        zipcode = ''
        latitude =''
        longitude = ''
    print('location----', city,cityid,statename,shortregion,stateid,zipcode,latitude,longitude,is_remote)
    return city,cityid,statename,shortregion,stateid,zipcode,latitude,longitude,is_remote

def check_date(posted_date):
    if posted_date.lower() == "just posted":
        posted_date = 'just now'
    elif 'posted' in posted_date.lower():
        posted_date = posted_date.lower().replace('Posted','')

    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist)
    yesterday_ist = (now_ist - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    try:
        parsed_date = dateparser.parse(str(posted_date))
        if parsed_date:
            parsed_date = ist.localize(datetime(parsed_date.year, parsed_date.month, parsed_date.day))
            is_posted = parsed_date == yesterday_ist
            parsed_date_str = parsed_date.strftime("%d-%m-%Y")
        else:
            print("Parsed date is None")
            is_posted, parsed_date_str = False, ''
    except Exception as e:
        logger.error(f"An error occurred--{posted_date}\nTraceback Info:\n{e}", exc_info=True)
        is_posted, parsed_date_str = False, ''

    return is_posted, parsed_date_str


def get_data_types_json():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),'data_types.json'), 'r') as file:
        data = json.load(file)

    # Separate the different categories
    job_types = data['jobtypes']
    salary_types = data['salarytypes']
    work_permit_types = data['workpermittypes']

    return job_types, salary_types, work_permit_types

# Function to find the key for a given job type value
def find_jobtype_key(jobtypename,job_types:dict):
    if type(jobtypename) == str:
        jobtypename = jobtypename.split(',')
    elif type(jobtypename) == None:
        jobtypename = []

    jobtypes_list = []
    for each_type in jobtypename:
        for key, values in job_types.items():
            if each_type.lower() in values or each_type.lower() == key.lower():
                jobtypes_list.append(key)
            
    return jobtypes_list

# Function to find the key for a given job type value
def find_workpermit_key(workpermits:list,work_permit_types:dict):
    workpermits_list = []
    for each_type in workpermits:
        for key, values in work_permit_types.items():
            if each_type.lower() in values or each_type.lower() == key.lower():
                workpermits_list.append(key)
    return workpermits_list

# Function to find the key for a given job type value
def find_salarytype_key(value,salary_types):
    for key, values in salary_types.items():
        if value.lower() in values or value.lower() == key.lower():
           salarytype = key
           break
        else:
            salarytype=''
    
    return salarytype