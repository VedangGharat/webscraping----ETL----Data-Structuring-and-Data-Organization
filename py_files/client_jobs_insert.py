import os
import re
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime
#importing connections here
import sys
from main import paths_to_folders
csv_jobs_path,log_file_path,sites_status_json,path_to_inputfiles = paths_to_folders()
from Pre_insert_jobs_data import pre_data_insert
import logging
import logging_config

logger = logging.getLogger()
from common_database_functions import insert_report,getMySqlData,get_client_companies,insert_client_comp,get_union_of_companies
from common_helper_functions import get_key_skills,read_key_skills,extract_job,get_location,get_data_types_json,find_jobtype_key,find_salarytype_key,find_workpermit_key

def client_read_csv_jobs(sitepath,report_name):
    job_types, salary_types, work_permit_types = get_data_types_json()
    solrCompany_union = get_union_of_companies()
    solrClient_comp = get_client_companies()
    
    required_columns = ['uid', 'companyid','companyname','logo_url', 'title', 'alias', 'jobtypename','jobtypeid','salaryrangefrom', 'salaryrangetype', 'description', 'qualifications', 'preferredskills','preferredskillsName','ai_skills','technologies_used', 'statename','stateid','shortregion', 'cityname', 'cityid', 'zipcode','latitude','longitude','is_remote','workmode','mapenable','contactname','contactemail','contactphone', 'created','workpermitname', 'workpermitid', 'jobscraped', 'jobid','joblink', 'jobapplylink', 'jobcategory','solr_search_str','source','source_name','industry_type','required_travel','posted_flag','status']

    df_jobs = pd.DataFrame(columns = required_columns)
    jobs_count_dict = {}
    n=0
    joblink_dupl =[]
        
    if os.path.exists(sitepath):
        jobs_data = pd.read_csv(sitepath, keep_default_na='',encoding = "latin-1").drop_duplicates(subset=['title', 'location', 'jobdescription'])
        jobs_count = 1
        for index,each_job in jobs_data.iterrows():
            if each_job['joblink'] == '' or (each_job['joblink'] != '' and each_job['joblink'] not in joblink_dupl):
                try:
                    uid,companyid, title, alias, salarytype, description, qualifications,statename, stateid, shortregion, city, cityid, contactname, contactemail,contactphone, created, required_travel, salaryrangefrom, zipcode, jobscraped, companyname, jobid,jobapplylink, jobcategory,latitude,longitude,logo_url,solr_search_str,source_name,industry_type = [''] * 30

                    #workpermitname, preferredskills, preferredskillsName, workpermitid, jobtypename, jobtypeid, ai_skills, technologies_used = [[]] *8
                    companyid = each_job['companyid']
                    if companyid in ['0',0,None,'','N/A','null']:
                        companyname = each_job['companyname'].strip()
                        print('companyname-----',companyname)
                        if companyname.lower() in solrCompany_union: #search in database
                            companyid = int(solrCompany_union[companyname.lower()][1])
                        else:
                            companyid = 0
                    
                    if companyid !=0:
                        title = each_job['title'].strip()
                        try:
                            location = each_job['location']
                            city = each_job['city'].strip()
                            
                            # Determine state and remote status
                            state = each_job['shortregion'].strip() if each_job['state'] == '' else each_job['state'].strip()
                            is_remote = 0 if state else 1
                            
                            if is_remote:
                                city = ''

                            # Get location details
                            cityname, cityid, statename, shortregion, stateid, zipcode, latitude, longitude, is_remote = get_location(
                                city, state, solrCity, solrcitysearch, solrStateName, solrShortregion, sqlzipcodeData
                            )
                        except Exception as e:
                            print('Exception:', e)
                            
                        try:
                            jobtypename = []
                            jobtypeid = []  
                            # Process job type names and IDs
                            jobtypename = each_job['jobtype']
                            jobtypes_names = find_jobtype_key(jobtypename, job_types)
                            
                            jobtypeid.extend(
                                solrJobtypeData[jobtype.lower()] for jobtype in jobtypes_names if jobtype.lower() in solrJobtypeData
                            )

                        except Exception as e:
                            print('Exception as e -------',e)

                        try:
                            # Process work permits
                            workpermitname = []
                            workpermitid = []
                            workpermit = each_job.get('workpermit', '').split(',')
                            
                            if not workpermit:
                                workpermitname = ["GC", "USC", "H1", "EAD"]
                                workpermitid = [1, 2, 3, 7]
                            else:   
                                for each_wrkpermt in workpermit:
                                    if each_wrkpermt.strip() in solrworkpermitData.keys():
                                        workpermitname.append(each_wrkpermt.strip())
                                        workpermitid.append(solrworkpermitData[each_wrkpermt.strip()])
                        except Exception as e:
                            print('Exception as e -------',e)
                            workpermitname = ["GC", "USC", "H1", "EAD"]
                            workpermitid = [1, 2, 3, 7]

                        try:
                            salaryrangefrom = each_job.get('salaryrange', '')
                            salarytype = ''

                            if salaryrangefrom:
                                salarytype_raw = each_job.get('salarytype', '')
                                if salarytype_raw:
                                    salarytype = find_salarytype_key(salarytype_raw.title(), salary_types)
                            print('salary info -----',salaryrangefrom, salarytype)
                        except Exception as e:
                            print('Exception at salary -------', e)

                        try:
                            if 'jobdescription_2' in each_job:
                                combined_description = each_job['jobdescription'] + " " + each_job['jobdescription_2']
                            else:
                                combined_description = each_job['jobdescription']
                            # Parse job description with BeautifulSoup
                            Jobdesc = BeautifulSoup('<div>' + combined_description + '</div>', 'html.parser')
                            
                            # Prepare skills_text
                            cleaned_text = Jobdesc.text.strip().replace("\xa0", " ")
                            skills_text = f'jobtitle - {title} {cleaned_text}'
                            
                            # Remove all attributes from tags
                            for tag in Jobdesc.find_all():
                                tag.attrs.clear()
                            
                            # Convert Jobdesc back to string and replace unwanted characters
                            jobdesc_str = str(Jobdesc).replace("'", "\'").replace('\n ', '').replace('\n','')
                            
                            # Initialize description with jobdesc_str
                            description = jobdesc_str
                            
                            # Apply style modifications based on tag presence
                            if jobdesc_str.startswith('<div>'):
                                print("yes")
                                description = re.sub(r'<div>', '<div style="font-family: Roboto, sans-serif; font-size: 16px; line-height: 30px;">', description, count=1)
                            
                            tag_styles = {
                                '<p>': '<p style="font-family: Roboto, sans-serif; font-size: 16px; line-height: 30px;">',
                                '<h4>': '<h4 style="font-family: Roboto, sans-serif; font-size: 18px; font-weight: 700;">',
                                '<li>': '<li style="font-family: Roboto, sans-serif; font-size: 16px; line-height: 30px;">',
                                '<span>': '<span style="font-family: Roboto, sans-serif; font-size: 16px;">'
                            }
                            
                            for tag, style in tag_styles.items():
                                if tag in jobdesc_str:
                                    description = description.replace(tag, style)
                                else:
                                    print(f'no {tag} tag')

                            # Specific modification for <h4> containing "Job Description"
                            description = re.sub(r'<h4>Job Description</h4>', '', description)

                            # Remove double <br> tags
                            description = re.sub(r'<br\s*/?>\s*<br\s*/?>', '<br>', description)
                            
                            # Handle non-ASCII characters
                            try:
                                description = re.sub(r'[^\x00-\x7F]+', ' ', description)
                                description = description.encode('latin1').decode('utf-8')
                            except UnicodeEncodeError:
                                pass

                        except Exception as e:
                            print('Exception Occured at description:', e)


                        
                        contactemail = each_job['email'] 
                        if contactemail != '':
                            contactname = contactemail.split('@')[0].strip().title()
                        else:
                            cemail = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z0-9]{2,7}\b', str(Jobdesc))
                            if cemail:
                                contactemail = cemail.group()
                                contactname = contactemail.split('@')[0].strip().title()       

                        # Find phone number in job description
                        try:
                            phone_pattern = r'\d{3}-\d{3}-\d{4}|\(\d{3}\)\s*\d{3}[-.\s]??-\d{4}|\(\d{3}\)\s*\d{3}[-.\s]??\d{4}|\(\d{3}\)\s*-\d{3}[-.\s]??-\d{4}|\(\d{3}\)\s*\d{3}[-.\s]??\d{4}|\d{3}[.\s]*\d{3}[-.\s]??\d{4}'
                            cmatch = re.search(phone_pattern, str(Jobdesc))
                            contactphone = cmatch.group() if cmatch else ''

                            print("contactemail:", contactemail)
                            print("contactno:", contactphone)
                        except Exception as e:
                            print(e)
                            print("Error at Contact Number")


                        if companyid in solrCompany:
                            uid = solrCompany[companyid][0]
                            if uid is None:
                                uid = ''
                            companyname = solrCompany[companyid][1]
                            logo_url = solrCompany[companyid][2]
                            if logo_url == '' or logo_url == None:
                                if solrCompany[companyid][3] != None:
                                    logo_url = f"https://jnp-cloud-data.s3.amazonaws.com/jnp_data/CompanyLogos/Company_{companyid}/{solrCompany[companyid][3]}"
                                else:
                                    logo_url = ''

                        try:
                            skills = each_job.get('skills', '').split(',')
                        except Exception as e:
                            print('Exception while fetching skills:', e)
                            skills = []

                        # Fetch skills from the database table
                        preferredskills = []
                        preferredskillsName = []
                        ski = get_key_skills(skills_text.lower(), matcher, nlp)

                        for skl in ski:
                            if skl in jobskill_dict:
                                preferredskills.append(jobskill_dict[skl])
                                preferredskillsName.append(skl)

                        if skills:
                            preferredskillsName.extend(skill.strip() for skill in skills if skill.strip())


                        # Fetch AI skills from ChatGPT
                        ai_skills = []
                        technologies_used = []
                        openai_skills = extract_job(re.sub(r'\s+', ' ', skills_text), title)

                        if openai_skills:
                            industry_type = openai_skills.get('jobcategory', '')
                            print('jobfield ---', industry_type)
                            
                            solr_search_str = openai_skills.get('solr_searchstr', '').replace('"fq":', '').replace('fq=', '')

                            ai_skills = list(set(openai_skills.get('skills', [])))
                            technologies_used = list(set(openai_skills.get('technologies_used', [])))
                            
                            title = openai_skills.get('job_role', '')
                            if isinstance(title, list) and title:
                                title = title[0]


                        # Other fields
                        jobid = each_job['jobid']
                        experience = each_job['experience'] or ''
                        source_name = each_job['source_name']
                        joblink = each_job['joblink']
                        joblink_dupl.append(joblink)

                        #companyid = int(each_job['companyid'])
                        qualifications = "Bachelor's Degree"
                        jobcategory = 13
                        required_travel = '0'
                        posted_flag = '0'
                        source = 'WSJ'
                        status = '1'

                        # Job apply link based on job link presence
                        jobapplylink = 1 if joblink else 0

                        title = title.replace('/', '//')
                        alias = title.lower()

                        # Assigning workmode & mapenable
                        workmode = 'true' if is_remote == 1 else 'false'
                        mapenable = 'false' if is_remote == 1 else 'true'


                        try:
                            if len(title) != 0 and len(description) != 0:
                                
                                df_jobs.loc[n] = [uid] + [companyid] + [companyname]+ [logo_url] +[title] + [alias] + [jobtypes_names]+ [jobtypeid]+ [salaryrangefrom]+[salarytype] + [description] + [qualifications] + [preferredskills] + [preferredskillsName] +[ai_skills] +[technologies_used]+ [statename] + [stateid] + [shortregion] + [cityname] + [cityid] +[zipcode]+[latitude]+[longitude]+[is_remote]+[workmode]+[mapenable]+ [contactname] + [contactemail] + [contactphone] + [created] +  [workpermitname]+[workpermitid] + [jobscraped] +  [jobid]+[joblink] + [jobapplylink] + [jobcategory]+[solr_search_str]+[source]+[source_name]+[industry_type]+[required_travel]+[posted_flag]+[status]
                                print(df_jobs.loc[n])
                                print('***************')
                                n = n + 1 
                            
                                jobs_count+=1
                            else:
                                pass
                        except Exception as e:
                            logger.error("An error occurred--", exc_info=True)
                            pass
                    else:
                        if companyname.lower() not in solrClient_comp.keys():
                            insert_client_comp(companyname.strip(),source_name)
                            print('client company added successfully')
                            solrClient_comp[companyname.lower()] = [None,None,None,None]
                except Exception as e:
                    print('Exception at records insert !!', e)
                    logger.error("An error occurred--", exc_info=True)
                
        # try:
        #     if df_jobs.empty:
        #         print('No records found')
        #     else:
        #         #df_jobs.drop_duplicates(subset=['title', 'location','jobtype', 'description'], inplace=True, ignore_index=True)
        #         df_jobs['jobs_order'] = df_jobs.groupby('companyid').cumcount() + 1
        #         df_jobs = pre_data_insert(df_jobs)
        #         insert_report(report_name,len(df_jobs))
        # except Exception as e:
        #     print('Exception at records insert !!', e)
        #     logger.error("An error occurred--", exc_info=True)

    

matcher,nlp = read_key_skills()
solrCity,solrcitysearch, solrShortregion, solrStateName, solrCountryId, solrCompany, solrSalaryRangetype,jobskill_dict,solrJobtypeData, solrworkpermitData,sqlzipcodeData,solrzipcodematch = getMySqlData()
