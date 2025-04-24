import os
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
import logging
from datetime import datetime, date, timedelta
from helper_for_location_filter import get_location_details
import re
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import csv
from helper_function_for_scroll_more_for_job_link import scroll_to_element_and_click
from helper_for_salary_range_type import get_salary_info
from main import paths_to_folders
import read_most_recent_jobs  # Ensure this module is implemented correctly
from client_jobs_insert import client_read_csv_jobs

from job_id_fetch import get_client_jobid,get_predata_joblink
from main import paths_to_folders
csv_jobs_path,log_file_path,sites_status_json,path_to_inputfiles = paths_to_folders()
import logging
import logging_config
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from fake_useragent import UserAgent
from datetime import datetime, timedelta

# Setup paths and logging
csv_jobs_path, log_file_path, sites_status_json, path_to_inputfiles = paths_to_folders()
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger()

def save_to_csv(job_data, directory):
    file_path = os.path.join(directory, "save_extracted_data.csv")
    os.makedirs(directory, exist_ok=True)
    file_exists = os.path.isfile(file_path)

    with open(file_path, mode='a', newline='', encoding='utf-8') as csvfile:
        fieldnames = job_data.keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()
        
        writer.writerow(job_data)

def save_to_json(data, json_jobs_path):
    json_file_path = os.path.join(json_jobs_path, 'linkedin_jobs_data.json')
    os.makedirs(json_jobs_path, exist_ok=True)

    for filename in os.listdir(json_jobs_path):
        file_path = os.path.join(json_jobs_path, filename)
        if file_path != json_file_path:
            os.remove(file_path)

    existing_data = []
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
            try:
                existing_data = json.load(json_file)
            except json.JSONDecodeError:
                pass
    
    existing_data.append(data)
    
    with open(json_file_path, 'w') as json_file:
        json.dump(existing_data, json_file, indent=4, cls=DateTimeEncoder)

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

def extract_emails(text):
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    return re.findall(email_pattern, text)

def complete_data(soup, link, job_role, directory):
    company_name = ""
    company_name_element = soup.find('a', class_="topcard__org-name-link topcard__flavor--black-link")
    if company_name_element:
        company_name = company_name_element.get_text(strip=True)

    location = ""
    location_element = soup.find('span', class_="topcard__flavor topcard__flavor--bullet")
    if location_element:
        location = location_element.get_text(strip=True)
    else:
        location = 'Remote'

    city, state, state_code = get_location_details(location)
    posted_date = datetime.now().date()

    criteria = {}
    for item in soup.find_all('li', class_='description__job-criteria-item'):
        header = item.find('h3', class_='description__job-criteria-subheader').text.strip()
        text = item.find('span', class_='description__job-criteria-text description__job-criteria-text--criteria').text.strip()
        criteria[header] = text

    employment_type = criteria.get("Employment type", "")

    jobid = ''
    if link.split('/')[-1].split('?')[0]:
        jobid_element = link.split('/')[-1].split('?')[0]
        match = re.search(r'-(\d+)$', jobid_element)
        if match:
            jobid = match.group(1)

    scroll_to_element_and_click(link)
    full_description_element = soup.find('div', class_='show-more-less-html__markup')
    full_description = str(full_description_element) if full_description_element else ""

    contact_emails = extract_emails(full_description)
    contact_email = contact_emails[0] if contact_emails else ""

    salary_type, min_salary, max_salary = "", "", ""
    salary_elm = soup.find('div', class_='salary compensation__salary')
    if salary_elm:
        salary_text = salary_elm.text.strip()
        salary_type, min_salary, max_salary = get_salary_info(salary_text)

    job_data = {
        'title': job_role,
        'companyname': company_name,
        'joblink': link,
        'location': location,
        'date_posted': posted_date,
        'state': state,
        'shortregion': state_code,
        'city': city,
        'jobtype': employment_type,
        'jobdescription': full_description,
        'jobid': jobid,
        'salaryrange': f"{min_salary} - {max_salary}",
        'salarytype': salary_type,
        'email': contact_email,
        'contact_name': "",
        'source_name': "linkedin",
        'company_logo': "company_logo",
        "experience": "",
        "workpermit": "",
        "skills": ""
    }

    print(job_data)
    save_to_csv(job_data, directory)
    # save_to_json(job_data, json_jobs_path)  # Uncomment if JSON saving is needed

def job_titles(soup, link, directory):
    job_title_element = soup.find('h1', class_='top-card-layout__title font-sans text-lg papabear:text-xl font-bold leading-open text-color-text mb-0 topcard__title')
    if job_title_element:
        job_title = job_title_element.get_text(strip=True)
        complete_data(soup, link, job_title, directory)

def main():
    directory = '/Users/vedanggharat/Movies/LinkedIn Jobs/text_links'
    most_recent_file = read_most_recent_jobs.find_most_recent_file(directory)
    print(most_recent_file)
    
    if most_recent_file:
        job_links = read_most_recent_jobs.read_job_links_from_txt(most_recent_file)

        # Job link processing from the CSV file
        db_joblinks = get_client_jobid('linkedin')
        df = pd.read_csv(os.path.join(path_to_inputfiles, 'linkedin_location_urls.csv'))
        file_save_path = os.path.join(csv_jobs_path, f'linkedin_jobs_1_{date.today().strftime("%Y-%m-%d")}.csv')
        
        URL = df['location_url'].tolist()
        within_joblinks = []
        url_count = 1

        for url in URL:
            within_joblinks = Linkedin(url, file_save_path, db_joblinks, within_joblinks, joblink_list_predata)
            url_count += 1
            if url_count == 26:
                client_read_csv_jobs(file_save_path, report_name='linkedin')
                file_save_path = os.path.join(csv_jobs_path, f'linkedin_jobs_2_{date.today().strftime("%Y-%m-%d")}.csv')
                joblink_list_predata = get_predata_joblink()

        client_read_csv_jobs(file_save_path, report_name='linkedin')

        for link in job_links:
            response = requests.get(link)
            soup = BeautifulSoup(response.text, 'html.parser')
            job_titles(soup, link, directory)
    else:
        print("No recent file to process.")

if __name__ == "__main__":
    main()
