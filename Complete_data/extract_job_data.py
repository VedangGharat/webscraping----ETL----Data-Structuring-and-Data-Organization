import os
import json
import requests
from bs4 import BeautifulSoup
import logging
import read_most_recent_jobs  # Ensure this module is implemented correctly
from datetime import datetime, timedelta
from helper_for_location_filter import get_location_details
import re
from datetime import datetime, date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
import time
import csv
from helper_function_for_scroll_more_for_job_link import scroll_to_element_and_click
from helper_for_salary_range_type import get_salary_info

# Setup logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger()



def save_to_csv(job_data):
    # Ensure the directory exists
    file_path = "/Users/vedanggharat/Movies/LinkedIn Jobs/save_extracted_data"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Check if file exists to determine if header is needed
    file_exists = os.path.isfile(file_path)
    
    # Write data to CSV
    with open(file_path, mode='a', newline='', encoding='utf-8') as csvfile:
        fieldnames = job_data.keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header only if file does not exist
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(job_data)
        
    
def save_to_json(data):
    # Define the directory path and file name for the single JSON file
    json_jobs_path = "/Users/vedanggharat/Movies/LinkedIn Jobs/json_links"
    json_file_path = os.path.join(json_jobs_path, 'linkedin_jobs_data.json')
    
    # Create the directory if it does not exist
    os.makedirs(json_jobs_path, exist_ok=True)
    
    # Remove all existing files in the directory (except the JSON file)
    for filename in os.listdir(json_jobs_path):
        file_path = os.path.join(json_jobs_path, filename)
        if file_path != json_file_path:  # Skip the target JSON file
            os.remove(file_path)
    
    # Load existing data from the JSON file if it exists
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
            try:
                existing_data = json.load(json_file)
            except json.JSONDecodeError:
                existing_data = []  # Handle any potential corruption or empty file
    else:
        existing_data = []
    
    # Append the new data to the existing data
    existing_data.append(data)
    
    # Custom encoder to handle non-serializable types like 'date'
    class DateTimeEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, date):
                return obj.isoformat()  # Convert 'date' object to 'YYYY-MM-DD' format
            return super(DateTimeEncoder, self).default(obj)
    
    # Save the updated data to the JSON file
    with open(json_file_path, 'w') as json_file:
        json.dump(existing_data, json_file, indent=4, cls=DateTimeEncoder)
            

def extract_emails(text):
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    return re.findall(email_pattern, text)


def complete_data(soup, link):
    
    # print("job", job_role)
    job_data = {}
    job_data['joblink'] = link
    ''' Get Company Name if available '''
    company_name = ""
    company_name_element = soup.find('a', class_="topcard__org-name-link topcard__flavor--black-link")
    company_name_check = company_name_element.get_text(strip=True) if company_name_element else ''

    if company_name_check != '':
        company_name = company_name_check
        job_data['companyname'] = company_name

    # print("Company Name:", company_name)
    
    
    ''' Get Location if available '''
    location = ""
    location_element = soup.find('span', class_="topcard__flavor topcard__flavor--bullet")
    location_check = location_element.get_text(strip=True) if location_element else ''

    if location_check != '':
        location = location_check
    else:
        location = 'Remote'
    job_data['location'] = location
    # print(location)
    
    ''' Get City, State, State_Code, Location '''
    city, state, state_code = get_location_details(location)
    job_data['state']= state
    job_data['shortregion']= state_code
    job_data['city']= city
    # print(city, state, state_code)
    
    ''' Get Date '''
    current_time = datetime.now()
    posted_date_elemet = current_time - timedelta(hours=24)
    posted_date = posted_date_elemet.date()
    job_data['date_posted'] = posted_date
    # print(posted_date)
    
    
    criteria = {}
    for item in soup.find_all('li', class_='description__job-criteria-item'):
        header = item.find('h3', class_='description__job-criteria-subheader').text.strip()
        text = item.find('span', class_='description__job-criteria-text description__job-criteria-text--criteria').text.strip()
        criteria[header] = text

    # Print the extracted information
    for key, value in criteria.items():
        if key == "Employment type":
            job_data['job_type'] = value
    # print(employment_type)
    
    ''' Get Job ID'''

    if link.split('/')[-1].split('?')[0] :
        jobid_element = link.split('/')[-1].split('?')[0]
        pattern = re.compile(r'-(\d+)$')
        match = pattern.search(jobid_element)
        if match:
            job_data['jobid'] = match.group(1)
    # print(jobid)
    
    
    ''' Get Job Description '''
    scroll_to_element_and_click(link)
    full_description = ""
    full_description_element = soup.find('div', class_='show-more-less-html__markup') #show-more-less-html__markup relative overflow-hidden
    if full_description_element:
        job_data['jobdescription'] = str(full_description_element)
    # print(full_description)
    
    
    ''' Get Email'''
    contact_emails = extract_emails(full_description)
    if contact_emails:
        job_data['email'] = contact_emails[0]
    # print(contact_emails, contact_email)
    
    
    ''' Get Salary '''
 
    salary_elm = soup.find('div', class_='salary compensation__salary')
    if salary_elm:
        salary_text = salary_elm.text.strip()
        salary_type, min_salary, max_salary = get_salary_info(salary_text)
        job_data['salaryrange'] = f"{min_salary} - {max_salary}"
        job_data['salarytype'] = salary_type
        
    job_data['source_name'] = "linkedin"
    job_data['company_logo'] = "company_logo"

    return job_data


def job_titles(soup):
    
    ''' Get Job Roles '''
    job_title = ''
    job_title_element = soup.find('h1', class_='top-card-layout__title font-sans text-lg papabear:text-xl font-bold leading-open text-color-text mb-0 topcard__title')    
    job_title_check = job_title_element.get_text(strip=True) if job_title_element else 'No title found'

    if job_title_check != 'No title found':
        job_title = job_title_check
        
    else:
        job_title = ""
    
    return job_title
    


def main():
    directory = '/Users/vedanggharat/Movies/LinkedIn Jobs/text_links'
    most_recent_file = read_most_recent_jobs.find_most_recent_file(directory)
    print(most_recent_file)
    if most_recent_file:
        # print(f"Processing file: {most_recent_file}")
        job_links = read_most_recent_jobs.read_job_links_from_txt(most_recent_file)
        # Placeholder for actual job descriptions set
        jobs_descriptions = set()
        # file_save_path = '/Users/vedanggharat/Movies/LinkedIn Jobs/Complete_data'
        
        for link in job_links:
            response = requests.get(link)
            soup = BeautifulSoup(response.text, 'html.parser')
            job_title = job_titles(soup)
            final_dict = complete_data(soup, link, job_title)
    else:
        print("No recent file to process.")

if __name__ == "__main__":
    main()

