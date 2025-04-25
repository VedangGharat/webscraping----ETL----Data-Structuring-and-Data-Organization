import os
import sys
import json
import requests
from bs4 import BeautifulSoup
import logging
import read_most_recent_jobs  # Ensure this module is implemented correctly
from datetime import datetime, timedelta
from helper_for_location_filter import get_location_details
import re
from datetime import datetime, date
import time
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
import csv
from config import extract_job_data as e
from helper_function_for_scroll_more_for_job_link import scroll_to_element_and_click
from helper_for_salary_range_type import get_salary_info
# from client_jobs_insert import client_read_csv_jobs
# from job_id_fetch import get_client_jobid,get_predata_joblink
# from main import paths_to_folders
# csv_jobs_path,log_file_path,sites_status_json,path_to_inputfiles = paths_to_folders()
# from main import paths_to_folders
# csv_jobs_path,log_file_path,sites_status_json,path_to_inputfiles = paths_to_folders()
# import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from fake_useragent import UserAgent
from selenium import webdriver
# Setup logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger()
            
    
""" Saving Data to Json"""
def save_to_json(data):
    # Define the directory path and file name for the single JSON file
    json_jobs_path = e.json_jobs_path
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

def save_to_csv(data, csv_file_path):
    """Save job data to a CSV file."""
    try:
        # Ensure the CSV file path is valid
        file_exists = os.path.isfile(csv_file_path)

        # Open the CSV file in append mode
        with open(csv_file_path, mode='a', newline='') as csv_file:
            # Create a CSV DictWriter
            writer = csv.DictWriter(csv_file, fieldnames=data.keys())

            # Write the header if the file does not exist
            if not file_exists:
                writer.writeheader()

            # Write the data
            writer.writerow(data)

        # print(f'Data has been appended to {csv_file_path}')
    except Exception as e:
        print(f'An error occurred while writing to CSV: {e}')
        
        

""" Extracting Emails """
def extract_emails(text):
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    return re.findall(email_pattern, text)


""" Extracting information required for data preprocessing """
def complete_data(soup, link, job_role, db_joblinks,within_joblinks,joblink_list_predata, csv_file_path):
    
    # print("job", job_role)
    try :
        if link:
            within_joblinks.append(link)
            ''' Get Company Name if available '''
            company_name = ""
            company_name_element = soup.find('a', class_="topcard__org-name-link topcard__flavor--black-link")
            company_name_check = company_name_element.get_text(strip=True) if company_name_element else ''

            if company_name_check != '':
                company_name = company_name_check
            else:
                company_name = ''
            # print("Company Name:", company_name)
            
            
            ''' Get Location if available '''
            location = ""
            location_element = soup.find('span', class_="topcard__flavor topcard__flavor--bullet")
            location_check = location_element.get_text(strip=True) if location_element else ''

            if location_check != '':
                location = location_check
            else:
                location = 'Remote'
            # print(location)
            
            ''' Get City, State, State_Code, Location '''
            city, state, state_code = get_location_details(location)
            # print(city, state, state_code)
            
            ''' Get Date '''
            current_time = datetime.now()
            posted_date_elemet = current_time - timedelta(hours=24)
            posted_date = posted_date_elemet.date()
            # print(posted_date)
            
            
            criteria = {}
            for item in soup.find_all('li', class_='description__job-criteria-item'):
                header = item.find('h3', class_='description__job-criteria-subheader').text.strip()
                text = item.find('span', class_='description__job-criteria-text description__job-criteria-text--criteria').text.strip()
                criteria[header] = text

            # Print the extracted information
            employment_type = ""
            for key, value in criteria.items():
                if key == "Employment type":
                    employment_type = value
            # print(employment_type)
            
            ''' Get Job ID'''
            jobid = ''
            if link.split('/')[-1].split('?')[0] :
                jobid_element = link.split('/')[-1].split('?')[0]
                pattern = re.compile(r'-(\d+)$')
                match = pattern.search(jobid_element)
                if match:
                    jobid = match.group(1)
            # print(jobid)
            
            
            ''' Get Job Description '''
            scroll_to_element_and_click(link)
            full_description = ""
            full_description_element = soup.find('div', class_='show-more-less-html__markup') #show-more-less-html__markup relative overflow-hidden
            full_description = str(full_description_element) if full_description_element else "" 
            # print(full_description)
            
            
            ''' Get Email'''
            contact_emails = extract_emails(full_description)
            contact_email = contact_emails[0] if contact_emails else ""
            # print(contact_emails, contact_email)
            
            
            ''' Get Salary '''
            salary_type = ""
            salary_range = ""
            salary_elm = soup.find('div', class_='salary compensation__salary')
            if salary_elm:
                salary_text = salary_elm.text.strip()
                salary_type, min_salary, max_salary = get_salary_info(salary_text)
                salary_range = f"{min_salary} - {max_salary}"
            else:
                salary_type = ""
                salary_range = ""
            # print(salary_type, salary_range)
            
            
            if link :
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
                    'salaryrange': salary_range,
                    'salarytype': salary_type,
                    'email': contact_email,
                    'contact_name': "",
                    'source_name': "linkedin",
                    'company_logo': "company_logo",
                    "experience" :"",
                    "workpermit" :"",
                    "skills":"",
                    "companyid":""
                }
            # print(job_data)
            save_to_csv(job_data, csv_file_path)
            # print(f"Scraped job: {job_role} at {company_name}")
            time.sleep(1) 
    except Exception as e: 
        logger.error("An error occurred--", exc_info=True)
        print(e)
        return within_joblinks
 
    return within_joblinks
    # save_to_json(job_data)


def job_titles(soup):
    # print(type(soup))
    ''' Get Job Roles '''
    job_title_element = soup.find('h1', class_='top-card-layout__title')
    
    if job_title_element:
        job_title = job_title_element.get_text(strip=True)
    else:
        job_title = ''
    
    # print(job_title)
    return job_title

def setup_webdriver():
    """Configure and return a headless Chrome WebDriver with randomized user agent."""
    ua = UserAgent()
    options = Options()
    options.add_argument(f'user-agent={ua.random}')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def main(dir_name, csv_name):
    driver = setup_webdriver()  # Initialize WebDriver
    try:
        directory = dir_name
        most_recent_file = read_most_recent_jobs.find_most_recent_file(directory)
        # print(f"Processing file: {most_recent_file}")
        joblink_list_predata = ""
        db_joblinks = ""
        within_joblinks = []
        # ... (remaining variable declarations remain unchanged)

        within_joblinks = []
    
    
        # Create the directory for CSV jobs if it doesn't exist
        csv_jobs_path = csv_name
        os.makedirs(csv_jobs_path, exist_ok=True)
        # Define the CSV file path with the current date and time
        csv_file_path = os.path.join(csv_jobs_path, f'linkedin_jobs_data_{datetime.now().strftime("%Y-%m-%d_%H-%M")}.csv')
        count = 1       
        
        if most_recent_file:
            job_links = read_most_recent_jobs.read_job_links_from_txt(most_recent_file)
            # print(f"Found {len(job_links)} job links to process")
            
            for idx, link in enumerate(set(job_links), 1):
                try:
                    driver.get(link)
                    time.sleep(2)  # Allow page load time
                    
                    # Get fully rendered page source
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    job_role = job_titles(soup)
                    
                    # print(f"\nProcessing job {idx}: {job_role}")
                    within_joblinks = complete_data(
                        soup, link, job_role, 
                        db_joblinks, within_joblinks,
                        joblink_list_predata, csv_file_path
                    )
                    
                except Exception as e:
                    logger.error(f"Error processing {link}: {str(e)}")
                    continue

            # print(f"\n{'='*50}\nFinished processing {len(job_links)} jobs")
        else:
            print("No recent file found for processing")
            
    finally:
        driver.quit()  # Ensure clean shutdown
        print("WebDriver session closed")


if __name__ == "__main__":
    main(e.text_links, e.saved_extracted_data)

