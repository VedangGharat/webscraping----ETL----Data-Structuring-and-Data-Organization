from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
import time
from selenium import webdriver
import csv
from datetime import datetime, timedelta, date
import os
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

def setup_webdriver():
    ua = UserAgent()
    RandomUserAgent = ua.random
    options = webdriver.ChromeOptions()
    options.add_argument(f'user-agent={RandomUserAgent}')
    options.add_argument('ignore-certificate-errors')
    options.add_argument('incognito')
    options.add_argument("--disable-notifications")
    options.add_argument("--start-maximized")
    # options.add_argument('headless')  # Run in headless mode
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver
driver = setup_webdriver()

logger = logging.getLogger()
def extract_emails(text):
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    return re.findall(email_pattern, text)

def extract_city_state(location):
    pattern = r'(?i)(?:Remote|Hybrid remote|On-Site|Hybrid)\s*in\s*([^0-9,\(\)]+),\s*([A-Za-z\s]+)|([^0-9,\(\)]+),\s*([A-Za-z\s]+)'
    match = re.search(pattern, location)
    if match:
        city = match.group(1) or match.group(3)
        state = match.group(2) or match.group(4)
        return f"{city.strip()}, {state.strip()}"
    
# def save_to_csv(data, file_save_path):
#     df = pd.DataFrame([data])
#     df.to_csv(file_save_path, mode='a', header=not os.path.isfile(os.path.basename(file_save_path)), index=False) 


def save_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, mode='a', header=not pd.io.common.file_exists(filename), index=False)

def Linkedin(url,file_save_path,db_joblinks,within_joblinks,joblink_list_predata):
    try:
        
        # url = URL.format( location=location)
        # html = requests.get(url).content 
        driver.get(url)
        # html = driver.get(url)
        time.sleep(2)
        print(f"LinkedIn URL: {url}")

        try:
            continue_button = driver.find_element(By.CLASS_NAME, "promo-bottom-sheet__dismiss")
            if continue_button.is_displayed():
                continue_button.click()
                print("Clicked 'Continue' button")
                time.sleep(3)  
        except Exception as e:
            pass

        # Scroll down the page in smaller increments until no more jobs are loaded
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
            last_height = driver.execute_script("return document.body.scrollHeight")
            driver.execute_script("window.scrollTo(0, arguments[0] * 0.95);", last_height)
            time.sleep(5) 

            new_height = driver.execute_script("return document.body.scrollHeight")
            # print(f"Scrolled to height: {new_height}")
            if new_height == last_height:
                break
            last_height = new_height

        # Click "See more jobs" button up to 5 times if present
        click_count = 0
        while click_count < 5:
            try:
                see_more_button = driver.find_element(By.XPATH, '//button[@aria-label="See more jobs"]')  #'//button[contains(@class, "infinite-scroller__show-more-button")]'
                if see_more_button.is_displayed():
                    see_more_button.click()
                    time.sleep(5)  
                    last_height = driver.execute_script("return document.body.scrollHeight")
                    while True:
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")  
                        time.sleep(5) 
                        new_height = driver.execute_script("return document.body.scrollHeight")
                        # print(f"Scrolled to height after click: {new_height}")
                        if new_height == last_height:
                            print("Reached Bottom")
                            break
                        last_height = new_height 

                    click_count += 1
                    # try:
                    #     see_more_button = driver.find_element(By.XPATH, '//button[@aria-label="See more jobs"]')
                    #     if see_more_button.is_displayed():
                    #         see_more_button.click()
                    #         print("Clicked 'See more jobs' button")
                    #         time.sleep(5)  
                    #     else:
                    #         break
                    # except Exception as e:
                    #     print(f"No more 'See more jobs' button found or an error occurred: {e}")
                    #     continue
                    # print("Click Count:", click_count)
                else:
                    last_height = driver.execute_script("return document.body.scrollHeight")
                    driver.execute_script("window.scrollTo(0, arguments[0] * 0.95);", last_height)
                    time.sleep(5) 
                    notification = driver.find_element(By.XPATH, '//p[contains(text(), "You\'ve viewed all jobs for this search")]')
                    if notification.is_displayed():
                        print("All jobs have been viewed.")
                        break 
                    break
            except :
                # print(f"No more 'See more jobs' button found or an error occurred: ")
                break
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        job_elem = soup.find_all('div', class_='base-card')
        print("Number of Job Elements:", len(job_elem))
        

        for e in job_elem[:100]:
            try :
                link_element = e.find('a')
                joblink = link_element['href'] if link_element else ''
                if joblink not in db_joblinks.keys() and joblink not in within_joblinks and joblink not in joblink_list_predata.keys():
                    within_joblinks.append(joblink)
                    details = e.find('div', class_='base-search-card__info')
                    job_title_element = details.find('h3', class_='base-search-card__title')
                    job_role = job_title_element.text.strip() if job_title_element else ''
                    company_element = e.find('h4', class_='base-search-card__subtitle')
                    company_name = company_element.text.strip() if company_element else ''
                    try :company_logo  = e.find('img')['data-delayed-url']
                    except : company_logo = ''
                    
                    posted_date_element = e.find('time', class_='job-search-card__listdate--new')
                    posted_date = posted_date_element['datetime'] if posted_date_element else ''
                    
                    response = requests.get(joblink)
                    time.sleep(1)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        
                        location_element = e.find('span', class_='job-search-card__location')
                        location = location_element.text.strip() if location_element else ''
                        
                        if 'united'in location.lower() :
                            location = location.replace(', United States', '')
                        
                        # print("Location BSplit:", location)
                        city = shortregion = state =  ''
                        jobid = ''
                        location_split = location.split(',')
                        # print("Location:", location_split)

                        if joblink.split('/')[-1].split('?')[0] :
                            jobid = joblink.split('/')[-1].split('?')[0]

                        if 'remote' in location.lower() or 'remote' in job_role.lower() :
                            city = shortregion = state = location = ''
                        
                        elif len(location_split) == 1 :
                            state = location_split[0]
                            location = city = shortregion = ''
                        elif len(location_split[-1].strip()) == 2 :
                            shortregion = location_split[-1]
                            city = location_split[0]
                            location = f'{city}, {shortregion}'
                            state  = ''
                        else :
                            state = location_split[-1]
                            city = location_split[0]
                            location = f'{city}, {state}'
                            shortregion  = ''  
                        
                        try:
                            job_salary_elm = soup.find('div', class_='compensation__salary').text.strip()
                            if '/' in job_salary_elm:
                                salary_nums = job_salary_elm.split('-')
                                job_salary_elm = ' - '.join(sal.split('/')[0].strip() for sal in salary_nums)
                                salary_type = salary_nums[0].split('/')[1].strip()
                        except:
                            job_salary_elm = ''
                            salary_type =''
                        
                        full_description_element = soup.find('div', class_='show-more-less-html__markup') #show-more-less-html__markup relative overflow-hidden
                        full_description = str(full_description_element) if full_description_element else ""
                        # if full_description_element :
                        #     print("*" * 100) 
                        # else :
                        #     print("No ----------")
                            

                        employment_type = ""
                        job_criteria_items = soup.find_all('li', class_='description__job-criteria-item')
                        for item in job_criteria_items:
                            header = item.find('h3', class_='description__job-criteria-subheader').text.strip()
                            if header == "Employment type":
                                employment_type = item.find('span', class_='description__job-criteria-text').text.strip()
                                break
                        contact_emails = extract_emails(full_description)
                        contact_email = contact_emails[0] if contact_emails else ""
                        
                        job_data = {
                            'title': job_role,
                            'companyname': company_name,
                            'joblink': joblink,
                            'location': location,
                            'date_posted': posted_date,
                            'state': state,
                            'shortregion': shortregion,
                            'city': city,
                            'jobtype': employment_type,
                            'jobdescription': full_description,
                            'jobid': jobid,
                            'salaryrange': job_salary_elm,
                            'salarytype': salary_type,
                            'email': contact_email,
                            'contact_name': "",
                            'source_name': "linkedin",
                            'company_logo': company_logo,
                            "experience" :"",
                            "workpermit" :"",
                            "skills":"",
                            "companyid":""
                        }
                        save_to_csv([job_data] ,file_save_path)
                        print(f"Scraped job: {job_role} at {company_name}")
                        time.sleep(1) 
            except Exception as e: 
                logger.error("An error occurred--", exc_info=True)
                print(e)
        return within_joblinks
    except Exception as e:
        print('Error:', e)
        logger.error("An error occurred--", exc_info=True)
        
joblink_list_predata = get_predata_joblink()
db_joblinks = get_client_jobid('linkedin')
df = pd.read_csv(os.path.join(path_to_inputfiles, '/Users/vedanggharat/Movies/LinkedIn Jobs/jobs_scraped_files/linkedin_jobs_on_title_2024-09-24_12-39.csv'))
print(df)
file_save_path = os.path.join(csv_jobs_path,f'linkedin_jobs_1_{date.today().strftime("%Y-%m-%d")}.csv')
URL = df['joblinks'].tolist()
within_joblinks = []
url_count = 1
for url in URL:
    within_joblinks = Linkedin(url,file_save_path,db_joblinks,within_joblinks,joblink_list_predata)
    url_count+=1
    if url_count == 26:
        client_read_csv_jobs(file_save_path,report_name = 'linkedin')
        file_save_path = os.path.join(csv_jobs_path,f'linkedin_jobs_2_{date.today().strftime("%Y-%m-%d")}.csv')
        joblink_list_predata = get_predata_joblink()
        
client_read_csv_jobs(file_save_path,report_name = 'linkedin')