from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
import time
from selenium import webdriver
import csv
from datetime import datetime, timedelta, date
import os
from random import randint
# from client_jobs_insert import client_read_csv_jobs
# from job_id_fetch import get_client_jobid
# from main import paths_to_folders
# csv_jobs_path,log_file_path,sites_status_json,path_to_inputfiles = paths_to_folders()
import logging
# import logging_config
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
# from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent
from datetime import datetime, timedelta
# import mysql.connector
# from mysql.connector import Error
# import pysolr
import json

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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

def save_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, mode='a', header=not pd.io.common.file_exists(filename), index=False)
    
def save_to_text(data, filename):
    print(data)
    with open(filename, 'a') as file:  # Open the file in append mode
        # Check if the file exists to decide if headers are needed
        header_needed = not os.path.exists(filename)
        # print(len(data))
        # If headers are needed, write them to the file
        if header_needed:
            headers = ', '.join(data[0].keys())  # Extract headers from the first item
            file.write(f"{headers}\n")
        
        # Write each dictionary in data as a line in the file
        for item in data:
            
            line = ', '.join(str(value) for value in item.values())
            file.write(f"{line}\n")

def linkedin_search(driver, search_titles, csv_file_path, text_file_path):
    
    for search_title in search_titles:
        url = f"https://www.linkedin.com/jobs/search/?currentJobId=3998245275&f_TPR=r3600&geoId=103644278&keywords={search_title.replace(' ', '%20')}&origin=JOB_SEARCH_PAGE_JOB_FILTER&refresh=true&sortBy=R&spellCorrectionEnabled=true"
        
        Linkedin([url], csv_file_path, text_file_path)

job_ids = set()
jobs_descriptions = set()
job_links = set()

def only_linkes(job_elem, csv_file_path, text_file_path):
    for e in job_elem:
        try :
            link_element = e.find('a') 
            joblink = link_element['href'] if link_element else '' 
            job_id = joblink.split('/')[-1].split('&')[0].split('-')[-1]

            if joblink and job_id not in job_ids:
                job_ids.add(job_id)
                
                job_data = {
                    'joblinks' : joblink
                }

                save_to_csv([job_data] ,csv_file_path)
                save_to_text([job_data] ,text_file_path)
                print(f"Scraped job link: {joblink} ")
                print()
        except : continue

def Linkedin(URL,csv_file_path, text_file_path):
    try:
        for url in URL:
            
            driver.get(url)
            time.sleep(randint(1, 10))
            print(f"LinkedIn URL: {url}")

            try:
                continue_button = driver.find_element(By.CLASS_NAME, "promo-bottom-sheet__dismiss")
                if continue_button.is_displayed():
                    continue_button.click()
                    print("Clicked 'Continue' button")
                    time.sleep(randint(1, 10))  
            except Exception as e:
                pass

            # Scroll down the page in smaller increments until no more jobs are loaded
            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
                last_height = driver.execute_script("return document.body.scrollHeight")
                driver.execute_script("window.scrollTo(0, arguments[0] * 0.95);", last_height)
                time.sleep(randint(1, 10)) 

                new_height = driver.execute_script("return document.body.scrollHeight")
                # print(f"Scrolled to height: {new_height}")
                if new_height == last_height:
                    break
                last_height = new_height

            # Click "See more jobs" button up to 5 times if present
            click_count = 0
            while click_count < 10:
                try:
                    see_more_button = driver.find_element(By.XPATH, '//button[@aria-label="See more jobs"]')  #'//button[contains(@class, "infinite-scroller__show-more-button")]'
                    if see_more_button.is_displayed():
                        see_more_button.click()
                        time.sleep(randint(1, 10))  
                        last_height = driver.execute_script("return document.body.scrollHeight")
                        while True:
                            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")  
                            time.sleep(randint(1, 10)) 
                            new_height = driver.execute_script("return document.body.scrollHeight")
                            # print(f"Scrolled to height after click: {new_height}")
                            if new_height == last_height:
                                print("Reached Bottom")
                                break
                            last_height = new_height 

                        click_count += 1
                    else:
                        last_height = driver.execute_script("return document.body.scrollHeight")
                        driver.execute_script("window.scrollTo(0, arguments[0] * 0.95);", last_height)
                        time.sleep(randint(1, 10)) 
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
            print("Number of Job Elements:", len(job_elem), job_elem)

            only_linkes(job_elem, csv_file_path, text_file_path)


    except Exception as e:
        print('Error:', e)
        logger.error("An error occurred--", exc_info=True)

if __name__ == '__main__':
    df_titles = pd.read_csv("/Users/vedanggharat/Movies/LinkedIn Jobs/Bs_title.csv")
    csv_jobs_path = "/Users/vedanggharat/Movies/LinkedIn Jobs/jobs_scraped_files"
    text_jobs_path = "/Users/vedanggharat/Movies/LinkedIn Jobs/text_links"
    os.makedirs(csv_jobs_path,exist_ok=True)
    os.makedirs(text_jobs_path,exist_ok=True)
    
    csv_file_path = os.path.join(csv_jobs_path,f'linkedin_jobs_on_title_{datetime.now().strftime("%Y-%m-%d_%H-%M")}.csv')
    text_file_path = os.path.join(text_jobs_path,f'linkedin_jobs_on_title_{datetime.now().strftime("%Y-%m-%d_%H-%M")}.txt')

    for _, row in df_titles.iterrows():
        search_titles = []
        search_titles.append(row['Search1'].strip())
        search_titles.extend(row['Search2'].split(','))
        
        linkedin_search(driver, search_titles, csv_file_path, text_file_path)

