import os
import logging
import re
from bs4 import BeautifulSoup
import requests
from datetime import datetime

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def find_most_recent_file(directory):
    """Find the most recent text file in the given directory."""
    files = [f for f in os.listdir(directory) if f.endswith('.txt')]
    if not files:
        logger.warning("No text files found in the directory.")
        return None

    # Date-time format that matches the filenames
    date_time_format = "%Y-%m-%d_%H-%M"

    def extract_date(file_name):
        """Extract date from file name assuming format `linkedin_jobs_on_title_YYYY-MM-DD_HH-MM.txt`."""
        try:
            # Extract date part from filename (assumes format like `linkedin_jobs_on_title_YYYY-MM-DD_HH-MM.txt`)
            date_str = file_name.split('_')[-2] + '_' + file_name.split('_')[-1].replace('.txt', '')
            return datetime.strptime(date_str, date_time_format)
        except ValueError:
            logger.error(f"Date parsing error for file: {file_name}")
            return None

    files_with_dates = [(f, extract_date(f)) for f in files]
    files_with_dates = [f for f in files_with_dates if f[1] is not None]
    
    if not files_with_dates:
        logger.warning("No valid date formats found in the file names.")
        return None
    
    files_with_dates.sort(key=lambda x: x[1], reverse=True)
    return os.path.join(directory, files_with_dates[0][0])

def read_job_links_from_txt(txt_file_path):
    job_links = []
    try:
        with open(txt_file_path, 'r', encoding='utf-8') as file:
            job_links = [line.strip() for line in file if line.strip()]
    except Exception as e:
        logger.error("Error reading text file", exc_info=True)
    return job_links
