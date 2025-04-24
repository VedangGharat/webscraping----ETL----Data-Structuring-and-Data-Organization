# logging_config.py

import logging
from logging.handlers import TimedRotatingFileHandler
import os
from datetime import date
from main import paths_to_folders
csv_jobs_path,log_file_path,sites_status_json,path_to_inputfiles = paths_to_folders()

def setup_logger():
       
    # Create the directory if it doesn't exist
    if not os.path.exists(log_file_path):
        os.makedirs(log_file_path)

    log_filename = os.path.join(log_file_path, f"errors_{date.today().strftime('%Y-%m-%d')}.log")
    logger = logging.getLogger()
    logger.setLevel(logging.ERROR)

    handler = TimedRotatingFileHandler(log_filename, when="midnight", interval=1)
    handler.suffix = "%Y-%m-%d"
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

setup_logger()