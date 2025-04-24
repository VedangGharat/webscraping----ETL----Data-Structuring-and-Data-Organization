from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from random import randint

def scroll_to_element_and_click(url):
    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('ignore-certificate-errors')  # Ignore certificate errors
    chrome_options.add_argument('incognito')  # Open in incognito mode
    chrome_options.add_argument("--disable-notifications")  # Disable notifications
    chrome_options.add_argument("--start-maximized")  # Start maximized

    # Set up the Chrome driver
    driver = webdriver.Chrome(options=chrome_options)

    try:
        # Open the URL
        driver.get(url)

        # Wait until the "Show more" button is present
        wait = WebDriverWait(driver, randint(1, 15))
        show_more_button = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'button.show-more-less-html__button.show-more-less-button')))

        # Scroll smoothly to the "Show more" button
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", show_more_button)

        # Wait for the smooth scroll to complete (if necessary)
        time.sleep(randint(1, 15))  # Adjust sleep time if necessary

        # Click the "Show more" button
        show_more_button.click()

        # Optionally: Wait for the new content to load
        time.sleep(randint(1, 15))  # Adjust sleep time if necessary

        # Optionally: Print or process the page source
        # print(driver.page_source)
    except:
        pass
    finally:
        # Close the browser
        driver.quit()
