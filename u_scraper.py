import csv
import json
import re
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
import time

search_keywords = ["Data Science"]
locations = ["Hong Kong SAR"]
pattern = r"\bposted\b"
flags = re.IGNORECASE

xpaths = {
    "search_keyword_input_field": '/html/body/div[1]/div/div[6]/div/div[1]/div/div/div/div/section/div[2]/form/div[2]/div[1]/div/div[1]/div/div[2]/div[1]/div/div[2]/div/div/input',
    "search_btn": '/html/body/div[1]/div/div[6]/div/div[1]/div/div/div/div/section/div[2]/form/div[2]/div[3]/button/span',
    "location_input_field": '/html/body/div[1]/div/div[6]/div/div/div/div/div/div/section/div[2]/form/div[2]/div[2]/div/div[2]/div[1]/div/div[2]/div/div/input',
    "job_card_list": '/html/body/div[1]/div/div[6]/div/section/div[2]/div/div/div[1]/div/div/div/div/div[1]/div/div[1]/div[2]/div[2]',
    "nxt_page_btn": '/html/body/div[1]/div/div[6]/div/section/div[2]/div/div/div[1]/div/div/div/div/div[1]/div/div[3]/div/nav/ul/li[5]'
}

css_selectors = {
    "job_detail_page": "[data-automation='jobDetailsPage']",
    "job_title": "[data-automation='job-detail-title']",
    "company_name": "[data-automation='advertiser-name']",
    "job_posting_url": "[data-automation='job-detail-apply']",
    "location": "[data-automation='job-detail-location']",
    "industry_type": "[data-automation='job-detail-classifications']",
    "work_type": "[data-automation='job-detail-work-type']",
    "job_description": "[data-automation='jobAdDetails']"
}

def trytogetobject(css_selector, parent):
    if css_selector != 'job_posting_url':
        try:
            capture_item = parent.find_element(By.CSS_SELECTOR, css_selectors[css_selector]).text
        except (NoSuchElementException, TimeoutException, StaleElementReferenceException) as e:
            print(f"Skipping {css_selector}, likely an ad or missing data: {e}")
    
    else:
        try:
            capture_item = parent.find_element(By.CSS_SELECTOR, css_selectors[css_selector]).get_attribute('href')
        except (NoSuchElementException, TimeoutException, StaleElementReferenceException) as e:
            print(f"Skipping {css_selector}, likely an ad or missing data: {e}")
    
    return capture_item

def calculate_posted_date(posted_time_text):
    """
    Calculates the approximate date a job was posted based on text like "Posted 14d ago".
    """
    if not posted_time_text:
        return None

    posted_time_text = posted_time_text.replace("Posted ", "").strip()

    if "d ago" in posted_time_text:
        days = int(posted_time_text.replace("d ago", ""))
        date = datetime.now() - timedelta(days=days)
        return date.strftime("%Y%m%d")
    elif "h ago" in posted_time_text:
        hours = int(posted_time_text.replace("h ago", ""))
        date = datetime.now() - timedelta(hours=hours)
        return date.strftime("%Y%m%d")
    elif "m ago" in posted_time_text:
        return datetime.now().strftime("%Y%m%d") 
    else:
        return None


def main():
    """
    This is the main function of the scraper.
    """
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    wait = WebDriverWait(driver, 10)

    output = []

    file_name = 'updated_job_posting.csv'
    
    existing_urls = set()
    if os.path.isfile(file_name):
        with open(file_name, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'job_posting_url' in row:
                    existing_urls.add(row['job_posting_url'])
    print(f"Found {len(existing_urls)} existing job URLs in {file_name}.")
    
    file_exists = os.path.isfile(file_name)
    print(f"file exist : {file_exists}")
    
    with open(file_name, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ["location", "search_keyword", "job_title", "company_name", "job_posting_url", "industry_type", "work_type", "posted_time", "job_description"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
            print("writing header because file did not exist")

        for location in locations:
            for keyword in search_keywords:
                driver.get("https://hk.jobsdb.com/")  # Replace with the actual URL

                # Input search keyword
                search_keyword_input = wait.until(EC.presence_of_element_located((By.XPATH, xpaths["search_keyword_input_field"])))
                search_keyword_input.send_keys(keyword)

                # Input location
                location_input = wait.until(EC.presence_of_element_located((By.XPATH, xpaths["location_input_field"])))
                location_input.send_keys(location)

                # Click search button
                search_btn = wait.until(EC.element_to_be_clickable((By.XPATH, xpaths["search_btn"])))
                search_btn.click()

                page = 1
                while True:
                    time.sleep(1)
                    job_card_list = wait.until(EC.presence_of_element_located((By.XPATH, xpaths["job_card_list"])))
                    job_cards = job_card_list.find_elements(By.XPATH, "./div")
                    
                    for job_card in job_cards:
                            time.sleep(0.5)
                            try:  
                                job_card.click()
                                job_detail_page = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, css_selectors["job_detail_page"])))
                            except:
                                print(f"unable to click job card {job_card}")
                                time.sleep(1)
                                continue
                            # job_title = job_detail_page.find_element(By.CSS_SELECTOR, css_selectors["job_title"]).text
                            job_title = trytogetobject('job_title', job_detail_page)
                            # company_name = job_detail_page.find_element(By.CSS_SELECTOR, css_selectors["company_name"]).text
                            company_name = trytogetobject('company_name', job_detail_page)
                            # job_posting_url = job_detail_page.find_element(By.CSS_SELECTOR, css_selectors["job_posting_url"]).get_attribute('href')
                            job_posting_url = trytogetobject('job_posting_url', job_detail_page)
                            
                            if job_posting_url in existing_urls:
                                print(f"Skipping duplicate job: {job_title}")
                                time.sleep(1)
                                continue
                            # location = job_detail_page.find_element(By.CSS_SELECTOR, css_selectors["location"]).text
                            location = trytogetobject('location', job_detail_page)
                            # industry_type = job_detail_page.find_element(By.CSS_SELECTOR, css_selectors["industry_type"]).text
                            industry_type = trytogetobject('industry_type', job_detail_page)
                            # work_type = job_detail_page.find_element(By.CSS_SELECTOR, css_selectors["work_type"]).text
                            work_type = trytogetobject('work_type', job_detail_page)
                            sub_elements = job_detail_page.find_elements(By.CSS_SELECTOR, "*")
                            for elements in sub_elements:
                                elements_text = elements.text
                                if "ago" in elements_text:
                                    match = re.search(pattern, elements_text, flags)
                                    end = match.end()
                                    posted_info = elements_text[match.start(): end + 8]
                                    break
                            posted_time = calculate_posted_date(posted_info)
                            print(posted_info, posted_time)
                            ## posted_time = calculate_posted_date(sub_elements.text)    
                            job_description = trytogetobject('job_description', job_detail_page)
                                               

                            job_data = {
                                "search_keyword": keyword,
                                "location": location,
                                "job_title": job_title,
                                "company_name": company_name,
                                "job_posting_url": job_posting_url,
                                "industry_type": industry_type,
                                "work_type": work_type,
                                "posted_time": posted_time,
                                "job_description": job_description
                            }

                            output.append(job_data)
                            writer.writerow(job_data)
                            csvfile.flush()
                            print(f'scraped {job_title} at {company_name}')

                    try:
                        page += 1
                        nxt_page_btn = driver.find_element(By.CSS_SELECTOR, f"[data-automation='page-{page}']")
                        nxt_page_btn.click()
                        time.sleep(4)  # Wait for page to load
                    except (NoSuchElementException, TimeoutException) as e:
                        print(f"No more pages or error navigating to next page: {e}")
                        break  # No next page button found

    with open('job_postings.json', 'w', encoding='utf-8') as jsonfile:
        json.dump(output, jsonfile, ensure_ascii=False, indent=4)

    driver.quit()


if __name__ == "__main__":
    main()