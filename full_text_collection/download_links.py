import os
import json
import time
import random
import requests
import gzip
import platform
import argparse
from queue import Queue, Empty
from threading import Thread, Event
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util import Retry

import certifi
import pandas as pd
from newsplease import NewsPlease, SimpleCrawler
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

session = requests.Session()
retries = Retry(total=3, backoff_factor=1,
                status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)

# List of bad sources, domain to failed times
bad_sources = {}
skip = [
    "upstract.com",
    "www.bloomberg.com"
]

scroll_pause_time = 2
FAIL_THRESHOLD = 8

# Define a global list of user agents for both Selenium and requests
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Mobile/15E148 Safari/604.1",
]


def new_chrome_options(extension_path=None):
    options = uc.ChromeOptions()
    options.add_argument(f"user-agent={random.choice(user_agents)}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--remote-allow-origins=*")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--allow-insecure-localhost")
    options.add_argument("--disable-gpu")
    if extension_path:
        options.add_argument(f'--load-extension={extension_path}')
    # If no extension_path is provided, you could either add a default extension or omit the argument.
    prefs = {
        # Disable images
        "profile.managed_default_content_settings.images": 2,
        # Disable videos (if supported)
        "profile.managed_default_content_settings.videos": 2
    }
    options.add_experimental_option("prefs", prefs)
    return options


def save_pdf_from_response(response, pdf_filename):
    """Save the PDF content from the response to a file."""
    try:
        with open(pdf_filename, 'wb') as pdf_file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:  # Filter out keep-alive new chunks
                    pdf_file.write(chunk)
        return f"Saved PDF: {pdf_filename}"
    except Exception as e:
        return f"Error saving PDF to file: {e}"


def download_pdf(link, pdf_filename, stream=True, verify_ssl=True):
    """Attempt to download a PDF with the given parameters."""
    try:
        response = requests.get(
            link,
            timeout=10,
            stream=stream,
            verify=certifi.where() if verify_ssl else False,
            headers={'User-Agent': random.choice(user_agents)},
        )
        if response.status_code == 200:
            return save_pdf_from_response(response, pdf_filename)
        else:
            return f"Failed to download PDF: HTTP {response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"Download error: {e}"


def process_pdf_download(link, pdf_filename):
    """Handle the PDF download process with multiple fallbacks."""
    # Attempt with streaming and SSL verification
    result = download_pdf(link, pdf_filename, stream=True, verify_ssl=True)
    if "Saved PDF" in result:
        return result

    # Retry without streaming
    print(f"Retrying {link} without streaming.")
    result = download_pdf(link, pdf_filename, stream=False, verify_ssl=True)
    if "Saved PDF" in result:
        return result

    # Final retry with SSL verification disabled
    print(f"Retrying {link} with SSL verification disabled.")
    result = download_pdf(link, pdf_filename, stream=False, verify_ssl=False)
    return result


def scroll_to_bottom(driver, pause_time=1):
    """Scrolls to the bottom of the page to load all dynamic content."""
    try:
        last_height = driver.execute_script("return document.body.scrollHeight")
        for _ in range(3):  # Limit to 3 scrolls
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(pause_time)  # Wait for new content to load
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
    except Exception as e:
        raise RuntimeError(f"Error while scrolling: {e}") from e


def load_page(driver, link, pause_time=2):
    """Loads a page in Selenium and waits for it to be fully loaded."""
    try:
        driver.get(link)

        # Save the handle of the current (link) tab
        link_tab = driver.current_window_handle

        # If any extra tabs are open, close them
        for handle in driver.window_handles:
            if handle != link_tab:
                driver.switch_to.window(handle)
                driver.close()

        # Switch back to the original link tab
        driver.switch_to.window(link_tab)

        WebDriverWait(driver, 3).until(
            lambda drv: drv.execute_script("return document.readyState") == "complete" and
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

        scroll_to_bottom(driver, pause_time=pause_time)
    except Exception as e:
        raise RuntimeError(f"Error loading page {link}: {e}") from e


def extract_body_content(driver):
    """Extracts the body content of the current page."""
    try:
        return driver.find_element(By.TAG_NAME, "body").get_attribute("outerHTML")
    except Exception as e:
        raise RuntimeError(f"Error extracting body content: {e}") from e


def save_html_content(html_content, html_filename):
    """Saves the gzipped HTML content to a file."""
    with gzip.open(html_filename, 'wt', encoding='utf-8') as f:
        f.write(html_content)
    print(f"Saved HTML: {html_filename}")


def save_article_json(article, article_filename):
    """Saves the article JSON content to a file."""
    with open(article_filename, "w", encoding="utf-8") as f:
        json.dump(article.get_serializable_dict(), f, indent=4)
    print(f"Saved article: {article_filename}")


def download_html_with_selenium(task_id, link, driver):
    """Downloads the article using Selenium."""
    try:
        load_page(driver, link)
        html = driver.page_source
        if not html:
            raise ValueError(f"Failed to fetch HTML with Selenium for {task_id}: {link}")
        return html
    except Exception as e:
        print(f"Error downloading with Selenium: {e}")
        return None


def quit_driver(driver):
    """Quits the Selenium driver."""
    try:
        if driver:
            driver.close()
            time.sleep(1)
            driver.quit()
    except Exception as e:
        print(f"Error quitting driver: {e}")


def reset_driver(driver=None, driver_executable_path=None, browser_executable_path=None, extension_path=None):
    """Resets the Selenium driver."""
    quit_driver(driver)

    options = new_chrome_options(extension_path=extension_path)
    chrome_args = {"options": options}
    if driver_executable_path:
        chrome_args["driver_executable_path"] = driver_executable_path
    if browser_executable_path:
        chrome_args["browser_executable_path"] = browser_executable_path
        
    driver = uc.Chrome(**chrome_args)
    time.sleep(1)  # Allow time for the driver to initialize
    return driver


def save_bad_sources():
    """Saves the bad sources to a JSON file."""
    with open("bad_sources.json", "w", encoding="utf-8") as f:
        json.dump(bad_sources, f, indent=4)
    print("Saved bad sources to bad_sources.json")

def load_bad_sources():
    """Loads the bad sources from a JSON file."""
    global bad_sources
    if os.path.exists("bad_sources.json"):
        with open("bad_sources.json", "r", encoding="utf-8") as f:
            bad_sources = json.load(f)
    print("Loaded bad sources from bad_sources.json")


def process_task(task_id, link, driver, output_dir):
    """
    Processes a single task: downloads as PDF if link is a PDF, otherwise saves the HTML content.
    """
    domain = link.split("/")[2]
    newsplease_fails = bad_sources.get(domain, {}).get("newsplease", 0)
    selenium_fails = bad_sources.get(domain, {}).get("selenium", 0)
    if newsplease_fails > FAIL_THRESHOLD and selenium_fails > FAIL_THRESHOLD:
        print(f"Skipping {task_id} due to repeated failures for {domain}.")
        return

    html_file = os.path.join(output_dir, "html", f"{task_id}.html.gz")
    article_file = os.path.join(output_dir, "json", f"{task_id}.json")
    user_agent = random.choice(user_agents)

    try:
        html = None

        if newsplease_fails <= FAIL_THRESHOLD:
            # Try to use newsplease to download the article
            html = SimpleCrawler.fetch_url(link, timeout=10, user_agent=user_agent)
            if not html:
                # If newsplease fails, try to download using Selenium
                print(f"Newsplease failed to fetch html for {task_id}: {link}")
                if domain not in bad_sources:
                    bad_sources[domain] = {"newsplease": 0, "selenium": 0}
                bad_sources[domain]["newsplease"] += 1

        if not html and selenium_fails <= FAIL_THRESHOLD:
            html = download_html_with_selenium(task_id, link, driver)
            if not html:
                if domain not in bad_sources:
                    bad_sources[domain] = {"newsplease": 0, "selenium": 0}
                bad_sources[domain]["selenium"] += 1
                raise ValueError(f"Failed to fetch html for {task_id}: {link}")

        # Save html content
        save_html_content(html, html_file)

        # Parse the article
        article = NewsPlease.from_html(html, url=link)
        if not (article and article.maintext):
            raise ValueError(f"Failed to parse article for {task_id}: {link}")

        # Save article content
        save_article_json(article, article_file)

    except Exception as e:
        print(f"Unexpected error processing task {task_id}: {e}")


class Worker(Thread):
    def __init__(self, task_queue, output_dir, stop_event,
                 driver_executable_path=None, browser_executable_path=None, extension_path=None):
        super().__init__()
        self.task_queue = task_queue
        self.output_dir = output_dir
        self.stop_event = stop_event
        self.driver_executable_path = driver_executable_path
        self.browser_executable_path = browser_executable_path
        self.extension_path = extension_path
        self.driver = None

    def run(self):
        # Initialize a single driver for this thread with the new parameters
        self.driver = reset_driver(driver_executable_path=self.driver_executable_path,
                                   browser_executable_path=self.browser_executable_path,
                                   extension_path=self.extension_path)
        successful = 0

        while not self.stop_event.is_set():
            try:
                # Get a task from the queue
                task_id, link = self.task_queue.get(timeout=1)
            except Empty:
                continue

            process_task(task_id, link, self.driver, self.output_dir)
            successful += 1

            if successful % 64 == 0 and successful > 0:
                # Reset the driver every 128 tasks
                self.driver = reset_driver(self.driver,
                                           driver_executable_path=self.driver_executable_path,
                                           browser_executable_path=self.browser_executable_path,
                                           extension_path=self.extension_path)

            # Finished processing this task
            self.task_queue.task_done()

        # Stop event set or no more tasks: close the driver
        quit_driver(self.driver)


def download_links_queue(input_file, output_dir, start=0, end=None, num_workers=4,
                         driver_executable_path=None, browser_executable_path=None, extension_path=None):
    """Download HTML content for a list of URLs using a queue of workers."""
    os.makedirs(output_dir, exist_ok=True)
    html_output_dir = os.path.join(output_dir, "html")
    os.makedirs(html_output_dir, exist_ok=True)
    article_output_dir = os.path.join(output_dir, "json")
    os.makedirs(article_output_dir, exist_ok=True)
    
    load_bad_sources()

    # Patch upfront (to ensure undetected_chromedriver setup)
    chrome_args = {}
    if driver_executable_path:
        chrome_args["driver_executable_path"] = driver_executable_path
    if browser_executable_path:
        chrome_args["browser_executable_path"] = browser_executable_path
    temp_driver = uc.Chrome(**chrome_args)
    temp_driver.close()

    stop_event = Event()
    task_queue = Queue()

    data = pd.read_csv(input_file)

    # Break tasks into batches of 50
    for row in data.itertuples(index=False):
        if row.index < start:
            continue
        if end and row.index >= end:
            break

        domain = row.url.split("/")[2]
        if domain in skip:  # Skip known bad sources
            continue

        output_file = os.path.join(output_dir, "html", f"{row.index}.html.gz")
        if os.path.exists(output_file):
            continue

        task_queue.put((row.index, row.url))

    print(f"Total tasks: {task_queue.qsize()}")

    # Start workers with the new parameters
    workers = [Worker(task_queue, output_dir, stop_event,
                      driver_executable_path=driver_executable_path,
                      browser_executable_path=browser_executable_path,
                      extension_path=extension_path)
               for _ in range(num_workers)]
    for w in workers:
        w.start()

    try:
        # Poll the queue instead of q.join() for interrupt handling
        while not task_queue.empty():
            time.sleep(1)
    except KeyboardInterrupt:
        print("User interrupted. Stopping workers...")
        # Signal workers to stop
        stop_event.set()

        # Clear any remaining tasks in the queue
        while not task_queue.empty():
            try:
                task_queue.get_nowait()
                task_queue.task_done()
            except Empty:
                break

    save_bad_sources()

    # Wait for all workers to finish
    print("Waiting for workers to finish...")
    stop_event.set()
    for w in workers:
        w.join()

    print("All workers have finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download articles using Selenium and NewsPlease")
    parser.add_argument("-i", "--input_file", help="Path to the CSV file containing URLs")
    parser.add_argument("-o", "--output_dir", help="Directory where HTML and JSON files will be saved")
    parser.add_argument("--start", type=int, default=0, help="Starting index for the task IDs (default: 0)")
    parser.add_argument("--end", type=int, default=None, help="Ending index for the task IDs (default: None)")
    parser.add_argument("--num_workers", type=int, default=8, help="Number of worker threads (default: 8)")
    parser.add_argument("--driver_executable_path", type=str, default=None, help="Path to the ChromeDriver executable")
    parser.add_argument("--browser_executable_path", type=str, default=None, help="Path to the Chrome browser executable")
    parser.add_argument("--extension_path", type=str, default=None, help="Path to the Chrome extension to load")
    args = parser.parse_args()

    # If running on Linux, attempt to start a virtual display
    display = None
    if platform.system() == "Linux":
        try:
            from pyvirtualdisplay import Display
            display = Display(visible=0, size=(1920, 1080))
            display.start()
            print("Virtual display started on Linux.")
        except ImportError as e:
            print("pyvirtualdisplay is not installed. Please install it to run on Linux.")

    try:
        download_links_queue(
            input_file=args.input_file,
            output_dir=args.output_dir,
            start=args.start,
            end=args.end,
            num_workers=args.num_workers,
            driver_executable_path=args.driver_executable_path,
            browser_executable_path=args.browser_executable_path,
            extension_path=args.extension_path
        )
    finally:
        if display:
            display.stop()
            print("Virtual display stopped.")
