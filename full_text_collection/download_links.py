import os
import time
import random
import requests
from queue import Queue, Empty
from threading import Thread, Event
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util import Retry


import certifi
import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

session = requests.Session()
retries = Retry(total=3, backoff_factor=1,
                status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)


scroll_pause_time = 2

# Define a global list of user agents for both Selenium and requests
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Mobile/15E148 Safari/604.1",
]


def new_chrome_options():
    options = uc.ChromeOptions()
    options.add_argument(f"user-agent={random.choice(user_agents)}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--remote-allow-origins=*")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--allow-insecure-localhost")
    prefs = {
        # Disable images
        "profile.managed_default_content_settings.images": 2,
        # Disable videos (if supported)
        "profile.managed_default_content_settings.videos": 2
    }
    options.add_experimental_option("prefs", prefs)
    # options.add_argument("--headless=new")
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


def scroll_to_bottom(driver, pause_time=2):
    """Scrolls to the bottom of the page to load all dynamic content."""
    try:
        last_height = driver.execute_script(
            "return document.body.scrollHeight")
        for _ in range(3):  # Limit to 3 scrolls
            driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(pause_time)
            new_height = driver.execute_script(
                "return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
    except Exception as e:
        raise RuntimeError(f"Error while scrolling: {e}") from e


def load_page(driver, link, pause_time=2):
    """Loads a page in Selenium and waits for it to be fully loaded."""
    try:
        driver.get(link)
        WebDriverWait(driver, 5).until(
            lambda drv: drv.execute_script(
                "return document.readyState") == "complete" and
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
    """Saves the HTML content to a file."""
    try:
        with open(html_filename, "w", encoding="utf-8") as html_file:
            html_file.write(html_content)
        return f"Saved body HTML: {html_filename}"
    except Exception as e:
        raise RuntimeError(
            f"Error writing HTML to file {html_filename}: {e}") from e


def process_html_download(link, driver, html_filename):
    """Main function to handle HTML download using Selenium."""
    try:
        load_page(driver, link)
        body_content = extract_body_content(driver)
        return save_html_content(body_content, html_filename)
    except RuntimeError as e:
        return str(e)


def process_task(task_id, link, driver, output_dir):
    """
    Processes a single task: downloads as PDF if link is a PDF, otherwise saves the HTML content.
    """
    pdf_filename = os.path.join(output_dir, f"{task_id}.pdf")
    if os.path.exists(pdf_filename):
        return f"PDF already exists: {pdf_filename}"

    html_filename = os.path.join(output_dir, f"{task_id}.html")
    if os.path.exists(html_filename):
        return f"HTML already exists: {html_filename}"

    headers = {'User-Agent': random.choice(user_agents)}

    # Determine if PDF or HTML by using requests.head()
    content_type = ""
    try:
        response = requests.head(
            link, allow_redirects=True, timeout=10,
            verify=certifi.where(), headers=headers
        )
        content_type = response.headers.get('Content-Type', '').lower()
    except requests.exceptions.RequestException:
        print(f"Error during requests.head() for link {link}")

    if 'application/pdf' in content_type or link.endswith(".pdf"):
        # Download PDF
        return process_pdf_download(link, pdf_filename)
    else:
        # Load HTML with Selenium
        return process_html_download(link, driver, html_filename)


class Worker(Thread):
    def __init__(self, task_queue, output_dir, stop_event):
        super().__init__()
        self.task_queue = task_queue
        self.output_dir = output_dir
        self.stop_event = stop_event
        self.driver = None

    def run(self):
        # Initialize a single driver for this thread
        self.driver = uc.Chrome(options=new_chrome_options())
        successful = 0

        while not self.stop_event.is_set():
            try:
                # Get a batch of tasks
                batch = self.task_queue.get(timeout=1)
            except Empty:
                continue

            for task_id, link in batch:
                if self.stop_event.is_set():
                    break

                result = process_task(
                    task_id, link, self.driver, self.output_dir)
                print(result)
                if isinstance(result, str) and result.startswith("Saved"):
                    successful += 1
                    # Reset the driver after each 64 tasks
                    if successful % 64 != 0:
                        continue

                    self.driver.close()
                    self.driver.quit()
                    self.driver = uc.Chrome(
                        options=new_chrome_options())

            # Finished processing this batch
            self.task_queue.task_done()

        # Stop event set or no more tasks: close the driver
        if self.driver:
            self.driver.close()
            self.driver.quit()


def download_links_queue(csv_file, output_dir, num_workers=4):
    data = pd.read_csv(csv_file)
    os.makedirs(output_dir, exist_ok=True)

    # Patch upfront (to ensure undetected_chromedriver setup)
    temp_driver = uc.Chrome()
    temp_driver.close()

    stop_event = Event()
    task_queue = Queue()

    # Break tasks into batches of 50
    tasks = [(row.id, row.link) for row in data.itertuples()]
    existing = set(int(file.split(".")[0]) for file in os.listdir(output_dir))
    tasks = [(tid, link) for tid, link in tasks if tid not in existing]
    print(f"Total tasks: {len(tasks)}")
    random.shuffle(tasks)

    batch_size = 64
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i:i+batch_size]
        task_queue.put(batch)

    # Start workers
    workers = [Worker(task_queue, output_dir, stop_event)
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

        # Clear any remaining batches
        while not task_queue.empty():
            try:
                task_queue.get_nowait()
                task_queue.task_done()
            except Empty:
                break

    # Wait for all workers to finish
    for w in workers:
        w.join()


if __name__ == "__main__":
    download_links_queue("links.csv", "downloads", num_workers=8)
