import requests
import re
import time
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException

def extract_story_ids(url):
    """Extracts actual story IDs from the Ground News topic page."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run without opening a browser
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    driver.get(url)

    try:
        print("Waiting for news articles to load...")
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CLASS_NAME, "text-22"))
        )
        print("News articles loaded successfully.")
    except TimeoutException:
        print("Timeout: News articles did not load. Exiting.")
        driver.quit()
        return []

    # Extract story IDs from the JavaScript script tag
    page_source = driver.page_source
    driver.quit()

    # Find all potential story IDs using regex
    found_ids = re.findall(r'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})', page_source)

    # Remove duplicates
    story_ids = list(set(found_ids))

    if story_ids:
        print(f"Extracted {len(story_ids)} actual story IDs.")
    else:
        print("No valid story IDs found in page metadata.")

    return story_ids



def get_story_data(story_id):
    """Fetches data for a given story ID from the Ground News API."""
    api_url = f"https://web-api-cdn.ground.news/api/v06/story/{story_id}/sourcesForWeb"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        print(f"Fetching {api_url} - Status Code: {response.status_code}")

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            print(f"Story ID '{story_id}' not found in API.")
        elif response.status_code == 502:
            print(f"Server issue (502 Bad Gateway) while fetching story {story_id}. Retrying later.")
        else:
            print(f"Unexpected error {response.status_code}: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"Request failed for {story_id}: {e}")

    return None  # Return None if request fails





def get_story_data(story_id):
    """Fetches data for a given story ID."""
    api_url = f"https://web-api-cdn.ground.news/api/v06/story/{story_id}/sourcesForWeb"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        print(f"Fetching {api_url} - Status Code: {response.status_code}")

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            print(f"⚠️ Story ID '{story_id}' not found in API.")
        elif response.status_code == 502:
            print(f"⚠️ Server issue (502 Bad Gateway) while fetching story {story_id}. Retrying later.")
        else:
            print(f"⚠️ Unexpected error {response.status_code}: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"⚠️ Request failed for {story_id}: {e}")

    return None  # Return None if request fails



def extract_relevant_info(story_json):
    """Extracts relevant fields from the API response."""
    print(f"Received API Data: {json.dumps(story_json, indent=2)}")  # Debugging line

    extracted_data = []
    for source in story_json.get("sources", []):
        extracted_data.append({
            "title": source.get("title", "Unknown"),
            "summary": source.get("originalDescription", "No summary available"),
            "media_source": source.get("sourceInfo", {}).get("name", "Unknown"),
            "bias": source.get("bias", {}).get("labelData", "Unknown"),
            "source_link": source.get("sourceInfo", {}).get("url", "No link available"),
        })
    
    print(f"Extracted {len(extracted_data)} articles")  # Debugging line
    return extracted_data
