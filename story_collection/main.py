# Main Function
import json
import time
import argparse
import os
from news_scraper import extract_story_ids, get_story_data

# Setup Selenium WebDriver once
def init_driver(headless=True):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

# Login Function
def login(driver, username, password):
    wait = WebDriverWait(driver, 15)  # Increase timeout
    driver.get("https://ground.news/login")

    try:
        # Enter username
        username_input = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@type='email']")))
        username_input.send_keys(username)
        print("Entered username.")

        # Enter password
        password_input = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@type='password']")))
        password_input.send_keys(password)
        print("Entered password.")

        # Click Submit
        submit_input = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@type='submit']")))
        submit_input.click()
        print("Submitted login form.")

        time.sleep(3)
        driver.refresh()

        # Verify login success
        if driver.find_elements(By.ID, "header-logout"):
            print("Login successful.")
            return True
        else:
            print("Login failed: Logout button not found.")
            return False

    except TimeoutException:
        print("Login failed: Timeout.")
    except NoSuchElementException:
        print("Login failed: Element not found.")
    except ElementClickInterceptedException:
        print("Login failed: Click was intercepted.")
    return False

# Read credentials safely
def load_credentials():
    try:
        with open("credentials.json", "r", encoding="utf-8") as f:
            creds = json.load(f)
        return creds["username"], creds["password"]
    except FileNotFoundError:
        print("Error: 'credentials.json' file not found. Please create it.")
        return None, None


def main(args):
    # Extract story IDs
    print(f"Extracting story IDs from {args.href}...")
    story_ids = extract_story_ids(f"https://ground.news{args.href}")

    if not story_ids:
        print("No story IDs found. Exiting.")
        return

    print(f"Found {len(story_ids)} story IDs.")

    # Fetch and process article data
    all_articles = []
    for story_id in story_ids:
        story_data = get_story_data(story_id)
        if story_data:
            all_articles.append(story_data)

    if not all_articles:
        print("No articles extracted. Exiting.")
        return

    # Save results to JSON
    output_file = f"scraped_articles_{args.tag}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_articles, f, indent=4, ensure_ascii=False)

    print(f"Saved {len(all_articles)} articles to {output_file}.")


# Argument Parser
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--href', type=str, default='/interest/gun-control',
                        help='The topic href to extract news articles from')
    parser.add_argument('--tag', type=str, default='latest',
                        help='Tag for output file versioning')
    parser.add_argument('--headless', action='store_true',
                        help='Run browser in headless mode')

    args = parser.parse_args()
    main(args)




