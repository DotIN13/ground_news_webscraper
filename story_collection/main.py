import json
import time
import argparse
import os
from news_scraper import extract_story_ids, get_story_data, extract_relevant_info
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

def init_driver(headless=True):
    """Initializes the Selenium WebDriver."""
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)


def scrape_all_topics():
    """Scrapes stories from all topics on Ground News."""
    topics = ["gun-control", "politics", "climate-change", "technology", "finance"]  # Add more topics

    print(" Starting full-topic scraping session...")

    # Initialize WebDriver once for efficiency
    driver = init_driver(headless=True)

    for topic in topics:
        print(f"\n Extracting story IDs from {topic}...")
        story_ids = extract_story_ids(driver, f"https://ground.news/interest/{topic}")

        if not story_ids:
            print(f"⚠️ No stories found for {topic}. Skipping...")
            continue

        all_articles = []
        for story_id in story_ids:
            story_data = get_story_data(story_id)
            if story_data:
                articles = extract_relevant_info(story_data)
                if articles:
                    all_articles.extend(articles)

        if all_articles:
            output_file = f"scraped_articles_{topic}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(all_articles, f, indent=4, ensure_ascii=False)
            print(f" Saved {len(all_articles)} articles for {topic} in {output_file}")

    driver.quit()  # Close the browser session after scraping all topics
    print("\n All topics scraped successfully!")


# Argument Parser
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--headless', action='store_true',
                        help='Run browser in headless mode')

    args = parser.parse_args()
    scrape_all_topics()









