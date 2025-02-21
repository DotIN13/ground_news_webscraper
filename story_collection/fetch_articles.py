import httpx
import json
import time

# Load story IDs from file
with open("event_ids.json", "r", encoding="utf-8") as f:
    events = json.load(f) 

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://ground.news/",
    "x-gn-v": "web"  # Required for API access
}

saved_articles = []
MAX_RETRIES = 3  # Number of retries for failed requests

for event in events:
    # Ensure we extract the correct event ID (either directly or from a dict)
    story_id = event if isinstance(event, str) else event.get("event_id")

    if not story_id:
        print(f"[ERROR] Missing event_id in: {event}")
        continue
    
    url = f"https://web-api-cdn.ground.news/api/public/event/{story_id}/sources"

    print(f"Fetching: {url}")  

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = httpx.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                saved_articles.append({"story_id": story_id, "articles": data})
                print(f"Success for {story_id}")
                break  # Exit retry loop on success

            elif response.status_code == 404:
                print(f"[WARNING] Story ID {story_id} not found (404). Skipping.")
                break  # No need to retry

            else:
                print(f"[ERROR] {story_id} - Status {response.status_code}. Retrying {attempt}/{MAX_RETRIES}...")

        except httpx.RequestError as e:
            print(f"[ERROR] Request failed for {story_id}: {e}")

        time.sleep(2)  # Wait before retrying

# Save the results
with open("articles.json", "w", encoding="utf-8") as f:
    json.dump(saved_articles, f, indent=2)

print(f"\n Scraping complete. Saved {len(saved_articles)} articles to 'articles.json'.")







