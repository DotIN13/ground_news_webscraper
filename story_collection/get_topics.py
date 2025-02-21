import httpx
import json

import httpx
import json
import time

BASE_URL = "https://web-api-cdn.ground.news/api/public/interest/453a847a-ac24-45d3-a937-63fc9d6a1318/events"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

all_event_ids = set() 

for offset in range(1, 9900):
    url = f"{BASE_URL}?sort=time&offset={offset}"
    print(f"Fetching event IDs from: {url}")  

    try:
        response = httpx.get(url, headers=headers, timeout=10)
        data = response.json()
        event_ids = data.get("eventIds", [])  
        all_event_ids.update(event_ids) 

        print(f" Collected {len(event_ids)} event IDs from sort=time, offset={offset}. Total unique: {len(all_event_ids)}")

        time.sleep(1) 

    except httpx.RequestError as e:
        print(f"[ERROR] Failed to fetch event IDs at offset {offset}: {e}")

with open("event_ids.json", "w", encoding="utf-8") as f:
    json.dump(list(all_event_ids), f, indent=2)

print(f" Finished! Collected {len(all_event_ids)} unique event IDs and saved them to event_ids.json")
