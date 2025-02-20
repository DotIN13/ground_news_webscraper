# Example usage:
# python get_story_ids.py --events 453a847a-ac24-45d3-a937-63fc9d6a1318 another_event_id --n 5

import argparse
import json
import os

import asyncio
import httpx


# Define a custom User-Agent header.
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; WOW64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.6788.76 Safari/537.36"


async def fetch_story_ids(client: httpx.AsyncClient, event_id: str, offset: int) -> list:
    """
    Fetch story IDs for a given event ID from the events API.
    """
    url = f"https://web-api-cdn.ground.news/api/public/interest/{event_id}/events?offset={offset}"
    headers = {"User-Agent": USER_AGENT}
    try:
        response = await client.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("eventIds", [])
    except Exception as e:
        print(f"Error fetching event {event_id}: {e}")
        return []


async def process_event(event_id: str, offset: int, n: int, client: httpx.AsyncClient, semaphore: asyncio.Semaphore):
    """
    Process one event: fetch story ids, take the top n, and write to a JSON file.
    """
    async with semaphore:
        story_ids = await fetch_story_ids(client, event_id, offset)
        top_story_ids = story_ids[:n]
        filename = f"story_ids_{event_id}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(top_story_ids, f, indent=2)
        print(
            f"Saved {len(top_story_ids)} story IDs for event {event_id} to {filename}")


async def main():
    parser = argparse.ArgumentParser(
        description="Async: Get top n story IDs for a list of event IDs and save each to a file."
    )
    parser.add_argument('--events', nargs='+', required=True,
                        help="List of event IDs (space separated).")
    parser.add_argument('--n', type=int, default=10,
                        help="Number of top story IDs to save per event (default: 10).")
    parser.add_argument('--offset', type=int, default=100,
                        help="Offset parameter for the API (default: 100).")
    parser.add_argument('--concurrency', type=int, default=5,
                        help="Maximum number of concurrent requests (default: 5).")
    args = parser.parse_args()

    semaphore = asyncio.Semaphore(args.concurrency)
    async with httpx.AsyncClient() as client:
        tasks = [
            process_event(event_id, args.offset, args.n, client, semaphore)
            for event_id in args.events
        ]
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
