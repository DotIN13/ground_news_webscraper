import argparse
import csv
import os
import asyncio
import httpx
import json

# Custom User-Agent header
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; WOW64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/132.0.6788.76 Safari/537.36"
)

async def fetch_news_source(client: httpx.AsyncClient, story_id: str) -> dict:
    """
    Fetch news source data for a given story ID.
    """
    url = f"https://web-api-cdn.ground.news/api/v06/story/{story_id}/sourcesForWeb"
    headers = {"User-Agent": USER_AGENT}
    try:
        response = await client.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"[ERROR] Fetching story {story_id}: {e}")
        return {}

async def process_story_id(story_id: str, output_dir: str, client: httpx.AsyncClient, semaphore: asyncio.Semaphore):
    """
    Fetch and save news source data for a given story ID.
    """
    async with semaphore:
        data = await fetch_news_source(client, story_id)
        if data:
            filename = os.path.join(output_dir, f"news_source_{story_id}.json")
            with open(filename, "w", encoding="utf-8") as file:
                json.dump(data, file, indent=2)
            print(f"[INFO] Saved news source for story ID {story_id} to {filename}")

def read_story_ids_from_csv(csv_filename: str) -> list:
    """
    Read story IDs from a CSV file (expects a column named 'story_id').
    """
    story_ids = []
    with open(csv_filename, "r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            story_ids.append(row["story_id"])
    return story_ids

async def main():
    parser = argparse.ArgumentParser(
        description="Download news sources for story IDs (from a CSV file) and save each to a JSON file."
    )
    parser.add_argument('--file', required=True, help="CSV file with story IDs (must include 'story_id' column).")
    parser.add_argument('--output_dir', default="news_sources", help="Directory to save news source files (default: news_sources).")
    parser.add_argument('--concurrency', type=int, default=5, help="Maximum concurrent requests (default: 5).")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    story_ids = read_story_ids_from_csv(args.file)

    semaphore = asyncio.Semaphore(args.concurrency)
    async with httpx.AsyncClient() as client:
        tasks = [process_story_id(sid, args.output_dir, client, semaphore) for sid in story_ids]
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
