# Example usage:
# python download_news_sources.py --file story_ids_453a847a-ac24-45d3-a937-63fc9d6a1318.json --output_dir sources_output

import argparse
import json
import os

import asyncio
import httpx


# Define a custom User-Agent header.
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; WOW64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.6788.76 Safari/537.36"


async def fetch_news_source(client: httpx.AsyncClient, story_id: str) -> dict:
    """
    Fetch the news source data for a given story ID from the story API.
    """
    url = f"https://web-api-cdn.ground.news/api/v06/story/{story_id}/sourcesForWeb"
    headers = {"User-Agent": USER_AGENT}
    try:
        response = await client.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching story {story_id}: {e}")
        return {}


async def process_story_id(story_id: str, output_dir: str, client: httpx.AsyncClient, semaphore: asyncio.Semaphore):
    """
    Process one story ID: fetch its news source and save the result to a file.
    """
    async with semaphore:
        data = await fetch_news_source(client, story_id)
        if data:
            filename = os.path.join(output_dir, f"news_source_{story_id}.json")
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            print(f"Saved news source for story ID {story_id} to {filename}")


async def main():
    parser = argparse.ArgumentParser(
        description="Async: Download news sources for story IDs and save each to a separate file."
    )
    parser.add_argument('--file', required=True,
                        help="JSON file containing story IDs (output from async_get_story_ids.py).")
    parser.add_argument('--output_dir', default="news_sources",
                        help="Directory to save news source files (default: news_sources).")
    parser.add_argument('--concurrency', type=int, default=5,
                        help="Maximum number of concurrent requests (default: 5).")
    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir, exist_ok=True)

    with open(args.file, "r", encoding="utf-8") as f:
        story_ids = json.load(f)

    semaphore = asyncio.Semaphore(args.concurrency)
    async with httpx.AsyncClient() as client:
        tasks = [
            process_story_id(story_id, args.output_dir, client, semaphore)
            for story_id in story_ids
        ]
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
