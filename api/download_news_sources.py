import random
import argparse
import csv
import os
import asyncio
import httpx
import json
import glob

# Custom User-Agent header
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; WOW64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/132.0.6788.76 Safari/537.36"
)

LIMIT = 1000

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

async def process_csv_file(csv_file: str, output_dir: str):
    """
    Process a single CSV file by sequentially fetching story data and saving all results to a JSON file.
    """
    print(f"[INFO] Processing CSV file: {csv_file}")
    base_name = os.path.splitext(os.path.basename(csv_file))[0]
    output_file = os.path.join(output_dir, f"{base_name}.json")
    if os.path.exists(output_file):
        print(f"[INFO] Output file already exists: {output_file}. Skipping.")
        return
    
    story_ids = read_story_ids_from_csv(csv_file)
    if LIMIT and len(story_ids) > LIMIT:
        story_ids = story_ids[:LIMIT]
    
    results = {}
    async with httpx.AsyncClient() as client:
        for story_id in story_ids:
            data = await fetch_news_source(client, story_id)
            if data:
                results[story_id] = data
                print(f"[INFO] Fetched news source for story ID {story_id}")
            await asyncio.sleep(0.5 + 5 * random.random())  # Rate limiting


    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(results, file, indent=4)

    print(f"[INFO] Saved aggregated news sources for {csv_file} to {output_file}")
    await asyncio.sleep(10 + 60 * random.random())  # Rate limiting

async def worker(queue: asyncio.Queue, output_dir: str):
    """
    Worker that continuously gets CSV file paths from the queue, processes them sequentially, and marks tasks as done.
    """
    while True:
        csv_file = await queue.get()
        if csv_file is None:  # Sentinel to signal worker shutdown
            queue.task_done()
            break
        await process_csv_file(csv_file, output_dir)
        queue.task_done()

async def main():
    parser = argparse.ArgumentParser(
        description="Process CSV files from an input directory using a queue and worker pattern. "
                    "Each CSV is processed by one worker sequentially (no nested concurrency) and results are saved to a JSON file."
    )
    parser.add_argument('-i', '--input-dir', required=True, help="Directory containing CSV files (each must include a 'story_id' column).")
    parser.add_argument("-o", '--output-dir', default="news_sources", help="Directory to save aggregated JSON files (default: news_sources).")
    parser.add_argument('-w', '--num-workers', type=int, default=5, help="Number of workers to process CSV files (default: 5).")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # Enqueue all CSV files found in the input directory.
    csv_files = glob.glob(os.path.join(args.input_dir, "*.csv"))
    if not csv_files:
        print(f"[ERROR] No CSV files found in directory {args.input_dir}")
        return

    queue = asyncio.Queue()
    for csv_file in csv_files:
        queue.put_nowait(csv_file)

    workers = [asyncio.create_task(worker(queue, args.output_dir)) for _ in range(args.num_workers)]

    await queue.join()

    # Signal workers to shut down.
    for _ in range(args.num_workers):
        queue.put_nowait(None)
    await asyncio.gather(*workers)

if __name__ == '__main__':
    asyncio.run(main())
