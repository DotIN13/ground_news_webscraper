import random
import argparse
import os
import asyncio
import httpx
import json
import pandas as pd
from typing import Set, Tuple

STEP = 100  # Number of story IDs to fetch in each request

# Custom User-Agent header
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; WOW64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/132.0.6788.76 Safari/537.36"
)

# --- Progress helpers using an in-memory dictionary ---

def load_progress(progress_csv_path: str) -> dict:
    """
    Load progress from CSV if it exists; otherwise, return an empty dict.
    """
    if os.path.exists(progress_csv_path):
        df = pd.read_csv(progress_csv_path, dtype={"interest_slug": str, "finished": bool})
        return dict(zip(df["interest_slug"], df["finished"]))
    return {}

async def update_progress(progress: dict, interest_slug: str, finished: bool, lock: asyncio.Lock) -> None:
    """
    Update (or add) an interest record in the in-memory progress dictionary.
    """
    async with lock:
        progress[interest_slug] = finished

async def is_interest_finished(progress: dict, interest_slug: str, lock: asyncio.Lock) -> bool:
    """
    Check if the interest is already marked as finished in the in-memory progress dictionary.
    """
    async with lock:
        return progress.get(interest_slug, False)

def save_progress(progress: dict, progress_csv_path: str) -> None:
    """
    Save the in-memory progress dictionary to a CSV file using pandas.
    """
    df = pd.DataFrame(list(progress.items()), columns=["interest_slug", "finished"])
    df.to_csv(progress_csv_path, index=False)

# --- CSV helpers for story IDs using pandas ---

def read_existing_story_csv(filename: str) -> Tuple[pd.DataFrame, Set[str]]:
    """
    Read an existing CSV file and return a DataFrame and a set of unique story IDs.
    """
    if os.path.exists(filename):
        df = pd.read_csv(filename, dtype={"offset": int, "story_id": str})
        unique_ids = set(df["story_id"].tolist())
    else:
        df = pd.DataFrame(columns=["offset", "story_id"])
        unique_ids = set()
    return df, unique_ids

def write_story_csv(filename: str, df: pd.DataFrame) -> None:
    """
    Write the DataFrame to a CSV file.
    """
    df.to_csv(filename, index=False)

# --- Fetching and processing functions ---

async def fetch_story_ids(client: httpx.AsyncClient, interest_id: str, offset: int) -> list:
    """
    Fetch story IDs for a given interest (by interest_id) starting from the specified offset.
    """
    url = f"https://web-api-cdn.ground.news/api/public/interest/{interest_id}/events?offset={offset}"
    headers = {"User-Agent": USER_AGENT}
    try:
        response = await client.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json().get("eventIds", [])
    except Exception as e:
        print(f"[ERROR] Interest {interest_id} at offset {offset}: {e}")
        return []

async def process_event(interest_id: str, interest_slug: str, initial_offset: int, top_n: int,
                        client: httpx.AsyncClient, semaphore: asyncio.Semaphore,
                        output_dir: str, progress: dict, progress_lock: asyncio.Lock) -> None:
    """
    Repeatedly fetch story IDs until we have at least top_n unique IDs for the interest.
    When a request returns no new IDs or the target is reached, mark the interest as finished.
    """
    filename = os.path.join(output_dir, "story_ids_by_interest", f"story_ids_{interest_slug}.csv")
    df, unique_ids = read_existing_story_csv(filename)
    current_offset = initial_offset

    while len(unique_ids) < top_n:
        async with semaphore:
            new_ids_list = await fetch_story_ids(client, interest_id, current_offset)

        if not new_ids_list:
            print(f"[INFO] No new IDs returned for interest {interest_slug} at offset {current_offset}. Marking as finished.")
            await update_progress(progress, interest_slug, True, progress_lock)
            break

        new_id_count = 0
        new_rows = []
        for i, sid in enumerate(new_ids_list):
            if sid in unique_ids:
                continue
            new_id_count += 1
            new_rows.append({"offset": current_offset + i, "story_id": sid})
            unique_ids.add(sid)

        print(f"[INFO] Interest {interest_slug} | Offset {current_offset} | Added {new_id_count} new IDs. Total unique IDs: {len(unique_ids)}")
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            df = pd.concat([df, new_df], ignore_index=True)
            write_story_csv(filename, df)

        if len(unique_ids) >= top_n:
            print(f"[INFO] Reached target of {top_n} unique IDs for interest {interest_slug}. Marking as finished.")
            await update_progress(progress, interest_slug, True, progress_lock)
            break

        current_offset += STEP
        await asyncio.sleep(random.random() + 0.2)

    print(f"[DONE] Finished processing interest {interest_slug}. Total unique story IDs: {len(unique_ids)}. CSV saved as {filename}")

async def process_interest(interest_name: str, endpoint: str, initial_offset: int, top_n: int,
                           client: httpx.AsyncClient, semaphore: asyncio.Semaphore,
                           output_dir: str, progress: dict, progress_lock: asyncio.Lock) -> None:
    """
    For a given interest, fetch its metadata, save it, extract its ID, and process story ID extraction.
    Skips the interest if it is already marked as finished in the in-memory progress.
    """
    headers = {"User-Agent": USER_AGENT}
    url = f"https://web-api-cdn.ground.news/api/public{endpoint}"
    try:
        response = await client.get(url, headers=headers, timeout=10, follow_redirects=True)
        response.raise_for_status()
        metadata = response.json()
    except Exception as e:
        print(f"[ERROR] Failed to fetch metadata for '{interest_name}' from {url}: {e}")
        return

    interest_info = metadata.get("interest", {})
    interest_id = interest_info.get("id")
    interest_slug = interest_info.get("slug") or interest_name.replace(" ", "_")
    if not interest_id:
        print(f"[ERROR] No interest id found in metadata for '{interest_name}'")
        return

    if await is_interest_finished(progress, interest_slug, progress_lock):
        print(f"[INFO] Interest {interest_slug} is already marked as finished. Skipping processing.")
        return

    metadata_filename = os.path.join(output_dir, "interests", f"metadata_{interest_slug}.json")
    try:
        with open(metadata_filename, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=4)
        print(f"[INFO] Saved metadata for '{interest_name}' as {metadata_filename}")
    except Exception as e:
        print(f"[ERROR] Could not save metadata for '{interest_name}': {e}")

    await process_event(interest_id, interest_slug, initial_offset, top_n,
                        client, semaphore, output_dir, progress, progress_lock)

async def worker(queue: asyncio.Queue, client: httpx.AsyncClient, semaphore: asyncio.Semaphore,
                 output_dir: str, initial_offset: int, top_n: int,
                 progress: dict, progress_lock: asyncio.Lock, total_count: int) -> None:
    """
    Worker that continuously processes interests from the queue.
    After each interest is processed, prints overall progress.
    """
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break
        interest_name, endpoint = item
        await process_interest(interest_name, endpoint, initial_offset, top_n,
                               client, semaphore, output_dir, progress, progress_lock)
        finished_count = sum(1 for v in progress.values() if v)
        print(f"[OVERALL PROGRESS] {finished_count}/{total_count} interests finished.")
        queue.task_done()

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Retrieve top_n unique story IDs for interests defined in a JSON file, saving metadata and story IDs."
    )
    parser.add_argument('-i', '--input-file', type=str, default='interests.json',
                        help="Path to the JSON file containing interest names and endpoints (default: interests.json).")
    parser.add_argument('-n', '--n', type=int, default=10,
                        help="Total number of unique story IDs to collect per interest (default: 10).")
    parser.add_argument('--offset', type=int, default=0,
                        help="Initial offset for the API (default: 0).")
    parser.add_argument('-w', '--workers', type=int, default=5,
                        help="Number of concurrent worker tasks and maximum concurrent HTTP requests (default: 5).")
    parser.add_argument('-o', '--output-dir', type=str, default='.',
                        help="Directory to save output files (default: current directory).")
    args = parser.parse_args()

    # Ensure output directories exist
    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, "story_ids_by_interest"), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, "interests"), exist_ok=True)

    progress_csv_path = os.path.join(args.output_dir, "progress.csv")
    progress = load_progress(progress_csv_path)
    progress_lock = asyncio.Lock()

    try:
        with open(args.input_file, "r", encoding="utf-8") as f:
            interests = json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load interests file '{args.input_file}': {e}")
        return

    total_count = len(interests)
    queue: asyncio.Queue = asyncio.Queue()
    for interest_name, endpoint in interests.items():
        queue.put_nowait((interest_name, endpoint))

    semaphore = asyncio.Semaphore(args.workers)

    try:
        async with httpx.AsyncClient() as client:
            worker_tasks = [
                asyncio.create_task(worker(queue, client, semaphore, args.output_dir,
                                             args.offset, args.n, progress, progress_lock, total_count))
                for _ in range(args.workers)
            ]

            # Add termination signals for each worker.
            for _ in range(args.workers):
                await queue.put(None)

            await queue.join()

            for task in worker_tasks:
                task.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
    except KeyboardInterrupt:
        print("[INFO] Keyboard interrupt received. Saving progress and exiting.")
        raise
    except Exception as e:
        print(f"[ERROR] Fatal error encountered: {e}")
        raise
    finally:
        save_progress(progress, progress_csv_path)
        print(f"[INFO] Progress saved to {progress_csv_path}")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[INFO] Exiting due to keyboard interrupt.")
