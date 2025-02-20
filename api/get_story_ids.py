import argparse
import csv
import os
import asyncio
import httpx

# Custom User-Agent header
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; WOW64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/132.0.6788.76 Safari/537.36"
)

async def fetch_story_ids(client: httpx.AsyncClient, event_id: str, offset: int) -> list:
    """
    Fetch story IDs for a given event starting from the specified offset.
    """
    url = f"https://web-api-cdn.ground.news/api/public/interest/{event_id}/events?offset={offset}"
    headers = {"User-Agent": USER_AGENT}
    try:
        response = await client.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json().get("eventIds", [])
    except Exception as e:
        print(f"[ERROR] Event {event_id} at offset {offset}: {e}")
        return []

def read_existing_csv(filename: str) -> tuple[list, set]:
    """
    Read an existing CSV file and return a list of rows and a set of unique story IDs.
    """
    rows = []
    unique_ids = set()
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                row["offset"] = int(row["offset"])
                rows.append(row)
                unique_ids.add(row["story_id"])
    return rows, unique_ids

def write_csv(filename: str, rows: list) -> None:
    """
    Write the rows (a list of dictionaries) to a CSV file.
    """
    with open(filename, "w", encoding="utf-8", newline="") as file:
        fieldnames = ["offset", "story_id"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

async def process_event(
        event_id: str,
        initial_offset: int,
        top_n: int,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore):
    """
    Repeatedly fetch story IDs until we have at least top_n unique IDs for the event.
    If a duplicate is found, shift all saved offsets by the current offset.
    """
    filename = f"story_ids_{event_id}.csv"
    rows, unique_ids = read_existing_csv(filename)
    
    # Start from the maximum offset seen so far (or initial_offset if none)
    current_offset = max((row["offset"] for row in rows), default=initial_offset)

    while len(unique_ids) < top_n:
        async with semaphore:
            new_ids_list = await fetch_story_ids(client, event_id, current_offset)
        
        if not new_ids_list:
            print(f"[INFO] No new IDs returned for event {event_id} at offset {current_offset}. Stopping.")
            break

        # Check if any returned story ID is a duplicate
        if set(new_ids_list) & unique_ids:
            # Shift existing offsets and update current_offset
            for row in rows:
                row["offset"] += current_offset
            current_offset = max(row["offset"] for row in rows)
            print(f"[INFO] Duplicate detected. Updated offsets; new starting offset is {current_offset}.")
            write_csv(filename, rows)
            continue

        # Append only new unique IDs
        new_unique_ids = []
        for sid in new_ids_list:
            if sid not in unique_ids:
                new_unique_ids.append(sid)
                unique_ids.add(sid)

        for sid in new_unique_ids:
            rows.append({"offset": current_offset, "story_id": sid})
        print(f"[INFO] Event {event_id} | Offset {current_offset} | Added {len(new_unique_ids)} new IDs. Total unique IDs: {len(unique_ids)}")
        write_csv(filename, rows)
        
        # Update current_offset based on the maximum offset recorded
        current_offset = max(row["offset"] for row in rows)
        await asyncio.sleep(0.1)

    print(f"[DONE] Finished processing event {event_id}. Total unique story IDs: {len(unique_ids)}. CSV saved as {filename}")

async def main():
    parser = argparse.ArgumentParser(
        description="Retrieve top_n unique story IDs for given event IDs and save to CSV files."
    )
    parser.add_argument('--events', nargs='+', required=True, help="List of event IDs (space separated).")
    parser.add_argument('--n', type=int, default=10, help="Total number of unique story IDs to collect per event (default: 10).")
    parser.add_argument('--offset', type=int, default=100, help="Initial offset for the API (default: 100).")
    parser.add_argument('--concurrency', type=int, default=5, help="Maximum concurrent requests (default: 5).")
    args = parser.parse_args()

    semaphore = asyncio.Semaphore(args.concurrency)
    async with httpx.AsyncClient() as client:
        tasks = [process_event(event_id, args.offset, args.n, client, semaphore) for event_id in args.events]
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
