import os
import json
import glob
import argparse
import pandas as pd
from tqdm import tqdm

LIMIT = 10


def main():
    parser = argparse.ArgumentParser(
        description="Add new URLs from JSON sources to CSV."
    )
    parser.add_argument("json_dir", help="Directory containing JSON files")
    parser.add_argument("csv_file", help="CSV file path to read and update")
    args = parser.parse_args()

    json_dir = args.json_dir
    csv_file = args.csv_file

    # Load the existing CSV if it exists, else create a new DataFrame with 'index' and 'url' columns.
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file)
    else:
        df = pd.DataFrame(columns=["index", "url"])

    # Create a set of existing URLs for quick lookup.
    existing_urls = set(df["url"].tolist())

    new_urls = set()

    # Get all JSON files in the directory.
    json_files = glob.glob(os.path.join(json_dir, "*.json"))
    for json_file in tqdm(json_files):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"Error reading {json_file}: {e}")
            continue

        # Each JSON file is expected to be a dictionary where each key maps to an object
        # that contains a "sources" array.
        data = sorted(data.values(), key=lambda x: len(
            x["sources"]), reverse=True)[:LIMIT]

        for value in data:
            sources = value.get("sources", [])
            for source in sources:
                url = source.get("url")
                if url and url not in existing_urls:
                    new_urls.add(url)
                    # Update to avoid duplicates across files.
                    existing_urls.add(url)

    if new_urls:
        # Determine the next index.
        next_index = int(df["index"].max()) + 1 if not df.empty else 0

        new_rows = []
        for url in new_urls:
            new_rows.append({"index": next_index, "url": url})
            next_index += 1

        # Create a DataFrame for the new rows and append it to the existing DataFrame.
        new_df = pd.DataFrame(new_rows)
        df = pd.concat([df, new_df], ignore_index=True)

        # Write the updated DataFrame back to the CSV.
        df.to_csv(csv_file, index=False)
        print(f"Added {len(new_urls)} new URL(s) to {csv_file}")
    else:
        print("No new URLs found.")


if __name__ == "__main__":
    main()
