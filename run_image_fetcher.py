import requests
import os
from bs4 import BeautifulSoup
import pandas as pd
import logging
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from functools import partial
from pathlib import Path
from argparse import ArgumentParser
import numpy as np


parser = ArgumentParser()
parser.add_argument("--input_file", type=str, help="Input CSV file with manga IDs.")
parser.add_argument("--num_nodes", type=int, default=4, help="Number of processes to use.")
parser.add_argument("--node_id", type=int, default=0, help="Node ID to process.")
parser.add_argument("--output_file", type=str, help="Output directory to save the image data.")
args = parser.parse_args()

output_dir = Path(args.output_file).parent
output_dir.mkdir(exist_ok=True)
output_file_path = Path(args.output_file)


# Set up logging to capture errors and other information.
logging.basicConfig(
    # filename='manga_chapters_crawl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuration
chapter_list_df = pd.read_csv(
    # "https://github.com/hav4ik/blogtruyen-backup/raw/refs/heads/main/blogtruyen_manga_list.csv",
    args.input_file,
    )
# Split dataframe to 10 parts
chapter_list_df = np.array_split(chapter_list_df, args.num_nodes)[args.node_id]
id_list = chapter_list_df["chapter_url"].tolist()
print("Number of manga IDs to process:", len(id_list))


# Check if the output file exists.
existing_data = None
if os.path.exists(output_file_path):
    # We are resuming the process, so we need to load the existing data.
    existing_data = pd.read_csv(output_file_path)
    existing_ids = existing_data["chapter_url"].unique()
    
    # check for missing IDs in the existing data.
    id_list = [id for id in id_list if id not in existing_ids]
    print(f"Resuming process. {len(existing_ids)} IDs already processed. {len(id_list)} IDs remaining.")
else:
    print("Starting a new process.")


origin_url = "https://blogtruyenmoi.com"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    "Referer": origin_url,
    "Accept-Encoding": "gzip, deflate",
}

# Define the function that processes a single manga ID.
def list_chapter_images(chapter_uri):
    try:
        chapter_full_url = f"{origin_url}/{chapter_uri}"
        
        # # Create a session for requests.
        # session = requests.Session()
        chapter_response = requests.get(chapter_full_url, headers=headers)
        print(chapter_full_url)
        # print(chapter_response.text)
        
        if chapter_response.status_code != 200:
            logging.error(f"Failed to fetch chapter {chapter_uri}. Status code: {chapter_response.status_code}")
            return None

        chapter_soup = BeautifulSoup(chapter_response.content, "html.parser")
        content = chapter_soup.find("article", {"id": "content"})
        if not content:
            logging.warning(f"No content found for chapter {chapter_uri}.")
            return None
        
        images = content.find_all("img")
        
        # Collect image information.
        image_list = []
        for i, image in enumerate(images):
            image_url = image["src"]
            image_list.append({
                "chapter_url": chapter_uri,
                "image_n": i + 1,
                "image_url": image_url
            })
        
        logging.info(f"Processed chapter {chapter_uri}.")
        return pd.DataFrame(image_list)

    except Exception as e:
        logging.error(f"Error processing chapter {chapter_uri}: {e}")
        return None


# Multiprocessing setup.
if __name__ == '__main__':
    
    # Use partial to pass the output_dir to the function.
    process_image_partial = partial(list_chapter_images)
    
    # Set up the number of processes to the number of CPU cores.
    num_processes = min(cpu_count(), 4)  # Adjust based on your system.
    
    # Use a Pool to parallelize the process.
    with Pool(processes=num_processes) as pool:
        results = list(tqdm(pool.imap_unordered(process_image_partial, id_list), total=len(id_list)))
    
    # Merge and save the results to a CSV file.
    good_csv_list = [df for df in results if df is not None and not df.empty]
    if good_csv_list:
        full_df = pd.concat(good_csv_list, ignore_index=True)
        if len(full_df) == 0:
            logging.warning(f"No new data to save for this node.")
            raise RuntimeError("No new data fetched in this session.")

        # Check if we have existing data to merge.
        if existing_data is not None:
            full_df = pd.concat([existing_data, full_df], ignore_index=True)

        full_df.to_csv(output_file_path, index=False)
        logging.info(f"Output Data saved in {output_file_path}.")
    else:
        logging.warning(f"No data to save for this node.")
        raise RuntimeError("No new data fetched in this session.")
