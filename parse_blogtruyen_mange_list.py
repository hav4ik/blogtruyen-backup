import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from pathlib import Path
import pandas as pd
from multiprocessing import Pool, cpu_count


# Base URL for the AJAX endpoint.
base_url = "https://blogtruyenmoi.com/ajax/Search/AjaxLoadListManga"

# Parameters for the request (you may need to modify these based on your needs).
key = "tatca"  # Change this if a specific key is needed.
order_by = "0"  # Change this to match the ordering criteria.

# Function to get manga titles from a specific page.
def get_manga_titles(page):
    params = {
        'key': key,
        'orderBy': order_by,
        'p': page
    }
    response = requests.get(base_url, params=params)
    return response


def get_page_k_list(page_k):
    r = get_manga_titles(page_k)  # Example: Get titles from the first page.
    soup = BeautifulSoup(r.text, 'html.parser')

    # Find all <p> elements that contain a child <span> with the class 'tiptip fs-12 ellipsis'.
    manga_elements = soup.select('p:has(span.tiptip.fs-12.ellipsis)')

    # Extract relevant information for each manga.
    manga_data = []
    for element in manga_elements:
        # Extract title and URL.
        a_tag = element.find('a')
        title = a_tag.get_text(strip=True) if a_tag else 'No Title'
        url = a_tag['href'] if a_tag else 'No URL'
        
        # Extract numeric values.
        spans = element.find_all('span', class_='fs-12')
        # span class should NOT contain 'ellipsis' to getting the title
        numeric_values = [span.get_text(strip=True) for span in spans if 'ellipsis' not in span['class']]
        
        # Find corresponding hidden tiptip-content div for description and image.
        tiptip_id = a_tag.parent['data-tiptip'] if a_tag and 'data-tiptip' in a_tag.parent.attrs else None
        hidden_div = soup.find('div', id=tiptip_id) if tiptip_id else None
        
        # Extract image URL if available.
        image_element = hidden_div.find('img') if hidden_div else None
        image_url = image_element['src'] if image_element else 'No Image'
        
        # Extract description if available.
        description = hidden_div.get_text(strip=True) if hidden_div else 'No Description'

        # Pad numeric values to 3 elements, if necessary.
        while len(numeric_values) < 3:
            numeric_values.append('N/A')
        
        # Store the extracted data.
        manga_data.append({
            'page': page_k,
            'title': title,
            'url': url,
            'chapters': numeric_values[0],
            'views': numeric_values[1],
            'comments': numeric_values[2],
            'cover_image_url': image_url,
            'description': description
        })

    # Print the extracted manga information.
    print(f">>> Extracted Manga Data from Page {page_k}; Total {len(manga_data)} manga found.")
    df = pd.DataFrame(manga_data)
    return df


# page_start = 1
# page_end = 3 # 1302
# output_dir = Path('blogtruyen_manga_list')
# output_dir.mkdir(exist_ok=True)
# for page_k in tqdm(range(page_start, page_end + 1)):
#     df = get_page_k_list(page_k)
#     df.to_csv(output_dir / f'blogtruyen_manga_list_page_{page_k}.csv', index=False)



import pandas as pd
import logging
from pathlib import Path
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from functools import partial

# Set up logging to capture errors and other information.
logging.basicConfig(
    filename='manga_crawl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Define the range of pages to be processed.
page_start = 1
page_end = 1302  # Change to 1302 or the number of pages you need.
output_dir = Path('blogtruyen_manga_list')
output_dir.mkdir(exist_ok=True)


# Define the function that processes a single page.
def process_page(page_k, output_dir, skip_existing=True):
    try:
        # Define the output file path.
        output_file = output_dir / f'blogtruyen_manga_list_page_{page_k}.csv'

        # skip if the file already exists.
        if skip_existing and output_file.exists():
            logging.info(f"Skipping page {page_k} as the file already exists.")
            return output_file

        # Fetch and parse the data for the given page.
        df = get_page_k_list(page_k)  # Replace with your actual function.
        
        # Check if the DataFrame is empty.
        if df.empty:
            logging.warning(f"Page {page_k} returned an empty DataFrame.")
            return None
        
        # Save the DataFrame to a CSV file.
        df.to_csv(output_file, index=False)
        
        logging.info(f"Successfully processed page {page_k} and saved to {output_file}.")
        return output_file

    except Exception as e:
        logging.error(f"Error processing page {page_k}: {e}")
        return None


# Use multiprocessing to parallelize the page fetching.
if __name__ == '__main__':
    # Set up the number of processes to the number of CPU cores.
    num_processes = min(cpu_count(), 4)  # Adjust the number of processes if needed.
    
    # Create a list of pages to process.
    pages = range(page_start, page_end + 1)
    
    # Use a Pool to parallelize the process.
    with Pool(processes=num_processes) as pool:
        # Use partial to pass the output_dir to the function.
        process_page_partial = partial(process_page, output_dir=output_dir)
        results = list(tqdm(pool.imap_unordered(process_page_partial, pages), total=len(pages)))
    
    # Filter out None values (failed processes).
    successful_files = [result for result in results if result]
    
    print(f"Successfully processed {len(successful_files)} pages out of {page_end - page_start + 1}.")
    print(f"Data saved in {output_dir}. Check 'manga_crawl.log' for details.")
