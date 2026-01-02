import logging
import requests
import pandas as pd
import io
import os
from urllib.parse import urlparse
from datetime import datetime

# activate debug logging for detailed output, it is useful in development phase
# Configure logging
logging.basicConfig(
    filename="./logs/ingestion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

urls = ["https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/economic_sector_province.csv", 
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/death_causes_province.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2008.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2009.csv,"
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2010.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2011.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2012.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2013.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2014.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2015.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2016.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2017.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2018.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2019.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2020.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2021.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2022.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2023.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/pobmun/data_retrieval_simulation/pobmun/pobmun2024.csv",
        ]
        


logging.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
raw_dir = "data/raw"
os.makedirs(raw_dir, exist_ok=True)


for idx, url in enumerate(urls):
    try:
        # log the URL being fetched
        logging.info(f"[{idx+1}/{len(urls)}] Fetching: {url}")

        response = requests.get(url)
        response.raise_for_status()
        logging.info(f"Successfully fetched data from {url}")
        
        # with this line we skip the bad formatted lines, but we dont want it because we want to keep track of them
        # df = pd.read_csv(io.StringIO(response.text),sep=',',on_bad_lines='skip')

        # so we do this instead
        df = pd.read_csv(io.StringIO(response.text),sep=";",on_bad_lines="warn")

        # extract file name from URL
        file_name = os.path.basename(urlparse(url).path)
        file_path = os.path.join(raw_dir, file_name)

        # save CSV as raw data
        df.to_csv(file_path, index=False)

        logging.info(
            f"Saved raw dataset: {file_name} "
            f"({len(df)} rows, {len(df.columns)} columns)"
        )

         # INITIAL DATA EXPLORATION
        logging.info(f"\nDataset Shape: {df.shape}")
        logging.debug(f"\nColumn Names & Types:\n{df.dtypes}")
        logging.info(f"\nTotal Missing: {df.isnull().sum().sum()}\n")

    # handle network errors and parsing errors
    except requests.RequestException as exc:
        logging.error(f"Network error fetching {url}: {exc}")

    except pd.errors.ParserError as exc:
        logging.error(f"Failed to parse CSV from {url}: {exc}")

       
logging.info(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

