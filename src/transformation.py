# TRANSFORM DATA FOR CLEANING AND STANDARDIZATION

import logging
import pandas as pd
from datetime import datetime


# Configure logging
logging.basicConfig(
    filename="./logs/transformation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
population_file = "data/raw/0201010101.csv"

population_df = pd.read_csv(population_file, sep=",")
logging.info(f"Loaded population data with shape: {population_df.shape}")

#1 
population_df["municipality_number"] = population_df["Lugar de residencia"].str.extract(r"(\d+)")
population_df["municipality_name"] = population_df["Lugar de residencia"].str.extract(r"([A-Za-z]+)")
logging.info("Extracted municipality number and name components.")

population_df = population_df.drop(columns=["Lugar de residencia"])
print(population_df.head())