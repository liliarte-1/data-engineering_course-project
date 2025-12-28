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
spain_info_df = pd.read_csv(population_file, sep=",")
logging.info("Loaded Spain-level aggregated data.")

spain_info_df = spain_info_df[spain_info_df["Lugar de residencia"].str.contains("España")]
logging.info(f"Filtered Spain rows: {spain_info_df.shape}")

#drop España row from population_df
population_df = population_df[~population_df["Lugar de residencia"].str.contains("España")]
logging.info(f"Removed Spain rows from population_df. New shape: {population_df.shape}")


#2 
population_df = population_df[~population_df["Lugar de residencia"].str.contains("MUNICIPIOS")]
logging.info("Removed MUNICIPIOS header rows.")

population_df["municipality_number"] = population_df["Lugar de residencia"].str.extract(r"(\d+)")
population_df["municipality_name"] = population_df["Lugar de residencia"].str.extract(r"([A-Za-z]+)")
logging.info("Extracted municipality number and name components.")

population_df = population_df.drop(columns=["Lugar de residencia"])
logging.info("Dropped 'Lugar de residencia' column.")

print(population_df.head())
print(spain_info_df.head())


#3
population_df.columns = population_df.columns.str.replace(
    r"^1 de enero\s+", "", regex=True)
logging.info("Cleaned date prefixes from population_df column names.")

spain_info_df.columns = spain_info_df.columns.str.replace(
    r"^1 de enero\s+", "", regex=True)
logging.info("Cleaned date prefixes from spain_info_df column names.")


#4
numeric_cols = [c for c in population_df.columns if c != "municipality_name" and c != "municipality_number"]
logging.info(f"Identified {len(numeric_cols)} numeric population columns.")

# if text have text "N.A." replace with NaN
population_df[numeric_cols] = population_df[numeric_cols].apply(pd.to_numeric, errors="coerce")
logging.info("Converted population values to numeric, coercing invalid values to NaN.")

# media per row
row_means = population_df[numeric_cols].mean(axis=1, skipna=True)
population_df[numeric_cols] = population_df[numeric_cols].T.fillna(row_means).T
logging.info("Imputed missing population values using row-wise means.")

# people are represented as integers, so we use Int64 dtype to allow for NaN values
population_df[numeric_cols] = population_df[numeric_cols].round(0).astype("Int64")
logging.info("Rounded population values and casted to Int64.")

print(population_df.head())
print(spain_info_df.head())


#5
# columns to keep for provinces_df
base_cols = population_df.columns.drop(
    ["municipality_number", "municipality_name"]
)
logging.info(f"Base columns selected for province aggregation: {len(base_cols)} columns.")

provinces_df = pd.DataFrame(columns=base_cols)
logging.info("Initialized empty provinces_df with base columns.")

# add province_name and province_number columns
provinces_df["province_name"] = pd.Series(dtype="string")
provinces_df["province_number"] = pd.Series(dtype="int")
logging.info("Added province_name and province_number columns.")

provinces_df.loc[0] = {"province_name": "Zaragoza", "province_number": 50000}
provinces_df.loc[1] = {"province_name": "Huesca", "province_number": 22000}
provinces_df.loc[2] = {"province_name": "Teruel", "province_number": 44000}
logging.info("Inserted province reference rows.")

print(provinces_df.head())


#6
# convert municipality_number to numeric to ensure proper division
population_df["municipality_number"] = pd.to_numeric(
    population_df["municipality_number"], errors="coerce"
).astype("Int64")
logging.info("Converted municipality_number to numeric type.")

provinces_df["province_code"] = provinces_df["province_number"] // 10000
logging.info("Computed province_code for provinces_df.")

grouped = (
    population_df
    .groupby(population_df["municipality_number"] // 10000)
    [base_cols]
    .sum()
)
logging.info("Aggregated population data by province code.")

for col in base_cols:
    if col not in ["province_name", "province_number"]:
        provinces_df[col] = (
            provinces_df["province_code"]
            .map(grouped[col])
        )

logging.info("Mapped aggregated population values to provinces_df.")

provinces_df.drop(columns="province_code", inplace=True)
logging.info("Dropped province_code column.")

print(provinces_df.head())

logging.info(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
