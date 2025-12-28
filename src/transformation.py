# TRANSFORM DATA FOR CLEANING AND STANDARDIZATION

import logging
import pandas as pd
from datetime import datetime
import re

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



#2 
population_df = population_df[~population_df["Lugar de residencia"].str.contains("MUNICIPIOS")]
logging.info("Removed MUNICIPIOS header rows.")

population_df["municipality_number"] = population_df["Lugar de residencia"].str.extract(r"^(\d+)")
population_df["municipality_name"] = population_df["Lugar de residencia"].str.extract(r"^\d+\s*(.*)$")
logging.info("Extracted municipality number and name components.")

population_df = population_df.drop(columns=["Lugar de residencia"])
logging.info("Dropped 'Lugar de residencia' column.")

print(population_df.head())

#3
population_df.columns = population_df.columns.str.replace(
    r"^1 de enero\s+", "", regex=True)
logging.info("Cleaned date prefixes from population_df column names.")



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


import re
import pandas as pd

#5 
logging.info("Starting long-format transformation (municipalities).")

id_cols = ["municipality_number", "municipality_name"]
value_cols = [c for c in population_df.columns if c not in id_cols]

year_cols = [
    c for c in value_cols
    if re.match(r"^(19\d{2}|20\d{2})\s+(Ambos sexos|Hombres|Mujeres)$", str(c))
]

logging.info(f"Detected {len(year_cols)} year+sex columns to melt into long format.")

# pivot wide to long: one row per (municipality, year, sex)
pop_long = population_df.melt(
    id_vars=id_cols,
    value_vars=year_cols,
    var_name="year_sex",
    value_name="population"
)

logging.info(f"Melt completed. pop_long shape: {pop_long.shape}")

# extract year and sex from the melted column
pop_long["year"] = pop_long["year_sex"].str.extract(r"^(19\d{2}|20\d{2})").astype(int)
pop_long["sex"] = pop_long["year_sex"].str.replace(
    r"^(19\d{2}|20\d{2})\s+", "", regex=True
).str.strip()

# drop helper column
pop_long = pop_long.drop(columns=["year_sex"])

# ensure numeric population with nullable Int64
pop_long["population"] = pd.to_numeric(pop_long["population"], errors="coerce").astype("Int64")

# safety logs
logging.info(
    "Long-format municipality dataset built. "
    f"Years: {pop_long['year'].min()}-{pop_long['year'].max()}, "
    f"Sex categories: {sorted(pop_long['sex'].dropna().unique().tolist())}"
)

print(pop_long.head())

#11
spain_info_df = pop_long.copy()
logging.info("Loaded Spain-level aggregated data.")

spain_info_df = spain_info_df[spain_info_df["municipality_name"].str.contains("España")]
logging.info(f"Filtered Spain rows: {spain_info_df.shape}")

#drop España row from population_df
pop_long = pop_long[~pop_long["municipality_name"].str.contains("España")]
logging.info(f"Removed Spain rows from population_df. New shape: {population_df.shape}")
print(spain_info_df.head())

# #6
# logging.info("Building provinces reference table (metadata).")

# provinces_df = pd.DataFrame([
#     {"province_name": "Zaragoza", "province_number": 50000},
#     {"province_name": "Huesca",   "province_number": 22000},
#     {"province_name": "Teruel",   "province_number": 44000},
# ])

# provinces_df["province_code"] = (provinces_df["province_number"] // 1000).astype(int)

# logging.info(f"Provinces reference table created with {len(provinces_df)} rows.")
# print(provinces_df)


# #7
# logging.info("Aggregating municipality long dataset to province level (long format).")

# # ensure correct numeric types
# pop_long["municipality_number"] = pd.to_numeric(
#     pop_long["municipality_number"], errors="coerce"
# ).astype("Int64")
# pop_long["population"] = pd.to_numeric(
#     pop_long["population"], errors="coerce"
# ).astype("Int64")

# # Compute province_code from municipality_number:
# # example: 50001 -> 50
# pop_long["province_code"] = (pop_long["municipality_number"] // 1000).astype("Int64")

# # aggregate population by province_code, year
# prov_long = (
#     pop_long
#     .groupby(["province_code", "year", "sex"], as_index=False)["population"]
#     .sum()
# )

# logging.info(f"Province aggregation done. prov_long shape: {prov_long.shape}")

# #8
# # IMPORTANT: merge province names from reference table to final dataset
# prov_long = prov_long.merge(
#     provinces_df[["province_code", "province_name", "province_number"]],
#     on="province_code",
#     how="left"
# )

# #9 is not needed anymore since we already pivoted above
# pop_long = pop_long.drop(columns=["province_code"])
# prov_long = prov_long.drop(columns=["province_code"])

# #10 final clean ordering
# pop_long = pop_long[[
#     "municipality_number", "municipality_name",
#     "year", "sex", "population"
# ]].sort_values(["year", "sex"], ignore_index=True)

# prov_long = prov_long[[
#     "province_number", "province_name",
#     "year", "sex", "population"
# ]].sort_values(["year", "sex"], ignore_index=True)

# logging.info(
#     "Province-level long dataset built successfully. "
#     f"Years: {pop_long['year'].min()}-{pop_long['year'].max()}."
# )

# logging.info(
#     "Province-level long dataset built successfully. "
#     f"Years: {prov_long['year'].min()}-{prov_long['year'].max()}."
# )

# print(pop_long.head(12))
# print(prov_long.head(12))




# pop_long.to_csv("data/staging/population_municipalities.csv", index=False)
# prov_long.to_csv("data/staging/population_provinces.csv", index=False)

