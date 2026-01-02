# TRANSFORM DATA FOR CLEANING AND STANDARDIZATION (MUNICIPALITIES ONLY)

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

# =========================
# POPULATION (MUNICIPALITIES)
# =========================

population_file = "data/raw/0201010101.csv"

population_df = pd.read_csv(population_file, sep=",")
logging.info(f"Loaded population data with shape: {population_df.shape}")

# Remove header rows like "MUNICIPIOS ..."
population_df = population_df[~population_df["Lugar de residencia"].astype(str).str.contains("MUNICIPIOS", na=False)]
logging.info("Removed MUNICIPIOS header rows.")

# Extract municipality number and name
population_df["municipality_number"] = population_df["Lugar de residencia"].astype(str).str.extract(r"^(\d+)")
population_df["municipality_name"] = population_df["Lugar de residencia"].astype(str).str.extract(r"^\d+\s*(.*)$")
logging.info("Extracted municipality number and name components.")

population_df = population_df.drop(columns=["Lugar de residencia"])
logging.info("Dropped 'Lugar de residencia' column.")

# Clean date prefixes in columns, e.g. "1 de enero 2020 Ambos sexos" -> "2020 Ambos sexos"
population_df.columns = population_df.columns.str.replace(r"^1 de enero\s+", "", regex=True)
logging.info("Cleaned date prefixes from population_df column names.")

# Identify numeric columns (all except ids)
numeric_cols = [c for c in population_df.columns if c not in ["municipality_name", "municipality_number"]]
logging.info(f"Identified {len(numeric_cols)} numeric population columns.")

# Convert to numeric (coerce invalid to NaN)
population_df[numeric_cols] = population_df[numeric_cols].apply(pd.to_numeric, errors="coerce")
logging.info("Converted population values to numeric, coercing invalid values to NaN.")

# Impute missing values with row-wise mean
row_means = population_df[numeric_cols].mean(axis=1, skipna=True)
population_df[numeric_cols] = population_df[numeric_cols].T.fillna(row_means).T
logging.info("Imputed missing population values using row-wise means.")

# Round and cast to Int64
population_df[numeric_cols] = population_df[numeric_cols].round(0).astype("Int64")
logging.info("Rounded population values and casted to Int64.")

# Long format: one row per (municipality, year, sex)
logging.info("Starting long-format transformation (municipalities).")

id_cols = ["municipality_number", "municipality_name"]
value_cols = [c for c in population_df.columns if c not in id_cols]

year_cols = [
    c for c in value_cols
    if re.match(r"^(19\d{2}|20\d{2})\s+(Ambos sexos|Hombres|Mujeres)$", str(c))
]
logging.info(f"Detected {len(year_cols)} year+sex columns to melt into long format.")

pop_long = population_df.melt(
    id_vars=id_cols,
    value_vars=year_cols,
    var_name="year_sex",
    value_name="population"
)
logging.info(f"Melt completed. pop_long shape: {pop_long.shape}")

# Extract year and sex
pop_long["year"] = pop_long["year_sex"].str.extract(r"^(19\d{2}|20\d{2})").astype(int)
pop_long["sex"] = pop_long["year_sex"].str.replace(r"^(19\d{2}|20\d{2})\s+", "", regex=True).str.strip()
pop_long = pop_long.drop(columns=["year_sex"])

# Ensure types
pop_long["municipality_number"] = pd.to_numeric(pop_long["municipality_number"], errors="coerce").astype("Int64")
pop_long["population"] = pd.to_numeric(pop_long["population"], errors="coerce").astype("Int64")

# Final ordering
pop_long = pop_long[[
    "municipality_number", "municipality_name",
    "year", "sex", "population"
]].sort_values(["year", "sex", "municipality_number"], ignore_index=True)

logging.info(
    "Municipality long dataset built successfully. "
    f"Years: {pop_long['year'].min()}-{pop_long['year'].max()}, "
    f"Sex categories: {sorted(pop_long['sex'].dropna().unique().tolist())}"
)

# Save municipalities population
pop_long.to_csv("data/staging/population_municipalities.csv", index=False)
logging.info("Saved cleaned population municipalities dataset to staging area.")


# =========================
# HOUSEHOLDS (MUNICIPALITIES)
# =========================

households_data = "data/raw/households_dataset.csv"

households_df = pd.read_csv(households_data, sep=",")
logging.info(f"Loaded households data with shape: {households_df.shape}")

# Extract municipality number and name
households_df["municipality_number"] = households_df["Lugar de residencia"].astype(str).str.extract(r"^(\d+)")
households_df["municipality_name"] = households_df["Lugar de residencia"].astype(str).str.extract(r"^\d+\s*(.*)$")
logging.info("Extracted municipality number and name components for households.")

households_df = households_df.drop(columns=["Lugar de residencia"])
logging.info("Dropped 'Lugar de residencia' column from households.")

# Convert types
households_df["municipality_number"] = pd.to_numeric(households_df["municipality_number"], errors="coerce").astype("Int64")
for col in ["number_of_households", "average_household_size", "number_of_dwellings", "average_rent_price"]:
    if col in households_df.columns:
        households_df[col] = pd.to_numeric(households_df[col], errors="coerce").astype("Float64")

# (Opcional pero recomendable) asegurar year como int si existe
if "year" in households_df.columns:
    households_df["year"] = pd.to_numeric(households_df["year"], errors="coerce").astype("Int64")

# Final ordering
final_cols = [
    "municipality_number", "municipality_name"
]
if "year" in households_df.columns:
    final_cols.append("year")
final_cols += [c for c in ["number_of_households", "average_household_size", "number_of_dwellings", "average_rent_price"] if c in households_df.columns]

households_df = households_df[final_cols].sort_values(
    ["year", "municipality_number"] if "year" in households_df.columns else ["municipality_number"],
    ignore_index=True
)

# Save municipalities households
households_df.to_csv("data/staging/households_municipalities.csv", index=False)
logging.info("Saved cleaned households municipalities dataset to staging area.")

logging.info("Transformation finished successfully.")
