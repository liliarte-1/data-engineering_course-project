1. # TRANSFORM DATA FOR CLEANING AND STANDARDIZATION

import logging
import pandas as pd
from datetime import datetime
import re
from pathlib import Path


# Configure logging
logging.basicConfig(
    filename="./logs/transformation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")


ruta = Path("data/raw")
archivos = sorted(ruta.glob("pobmun*.csv*"))
logging.info(f"Found {len(archivos)} source files in {ruta}")

dfs = []
df_total = pd.DataFrame()


for f in archivos:
    #1
    df = pd.read_csv(f, skiprows=1, sep=",")
    df.reset_index(drop=True, inplace=True)
    logging.info(f"Read file {f.name} with shape {df.shape}")

    #2
    year = int(re.search(r"\d+", f.stem).group())
    df["year"] = year
    logging.info(f"Detected year {year} from filename {f.name}")

    #3
    import pandas as pd

    df.columns = [
        "CPRO", "CPRO_NAME", "MUN_NUMBER", "MUN_NAME",
        "POBLATION", "MALE", "FEMALE", "YEAR"
    ]

    #int cols
    int_cols = ["CPRO", "MUN_NUMBER", "POBLATION", "MALE", "FEMALE", "YEAR"]

    # clean int cols that have dots as thousands separator and spaces
    for c in int_cols:
        df[c] = (
            df[c].astype("string")
                .str.strip()
                .str.replace(r"\.", "", regex=True)      # 2.467 -> 2467 
                .str.replace(r"\s+", "", regex=True)     # remove spaces
                .str.zfill(2)                          # fill leading zeros
                .replace({"": pd.NA})
                .astype("Int64")
        )

        
    # report missing counts after cleaning numeric-like columns for this file
    try:
        missing_int_after = df[int_cols].isnull().sum().to_dict()
        logging.info(f"Missing counts in int cols after cleaning for {f.name}: {missing_int_after}")
    except Exception as e:
        logging.info(f"Could not compute post-cleaning missing counts for {f.name}: {e}")

    # string cols
    #5 
    df["CPRO_NAME"] = df["CPRO_NAME"].str.replace(",", "", regex=False)
    df["CPRO_NAME"] = df["CPRO_NAME"].str.replace("(", "", regex=False)
    df["CPRO_NAME"] = df["CPRO_NAME"].str.replace(")", "", regex=False)
    
    df["MUN_NAME"]  = df["MUN_NAME"].str.replace(",", "", regex=False)
    df["MUN_NAME"] = df["MUN_NAME"].str.replace("(", "", regex=False)
    df["MUN_NAME"] = df["MUN_NAME"].str.replace(")", "", regex=False)

    df["CPRO_NAME"] = df["CPRO_NAME"].astype("string")
    df["MUN_NAME"]  = df["MUN_NAME"].astype("string")

    # remove the file's header/metadata row and report counts
    df = df.iloc[1:]  # remove first row
    logging.info(f"After cleaning, {f.name} has {df.shape[0]} rows; unique CPRO_NAMEs: {df['CPRO_NAME'].nunique(dropna=True)}")
    dfs.append(df)
    df_total = pd.concat([df_total, df], ignore_index=True)
    logging.info(f"Appended {df.shape[0]} rows from {f.name}; combined dataset now {df_total.shape}")

#4
logging.info(f"Total combined dataset shape: {df_total.shape}")
logging.info("Missing values by column:")
logging.info(df_total.isnull().sum())


#6
logging.info(f"Missing before drop - MALE: {df_total['MALE'].isnull().sum()}, FEMALE: {df_total['FEMALE'].isnull().sum()}")
df_total = df_total.dropna(subset=["MALE", "FEMALE"])
logging.info(f"Dropped rows with missing MALE/FEMALE. New shape: {df_total.shape}")

#7
logging.info(f"Missing before drop - MUN_NAME: {df_total['MUN_NAME'].isnull().sum()}, MUN_NUMBER: {df_total['MUN_NUMBER'].isnull().sum()}")
df_total = df_total.dropna(subset=["MUN_NAME", "MUN_NUMBER"])
logging.info(f"Dropped rows with missing MUN_NAME/MUN_NUMBER. New shape: {df_total.shape}")

# 8 
# IMPUTE MISSING CPRO_NAME USING REFERENCE FILE
# ref = pd.read_csv(
#     "data/staging/codauto_cpro.csv",
#     sep=None,
#     engine="python",
#     encoding="cp1252"
# )

# # no comunity codes needed
# ref.drop(columns=["CODAUTO_NAME", "CODAUTO"], inplace=True, errors="ignore")

# # mapping CPRO -> CPRO_NAME
# cpro_to_name = (
#     ref.dropna(subset=["CPRO_NAME"])
#        .drop_duplicates("CPRO")
#        .set_index("CPRO")["CPRO_NAME"]
# )

# # impute missing CPRO_NAME from CPRO code
# mask_missing = df_total["CPRO_NAME"].fillna("").astype("string").str.strip().eq("")
# df_total.loc[mask_missing, "CPRO_NAME"] = df_total.loc[mask_missing, "CPRO"].map(cpro_to_name)

logging.info(f"Missing before final drop - CPRO: {df_total['CPRO'].isnull().sum()}, CPRO_NAME: {df_total['CPRO_NAME'].isnull().sum()}")
df_total = df_total.dropna(subset=["CPRO", "CPRO_NAME"])
logging.info(f"Total combined dataset shape: {df_total.shape}")
logging.info("Missing values by column:")
logging.info(df_total.isnull().sum())


#9
codauto = pd.read_csv("data/raw/codauto_cpro.csv", sep=";")
logging.info(f"Loaded codauto reference with shape {codauto.shape}; unique CPRO: {codauto['CPRO'].nunique(dropna=True)}")
codauto["CPRO_NAME"] = codauto["CPRO_NAME"].str.replace(",", "", regex=False)
codauto["CPRO_NAME"] = codauto["CPRO_NAME"].str.replace("(", "", regex=False)
codauto["CPRO_NAME"] = codauto["CPRO_NAME"].str.replace(")", "", regex=False)
codauto["CODAUTO_NAME"] = codauto["CODAUTO_NAME"].str.replace(",", "", regex=False)
codauto["CODAUTO_NAME"] = codauto["CODAUTO_NAME"].str.replace("(", "", regex=False)
codauto["CODAUTO_NAME"] = codauto["CODAUTO_NAME"].str.replace(")", "", regex=False)


codauto["CPRO_NAME"] = codauto["CPRO_NAME"].astype("string")
codauto["CODAUTO_NAME"]  = codauto["CODAUTO_NAME"].astype("string")
codauto["CPRO"] = codauto["CPRO"].astype("Int64")
codauto["CODAUTO"] = codauto["CODAUTO"].astype("Int64")

#10
economic_df = pd.read_csv(
    "data/raw/economic_sector_province.csv",
    sep=",").copy()
logging.info(f"Loaded economic sector file with shape {economic_df.shape}")

economic_df["Provincias"] = economic_df["Provincias"].astype("string").str.strip()
economic_df = economic_df[~economic_df["Provincias"].str.lower().eq("total nacional")].copy()
logging.info(f"Filtered economic sector rows, new shape {economic_df.shape}")

#11
# 1) Normaliza Total a numérico (quita miles y convierte)
economic_df["Total"] = (
    economic_df["Total"]
    .astype(str)
    .str.strip()
    .str.replace(".", "", regex=False)     # miles
    .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
)

economic_df["Total"] = pd.to_numeric(economic_df["Total"], errors="coerce")

# 2) Rellena NaN con la media de su provincia
economic_df["Total"] = economic_df["Total"].fillna(
    economic_df.groupby("Provincias")["Total"].transform("mean")
)

# 3) Si aún quedan NaN (provincias enteras vacías), usa media global
economic_df["Total"] = economic_df["Total"].fillna(economic_df["Total"].mean())

# 4) Entero final (redondea antes)
economic_df["Total"] = economic_df["Total"].round().astype("Int64")

#12
ext = economic_df["Provincias"].str.extract(r"^\s*(\d{1,2})\s*[-–:]*\s*(.+?)\s*$")
economic_df["CPRO"] = ext[0].astype("string").str.zfill(2)
economic_df["CPRO_NAME"] = ext[1].astype("string").str.strip()

# 13
# extract year and quarter from "Periodo" column
tmp = economic_df["Periodo"].astype("string").str.strip().str.extract(r"^(\d{4})T([1-4])$")
economic_df["YEAR"] = tmp[0].astype("Int64")
economic_df["QUARTER"] = tmp[1].astype("Int64")
# trimester no longer needed
economic_df.drop(columns=["Periodo", "QUARTER"], inplace=True)
# IMPORNTANT: average total by CPRO, CPRO_NAME, SECTOR and YEAR
economic_df = (
    economic_df
        .groupby(["CPRO", "CPRO_NAME", "Sector económico", "YEAR"], as_index=False)["Total"]
        .mean()
)
#14
# rename and reorder columns
economic_df.columns = [
        "CPRO", "CPRO_NAME", "ECONOMIC_SECTOR", "YEAR",
        "TOTAL",
    ]
logging.info(f"Economic dataset aggregated to shape {economic_df.shape} and columns {list(economic_df.columns)}")

# Finalize transformation
logging.info(f"Transformation completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


deathcauses_df = pd.read_csv(
    "data/raw/death_causes_province.csv",
    sep=",",
)

#15
deathcauses_df["Total"] = (
    deathcauses_df["Total"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .replace("nan", pd.NA)
        .astype("Int64")
)

#16
deathcauses_df["Provincias"] = deathcauses_df["Provincias"].astype("string").str.strip()
deathcauses_df = deathcauses_df[~deathcauses_df["Provincias"].str.lower().eq("nacional")].copy()
deathcauses_df = deathcauses_df[~deathcauses_df["Provincias"].str.lower().eq("extranjero")].copy()
deathcauses_df.reset_index(drop=True, inplace=True)

#17 
deathcauses_df["Total"] = (
    deathcauses_df["Total"]
    .astype(str)
    .str.strip()
    .str.replace(".", "", regex=False)     # miles
    .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
)

deathcauses_df["Total"] = pd.to_numeric(deathcauses_df["Total"], errors="coerce")
deathcauses_df["Total"] = deathcauses_df["Total"].fillna(
    deathcauses_df.groupby(["Provincias", "Causa de muerte"])["Total"].transform("mean")
)

deathcauses_df["Total"] = deathcauses_df["Total"].fillna(deathcauses_df["Total"].mean())
deathcauses_df["Total"] = deathcauses_df["Total"].round().astype("Int64")


#18
ext = deathcauses_df["Provincias"].str.extract(r"^\s*(\d{1,2})\s*[-–:]*\s*(.+?)\s*$")
deathcauses_df["CPRO"] = ext[0].astype("string").str.zfill(2)
deathcauses_df["CPRO_NAME"] = ext[1].astype("string").str.strip()
deathcauses_df.drop(columns=["Provincias"], inplace=True)

# check extraction failures (DEBUG)
# mask_fail = ext[0].isna() | ext[1].isna()
# logging.info(deathcauses_df.loc[mask_fail, "Provincias"].value_counts(dropna=False).head(50))

#19
deathcauses_df.columns = [
        "DEATH_CAUSE", "SEX", "YEAR", "TOTAL",
        "CPRO", "CPRO_NAME"
    ]

logging.info("FINAL REPORT AFTER TRANSFORMATION")

logging.info(f"Combined main dataset shape after transformation: {df_total.shape}")
logging.info(df_total.isnull().sum())    
logging.info(df_total.columns)

logging.info(f"Economic dataset shape after transformation: {economic_df.shape}")
logging.info(economic_df.isnull().sum())
logging.info(economic_df.columns)

logging.info(f"Death Causes dataset shape after transformation: {deathcauses_df.shape}")
logging.info(deathcauses_df.isnull().sum())    
logging.info(deathcauses_df.columns)

logging.info(f"Codauto reference dataset shape after transformation: {codauto.shape}")
logging.info(codauto.isnull().sum())
logging.info(codauto.columns)

#20
df_total["CPRO"] = df_total["CPRO"].astype("string").str.zfill(2)
economic_df["CPRO"] = economic_df["CPRO"].astype("string").str.zfill(2)
deathcauses_df["CPRO"] = deathcauses_df["CPRO"].astype("string").str.zfill(2)
codauto["CPRO"] = codauto["CPRO"].astype("string").str.zfill(2)

df_total["CPRO"] = (
    df_total["CPRO"]
    .astype(int)
    // 10
)
economic_df["CPRO"] = economic_df["CPRO"].astype("Int64")
deathcauses_df["CPRO"] = deathcauses_df["CPRO"].astype("Int64")
codauto["CPRO"] = codauto["CPRO"].astype("Int64")


logging.info("Saving transformed datasets to data/staging/")
df_total.to_csv("data/staging/pobmun_combined_transformed.csv", index=False)
economic_df.to_csv("data/staging/economic_sector_province_transformed.csv", index=False)
deathcauses_df.to_csv("data/staging/death_causes_province_transformed.csv", index=False)
codauto.to_csv("data/staging/codauto_cpro_transformed.csv", index=False)

logging.info(f"Transformation process completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")