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

dfs = []
df_total = pd.DataFrame()


for f in archivos:
    #1
    df = pd.read_csv(f, skiprows=1, sep=",")
    df.reset_index(drop=True, inplace=True)

    #2
    year = int(re.search(r"\d+", f.stem).group())
    df["year"] = year
    print(df.head())

    #3
    df.columns = [
        "CPRO", "PROVINCE", "MUN_NUMBER", "MUN_NAME",
        "POBLATION", "MALE", "FEMALE", "YEAR"
    ]

    df = df.iloc[1:]  # remove first row
    print(df.head())
    dfs.append(df)
    df_total = pd.concat([df_total, df], ignore_index=True)

logging.info("Missing values by column:")
logging.info(df_total.isnull().sum())

df_total.to_csv("data/staging/pobmun_total.csv", index=False)


            



