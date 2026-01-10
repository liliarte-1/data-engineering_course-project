# TRANSFORM DATA FOR CLEANING AND STANDARDIZATION

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
logger = logging.getLogger(__name__)

logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")


# -----------------------------
# Helpers (to avoid repetition)
# -----------------------------
def remove_punctuation_parentheses(s: pd.Series) -> pd.Series:
    """Remove commas and parentheses, keep as pandas string dtype."""
    return (
        s.astype("string")
         .str.replace(",", "", regex=False)
         .str.replace("(", "", regex=False)
         .str.replace(")", "", regex=False)
    )


def clean_int_like(series: pd.Series, *, zfill: int | None = None) -> pd.Series:
    """
    Clean numeric-like strings:
    - trims
    - removes '.' as thousands separator
    - removes spaces
    - optionally left-pads with zeros (zfill)
    - converts to nullable Int64
    """
    out = (
        series.astype("string")
              .str.strip()
              .str.replace(r"\.", "", regex=True)      # 2.467 -> 2467
              .str.replace(r"\s+", "", regex=True)     # remove spaces
    )
    if zfill is not None:
        out = out.str.zfill(zfill)

    return (
        out.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
           .astype("Int64")
    )


def extract_year_from_filename(path: Path) -> int:
    m = re.search(r"\d{4}", path.stem)
    return int(m.group()) if m else pd.NA


def parse_provincia_field(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """
    Extract CPRO and CPRO_NAME from strings like:
    '28 - Madrid' / '28: Madrid' / '28–Madrid'
    """
    ext = series.astype("string").str.extract(r"^\s*(\d{1,2})\s*[-–:]*\s*(.+?)\s*$")
    cpro = ext[0].astype("string").str.zfill(2)
    name = ext[1].astype("string").str.strip()
    return cpro, name


def normalize_total_with_imputation(df: pd.DataFrame, total_col: str, group_cols: list[str]) -> pd.Series:
    """
    Normalize Total:
    - remove thousands separators
    - to numeric
    - fill NaN with group mean
    - fill remaining with global mean
    - round and Int64
    """
    tmp = (
        df[total_col].astype(str)
                    .str.strip()
                    .str.replace(".", "", regex=False)
                    .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    )
    tmp = pd.to_numeric(tmp, errors="coerce")

    tmp = tmp.fillna(df.groupby(group_cols)[total_col].transform("mean"))
    tmp = tmp.fillna(tmp.mean())

    return tmp.round().astype("Int64")


def cpro_div10_if_needed(cpro: pd.Series) -> pd.Series:
    """
    Normalize CPRO by dividing by 10 when needed.
    Examples:
    - 280 -> 28
    - 10  -> 1
    - 20  -> 2
    - 28  -> 28 (unchanged)
    """
    c = pd.to_numeric(cpro, errors="coerce").astype("Int64")

    # divide by 10 when value ends with 0 and is >= 10
    mask = (c >= 10) & (c % 10 == 0)
    c.loc[mask] = (c.loc[mask] // 10).astype("Int64")

    return c


def normalize_cpro_string(cpro: pd.Series) -> pd.Series:
    """
    Normalize CPRO as a 2-digit string.
    - trims
    - keeps only digits
    - if value has 3+ digits (e.g. 280), divide by 10
    - zero-pad to 2 digits
    """
    s = (
        cpro.astype("string")
            .str.strip()
            .str.replace(r"\D+", "", regex=True)  # keep digits only
    )

    # numeric step for /10 when needed
    n = pd.to_numeric(s, errors="coerce")

    mask = n >= 100
    n.loc[mask] = (n.loc[mask] // 10)

    return n.astype("Int64").astype("string").str.zfill(2)


# -----------------------------
# Pobmun combined (many files)
# -----------------------------
ruta = Path("data/raw")
archivos = sorted(ruta.glob("pobmun*.csv*"))
logger.info(f"Found {len(archivos)} source files in {ruta}")

dfs: list[pd.DataFrame] = []
df_total = pd.DataFrame()

for f in archivos:
    #1
    df = pd.read_csv(f, skiprows=1, sep=",")
    df.reset_index(drop=True, inplace=True)
    logger.info(f"Read file {f.name} with shape {df.shape}")

    #2
    year = int(re.search(r"\d+", f.stem).group())
    df["year"] = year
    logger.info(f"Detected year {year} from filename {f.name}")

    #3
    df.columns = [
        "CPRO", "CPRO_NAME", "MUN_NUMBER", "MUN_NAME",
        "POBLATION", "MALE", "FEMALE", "YEAR"
    ]

    # int cols
    int_cols = ["CPRO", "MUN_NUMBER", "POBLATION", "MALE", "FEMALE", "YEAR"]

    # clean int cols that have dots as thousands separator and spaces
    # Note: zfill on ALL int columns is not always meaningful; keeping your behavior:
    for c in int_cols:
        # municipality codes often want zfill; keeping zfill(2) as you had
        zfill = 2 if c in ["CPRO", "MUN_NUMBER"] else None
        df[c] = clean_int_like(df[c], zfill=zfill)

        # (FIX) Only for 2009 and 2016: MUN_NUMBER comes inflated (e.g. 730 instead of 73)
    if year in (2009, 2016):
        df["MUN_NUMBER"] = (pd.to_numeric(df["MUN_NUMBER"], errors="coerce") // 10).astype("Int64")
        logger.info(f"Applied MUN_NUMBER // 10 fix for year {year} in file {f.name}")

    # report missing counts after cleaning numeric-like columns for this file
    try:
        missing_int_after = df[int_cols].isnull().sum().to_dict()
        logger.info(f"Missing counts in int cols after cleaning for {f.name}: {missing_int_after}")
    except Exception as e:
        logger.info(f"Could not compute post-cleaning missing counts for {f.name}: {e}")

    #5 (string cols)
    df["CPRO_NAME"] = remove_punctuation_parentheses(df["CPRO_NAME"])
    df["MUN_NAME"] = remove_punctuation_parentheses(df["MUN_NAME"])

    # remove the file's header/metadata row and report counts
    df = df.iloc[1:]  # remove first row
    logger.info(
        f"After cleaning, {f.name} has {df.shape[0]} rows; "
        f"unique CPRO_NAMEs: {df['CPRO_NAME'].nunique(dropna=True)}"
    )

    dfs.append(df)
    df_total = pd.concat([df_total, df], ignore_index=True)
    logger.info(f"Appended {df.shape[0]} rows from {f.name}; combined dataset now {df_total.shape}")

#4
logger.info(f"Total combined dataset shape: {df_total.shape}")
logger.info("Missing values by column:")
logger.info(df_total.isnull().sum())


#6
logger.info(
    f"Missing before drop - MALE: {df_total['MALE'].isnull().sum()}, "
    f"FEMALE: {df_total['FEMALE'].isnull().sum()}"
)
df_total = df_total.dropna(subset=["MALE", "FEMALE"])
logger.info(f"Dropped rows with missing MALE/FEMALE. New shape: {df_total.shape}")

#7
logger.info(
    f"Missing before drop - MUN_NAME: {df_total['MUN_NAME'].isnull().sum()}, "
    f"MUN_NUMBER: {df_total['MUN_NUMBER'].isnull().sum()}"
)
df_total = df_total.dropna(subset=["MUN_NAME", "MUN_NUMBER"])
logger.info(f"Dropped rows with missing MUN_NAME/MUN_NUMBER. New shape: {df_total.shape}")

logger.info(
    f"Missing before final drop - CPRO: {df_total['CPRO'].isnull().sum()}, "
    f"CPRO_NAME: {df_total['CPRO_NAME'].isnull().sum()}"
)
df_total = df_total.dropna(subset=["CPRO", "CPRO_NAME"])
logger.info(f"Total combined dataset shape: {df_total.shape}")
logger.info("Missing values by column:")
logger.info(df_total.isnull().sum())


# -----------------------------
# Reference codauto
# -----------------------------
#9
codauto = pd.read_csv("data/raw/codauto_cpro.csv", sep=";")
logger.info(f"Loaded codauto reference with shape {codauto.shape}; unique CPRO: {codauto['CPRO'].nunique(dropna=True)}")

codauto["CPRO_NAME"] = remove_punctuation_parentheses(codauto["CPRO_NAME"])
codauto["CODAUTO_NAME"] = remove_punctuation_parentheses(codauto["CODAUTO_NAME"])

codauto["CPRO"] = clean_int_like(codauto["CPRO"], zfill=2)
codauto["CODAUTO"] = clean_int_like(codauto["CODAUTO"])


# -----------------------------
# Economic sector (province)
# -----------------------------
#10
economic_df = pd.read_csv("data/raw/economic_sector_province.csv", sep=",").copy()
logger.info(f"Loaded economic sector file with shape {economic_df.shape}")

economic_df["Provincias"] = economic_df["Provincias"].astype("string").str.strip()
economic_df = economic_df[~economic_df["Provincias"].str.lower().eq("total nacional")].copy()
logger.info(f"Filtered economic sector rows, new shape {economic_df.shape}")

#11
# Normaliza Total a numérico + imputación
economic_df["Total"] = pd.to_numeric(
    economic_df["Total"].astype(str).str.strip().str.replace(".", "", regex=False),
    errors="coerce"
)
economic_df["Total"] = economic_df["Total"].fillna(
    economic_df.groupby("Provincias")["Total"].transform("mean")
)
economic_df["Total"] = economic_df["Total"].fillna(economic_df["Total"].mean())
economic_df["Total"] = economic_df["Total"].round().astype("Int64")

#12
economic_df["CPRO"], economic_df["CPRO_NAME"] = parse_provincia_field(economic_df["Provincias"])

#13
tmp = economic_df["Periodo"].astype("string").str.strip().str.extract(r"^(\d{4})T([1-4])$")
economic_df["YEAR"] = tmp[0].astype("Int64")
economic_df["QUARTER"] = tmp[1].astype("Int64")

# trimester no longer needed
economic_df.drop(columns=["Periodo", "QUARTER"], inplace=True)

# IMPORTANT: average total by CPRO, CPRO_NAME, SECTOR and YEAR
economic_df = (
    economic_df
        .groupby(["CPRO", "CPRO_NAME", "Sector económico", "YEAR"], as_index=False)["Total"]
        .mean()
)

#14
economic_df.columns = ["CPRO", "CPRO_NAME", "ECONOMIC_SECTOR", "YEAR", "TOTAL"]
logger.info(f"Economic dataset aggregated to shape {economic_df.shape} and columns {list(economic_df.columns)}")

logger.info(f"Transformation completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# -----------------------------
# Death causes (province)
# -----------------------------
deathcauses_df = pd.read_csv("data/raw/death_causes_province.csv", sep=",")

#15
deathcauses_df["Total"] = (
    deathcauses_df["Total"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .replace("nan", pd.NA)
)
deathcauses_df["Total"] = pd.to_numeric(deathcauses_df["Total"], errors="coerce").astype("Int64")

#16
deathcauses_df["Provincias"] = deathcauses_df["Provincias"].astype("string").str.strip()
deathcauses_df = deathcauses_df[~deathcauses_df["Provincias"].str.lower().eq("nacional")].copy()
deathcauses_df = deathcauses_df[~deathcauses_df["Provincias"].str.lower().eq("extranjero")].copy()
deathcauses_df.reset_index(drop=True, inplace=True)

#17
# Normaliza + imputación por provincia y causa
deathcauses_df["Total"] = (
    deathcauses_df["Total"]
    .astype(str)
    .str.strip()
    .str.replace(".", "", regex=False)
    .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
)
deathcauses_df["Total"] = pd.to_numeric(deathcauses_df["Total"], errors="coerce")
deathcauses_df["Total"] = deathcauses_df["Total"].fillna(
    deathcauses_df.groupby(["Provincias", "Causa de muerte"])["Total"].transform("mean")
)
deathcauses_df["Total"] = deathcauses_df["Total"].fillna(deathcauses_df["Total"].mean())
deathcauses_df["Total"] = deathcauses_df["Total"].round().astype("Int64")

#18
deathcauses_df["CPRO"], deathcauses_df["CPRO_NAME"] = parse_provincia_field(deathcauses_df["Provincias"])
deathcauses_df.drop(columns=["Provincias"], inplace=True)

#19
deathcauses_df.columns = ["DEATH_CAUSE", "SEX", "YEAR", "TOTAL", "CPRO", "CPRO_NAME"]


logger.info("FINAL REPORT AFTER TRANSFORMATION")

logger.info(f"Combined main dataset shape after transformation: {df_total.shape}")
logger.info(df_total.isnull().sum())
logger.info(df_total.columns)

logger.info(f"Economic dataset shape after transformation: {economic_df.shape}")
logger.info(economic_df.isnull().sum())
logger.info(economic_df.columns)

logger.info(f"Death Causes dataset shape after transformation: {deathcauses_df.shape}")
logger.info(deathcauses_df.isnull().sum())
logger.info(deathcauses_df.columns)

logger.info(f"Codauto reference dataset shape after transformation: {codauto.shape}")
logger.info(codauto.isnull().sum())
logger.info(codauto.columns)


#20
# Normalize CPRO as 2-digit STRING across all datasets (DW-safe key)
df_total["CPRO"] = cpro_div10_if_needed(df_total["CPRO"])
df_total["CPRO"] = normalize_cpro_string(df_total["CPRO"])
economic_df["CPRO"] = normalize_cpro_string(economic_df["CPRO"])
deathcauses_df["CPRO"] = normalize_cpro_string(deathcauses_df["CPRO"])
codauto["CPRO"] = normalize_cpro_string(codauto["CPRO"])

#21
deathcauses_df[["DEATH_CAUSE_CODE", "DEATH_CAUSE_NAME"]] = (
    deathcauses_df["DEATH_CAUSE"]
        .astype("string")
        .str.strip()
        .str.split(r"\s{2,}", n=1, expand=True)
)
deathcauses_df.drop(columns=["DEATH_CAUSE"], inplace=True)



logger.info("Saving transformed datasets to data/staging/")
df_total.to_csv("data/staging/pobmun_combined_transformed.csv", index=False)
economic_df.to_csv("data/staging/economic_sector_province_transformed.csv", index=False)
deathcauses_df.to_csv("data/staging/death_causes_province_transformed.csv", index=False)
codauto.to_csv("data/staging/codauto_cpro_transformed.csv", index=False)



logger.info(f"Transformation process completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
