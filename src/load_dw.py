from __future__ import annotations
import os
import sys
import time
from pathlib import Path
import pandas as pd
import pyodbc
import logging

# logging 
Path("./logs").mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename="./logs/load_dw.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# csv
PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data" / "staging"
CSV_CODAUTO = DATA_DIR / "codauto_cpro_transformed.csv"
CSV_DEATH  = DATA_DIR / "death_causes_province_transformed.csv"
CSV_SECTOR = DATA_DIR / "economic_sector_province_transformed.csv"
CSV_POB    = DATA_DIR / "pobmun_combined_transformed.csv"
CLEAR_BEFORE_LOAD = True  # True = IMPORTANT, CLEAR AND RELOADS BEFORE ADDING NEW DATA


# CONFIG: connection (Azure SQL)
SERVER   = os.getenv("AZURE_SQL_SERVER",   "srv-dw-liliarte-01.database.windows.net")
DATABASE = os.getenv("AZURE_SQL_DATABASE", "dw_final_project")
USERNAME = os.getenv("AZURE_SQL_USERNAME", "dwadmin")
PASSWORD = os.getenv("AZURE_SQL_PASSWORD", "usj2526.")
DRIVER = ("ODBC Driver 18 for SQL Server")

CONNECTION = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)



# HELPERS

def require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"No encuentro el archivo: {path}")


def clean_str(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip().fillna("")


def to_int_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int64")


def chunked(seq, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]


def exec_many(cursor, sql: str, rows: list[tuple], batch_size: int = 5000) -> None:
    if not rows:
        return
    cursor.fast_executemany = True
    for batch in chunked(rows, batch_size):
        cursor.executemany(sql, batch)


def clear_tables(cursor) -> None:
    # Facts
    cursor.execute("DELETE FROM dw.fact_population_municipality;")
    cursor.execute("DELETE FROM dw.fact_economic_sector;")
    cursor.execute("DELETE FROM dw.fact_deaths;")

    # Dims
    cursor.execute("DELETE FROM dw.dim_municipality;")
    cursor.execute("DELETE FROM dw.dim_economic_sector;")
    cursor.execute("DELETE FROM dw.dim_death_cause;")
    cursor.execute("DELETE FROM dw.dim_sex;")
    cursor.execute("DELETE FROM dw.dim_time;")
    cursor.execute("DELETE FROM dw.dim_province;")
    cursor.execute("DELETE FROM dw.dim_autonomy;")


# MAIN
def main() -> int:
    start_ts = time.time()
    logger.info("==== load_dw START ====")

    # validate CSV files exist
    for f in (CSV_CODAUTO, CSV_DEATH, CSV_SECTOR, CSV_POB):
        logger.info(f"Checking input file exists: {f}")
        require_file(f)

    # read CSVs
    logger.info("Reading CSVs from staging...")
    df_cod = pd.read_csv(CSV_CODAUTO)
    df_dea = pd.read_csv(CSV_DEATH)
    df_sec = pd.read_csv(CSV_SECTOR)
    df_pob = pd.read_csv(CSV_POB)
    logger.info(
        f"Rows read -> codauto:{len(df_cod)} deaths:{len(df_dea)} sector:{len(df_sec)} pob:{len(df_pob)}"
    )

    # normalice data types and clean strings
    logger.info("Normalizing dtypes and cleaning strings...")
    df_cod["CODAUTO"] = to_int_series(df_cod["CODAUTO"])
    df_cod["CPRO"] = to_int_series(df_cod["CPRO"])
    df_cod["CODAUTO_NAME"] = clean_str(df_cod["CODAUTO_NAME"])
    df_cod["CPRO_NAME"] = clean_str(df_cod["CPRO_NAME"])

    df_dea["CPRO"] = to_int_series(df_dea["CPRO"])
    df_dea["YEAR"] = to_int_series(df_dea["YEAR"])
    df_dea["TOTAL"] = to_int_series(df_dea["TOTAL"])
    df_dea["SEX"] = clean_str(df_dea["SEX"])
    df_dea["DEATH_CAUSE_CODE"] = clean_str(df_dea["DEATH_CAUSE_CODE"])
    df_dea["DEATH_CAUSE_NAME"] = clean_str(df_dea["DEATH_CAUSE_NAME"])

    df_sec["CPRO"] = to_int_series(df_sec["CPRO"])
    df_sec["YEAR"] = to_int_series(df_sec["YEAR"])
    df_sec["TOTAL"] = pd.to_numeric(df_sec["TOTAL"], errors="coerce")
    df_sec["ECONOMIC_SECTOR"] = clean_str(df_sec["ECONOMIC_SECTOR"])

    df_pob["CPRO"] = to_int_series(df_pob["CPRO"])
    df_pob["MUN_NUMBER"] = to_int_series(df_pob["MUN_NUMBER"])
    df_pob["YEAR"] = to_int_series(df_pob["YEAR"])
    df_pob["POBLATION"] = to_int_series(df_pob["POBLATION"])
    df_pob["MALE"] = to_int_series(df_pob["MALE"])
    df_pob["FEMALE"] = to_int_series(df_pob["FEMALE"])
    df_pob["MUN_NAME"] = clean_str(df_pob["MUN_NAME"])

    # dims
    logger.info("Building dimensions...")
    dim_autonomy = (
        df_cod[["CODAUTO", "CODAUTO_NAME"]]
        .dropna(subset=["CODAUTO"])
        .drop_duplicates()
        .sort_values("CODAUTO")
    )

    dim_province = (
        df_cod[["CPRO", "CODAUTO", "CPRO_NAME"]]
        .dropna(subset=["CPRO", "CODAUTO"])
        .drop_duplicates(subset=["CPRO"])
        .sort_values("CPRO")
    )

    years = pd.concat([df_dea[["YEAR"]], df_sec[["YEAR"]], df_pob[["YEAR"]]], ignore_index=True)
    dim_time = (
        years.dropna(subset=["YEAR"])
        .drop_duplicates()
        .sort_values("YEAR")
    )

    dim_sex = df_dea[["SEX"]].drop_duplicates().sort_values("SEX")

    dim_death_cause = (
        df_dea[["DEATH_CAUSE_CODE", "DEATH_CAUSE_NAME"]]
        .drop_duplicates(subset=["DEATH_CAUSE_CODE"])
        .sort_values("DEATH_CAUSE_CODE")
    )

    dim_economic_sector = df_sec[["ECONOMIC_SECTOR"]].drop_duplicates().sort_values("ECONOMIC_SECTOR")

    dim_municipality = (
        df_pob[["CPRO", "MUN_NUMBER", "MUN_NAME"]]
        .dropna(subset=["CPRO", "MUN_NUMBER"])
        .drop_duplicates(subset=["CPRO", "MUN_NUMBER"])
        .sort_values(["CPRO", "MUN_NUMBER"])
    )

    logger.info(
        "Dim sizes -> autonomy:%d province:%d time:%d sex:%d death_cause:%d econ_sector:%d municipality:%d",
        len(dim_autonomy), len(dim_province), len(dim_time), len(dim_sex),
        len(dim_death_cause), len(dim_economic_sector), len(dim_municipality)
    )

    # facts
    logger.info("Building facts...")
    fact_deaths = (
        df_dea[["CPRO", "YEAR", "SEX", "DEATH_CAUSE_CODE", "TOTAL"]]
        .dropna(subset=["CPRO", "YEAR", "SEX", "DEATH_CAUSE_CODE", "TOTAL"])
        .rename(columns={"TOTAL": "TOTAL_DEATHS"})
    )

    fact_economic_sector = (
        df_sec[["CPRO", "YEAR", "ECONOMIC_SECTOR", "TOTAL"]]
        .dropna(subset=["CPRO", "YEAR", "ECONOMIC_SECTOR", "TOTAL"])
        .rename(columns={"TOTAL": "TOTAL_VALUE"})
    )

    fact_population = (
        df_pob[["CPRO", "MUN_NUMBER", "YEAR", "POBLATION", "MALE", "FEMALE"]]
        .dropna(subset=["CPRO", "MUN_NUMBER", "YEAR", "POBLATION", "MALE", "FEMALE"])
        .rename(columns={
            "POBLATION": "POPULATION_TOTAL",
            "MALE": "MALE_TOTAL",
            "FEMALE": "FEMALE_TOTAL",
        })
    )

    # duplicates check
    dup_count = fact_population.duplicated(subset=["CPRO", "MUN_NUMBER", "YEAR"]).sum()
    if dup_count > 0:
        logger.warning(
            "Population: %d duplicates detected by (CPRO, MUN_NUMBER, YEAR). Aggregating using MAX.",
            int(dup_count)
        )

    fact_population = (
        fact_population
        .groupby(["CPRO", "MUN_NUMBER", "YEAR"], as_index=False)
        .agg({
            "POPULATION_TOTAL": "max",
            "MALE_TOTAL": "max",
            "FEMALE_TOTAL": "max",
        })
    )

    logger.info(
        "Fact sizes -> deaths:%d sector:%d population:%d",
        len(fact_deaths), len(fact_economic_sector), len(fact_population)
    )

    # charge data warehouse
    logger.info(f"Connecting to SQL Server with driver: {DRIVER}")
    cn = pyodbc.connect(CONNECTION)
    cn.autocommit = False

    try:
        cur = cn.cursor()

        if CLEAR_BEFORE_LOAD:
            logger.info("Clearing tables (facts -> dims)...")
            clear_tables(cur)
            cn.commit()
            logger.info("Tables cleared and committed")

        # insert dims
        logger.info("Inserting dimensions...")

        exec_many(
            cur,
            "INSERT INTO dw.dim_autonomy (CODAUTO, CODAUTO_NAME) VALUES (?, ?);",
            [(int(r.CODAUTO), str(r.CODAUTO_NAME)) for r in dim_autonomy.itertuples(index=False)]
        )
        logger.info("Inserted dim_autonomy: %d rows", len(dim_autonomy))

        exec_many(
            cur,
            "INSERT INTO dw.dim_province (CPRO, CODAUTO, CPRO_NAME) VALUES (?, ?, ?);",
            [(int(r.CPRO), int(r.CODAUTO), str(r.CPRO_NAME)) for r in dim_province.itertuples(index=False)]
        )
        logger.info("Inserted dim_province: %d rows", len(dim_province))

        exec_many(
            cur,
            "INSERT INTO dw.dim_time ([YEAR]) VALUES (?);",
            [(int(r.YEAR),) for r in dim_time.itertuples(index=False)]
        )
        logger.info("Inserted dim_time: %d rows", len(dim_time))

        exec_many(
            cur,
            "INSERT INTO dw.dim_sex (SEX) VALUES (?);",
            [(str(r.SEX),) for r in dim_sex.itertuples(index=False)]
        )
        logger.info("Inserted dim_sex: %d rows", len(dim_sex))

        exec_many(
            cur,
            "INSERT INTO dw.dim_death_cause (DEATH_CAUSE_CODE, DEATH_CAUSE_NAME) VALUES (?, ?);",
            [(str(r.DEATH_CAUSE_CODE), str(r.DEATH_CAUSE_NAME)) for r in dim_death_cause.itertuples(index=False)]
        )
        logger.info("Inserted dim_death_cause: %d rows", len(dim_death_cause))

        exec_many(
            cur,
            "INSERT INTO dw.dim_economic_sector (ECONOMIC_SECTOR) VALUES (?);",
            [(str(r.ECONOMIC_SECTOR),) for r in dim_economic_sector.itertuples(index=False)]
        )
        logger.info("Inserted dim_economic_sector: %d rows", len(dim_economic_sector))

        exec_many(
            cur,
            "INSERT INTO dw.dim_municipality (CPRO, MUN_NUMBER, MUN_NAME) VALUES (?, ?, ?);",
            [(int(r.CPRO), int(r.MUN_NUMBER), str(r.MUN_NAME)) for r in dim_municipality.itertuples(index=False)]
        )
        logger.info("Inserted dim_municipality: %d rows", len(dim_municipality))

        cn.commit()
        logger.info("Dimensions committed successfully")

        # insert facts
        logger.info("Inserting facts...")

        exec_many(
            cur,
            "INSERT INTO dw.fact_deaths (CPRO, [YEAR], SEX, DEATH_CAUSE_CODE, TOTAL_DEATHS) VALUES (?, ?, ?, ?, ?);",
            [(int(r.CPRO), int(r.YEAR), str(r.SEX), str(r.DEATH_CAUSE_CODE), int(r.TOTAL_DEATHS))
             for r in fact_deaths.itertuples(index=False)]
        )
        logger.info("Inserted fact_deaths: %d rows", len(fact_deaths))

        exec_many(
            cur,
            "INSERT INTO dw.fact_economic_sector (CPRO, [YEAR], ECONOMIC_SECTOR, TOTAL_VALUE) VALUES (?, ?, ?, ?);",
            [(int(r.CPRO), int(r.YEAR), str(r.ECONOMIC_SECTOR), float(r.TOTAL_VALUE))
             for r in fact_economic_sector.itertuples(index=False)]
        )
        logger.info("Inserted fact_economic_sector: %d rows", len(fact_economic_sector))

        exec_many(
            cur,
            "INSERT INTO dw.fact_population_municipality "
            "(CPRO, MUN_NUMBER, [YEAR], POPULATION_TOTAL, MALE_TOTAL, FEMALE_TOTAL) "
            "VALUES (?, ?, ?, ?, ?, ?);",
            [(int(r.CPRO), int(r.MUN_NUMBER), int(r.YEAR), int(r.POPULATION_TOTAL), int(r.MALE_TOTAL), int(r.FEMALE_TOTAL))
             for r in fact_population.itertuples(index=False)]
        )
        logger.info("Inserted fact_population_municipality: %d rows", len(fact_population))

        cn.commit()
        logger.info("Facts committed successfully")
        logger.info("==== load_dw SUCCESS in %.2fs ====", time.time() - start_ts)

    except Exception:
        cn.rollback()
        logger.exception("ERROR during load_dw: rollback applied")
        raise
    finally:
        cn.close()
        logger.info("SQL connection closed")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
