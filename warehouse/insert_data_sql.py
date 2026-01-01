
"""
Load DW (Azure SQL) from provided CSVs into schema dw.

Assumptions:
- Schema dw and tables already exist (per your DDL).
- CSVs are the ones uploaded with these columns:

Population
- population_municipalities.csv: municipality_number, municipality_name, year, sex, population
- population_provinces.csv:      province_number,      province_name,      year, sex, population
- population_spain.csv:          municipality_number(=0), municipality_name(=España), year, sex, population

Households
- households_municipalities.csv: municipality_number, municipality_name, year,
                                number_of_households, average_household_size,
                                number_of_dwellings, average_rent_price
- households_provinces.csv:      province_number, province_name, year,
                                number_of_households, average_household_size,
                                number_of_dwellings, average_rent_price
- households_spain.csv:          municipality_number(=0), municipality_name(=España), year,
                                number_of_households, average_household_size,
                                number_of_dwellings, average_rent_price

Notes:
- For population we load ONLY sex == 'Ambos sexos' into the fact table.
- geo_code is stored as a string. We keep Spain as geo_code='0' to match the CSVs.
- province_code for municipalities is derived as (municipality_number // 1000) * 1000 (e.g., 44001 -> 44000).

Security:
- Avoid hardcoding passwords in code. Prefer environment variables.
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pyodbc


# ----------------------------
# 1) Connection configuration
# ----------------------------

SERVER   = os.getenv("AZURE_SQL_SERVER",   "srv-dw-liliarte-01.database.windows.net")
DATABASE = os.getenv("AZURE_SQL_DATABASE", "dw_final_project")
USERNAME = os.getenv("AZURE_SQL_USERNAME", "dwadmin")
PASSWORD = os.getenv("AZURE_SQL_PASSWORD", "usj2526.")  # Prefer env var AZURE_SQL_PASSWORD

CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)

# ----------------------------
# 2) CSV paths (update if needed)
# ----------------------------

CSV_FILES = {
    "population_municipalities": "data/staging/population_municipalities.csv",
    "population_provinces":      "data/staging/population_provinces.csv",
    "population_spain":          "data/staging/population_spain.csv",
    "households_municipalities": "data/staging/households_municipalities.csv",
    "households_provinces":      "data/staging/households_provinces.csv",
    "households_spain":          "data/staging/households_spain.csv",
}

# dataset_code values to store in dim_source_dataset and fact_indicator
DATASET_META = {
    "population_municipalities": {
        "dataset_code": "population_municipalities_csv",
        "dataset_name": "Population by municipalities",
        "source_org": "INE",
        "source_url": None,
        "version_tag": None,
    },
    "population_provinces": {
        "dataset_code": "population_provinces_csv",
        "dataset_name": "Population by provinces",
        "source_org": "INE",
        "source_url": None,
        "version_tag": None,
    },
    "population_spain": {
        "dataset_code": "population_spain_csv",
        "dataset_name": "Population Spain total",
        "source_org": "INE",
        "source_url": None,
        "version_tag": None,
    },
    "households_municipalities": {
        "dataset_code": "households_municipalities_csv",
        "dataset_name": "Households & housing by municipalities",
        "source_org": "INE",
        "source_url": None,
        "version_tag": None,
    },
    "households_provinces": {
        "dataset_code": "households_provinces_csv",
        "dataset_name": "Households & housing by provinces",
        "source_org": "INE",
        "source_url": None,
        "version_tag": None,
    },
    "households_spain": {
        "dataset_code": "households_spain_csv",
        "dataset_name": "Households & housing Spain total",
        "source_org": "INE",
        "source_url": None,
        "version_tag": None,
    },
}

INDICATORS = [
    {
        "indicator_code": "population",
        "indicator_name": "Population",
        "unit": "persons",
        "topic": "demography",
    },
    {
        "indicator_code": "number_of_households",
        "indicator_name": "Number of households",
        "unit": "households",
        "topic": "households",
    },
    {
        "indicator_code": "average_household_size",
        "indicator_name": "Average household size",
        "unit": "persons_per_household",
        "topic": "households",
    },
    {
        "indicator_code": "number_of_dwellings",
        "indicator_name": "Number of dwellings",
        "unit": "dwellings",
        "topic": "housing",
    },
    {
        "indicator_code": "average_rent_price",
        "indicator_name": "Average rent price",
        "unit": "EUR",
        "topic": "housing",
    },
]


# ----------------------------
# 3) Helpers
# ----------------------------

def as_int_series(s: pd.Series) -> pd.Series:
    """Convert potentially mixed numeric column to Int64 (nullable) safely."""
    return pd.to_numeric(s, errors="coerce").astype("Int64")

def code_to_str(series_int: pd.Series) -> pd.Series:
    """Convert Int64 codes to strings without .0, keeping nulls as <NA>."""
    return series_int.astype("Int64").astype(str)

def connect() -> pyodbc.Connection:
    conn = pyodbc.connect(CONN_STR, timeout=30)
    conn.autocommit = False
    return conn

def chunks(lst: List[Tuple], n: int) -> Iterable[List[Tuple]]:
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


# ----------------------------
# 4) Extract + Transform
# ----------------------------

def load_csvs() -> Dict[str, pd.DataFrame]:
    dfs: Dict[str, pd.DataFrame] = {}
    for k, path in CSV_FILES.items():
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing CSV: {path}")
        dfs[k] = pd.read_csv(path)
    return dfs

def build_dim_time(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    years = set()
    for df in dfs.values():
        if "year" in df.columns:
            years |= set(pd.to_numeric(df["year"], errors="coerce").dropna().astype(int).tolist())
    return pd.DataFrame({"year": sorted(years)})

def build_dim_source_dataset() -> pd.DataFrame:
    return pd.DataFrame([v for v in DATASET_META.values()])

def build_dim_indicator() -> pd.DataFrame:
    return pd.DataFrame(INDICATORS)

def build_dim_geo(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []

    # Country (Spain) from any dataset that has municipality_number==0 or name España
    # We'll store geo_code='0' and geo_level='country'
    rows.append({"geo_code": "0", "geo_name": "España", "geo_level": "country", "province_code": None})

    # Provinces
    for key in ["population_provinces", "households_provinces"]:
        df = dfs.get(key)
        if df is None:
            continue
        prov_num = as_int_series(df["province_number"])
        prov_name = df["province_name"].astype(str)
        tmp = pd.DataFrame({"geo_code": code_to_str(prov_num), "geo_name": prov_name})
        tmp["geo_level"] = "province"
        tmp["province_code"] = None
        rows.append(tmp.drop_duplicates())

    # Municipalities
    for key in ["population_municipalities", "households_municipalities"]:
        df = dfs.get(key)
        if df is None:
            continue
        mun_num = as_int_series(df["municipality_number"])
        mun_name = df["municipality_name"].astype(str)
        prov_code = ((mun_num // 1000) * 1000).astype("Int64")
        tmp = pd.DataFrame(
            {
                "geo_code": code_to_str(mun_num),
                "geo_name": mun_name,
                "geo_level": "municipality",
                "province_code": code_to_str(prov_code),
            }
        )
        rows.append(tmp.drop_duplicates())

    dim_geo = pd.concat([r if isinstance(r, pd.DataFrame) else pd.DataFrame([r]) for r in rows], ignore_index=True)
    dim_geo = dim_geo.drop_duplicates(subset=["geo_code"]).reset_index(drop=True)
    return dim_geo

def build_fact_indicator(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    facts = []

    # Population datasets (filter Ambos sexos)
    for key, geo_cols in [
        ("population_municipalities", ("municipality_number", "municipality_name", "municipality")),
        ("population_provinces", ("province_number", "province_name", "province")),
        ("population_spain", ("municipality_number", "municipality_name", "country")),
    ]:
        df = dfs[key].copy()
        df = df[df["sex"].astype(str).str.strip() == "Ambos sexos"].copy()
        code_col, name_col, _level = geo_cols
        geo_code = code_to_str(as_int_series(df[code_col]))
        year = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        value = pd.to_numeric(df["population"], errors="coerce")

        dataset_code = DATASET_META[key]["dataset_code"]
        tmp = pd.DataFrame(
            {
                "year": year,
                "geo_code": geo_code,
                "indicator_code": "population",
                "dataset_code": dataset_code,
                "value": value,
            }
        )
        tmp = tmp.dropna(subset=["year", "geo_code", "value"])
        facts.append(tmp)

    # Households datasets (4 indicators)
    household_ind_cols = [
        "number_of_households",
        "average_household_size",
        "number_of_dwellings",
        "average_rent_price",
    ]
    for key, code_col in [
        ("households_municipalities", "municipality_number"),
        ("households_provinces", "province_number"),
        ("households_spain", "municipality_number"),
    ]:
        df = dfs[key].copy()
        geo_code = code_to_str(as_int_series(df[code_col]))
        year = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        dataset_code = DATASET_META[key]["dataset_code"]

        for ind in household_ind_cols:
            value = pd.to_numeric(df[ind], errors="coerce")
            tmp = pd.DataFrame(
                {
                    "year": year,
                    "geo_code": geo_code,
                    "indicator_code": ind,
                    "dataset_code": dataset_code,
                    "value": value,
                }
            )
            tmp = tmp.dropna(subset=["year", "geo_code", "value"])
            facts.append(tmp)

    fact = pd.concat(facts, ignore_index=True)

    # Ensure types
    fact["year"] = fact["year"].astype(int)
    fact["geo_code"] = fact["geo_code"].astype(str)
    fact["indicator_code"] = fact["indicator_code"].astype(str)
    fact["dataset_code"] = fact["dataset_code"].astype(str)

    # Value -> Decimal compatible (keep float, let SQL cast); round to 4 decimals to match schema
    fact["value"] = pd.to_numeric(fact["value"], errors="coerce").round(4)

    # drop duplicates just in case
    fact = fact.drop_duplicates(subset=["year", "geo_code", "indicator_code", "dataset_code"])
    return fact


# ----------------------------
# 5) Load (set-based MERGE via temp tables)
# ----------------------------

def merge_dim_time(cursor: pyodbc.Cursor, dim_time: pd.DataFrame) -> None:
    cursor.execute("IF OBJECT_ID('tempdb..#stg_dim_time') IS NOT NULL DROP TABLE #stg_dim_time;")
    cursor.execute("CREATE TABLE #stg_dim_time ([year] INT NOT NULL);")

    rows = [(int(y),) for y in dim_time["year"].tolist()]
    cursor.fast_executemany = True
    cursor.executemany("INSERT INTO #stg_dim_time([year]) VALUES (?);", rows)

    cursor.execute("""
        MERGE dw.dim_time AS tgt
        USING (SELECT DISTINCT [year] FROM #stg_dim_time) AS src
        ON tgt.[year] = src.[year]
        WHEN NOT MATCHED THEN
            INSERT ([year]) VALUES (src.[year]);
    """)

def merge_dim_indicator(cursor: pyodbc.Cursor, dim_indicator: pd.DataFrame) -> None:
    cursor.execute("IF OBJECT_ID('tempdb..#stg_dim_indicator') IS NOT NULL DROP TABLE #stg_dim_indicator;")
    cursor.execute("""
        CREATE TABLE #stg_dim_indicator (
            indicator_code VARCHAR(80) NOT NULL,
            indicator_name VARCHAR(200) NOT NULL,
            unit VARCHAR(50) NULL,
            topic VARCHAR(120) NULL
        );
    """)
    rows = [
        (r["indicator_code"], r["indicator_name"], r.get("unit"), r.get("topic"))
        for _, r in dim_indicator.iterrows()
    ]
    cursor.fast_executemany = True
    cursor.executemany(
        "INSERT INTO #stg_dim_indicator(indicator_code, indicator_name, unit, topic) VALUES (?,?,?,?);",
        rows,
    )

    cursor.execute("""
        MERGE dw.dim_indicator AS tgt
        USING (SELECT DISTINCT indicator_code, indicator_name, unit, topic FROM #stg_dim_indicator) AS src
        ON tgt.indicator_code = src.indicator_code
        WHEN NOT MATCHED THEN
            INSERT (indicator_code, indicator_name, unit, topic)
            VALUES (src.indicator_code, src.indicator_name, src.unit, src.topic)
        WHEN MATCHED AND (
            tgt.indicator_name <> src.indicator_name
            OR ISNULL(tgt.unit,'') <> ISNULL(src.unit,'')
            OR ISNULL(tgt.topic,'') <> ISNULL(src.topic,'')
        ) THEN
            UPDATE SET indicator_name = src.indicator_name,
                       unit = src.unit,
                       topic = src.topic;
    """)

def merge_dim_source_dataset(cursor: pyodbc.Cursor, dim_ds: pd.DataFrame) -> None:
    cursor.execute("IF OBJECT_ID('tempdb..#stg_dim_source_dataset') IS NOT NULL DROP TABLE #stg_dim_source_dataset;")
    cursor.execute("""
        CREATE TABLE #stg_dim_source_dataset (
            dataset_code VARCHAR(80) NOT NULL,
            dataset_name VARCHAR(200) NOT NULL,
            source_org VARCHAR(200) NULL,
            source_url VARCHAR(400) NULL,
            version_tag VARCHAR(80) NULL
        );
    """)
    rows = [
        (r["dataset_code"], r["dataset_name"], r.get("source_org"), r.get("source_url"), r.get("version_tag"))
        for _, r in dim_ds.iterrows()
    ]
    cursor.fast_executemany = True
    cursor.executemany(
        "INSERT INTO #stg_dim_source_dataset(dataset_code, dataset_name, source_org, source_url, version_tag) VALUES (?,?,?,?,?);",
        rows,
    )

    cursor.execute("""
        MERGE dw.dim_source_dataset AS tgt
        USING (SELECT DISTINCT dataset_code, dataset_name, source_org, source_url, version_tag FROM #stg_dim_source_dataset) AS src
        ON tgt.dataset_code = src.dataset_code
        WHEN NOT MATCHED THEN
            INSERT (dataset_code, dataset_name, source_org, source_url, version_tag)
            VALUES (src.dataset_code, src.dataset_name, src.source_org, src.source_url, src.version_tag)
        WHEN MATCHED AND (
            tgt.dataset_name <> src.dataset_name
            OR ISNULL(tgt.source_org,'') <> ISNULL(src.source_org,'')
            OR ISNULL(tgt.source_url,'') <> ISNULL(src.source_url,'')
            OR ISNULL(tgt.version_tag,'') <> ISNULL(src.version_tag,'')
        ) THEN
            UPDATE SET dataset_name = src.dataset_name,
                       source_org   = src.source_org,
                       source_url   = src.source_url,
                       version_tag  = src.version_tag;
    """)

def merge_dim_geo(cursor: pyodbc.Cursor, dim_geo: pd.DataFrame) -> None:
    cursor.execute("IF OBJECT_ID('tempdb..#stg_dim_geo') IS NOT NULL DROP TABLE #stg_dim_geo;")
    cursor.execute("""
        CREATE TABLE #stg_dim_geo (
            geo_code VARCHAR(20) NOT NULL,
            geo_name VARCHAR(200) NOT NULL,
            geo_level VARCHAR(12) NOT NULL,
            province_code VARCHAR(20) NULL
        );
    """)

    # Normalize null province_code
    dim_geo = dim_geo.copy()
    dim_geo["province_code"] = dim_geo["province_code"].replace({"<NA>": None, "nan": None})
    rows = [(r["geo_code"], r["geo_name"], r["geo_level"], r.get("province_code")) for _, r in dim_geo.iterrows()]

    cursor.fast_executemany = True
    cursor.executemany(
        "INSERT INTO #stg_dim_geo(geo_code, geo_name, geo_level, province_code) VALUES (?,?,?,?);",
        rows,
    )

    cursor.execute("""
        MERGE dw.dim_geo AS tgt
        USING (SELECT DISTINCT geo_code, geo_name, geo_level, province_code FROM #stg_dim_geo) AS src
        ON tgt.geo_code = src.geo_code
        WHEN NOT MATCHED THEN
            INSERT (geo_code, geo_name, geo_level, province_code)
            VALUES (src.geo_code, src.geo_name, src.geo_level, src.province_code)
        WHEN MATCHED AND (
            tgt.geo_name <> src.geo_name
            OR tgt.geo_level <> src.geo_level
            OR ISNULL(tgt.province_code,'') <> ISNULL(src.province_code,'')
        ) THEN
            UPDATE SET geo_name = src.geo_name,
                       geo_level = src.geo_level,
                       province_code = src.province_code;
    """)

def merge_fact_indicator(cursor: pyodbc.Cursor, fact: pd.DataFrame, batch_rows: int = 50000) -> None:
    cursor.execute("IF OBJECT_ID('tempdb..#stg_fact_indicator') IS NOT NULL DROP TABLE #stg_fact_indicator;")
    cursor.execute("""
        CREATE TABLE #stg_fact_indicator (
            [year] INT NOT NULL,
            geo_code VARCHAR(20) NOT NULL,
            indicator_code VARCHAR(80) NOT NULL,
            dataset_code VARCHAR(80) NOT NULL,
            value DECIMAL(18,4) NOT NULL
        );
    """)

    # Insert stage in batches
    cursor.fast_executemany = True

    tuples: List[Tuple] = []
    for _, r in fact.iterrows():
        tuples.append((int(r["year"]), str(r["geo_code"]), str(r["indicator_code"]), str(r["dataset_code"]), float(r["value"])))

    insert_sql = "INSERT INTO #stg_fact_indicator([year], geo_code, indicator_code, dataset_code, value) VALUES (?,?,?,?,?);"
    for batch in chunks(tuples, batch_rows):
        cursor.executemany(insert_sql, batch)

    # MERGE into target
    cursor.execute("""
        MERGE dw.fact_indicator AS tgt
        USING (SELECT DISTINCT [year], geo_code, indicator_code, dataset_code, value FROM #stg_fact_indicator) AS src
        ON  tgt.[year] = src.[year]
        AND tgt.geo_code = src.geo_code
        AND tgt.indicator_code = src.indicator_code
        AND tgt.dataset_code = src.dataset_code
        WHEN NOT MATCHED THEN
            INSERT ([year], geo_code, indicator_code, dataset_code, value)
            VALUES (src.[year], src.geo_code, src.indicator_code, src.dataset_code, src.value)
        WHEN MATCHED AND tgt.value <> src.value THEN
            UPDATE SET value = src.value;
    """)


# ----------------------------
# 6) Main
# ----------------------------

def main() -> None:
    print("📥 Reading CSVs...")
    dfs = load_csvs()

    print("🧱 Building dimensions/facts...")
    dim_time = build_dim_time(dfs)
    dim_indicator = build_dim_indicator()
    dim_dataset = build_dim_source_dataset()
    dim_geo = build_dim_geo(dfs)
    fact = build_fact_indicator(dfs)

    print(f"   dim_time: {len(dim_time):,} rows")
    print(f"   dim_indicator: {len(dim_indicator):,} rows")
    print(f"   dim_source_dataset: {len(dim_dataset):,} rows")
    print(f"   dim_geo: {len(dim_geo):,} rows")
    print(f"   fact_indicator: {len(fact):,} rows")

    print("🔌 Connecting to Azure SQL...")
    conn = connect()
    try:
        cur = conn.cursor()

        print("➡️  Loading dimensions (MERGE)...")
        merge_dim_time(cur, dim_time)
        merge_dim_indicator(cur, dim_indicator)
        merge_dim_source_dataset(cur, dim_dataset)
        merge_dim_geo(cur, dim_geo)

        print("➡️  Loading facts (MERGE)...")
        merge_fact_indicator(cur, fact)

        conn.commit()
        print("✅ Load completed successfully.")
    except Exception as e:
        conn.rollback()
        print("❌ Load failed. Rolled back.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
