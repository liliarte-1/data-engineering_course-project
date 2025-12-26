"""
DATA CLEANING EXERCISE
=====================
Retrieve, explore, and clean an e-commerce customer orders dataset
"""
from datetime import datetime
import requests
import pandas as pd
import io

print("=" * 70)
print("DATA CLEANING EXERCISE - E-COMMERCE CUSTOMER ORDERS")
print("=" * 70)
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ============================================================================
# STEP 1: RETRIEVE DATA FROM WEB SOURCE
# ============================================================================
print("STEP 1: RETRIEVE DATA FROM WEB SOURCE")
urls = ["https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/0201010101.csv", 
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/households_dataset.csv",
        "https://raw.githubusercontent.com/liliarte-1/data-engineering_course-project/refs/heads/main/data_retrieval_simulation/socioeconomic_dataset.csv"]

response = requests.get(url)
print(response.text)


# df = pd.read_csv("exercise.csv")
# # df = pd.read_csv(response.text)
# print(f"Rows {len(df)}, Columns{len(df.columns)}")

try:
    print(f"Fetching data from: {url}")
    response = requests.get(url, timeout=10)
 
    print("Response:", response.text)
   
    print("✓ Data fetched from web source, loading into DataFrame...")
    # print("Response:", response.text)  
    
    # #de esta forma no lee las lineas que estan mal formateadas, podemos llevar un registrs y hacer un post-proceso
    # df = pd.read_csv(io.StringIO(response.text),sep=',',on_bad_lines='skip')
    df = pd.read_csv(io.StringIO(response.text),sep=',',on_bad_lines='warn')
    print(f"✓ Data retrieved successfully!")
    print(f"✓ Status Code: {response.status_code}")
    print(f"✓ Rows: {len(df)}, Columns: {len(df.columns)}\n")
    print(df.head())
   
except Exception as e:
    print(f"✗ Error: {e}")
    raise e