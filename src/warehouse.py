import pyodbc

server = "srv-dw-liliarte-01.database.windows.net"
database = "dw_final_project"
username = "dwadmin"
password = "usj2526."

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)

try:
    conn = pyodbc.connect(conn_str, timeout=10)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    print("✅ Conexión OK con Azure SQL")
    conn.close()
except Exception as e:
    print("❌ Error de conexión")
    print(e)
