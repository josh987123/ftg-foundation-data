import os
import pyodbc

print("Starting Foundation connection test...")

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=sql.foundationsoft.com,9000;"
    "DATABASE=foundation;"
    f"UID={os.environ['FOUNDATION_SQL_USER']};"
    f"PWD={os.environ['FOUNDATION_SQL_PASSWORD']};",
    timeout=5
)

cursor = conn.cursor()
cursor.execute("SELECT 1")

print("FOUNDATION CONNECTION SUCCESSFUL")
