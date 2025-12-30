import os
import pyodbc
import pandas as pd

SERVER = "sql.foundationsoft.com,9000"
DATABASE = "Cas_5587"  # MUST match the database name that worked in your test
SCHEMA = "dbo"
TABLE = "accounts"
OUTFILE = "data/accounts.csv"

REMOVE_COLS = {
    "apply_subdivision",
    "inc_exp_type",
    "overhead_percent",
    "overhead_formula_percent",
    "jc_income_expense",
    "force_job_costing",
    "company_no",
}

def connect():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={os.environ['FOUNDATION_SQL_USER']};"
        f"PWD={os.environ['FOUNDATION_SQL_PASSWORD']};",
        timeout=20
    )

def get_first_n_columns(conn, n=10):
    # Pull column names in ordinal order (matches Power Query "first 10 columns")
    sql = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
    """
    cur = conn.cursor()
    rows = cur.execute(sql, (SCHEMA, TABLE)).fetchall()
    cols = [r[0] for r in rows]
    return cols[:n]

def quote_ident(name: str) -> str:
    # Safe bracket quoting for SQL Server identifiers
    return "[" + name.replace("]", "]]") + "]"

def main():
    print("Exporting Accounts → accounts.csv ...")

    conn = connect()

    # Replicate Power Query: take first 10 columns, then remove unwanted fields
    first_10 = get_first_n_columns(conn, n=10)
    selected_cols = [c for c in first_10 if c not in REMOVE_COLS]

    # Ensure account_no is present so we can build Account_Key
    if "account_no" not in selected_cols:
        selected_cols.append("account_no")

    col_sql = ", ".join(quote_ident(c) for c in selected_cols)
    sql = f"SELECT {col_sql} FROM {quote_ident(SCHEMA)}.{quote_ident(TABLE)}"

    df = pd.read_sql(sql, conn)

    if "account_no" not in df.columns:
        raise RuntimeError("account_no column not found; cannot compute Account_Key")

    # ------------------------------------------------------------
    # Normalize account_no to MATCH Power Query behavior exactly
    # - Convert to text
    # - Strip trailing '.0'
    # ------------------------------------------------------------
    df["account_no"] = (
        df["account_no"]
          .astype(str)
          .str.replace(r"\.0$", "", regex=True)
          .str.strip()
    )

    # Build Account_Key (PadStart to 4)
    df["Account_Key"] = df["account_no"].str.zfill(4)

    df.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(df)} rows, {len(df.columns)} columns)")

if __name__ == "__main__":
    main()
