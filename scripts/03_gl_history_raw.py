import os
import pyodbc
import pandas as pd
from datetime import date

SERVER = "sql.foundationsoft.com,9000"
DATABASE = "Cas_5587"
OUTFILE = "data/gl_history_raw.csv"

QUERY_TIMEOUT_SECONDS = 900  # 15 minutes per month

# Optional hard performance lever
# Example: set GL_START_DATE=2022-01-01 in GitHub secrets
GL_START_DATE = os.getenv("GL_START_DATE")

def connect():
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={os.environ['FOUNDATION_SQL_USER']};"
        f"PWD={os.environ['FOUNDATION_SQL_PASSWORD']};"
        "TrustServerCertificate=yes;",
        timeout=30
    )
    conn.timeout = QUERY_TIMEOUT_SECONDS
    return conn

def month_range(start, end):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        ms = date(y, m, 1)
        if m == 12:
            nms = date(y + 1, 1, 1)
        else:
            nms = date(y, m + 1, 1)
        yield ms, nms
        m += 1
        if m == 13:
            m = 1
            y += 1

def normalize(df):
    for col in ["Account", "Job", "FullAccountNo"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"\.0$", "", regex=True)
                .str.strip()
            )
    return df

def main():
    print("Exporting RAW GL History → gl_history_raw.csv")

    if os.path.exists(OUTFILE):
        os.remove(OUTFILE)

    conn = connect()

    # Determine date bounds
    bounds_sql = """
        SELECT
            MIN(COALESCE(date_booked, date_posted)),
            MAX(COALESCE(date_booked, date_posted))
        FROM dbo.gl_history
    """
    min_dt, max_dt = pd.read_sql(bounds_sql, conn).iloc[0]

    if GL_START_DATE:
        min_dt = max(pd.to_datetime(GL_START_DATE).date(), min_dt)

    total_rows = 0

    for start, end in month_range(min_dt, max_dt):
        print(f"→ Pulling GL month {start} …")

        sql = f"""
        SELECT
            basic_account_no AS Account,
            job_no AS Job,
            journal_no AS Jrnl,
            transaction_no AS TrxNo,
            line_no AS Line,
            full_account_no AS FullAccountNo,
            amount_db AS Debit,
            amount_cr AS Credit,
            description,
            vendor_no,
            voucher_no,
            audit_number,
            customer_no,
            ar_invoice_no,
            cash_trx_no,
            record_status,
            ar_invoice_id,
            basic_account_id,
            cash_trx_id,
            customer_id,
            full_account_id,
            job_id,
            job_trx_id,
            journal_id,
            line_id,
            transaction_id,
            vendor_id,
            voucher_id,
            COALESCE(date_booked, date_posted) AS ActivityDate,
            DATEFROMPARTS(
                YEAR(COALESCE(date_booked, date_posted)),
                MONTH(COALESCE(date_booked, date_posted)),
                1
            ) AS MonthStart
        FROM dbo.gl_history WITH (NOLOCK)
        WHERE COALESCE(date_booked, date_posted) >= ?
          AND COALESCE(date_booked, date_posted) < ?
        """

        df = pd.read_sql(sql, conn, params=[start, end])

        if df.empty:
            continue

        df = normalize(df)

        write_header = not os.path.exists(OUTFILE)
        df.to_csv(OUTFILE, mode="a", header=write_header, index=False)

        total_rows += len(df)
        print(f"   wrote {len(df)} rows (total {total_rows})")

    print(f"Wrote {OUTFILE} ({total_rows} rows)")

if __name__ == "__main__":
    main()
