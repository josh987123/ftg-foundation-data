import os
import pyodbc
import pandas as pd

SERVER = "sql.foundationsoft.com,9000"
DATABASE = "Cas_5587"   # MUST match what worked earlier
OUTFILE = "data/gl_history_raw.csv"

def connect():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={os.environ['FOUNDATION_SQL_USER']};"
        f"PWD={os.environ['FOUNDATION_SQL_PASSWORD']};",
        timeout=30
    )

def main():
    print("Exporting RAW GL History → gl_history_raw.csv")

    sql = """
    SELECT
        basic_account_no       AS Account,
        job_no                 AS Job,
        journal_no             AS Jrnl,
        transaction_no         AS TrxNo,
        line_no                AS Line,
        full_account_no        AS FullAccountNo,
        amount_db              AS Debit,
        amount_cr              AS Credit,
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

    FROM dbo.gl_history
    ORDER BY
        COALESCE(date_booked, date_posted),
        journal_no,
        line_no
    """

    conn = connect()
    df = pd.read_sql(sql, conn)

    # Normalize text fields to match Power Query behavior
    for col in ["Account", "Job", "FullAccountNo"]:
        if col in df.columns:
            df[col] = (
                df[col]
                  .astype(str)
                  .str.replace(r"\.0$", "", regex=True)
                  .str.strip()
            )

    df.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(df)} rows, {len(df.columns)} columns)")

if __name__ == "__main__":
    main()
