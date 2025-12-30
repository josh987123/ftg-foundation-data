import os
import pyodbc
import pandas as pd

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
SERVER = "sql.foundationsoft.com,9000"
DATABASE = "Cas_5587"
OUTFILE = "data/ar_receipt_job_allocation.csv"

def connect():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={os.environ['FOUNDATION_SQL_USER']};"
        f"PWD={os.environ['FOUNDATION_SQL_PASSWORD']};",
        timeout=30
    )

def normalize_text(series):
    return (
        series.astype(str)
              .str.replace(r"\.0$", "", regex=True)
              .str.strip()
              .replace({"nan": "", "None": ""})
    )

def main():
    print("Exporting ar_receipt_job_allocation.csv ...")
    conn = connect()

    sql = """
    SELECT
      c.company_no                               AS company_no,
      CAST(c.check_no AS varchar(20))            AS receipt_document_no,
      c.cash_receipt_no                          AS receipt_no,
      c.receipt_date                             AS receipt_date,

      (
        SELECT SUM(ci2.cash_amount)
        FROM ar_cash_invoice ci2
        WHERE ci2.company_no      = c.company_no
          AND ci2.cash_receipt_no = c.cash_receipt_no
      )                                          AS receipt_amount,

      c.cash_receipt_type                        AS receipt_type,
      c.cash_receipt_source                      AS receipt_source,
      c.cash_flag                                AS receipt_subtype,
      c.customer_no                              AS customer_no,

      ci.invoice_no                              AS invoice_no,
      ci.line_no                                 AS line_no,

      ci.cash_amount                             AS applied_amount,

      i.job_no                                   AS job_no,
      j.description                              AS job_description

    FROM ar_cash c
    JOIN ar_cash_invoice ci
      ON ci.company_no      = c.company_no
     AND ci.cash_receipt_no = c.cash_receipt_no

    JOIN ar_invoice i
      ON i.company_no = ci.company_no
     AND i.invoice_no = ci.invoice_no

    LEFT JOIN jobs j
      ON j.job_no = i.job_no

    WHERE
      c.reversal <> 'Y'

    ORDER BY
      c.receipt_date DESC,
      c.cash_receipt_no,
      ci.invoice_no,
      ci.line_no
    """

    df = pd.read_sql(sql, conn)

    # ------------------------------------------------------------
    # Light normalization (matches prior patterns)
    # ------------------------------------------------------------
    TEXT_COLS = [
        "receipt_document_no",
        "receipt_type",
        "receipt_source",
        "receipt_subtype",
        "customer_no",
        "invoice_no",
        "job_no",
        "job_description",
    ]

    for col in TEXT_COLS:
        if col in df.columns:
            df[col] = normalize_text(df[col])

    MONEY_COLS = ["receipt_amount", "applied_amount"]
    for col in MONEY_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    df["receipt_date"] = pd.to_datetime(df["receipt_date"], errors="coerce")

    df.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(df)} rows, {len(df.columns)} columns)")

if __name__ == "__main__":
    main()
