import os
import pyodbc
import pandas as pd

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
SERVER = "sql.foundationsoft.com,9000"
DATABASE = "Cas_5587"
OUTFILE = "data/ap_payment_job_allocation.csv"

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
    print("Exporting ap_payment_job_allocation.csv ...")
    conn = connect()

    sql = """
    SELECT
      c.company_no                               AS company_no,
      CAST(c.check_no AS varchar(20))            AS payment_document_no,
      c.check_date                               AS payment_date,

      (
        SELECT SUM(h2.cash_amount)
        FROM ap_history h2
        WHERE h2.company_no = c.company_no
          AND h2.check_no   = c.check_no
          AND h2.gl_cash BETWEEN 1000 AND 1999
      )                                          AS payment_amount,

      c.check_type                               AS payment_type,
      c.source                                   AS payment_source,
      c.type                                     AS payment_subtype,
      c.vendor_no                                AS vendor_no,
      c.name                                     AS vendor_name,

      h.voucher_no                               AS voucher_no,
      h.line_no                                  AS line_no,

      h.cash_amount                              AS applied_amount,
      h.gl_cash                                  AS gl_cash_account,

      CASE
        WHEN h.gl_cash BETWEEN 1000 AND 1999
        THEN h.cash_amount
        ELSE 0
      END                                        AS cash_applied_amount,

      CASE
        WHEN h.gl_cash BETWEEN 1000 AND 1999 THEN 1
        ELSE 0
      END                                        AS is_cash_row,

      CASE
        WHEN h.gl_cash BETWEEN 1000 AND 1999 THEN 'Cash payment (bank)'
        ELSE 'Non-cash AP adjustment / liability'
      END                                        AS reconciliation_note,

      d.job_no                                   AS job_no,
      j.description                              AS job_description

    FROM ap_check c
    JOIN ap_history h
      ON h.company_no = c.company_no
     AND h.check_no   = c.check_no

    JOIN ap_invoice_d d
      ON d.company_no = h.company_no
     AND d.voucher_no = h.voucher_no
     AND d.line_no    = h.line_no

    LEFT JOIN jobs j
      ON j.job_no = d.job_no

    WHERE
      c.void_flag <> 'Y'

    ORDER BY
      c.check_date DESC,
      c.check_no,
      h.voucher_no,
      h.line_no
    """

    df = pd.read_sql(sql, conn)

    # ------------------------------------------------------------
    # Type normalization (light, non-destructive)
    # ------------------------------------------------------------
    TEXT_COLS = [
        "payment_document_no",
        "payment_type",
        "payment_source",
        "payment_subtype",
        "vendor_no",
        "vendor_name",
        "voucher_no",
        "job_no",
        "job_description",
        "reconciliation_note",
    ]

    for col in TEXT_COLS:
        if col in df.columns:
            df[col] = (
                df[col]
                  .astype(str)
                  .str.replace(r"\.0$", "", regex=True)
                  .str.strip()
                  .replace({"nan": "", "None": ""})
            )

    MONEY_COLS = ["payment_amount", "applied_amount", "cash_applied_amount"]
    for col in MONEY_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    df.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(df)} rows, {len(df.columns)} columns)")

if __name__ == "__main__":
    main()
