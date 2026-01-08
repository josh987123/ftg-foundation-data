import os
import pyodbc
import pandas as pd
from datetime import date

# ==========================================================
# CONFIG
# ==========================================================
SERVER = "sql.foundationsoft.com,9000"
DATABASE = "Cas_5587"
OUTFILE = "data/ar_invoice_summary.csv"

# Match Foundation report date exactly
AS_OF_DATE = date(2026, 1, 7)

# Optional: hide tiny residuals that make dashboards look wrong
# Set to 0 to keep everything.
MIN_TOTAL_DUE = 1.00  # dollars

# ==========================================================
# DB CONNECTION (MATCHES WORKING SCRIPTS)
# ==========================================================
def connect():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={os.environ['FOUNDATION_SQL_USER']};"
        f"PWD={os.environ['FOUNDATION_SQL_PASSWORD']};",
        timeout=30
    )

# ==========================================================
# MAIN
# ==========================================================
def main():
    print("Exporting Foundation-aligned AR Invoice Aging…")
    conn = connect()

    sql = f"""
    DECLARE @AsOfDate date = '{AS_OF_DATE}';

    WITH Invoices AS (
        SELECT
            i.company_no,
            RTRIM(LTRIM(i.invoice_no)) AS invoice_no,

            RTRIM(LTRIM(i.customer_no)) AS customer_no,
            c.name AS customer_name,

            RTRIM(LTRIM(i.job_no)) AS job_no,
            j.description AS job_description,
            pm.description AS project_manager_name,

            i.invoice_date,
            ISNULL(i.invoice_amount,0) AS invoice_amount,

            -- IMPORTANT:
            -- In Foundation, ar_invoice.amount_due is already the remaining balance (net of payments).
            -- Do NOT subtract cash applied again or you will double-count payments and zero out collectible.
            ISNULL(i.amount_due,0) AS amount_due,

            ISNULL(i.retainage_amount,0) AS retainage_amount

        FROM ar_invoice i
        LEFT JOIN customers c
            ON c.customer_no = i.customer_no
        LEFT JOIN jobs j
            ON j.job_no = i.job_no
        LEFT JOIN project_managers pm
            ON pm.project_manager_no = j.project_manager_no

        WHERE
            i.record_status = 'A'
            AND i.company_no = 1
            AND i.posted_flag = 'Y'
            AND i.closed_flag = 'N'
            AND i.invoice_source = 'O'
            AND ISNULL(i.invoice_amount,0) > 0
            AND ISNULL(i.amount_due,0) >= 0

            -- Exclude reposted invoices
            AND NOT EXISTS (
                SELECT 1
                FROM ar_invoice x
                WHERE x.record_status = 'A'
                  AND x.posted_flag = 'Y'
                  AND x.company_no = i.company_no
                  AND RTRIM(LTRIM(x.original_invoice_no)) = RTRIM(LTRIM(i.invoice_no))
                  AND RTRIM(LTRIM(x.invoice_no)) <> RTRIM(LTRIM(i.invoice_no))
            )
    )

    SELECT
        company_no,
        invoice_no,
        customer_no,
        customer_name,
        job_no,
        job_description,
        project_manager_name,
        invoice_date,
        invoice_amount,
        amount_due,
        retainage_amount,

        -- Keep the column for downstream compatibility, but compute correctly:
        -- collectible remaining = amount_due - retainage_amount (floor at 0)
        CASE
            WHEN (ISNULL(amount_due,0) - ISNULL(retainage_amount,0)) <= 0 THEN 0
            ELSE (ISNULL(amount_due,0) - ISNULL(retainage_amount,0))
        END AS calculated_amount_due,

        DATEDIFF(day, invoice_date, @AsOfDate) AS days_outstanding,

        CASE
            WHEN DATEDIFF(day, invoice_date, @AsOfDate) <= 30 THEN '0–30'
            WHEN DATEDIFF(day, invoice_date, @AsOfDate) <= 60 THEN '31–60'
            WHEN DATEDIFF(day, invoice_date, @AsOfDate) <= 90 THEN '61–90'
            ELSE '91+'
        END AS aging_bucket

    FROM Invoices
    WHERE ISNULL(amount_due,0) >= {MIN_TOTAL_DUE}
    ORDER BY customer_name, job_no, invoice_no;
    """

    df = pd.read_sql(sql, conn)

    money_cols = [
        "invoice_amount",
        "amount_due",
        "retainage_amount",
        "calculated_amount_due",
    ]

    for col in money_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")

    df.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(df)} rows)")

if __name__ == "__main__":
    main()
