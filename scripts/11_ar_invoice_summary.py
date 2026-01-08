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

# 🔑 AR aging date = today (PDF-faithful, no overrides)
AS_OF_DATE = date.today()

# ==========================================================
# DB CONNECTION
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
    print(f"Exporting PDF-faithful AR Aging as of {AS_OF_DATE} …")
    conn = connect()

    sql = f"""
    DECLARE @AsOfDate date = '{AS_OF_DATE}';

    /* ------------------------------------------------------
       Base invoice set
       ------------------------------------------------------ */
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
            ISNULL(i.invoice_amount, 0) AS invoice_amount,
            ISNULL(i.amount_due, 0) AS amount_due,
            ISNULL(i.retainage_amount, 0) AS retainage_amount
        FROM ar_invoice i
        LEFT JOIN customers c ON c.customer_no = i.customer_no
        LEFT JOIN jobs j ON j.job_no = i.job_no
        LEFT JOIN project_managers pm ON pm.project_manager_no = j.project_manager_no
        WHERE
            i.record_status = 'A'
            AND i.company_no = 1
            AND i.posted_flag = 'Y'
            AND i.closed_flag = 'N'
            AND i.invoice_source = 'O'
            AND ISNULL(i.invoice_amount, 0) <> 0
            AND NOT EXISTS (
                SELECT 1
                FROM ar_invoice x
                WHERE x.record_status = 'A'
                  AND x.posted_flag = 'Y'
                  AND x.company_no = i.company_no
                  AND RTRIM(LTRIM(x.original_invoice_no)) = RTRIM(LTRIM(i.invoice_no))
                  AND RTRIM(LTRIM(x.invoice_no)) <> RTRIM(LTRIM(i.invoice_no))
            )
    ),

    /* ------------------------------------------------------
       Cash applied up to AsOfDate (Foundation-correct)
       ------------------------------------------------------ */
    CashApplied AS (
        SELECT
            RTRIM(LTRIM(ci.invoice_no)) AS invoice_no,
            SUM(ISNULL(ci.cash_amount, 0)) AS cash_applied
        FROM ar_cash c
        JOIN ar_cash_invoice ci
          ON ci.company_no = c.company_no
         AND ci.cash_receipt_no = c.cash_receipt_no
        WHERE
            c.record_status = 'A'
            AND c.reversal <> 'Y'
            AND c.receipt_date <= @AsOfDate
        GROUP BY
            RTRIM(LTRIM(ci.invoice_no))
    )

    /* ------------------------------------------------------
       Final AR Aging Output (PDF-faithful)
       ------------------------------------------------------ */
    SELECT
        i.company_no,
        i.invoice_no,
        i.customer_no,
        i.customer_name,
        i.job_no,
        i.job_description,
        i.project_manager_name,
        i.invoice_date,

        ROUND(i.invoice_amount, 2) AS invoice_amount,
        ROUND(ISNULL(ca.cash_applied, 0), 2) AS cash_applied,

        /* PDF Grand Total contributor */
        ROUND(i.invoice_amount - ISNULL(ca.cash_applied, 0), 2) AS total_due,

        /* Retainage display */
        ROUND(
            CASE
                WHEN i.retainage_amount <= 0 THEN 0
                WHEN (i.invoice_amount - ISNULL(ca.cash_applied, 0)) <= 0 THEN 0
                WHEN i.retainage_amount > (i.invoice_amount - ISNULL(ca.cash_applied, 0))
                    THEN (i.invoice_amount - ISNULL(ca.cash_applied, 0))
                ELSE i.retainage_amount
            END,
            2
        ) AS retainage_amount,

        /* Net receivable (PDF Net Receivable Total) */
        ROUND(
            (i.invoice_amount - ISNULL(ca.cash_applied, 0)) -
            CASE
                WHEN i.retainage_amount <= 0 THEN 0
                WHEN (i.invoice_amount - ISNULL(ca.cash_applied, 0)) <= 0 THEN 0
                WHEN i.retainage_amount > (i.invoice_amount - ISNULL(ca.cash_applied, 0))
                    THEN (i.invoice_amount - ISNULL(ca.cash_applied, 0))
                ELSE i.retainage_amount
            END,
            2
        ) AS calculated_amount_due,

        DATEDIFF(day, i.invoice_date, @AsOfDate) AS days_outstanding,

        CASE
            WHEN DATEDIFF(day, i.invoice_date, @AsOfDate) <= 30 THEN '0-30'
            WHEN DATEDIFF(day, i.invoice_date, @AsOfDate) <= 60 THEN '31-60'
            WHEN DATEDIFF(day, i.invoice_date, @AsOfDate) <= 90 THEN '61-90'
            ELSE '90+'
        END AS aging_bucket

    FROM Invoices i
    LEFT JOIN CashApplied ca
        ON ca.invoice_no = i.invoice_no

    /* PDF-faithful inclusion rule */
    WHERE
        ROUND(i.invoice_amount - ISNULL(ca.cash_applied, 0), 2) <> 0

    ORDER BY
        customer_name,
        job_no,
        invoice_no;
    """

    df = pd.read_sql(sql, conn)

    # ------------------------------------------------------
    # Final formatting
    # ------------------------------------------------------
    money_cols = [
        "invoice_amount",
        "cash_applied",
        "total_due",
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
