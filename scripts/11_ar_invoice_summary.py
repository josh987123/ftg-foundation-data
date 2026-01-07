import os
import pyodbc
import pandas as pd

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
SERVER = "sql.foundationsoft.com,9000"
DATABASE = "Cas_5587"
OUTFILE = "data/ar_invoice_summary.csv"

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
    print("Exporting ar_invoice_summary.csv (Foundation-faithful, job-gated) ...")
    conn = connect()

    # ------------------------------------------------------------
    # FOUNDATION-FAITHFUL AR AGING SQL (JOB-GATED)
    # ------------------------------------------------------------
    sql = """
    DECLARE @AsOfDate date = GETDATE();

    WITH InvoiceBase AS (
        SELECT
            i.company_no,
            RTRIM(LTRIM(i.invoice_no))  AS invoice_no,
            RTRIM(LTRIM(i.customer_no)) AS customer_no,
            c.name                      AS customer_name,
            i.job_no,
            j.description               AS job_description,
            pm.description              AS project_manager_name,
            i.invoice_date,
            i.invoice_amount,
            i.amount_due,
            ISNULL(i.retainage_percent, 0) AS retainage_percent
        FROM ar_invoice i

        -- 🔑 JOB-BASED AR ELIGIBILITY GATE
        INNER JOIN ar_invoice_jc jc
            ON jc.invoice_id = i.invoice_id

        LEFT JOIN customers c
            ON c.customer_no = i.customer_no
        LEFT JOIN jobs j
            ON j.job_no = i.job_no
        LEFT JOIN project_managers pm
            ON pm.project_manager_no = j.project_manager_no
        WHERE
            i.company_no = 1
            AND i.posted_flag = 'Y'
            AND i.closed_flag = 'N'
            AND ISNULL(i.amount_due, 0) > 0
    ),
    RetainageCalc AS (
        SELECT
            *,
            CASE
                WHEN retainage_percent = 0 THEN 0
                ELSE ROUND(invoice_amount * retainage_percent / 100.0, 2)
            END AS retainage_amount,
            CASE
                WHEN amount_due <
                     ROUND(invoice_amount * retainage_percent / 100.0, 2)
                THEN amount_due
                ELSE ROUND(invoice_amount * retainage_percent / 100.0, 2)
            END AS retainage_capped
        FROM InvoiceBase
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
        amount_due AS remaining_balance,          -- 🔑 Foundation definition
        retainage_capped AS retainage_amount,
        amount_due AS calculated_amount_due,
        DATEDIFF(day, invoice_date, @AsOfDate) AS days_outstanding,
        CASE
            WHEN DATEDIFF(day, invoice_date, @AsOfDate) <= 30 THEN '0–30'
            WHEN DATEDIFF(day, invoice_date, @AsOfDate) <= 60 THEN '31–60'
            WHEN DATEDIFF(day, invoice_date, @AsOfDate) <= 90 THEN '61–90'
            ELSE '91+'
        END AS aging_bucket
    FROM RetainageCalc
    ORDER BY invoice_date;
    """

    df = pd.read_sql(sql, conn)

    # ------------------------------------------------------------
    # Final formatting
    # ------------------------------------------------------------
    MONEY_COLS = [
        "invoice_amount",
        "remaining_balance",
        "retainage_amount",
        "calculated_amount_due",
    ]

    for col in MONEY_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")

    df.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(df)} rows, {len(df.columns)} columns)")

if __name__ == "__main__":
    main()
