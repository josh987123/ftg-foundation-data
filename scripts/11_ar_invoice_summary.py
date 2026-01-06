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
    print("Exporting ar_invoice_summary.csv (Foundation-faithful) ...")
    conn = connect()

    # ------------------------------------------------------------
    # FOUNDATION-FAITHFUL AR AGING SQL
    # ------------------------------------------------------------
    sql = """
    DECLARE @AsOfDate date = GETDATE();

    WITH InvoiceBase AS (
        SELECT
            i.company_no,
            RTRIM(LTRIM(i.invoice_no))   AS invoice_no,
            RTRIM(LTRIM(i.customer_no))  AS customer_no,
            c.name                       AS customer_name,
            i.job_no,
            j.description                AS job_description,
            pm.description               AS project_manager_name,
            i.invoice_date,
            i.invoice_amount,
            ISNULL(i.retainage_percent, 0) AS retainage_percent
        FROM ar_invoice i
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
            AND i.invoice_source = 'O'
            AND ISNULL(i.invoice_amount,0) > 0
    ),
    CashApplied AS (
        SELECT
            RTRIM(LTRIM(ci.invoice_no)) AS invoice_no,
            SUM(ci.cash_amount)        AS cash_applied
        FROM ar_cash c
        JOIN ar_cash_invoice ci
          ON ci.company_no      = c.company_no
         AND ci.cash_receipt_no = c.cash_receipt_no
        WHERE
            c.reversal <> 'Y'
        GROUP BY
            RTRIM(LTRIM(ci.invoice_no))
    ),
    Balances AS (
        SELECT
            i.*,
            ISNULL(c.cash_applied, 0) AS cash_applied,
            CASE
                WHEN i.invoice_amount - ISNULL(c.cash_applied,0) < 0 THEN 0
                ELSE i.invoice_amount - ISNULL(c.cash_applied,0)
            END AS remaining_balance
        FROM InvoiceBase i
        LEFT JOIN CashApplied c
            ON c.invoice_no = i.invoice_no
    ),
    RetainageCalc AS (
        SELECT
            *,
            CASE
                WHEN retainage_percent = 0 THEN 0
                ELSE ROUND(invoice_amount * retainage_percent / 100.0, 2)
            END AS retainage_amount,
            CASE
                WHEN
                    CASE
                        WHEN invoice_amount - cash_applied < 0 THEN 0
                        ELSE invoice_amount - cash_applied
                    END
                    <
                    ROUND(invoice_amount * retainage_percent / 100.0, 2)
                THEN
                    CASE
                        WHEN invoice_amount - cash_applied < 0 THEN 0
                        ELSE invoice_amount - cash_applied
                    END
                ELSE
                    ROUND(invoice_amount * retainage_percent / 100.0, 2)
            END AS retainage_capped
        FROM Balances
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
        cash_applied,
        remaining_balance,
        retainage_capped AS retainage_amount,
        CASE
            WHEN remaining_balance - retainage_capped < 0 THEN 0
            ELSE remaining_balance - retainage_capped
        END AS calculated_amount_due,
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
        "cash_applied",
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
