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
    print("Exporting Foundation-aligned AR Invoice Aging…")
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
            i.invoice_amount,
            i.amount_due,
            ISNULL(i.retainage_amount,0) AS retainage_amount
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
            AND ISNULL(i.invoice_amount,0) > 0
            AND ISNULL(i.amount_due,0) >= 0
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
       Cash applied up to AsOfDate
       ------------------------------------------------------ */
    CashApplied AS (
        SELECT
            RTRIM(LTRIM(h.invoice_no)) AS invoice_no,
            SUM(ISNULL(h.cash_amount,0)) AS cash_applied
        FROM ar_history h
        WHERE
            h.record_status = 'A'
            AND h.tran_date <= @AsOfDate
        GROUP BY
            RTRIM(LTRIM(h.invoice_no))
    ),

    /* ------------------------------------------------------
       Audit-cleared retainage invoices
       ------------------------------------------------------ */
    AuditCleared AS (
        SELECT DISTINCT
            RTRIM(LTRIM(COALESCE(h.adjust_invoice_no, h.invoice_no))) AS invoice_no
        FROM ar_history h
        WHERE
            h.record_status = 'A'
            AND ISNULL(h.cash_amount,0) <> 0
    )

    /* ------------------------------------------------------
       Final AR output (Foundation-aligned)
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
        i.invoice_amount,
        i.amount_due,
        i.retainage_amount,

        /* Net collectible per Foundation audit logic */
        ROUND(
            i.invoice_amount
            - ISNULL(ca.cash_applied,0)
            - i.retainage_amount,
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
    LEFT JOIN AuditCleared a
        ON a.invoice_no = i.invoice_no

    /* ------------------------------------------------------
       Canonical exclusion rules (FINAL)
       ------------------------------------------------------ */
    WHERE
        /* Must have positive collectible */
        ROUND(
            i.invoice_amount
            - ISNULL(ca.cash_applied,0)
            - i.retainage_amount,
            2
        ) > 0

        /* Exclude audit-cleared retainage-only invoices */
        AND NOT (
            a.invoice_no IS NOT NULL
            AND i.retainage_amount > 0
            AND i.amount_due = i.invoice_amount
        )

        /* Exclude legacy audit-cleared invoices with non-cash adjustments */
        AND NOT EXISTS (
            SELECT 1
            FROM ar_history h2
            WHERE
                h2.record_status = 'A'
                AND RTRIM(LTRIM(h2.invoice_no)) = i.invoice_no
                AND ISNULL(h2.cash_amount,0) = 0
                AND ISNULL(h2.adjust_invoice_amount,0) <> 0
        )

    ORDER BY
        customer_name,
        job_no,
        invoice_no;
    """

    df = pd.read_sql(sql, conn)

    # ==========================================================
    # FINAL FORMATTING
    # ==========================================================
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

# ==========================================================
# ENTRY POINT
# ==========================================================
if __name__ == "__main__":
    main()
