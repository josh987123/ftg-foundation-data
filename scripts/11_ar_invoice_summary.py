import os
import pyodbc
import pandas as pd
from datetime import datetime

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

def normalize_text(series):
    return (
        series.astype(str)
              .str.replace(r"\.0$", "", regex=True)
              .str.strip()
              .replace({"nan": "", "None": ""})
    )

def main():
    print("Exporting ar_invoice_summary.csv ...")
    conn = connect()

    # ------------------------------------------------------------
    # Base invoices (original invoices only)
    # ------------------------------------------------------------
    invoices_sql = """
    SELECT
        i.company_no,
        RTRIM(LTRIM(i.invoice_no)) AS invoice_no,

        RTRIM(LTRIM(i.customer_no)) AS customer_no,
        c.name AS customer_name,

        i.invoice_date,
        i.due_date,

        RTRIM(LTRIM(i.job_no)) AS job_no,
        j.description AS job_description,
        pm.description AS project_manager_name,

        i.invoice_amount,
        i.amount_due,
        i.retainage_amount

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
        AND LEN(RTRIM(LTRIM(i.invoice_no))) >= 4
        AND (ISNULL(i.amount_due,0) <> 0 OR ISNULL(i.retainage_amount,0) <> 0)
        AND ISNULL(i.adjust_invoice_amount,0) = 0
        AND ISNULL(i.invoice_amount,0) > 0
        AND NOT EXISTS (
            SELECT 1
            FROM ar_invoice x
            WHERE x.record_status = 'A'
              AND x.posted_flag = 'Y'
              AND x.company_no = i.company_no
              AND RTRIM(LTRIM(x.original_invoice_no)) = RTRIM(LTRIM(i.invoice_no))
              AND RTRIM(LTRIM(x.invoice_no)) <> RTRIM(LTRIM(i.invoice_no))
        )
    """

    invoices = pd.read_sql(invoices_sql, conn)

    # ------------------------------------------------------------
    # Cash applied
    # ------------------------------------------------------------
    cash_sql = """
    SELECT
        ci.company_no,
        ci.invoice_source,
        RTRIM(LTRIM(ci.invoice_no)) AS invoice_no,
        SUM(
            ISNULL(ci.ar_amount,0)
          + ISNULL(ci.retainage_amount,0)
          + ISNULL(ci.discount_amount,0)
        ) AS total_cash_applied
    FROM ar_cash_invoice ci
    WHERE ci.record_status = 'A'
    GROUP BY
        ci.company_no,
        ci.invoice_source,
        RTRIM(LTRIM(ci.invoice_no))
    """

    cash = pd.read_sql(cash_sql, conn)

    # ------------------------------------------------------------
    # Adjustments applied
    # ------------------------------------------------------------
    adj_sql = """
    SELECT
        a.company_no,
        RTRIM(LTRIM(a.original_invoice_no)) AS invoice_no,
        SUM(ISNULL(a.amount_due,0)) AS total_adjustments_applied
    FROM ar_invoice a
    WHERE
        a.record_status = 'A'
        AND a.posted_flag = 'Y'
        AND a.invoice_source = 'A'
        AND a.original_invoice_no IS NOT NULL
    GROUP BY
        a.company_no,
        RTRIM(LTRIM(a.original_invoice_no))
    """

    adj = pd.read_sql(adj_sql, conn)

    # ------------------------------------------------------------
    # Join cash + adjustments
    # ------------------------------------------------------------
    df = invoices.merge(
        cash,
        how="left",
        on=["company_no", "invoice_no"]
    )

    df["total_cash_applied"] = (
        pd.to_numeric(df["total_cash_applied"], errors="coerce").fillna(0.0)
    )

    df = df.merge(
        adj,
        how="left",
        on=["company_no", "invoice_no"]
    )

    df["total_adjustments_applied"] = (
        pd.to_numeric(df["total_adjustments_applied"], errors="coerce").fillna(0.0)
    )

    # ------------------------------------------------------------
    # Calculated amount due
    # ------------------------------------------------------------
    df["calculated_amount_due"] = (
        df["amount_due"]
        - df["total_cash_applied"]
        - df["total_adjustments_applied"]
    ).round(2)

    df = df[df["calculated_amount_due"] > 0]

    # ------------------------------------------------------------
    # Aging
    # ------------------------------------------------------------
    today = pd.Timestamp(datetime.now().date())

    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
    df["due_date"] = pd.to_datetime(df["due_date"], errors="coerce")

    df["days_outstanding"] = df.apply(
        lambda r:
            (today - r["due_date"]).days
            if pd.notna(r["due_date"])
            else (today - r["invoice_date"]).days,
        axis=1
    )

    def aging_bucket(days):
        if days <= 30:
            return "0–30"
        if days <= 60:
            return "31–60"
        if days <= 90:
            return "61–90"
        return "90+"

    df["aging_bucket"] = df["days_outstanding"].apply(aging_bucket)

    # ------------------------------------------------------------
    # Final formatting
    # ------------------------------------------------------------
    MONEY_COLS = [
        "invoice_amount",
        "amount_due",
        "retainage_amount",
        "calculated_amount_due",
    ]

    for col in MONEY_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    df.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(df)} rows, {len(df.columns)} columns)")

if __name__ == "__main__":
    main()
