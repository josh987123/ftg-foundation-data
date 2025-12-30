import os
import pyodbc
import pandas as pd
from datetime import datetime

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
SERVER = "sql.foundationsoft.com,9000"
DATABASE = "Cas_5587"
OUTFILE = "data/ap_invoice_summary.csv"

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
    print("Exporting ap_invoice_summary.csv ...")
    conn = connect()

    # ------------------------------------------------------------
    # SOURCE: payments (invoice + payment detail)
    # ------------------------------------------------------------
    sql = """
    SELECT
        invoice_no,
        invoice_date,
        invoice_amount,
        vendor_name,
        retainage_percent,
        retainage_amount,
        job_no,
        job_description,
        project_manager_name,
        void_flag,
        cash_amount
    FROM dbo.payments
    """

    df = pd.read_sql(sql, conn)

    # ------------------------------------------------------------
    # Normalize types
    # ------------------------------------------------------------
    TEXT_COLS = [
        "invoice_no",
        "vendor_name",
        "job_no",
        "job_description",
        "project_manager_name",
    ]
    for col in TEXT_COLS:
        if col in df.columns:
            df[col] = normalize_text(df[col])

    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
    df["invoice_amount"] = pd.to_numeric(df["invoice_amount"], errors="coerce").fillna(0.0)
    df["retainage_percent"] = pd.to_numeric(df["retainage_percent"], errors="coerce")
    df["retainage_amount"] = pd.to_numeric(df["retainage_amount"], errors="coerce")
    df["cash_amount"] = pd.to_numeric(df["cash_amount"], errors="coerce")

    # ------------------------------------------------------------
    # Effective cash amount (ignore voided payments)
    # ------------------------------------------------------------
    df["effective_cash_amount"] = df.apply(
        lambda r: 0.0
        if r["void_flag"] == 1 or pd.isna(r["cash_amount"])
        else r["cash_amount"],
        axis=1
    )

    # ------------------------------------------------------------
    # Group to one row per invoice
    # ------------------------------------------------------------
    grouped = (
        df.groupby(
            [
                "invoice_no",
                "invoice_date",
                "invoice_amount",
                "vendor_name",
                "retainage_percent",
                "retainage_amount",
                "job_no",
                "job_description",
                "project_manager_name",
            ],
            as_index=False
        )
        .agg(amount_paid_to_date=("effective_cash_amount", "sum"))
    )

    # ------------------------------------------------------------
    # Remaining balance (no negatives)
    # ------------------------------------------------------------
    grouped["remaining_balance"] = (
        grouped["invoice_amount"] - grouped["amount_paid_to_date"]
    ).clip(lower=0)

    # ------------------------------------------------------------
    # Payment status
    # ------------------------------------------------------------
    def payment_status(row):
        if row["amount_paid_to_date"] == 0:
            return "Open"
        elif row["remaining_balance"] <= 0:
            return "Paid"
        else:
            return "Partially Paid"

    grouped["payment_status"] = grouped.apply(payment_status, axis=1)

    # ------------------------------------------------------------
    # Days outstanding
    # ------------------------------------------------------------
    today = pd.Timestamp(datetime.now().date())
    grouped["days_outstanding"] = grouped["invoice_date"].apply(
        lambda d: None if pd.isna(d) else (today - d).days
    )

    # ------------------------------------------------------------
    # Aging bucket
    # ------------------------------------------------------------
    def aging_bucket(days):
        if days is None:
            return None
        if days <= 30:
            return "0–30"
        if days <= 60:
            return "31–60"
        if days <= 90:
            return "61–90"
        return "90+"

    grouped["aging_bucket"] = grouped["days_outstanding"].apply(aging_bucket)

    # ------------------------------------------------------------
    # Final formatting
    # ------------------------------------------------------------
    MONEY_COLS = ["invoice_amount", "amount_paid_to_date", "remaining_balance"]
    for col in MONEY_COLS:
        grouped[col] = grouped[col].round(2)

    grouped.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(grouped)} rows, {len(grouped.columns)} columns)")

if __name__ == "__main__":
    main()
