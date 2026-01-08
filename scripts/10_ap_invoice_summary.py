import pandas as pd
from datetime import datetime

OUTFILE = "data/ap_invoice_summary.csv"

def normalize_text(series):
    return (
        series.astype(str)
              .str.replace(r"\.0$", "", regex=True)
              .str.strip()
              .replace({"nan": "", "None": ""})
    )

def main():
    print("Exporting ap_invoice_summary.csv ...")

    df = pd.read_csv("data/payments.csv", low_memory=False)

    # ------------------------------------------------------------
    # Normalize text fields
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

    # ------------------------------------------------------------
    # Normalize numeric & date fields
    # ------------------------------------------------------------
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
    df["invoice_amount"] = pd.to_numeric(df["invoice_amount"], errors="coerce").fillna(0.0)
    df["retainage_amount"] = pd.to_numeric(df["retainage_amount"], errors="coerce").fillna(0.0)
    df["cash_amount"] = pd.to_numeric(df["cash_amount"], errors="coerce").fillna(0.0)
    df["void_flag"] = pd.to_numeric(df["void_flag"], errors="coerce").fillna(0)

    # ------------------------------------------------------------
    # On-hold detection (PDF-faithful)
    # ------------------------------------------------------------
    HOLD_COL_CANDIDATES = [
        "hold_flag",
        "invoice_hold_flag",
        "on_hold",
        "hold",
    ]

    df["on_hold_flag"] = False
    for col in HOLD_COL_CANDIDATES:
        if col in df.columns:
            df["on_hold_flag"] = df[col].astype(str).str.upper().isin(["Y", "1", "TRUE"])
            break

    # ------------------------------------------------------------
    # Effective cash logic (ignore voided payments)
    # ------------------------------------------------------------
    df["effective_cash_amount"] = df.apply(
        lambda r: 0.0 if r["void_flag"] == 1 else r["cash_amount"],
        axis=1
    )

    # ------------------------------------------------------------
    # Group to invoice level
    # ------------------------------------------------------------
    grouped = (
        df.groupby(
            [
                "invoice_no",
                "invoice_date",
                "invoice_amount",
                "vendor_name",
                "retainage_amount",
                "job_no",
                "job_description",
                "project_manager_name",
                "on_hold_flag",
            ],
            as_index=False
        )
        .agg(amount_paid_to_date=("effective_cash_amount", "sum"))
    )

    # ------------------------------------------------------------
    # Remaining balance
    # ------------------------------------------------------------
    grouped["gross_remaining"] = (
        grouped["invoice_amount"] - grouped["amount_paid_to_date"]
    )

    # ------------------------------------------------------------
    # Credit detection
    # ------------------------------------------------------------
    grouped["is_credit"] = grouped["invoice_amount"] < 0

    # ------------------------------------------------------------
    # PDF-faithful aging amount
    # ------------------------------------------------------------
    grouped["open_for_aging"] = grouped.apply(
        lambda r: 0.0 if r["is_credit"] else max(r["gross_remaining"], 0),
        axis=1
    )

    # ------------------------------------------------------------
    # Aging calculation (dynamic)
    # ------------------------------------------------------------
    today = pd.Timestamp(datetime.now().date())
    grouped["days_outstanding"] = grouped["invoice_date"].apply(
        lambda d: None if pd.isna(d) else (today - d).days
    )

    def aging_bucket(row):
        if row["is_credit"]:
            return "CREDIT"
        days = row["days_outstanding"]
        if days is None:
            return None
        if days <= 30:
            return "0-30"
        if days <= 60:
            return "31-60"
        if days <= 90:
            return "61-90"
        return "90+"

    grouped["aging_bucket"] = grouped.apply(aging_bucket, axis=1)

    # ------------------------------------------------------------
    # Payment status
    # ------------------------------------------------------------
    def payment_status(row):
        if row["is_credit"]:
            return "Credit"
        if row["gross_remaining"] == 0:
            return "Paid"
        if row["on_hold_flag"]:
            return "On Hold"
        if row["amount_paid_to_date"] == 0:
            return "Open"
        return "Partially Paid"

    grouped["payment_status"] = grouped.apply(payment_status, axis=1)

    # ------------------------------------------------------------
    # Final formatting
    # ------------------------------------------------------------
    MONEY_COLS = [
        "invoice_amount",
        "amount_paid_to_date",
        "gross_remaining",
        "retainage_amount",
        "open_for_aging",
    ]
    for col in MONEY_COLS:
        grouped[col] = grouped[col].round(2)

    grouped.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(grouped)} rows, {len(grouped.columns)} columns)")

if __name__ == "__main__":
    main()
