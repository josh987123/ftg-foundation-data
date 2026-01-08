import pandas as pd
from datetime import datetime

INFILE = "data/payments.csv"
OUTFILE = "data/ap_invoice_summary.csv"


def main():
    print("Building ap_invoice_summary.csv ...")

    df = pd.read_csv(INFILE, low_memory=False)

    # ------------------------------------------------------
    # Remove voided payment rows
    # ------------------------------------------------------
    df["void_flag"] = (
        pd.to_numeric(df["void_flag"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df = df[df["void_flag"] != 1].copy()

    # ------------------------------------------------------
    # Normalize dates
    # ------------------------------------------------------
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    # ------------------------------------------------------
    # ONE ROW PER INVOICE (stable identity)
    # ------------------------------------------------------
    grouped = (
        df.groupby(["invoice_no", "vendor_name", "job_no"], dropna=False)
        .agg(
            # Reference dates
            invoice_date=("invoice_date", "min"),
            transaction_date=("transaction_date", "first"),  # AGING ANCHOR

            # Amounts
            invoice_amount=("invoice_amount", "max"),
            amount_paid=("cash_amount", "sum"),

            # ORIGINAL retainage from header (will be adjusted below)
            retainage_amount=("retainage_amount", "max"),

            # Context
            job_description=("job_description", "first"),
            project_manager_name=("project_manager_name", "first"),
        )
        .reset_index()
    )

    # ------------------------------------------------------
    # As-of date (Foundation-style)
    # ------------------------------------------------------
    as_of_date = pd.Timestamp(datetime.now().date())

    # ------------------------------------------------------
    # Foundation-faithful AP math
    # ------------------------------------------------------
    grouped["total_due"] = grouped["invoice_amount"] - grouped["amount_paid"]

    # AP aging NEVER subtracts retainage
    grouped["open_for_aging"] = grouped["total_due"].apply(
        lambda x: max(x, 0)
    )

    # ------------------------------------------------------
    # REMAINING RETAINAGE (Foundation behavior)
    #
    # Payments apply:
    #   1) Non-retainage portion
    #   2) Then reduce retainage
    # ------------------------------------------------------
    grouped["non_retainage_portion"] = (
        grouped["invoice_amount"] - grouped["retainage_amount"]
    )

    grouped["overpay_into_retainage"] = (
        grouped["amount_paid"] - grouped["non_retainage_portion"]
    ).clip(lower=0)

    grouped["remaining_retainage"] = (
        grouped["retainage_amount"] - grouped["overpay_into_retainage"]
    ).clip(lower=0)

    # ------------------------------------------------------
    # Days outstanding (transaction-date based)
    # ------------------------------------------------------
    grouped["days_outstanding"] = (
        as_of_date - grouped["transaction_date"]
    ).dt.days

    # ------------------------------------------------------
    # Aging buckets (Foundation AP rules)
    # ------------------------------------------------------
    def bucket(days):
        if pd.isna(days):
            return None
        if days <= 30:
            return "0-30"
        elif days <= 60:
            return "31-60"
        elif days <= 90:
            return "61-90"
        else:
            return "90+"

    grouped["aging_bucket"] = grouped["days_outstanding"].apply(bucket)

    # ------------------------------------------------------
    # Keep only invoices with a balance
    # ------------------------------------------------------
    grouped = grouped[grouped["total_due"] != 0]

    # ------------------------------------------------------
    # Final column order (explicit & stable)
    # ------------------------------------------------------
    final = grouped[
        [
            "invoice_no",
            "vendor_name",
            "job_no",
            "job_description",
            "project_manager_name",

            "invoice_date",
            "transaction_date",

            "invoice_amount",
            "amount_paid",
            "total_due",

            # IMPORTANT:
            # remaining_retainage replaces original retainage for display
            "remaining_retainage",
            "open_for_aging",

            "days_outstanding",
            "aging_bucket",
        ]
    ]

    final.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(final)} invoices)")


if __name__ == "__main__":
    main()
