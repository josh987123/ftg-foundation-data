import pandas as pd
from datetime import datetime

INFILE = "data/payments.csv"
OUTFILE = "data/ap_invoice_summary.csv"

def main():
    df = pd.read_csv(INFILE, low_memory=False)

    # Remove voided rows
    df = df[df["void_flag"] != "Y"].copy()

    # Normalize dates
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")

    # ------------------------------------------------------
    # ONE ROW PER INVOICE (critical fix)
    # ------------------------------------------------------
    grouped = (
        df.groupby(["invoice_no", "vendor_name", "job_no"], dropna=False)
          .agg(
              invoice_date=("invoice_date", "min"),          # EARLIEST invoice date
              invoice_amount=("invoice_amount", "max"),
              retainage_amount=("retainage_amount", "max"),
              amount_paid=("cash_amount", "sum"),
              job_description=("job_description", "first"),
              project_manager_name=("project_manager_name", "first"),
          )
          .reset_index()
    )

    as_of_date = pd.Timestamp(datetime.now().date())

    # ------------------------------------------------------
    # Foundation-faithful AP math
    # ------------------------------------------------------
    grouped["total_due"] = grouped["invoice_amount"] - grouped["amount_paid"]

    grouped["retainage"] = grouped.apply(
        lambda r: max(
            min(r["retainage_amount"], r["total_due"]),
            0
        ),
        axis=1,
    )

    grouped["open_for_aging"] = grouped.apply(
        lambda r: max(r["total_due"] - r["retainage"], 0),
        axis=1,
    )

    grouped["days_outstanding"] = (
        as_of_date - grouped["invoice_date"]
    ).dt.days

    # ------------------------------------------------------
    # Aging buckets (invoice-date based)
    # ------------------------------------------------------
    def bucket(days):
        if days <= 30:
            return "0-30"
        elif days <= 60:
            return "31-60"
        elif days <= 90:
            return "61-90"
        else:
            return "90+"

    grouped["aging_bucket"] = grouped["days_outstanding"].apply(bucket)

    # Only keep invoices with balance
    grouped = grouped[grouped["total_due"] != 0]

    grouped.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(grouped)} invoices)")

if __name__ == "__main__":
    main()
