import os
import pyodbc
import pandas as pd

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
SERVER = "sql.foundationsoft.com,9000"
DATABASE = "Cas_5587"
OUTFILE = "data/payments.csv"

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
    print("Exporting payments.csv ...")
    conn = connect()

    # ------------------------------------------------------------
    # AP INVOICE HEADER
    # ------------------------------------------------------------
    ap_h = pd.read_sql(
        """
        SELECT
            voucher_no,
            invoice_no,
            vendor_no,
            invoice_date,
            invoice_amount,
            retainage_percent,
            retainage_amount,
            job_no
        FROM dbo.ap_invoice_h
        WHERE invoice_amount IS NOT NULL
        """,
        conn
    )

    # ------------------------------------------------------------
    # AP INVOICE DETAIL
    # ------------------------------------------------------------
    ap_d = pd.read_sql(
        """
        SELECT
            voucher_no,
            cost_class_no,
            cost_code_no,
            account_no
        FROM dbo.ap_invoice_d
        """,
        conn
    )

    df = ap_h.merge(ap_d, how="left", on="voucher_no")

    # ------------------------------------------------------------
    # PAYMENT SOURCES
    # ------------------------------------------------------------
    check_pmt = pd.read_sql(
        "SELECT voucher_no, cash_amount, void_flag FROM dbo.ap_check_vch",
        conn
    )

    pmt = pd.read_sql(
        "SELECT voucher_no, cash_amount FROM dbo.ap_pmt_vch",
        conn
    )
    pmt["void_flag"] = 0

    prepmt = pd.read_sql(
        "SELECT voucher_no, cash_amount FROM dbo.ap_pre_pmt_vch",
        conn
    )
    prepmt["void_flag"] = 0

    precheck = pd.read_sql(
        "SELECT voucher_no, cash_amount FROM dbo.ap_pre_check_vch",
        conn
    )
    precheck["void_flag"] = 0

    all_payments = pd.concat(
        [check_pmt, pmt, prepmt, precheck],
        ignore_index=True
    )

    df = df.merge(all_payments, how="left", on="voucher_no")

    df["cash_amount"] = pd.to_numeric(df["cash_amount"], errors="coerce").fillna(0.0)

    # ------------------------------------------------------------
    # VENDORS
    # ------------------------------------------------------------
    vendors = pd.read_sql(
        "SELECT vendor_no, name FROM dbo.vendors",
        conn
    )
    vendors["vendor_no"] = normalize_text(vendors["vendor_no"])
    vendors["vendor_name"] = normalize_text(vendors["name"])
    vendors = vendors[["vendor_no", "vendor_name"]]

    df["vendor_no"] = normalize_text(df["vendor_no"])
    df = df.merge(vendors, how="left", on="vendor_no")

    # ------------------------------------------------------------
    # JOBS
    # ------------------------------------------------------------
    jobs = pd.read_sql(
        "SELECT job_no, description, project_manager_no FROM dbo.jobs",
        conn
    )
    jobs["job_no"] = normalize_text(jobs["job_no"])
    jobs["job_description"] = normalize_text(jobs["description"])
    jobs["project_manager_no"] = normalize_text(jobs["project_manager_no"])
    jobs = jobs[["job_no", "job_description", "project_manager_no"]]

    df["job_no"] = normalize_text(df["job_no"])
    df = df.merge(jobs, how="left", on="job_no")

    # ------------------------------------------------------------
    # PROJECT MANAGERS
    # ------------------------------------------------------------
    pms = pd.read_sql(
        "SELECT project_manager_no, description FROM dbo.project_managers",
        conn
    )
    pms["project_manager_no"] = normalize_text(pms["project_manager_no"])
    pms["project_manager_name"] = normalize_text(pms["description"])
    pms = pms[["project_manager_no", "project_manager_name"]]

    df = df.merge(pms, how="left", on="project_manager_no")

    # ------------------------------------------------------------
    # FINAL SCHEMA
    # ------------------------------------------------------------
    final = df[
        [
            "invoice_no",
            "invoice_date",
            "invoice_amount",
            "vendor_name",
            "retainage_percent",
            "retainage_amount",
            "cash_amount",
            "void_flag",
            "job_no",
            "job_description",
            "project_manager_name",
        ]
    ]

    # Type normalization
    final["invoice_no"] = normalize_text(final["invoice_no"])
    final["job_no"] = normalize_text(final["job_no"])
    final["invoice_date"] = pd.to_datetime(final["invoice_date"], errors="coerce")

    MONEY_COLS = ["invoice_amount", "retainage_amount", "cash_amount"]
    for col in MONEY_COLS:
        final[col] = pd.to_numeric(final[col], errors="coerce").round(2)

    final.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(final)} rows, {len(final.columns)} columns)")

if __name__ == "__main__":
    main()
