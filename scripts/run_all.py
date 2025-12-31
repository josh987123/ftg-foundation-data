import subprocess
import sys

# ============================================================
# Ordered list of pipeline steps (STRICTLY SEQUENTIAL)
# ============================================================

STEPS = [
    # --------------------------------------------------------
    # Connectivity
    # --------------------------------------------------------
    "scripts/01_test_connection.py",

    # --------------------------------------------------------
    # Dimensions
    # --------------------------------------------------------
    "scripts/02_accounts.py",

    # --------------------------------------------------------
    # GL pipeline (locked)
    # --------------------------------------------------------
    "scripts/03_gl_history_raw.py",
    "scripts/04_gl_history_derived.py",
    "scripts/05_gl_history_all.py",

    # --------------------------------------------------------
    # Jobs CSVs
    # --------------------------------------------------------
    "scripts/06_job_budgets.py",
    "scripts/07_job_actuals.py",
    "scripts/08_job_billed_revenue.py",

    # --------------------------------------------------------
    # AP base + summary
    # --------------------------------------------------------
    "scripts/09_payments.py",
    "scripts/10_ap_invoice_summary.py",

    # --------------------------------------------------------
    # AR summary
    # --------------------------------------------------------
    "scripts/11_ar_invoice_summary.py",

    # --------------------------------------------------------
    # AP payment allocations
    # --------------------------------------------------------
    "scripts/12_ap_payment_job_allocation.py",

    # --------------------------------------------------------
    # AR receipt allocations
    # --------------------------------------------------------
    "scripts/13_ar_receipt_job_allocation.py",

    # --------------------------------------------------------
    # Labor allocations
    # --------------------------------------------------------
    "scripts/14_labor_job_allocation.py",

    # --------------------------------------------------------
    # JSON builders
    # --------------------------------------------------------
    "scripts/json/01_build_financials_gl.py",
    "scripts/json/02_build_financials_jobs.py",
    "scripts/json/03_build_ap_invoices.py",
    "scripts/json/04_build_ar_invoices.py",
    "scripts/json/05_build_ap_payment_allocations.py",
    "scripts/json/06_build_ar_receipt_allocations.py",
    "scripts/json/07_build_labor_job_allocation.py",

    # --------------------------------------------------------
    # Health / observability
    # --------------------------------------------------------
    "scripts/99_write_pipeline_health.py",
]

def run_step(path):
    print(f"\n=== Running {path} ===")
    subprocess.run([sys.executable, path], check=True)
    print(f"=== Finished {path} ===")

def main():
    for step in STEPS:
        run_step(step)
    print("\nALL PIPELINE STEPS COMPLETED SUCCESSFULLY")

if __name__ == "__main__":
    main()
