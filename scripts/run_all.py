import subprocess
import sys

# ============================================================
# Ordered list of pipeline steps
# IMPORTANT:
# - Runs strictly sequentially
# - Each script must fully finish before the next starts
# - Add new datasets by appending ONE line here
# ============================================================

STEPS = [
    # --------------------------------------------------------
    # Connectivity check
    # --------------------------------------------------------
    "scripts/01_test_connection.py",

    # --------------------------------------------------------
    # Dimension / reference tables
    # --------------------------------------------------------
    "scripts/02_accounts.py",

    # --------------------------------------------------------
    # GL pipeline (locked)
    # --------------------------------------------------------
    "scripts/03_gl_history_raw.py",
    "scripts/04_gl_history_derived.py",
    "scripts/05_gl_history_all.py",

    # --------------------------------------------------------
    # Jobs CSVs (Layer 1: SQL -> CSV)
    # --------------------------------------------------------
    "scripts/06_job_budgets.py",

    # --------------------------------------------------------
    # JSON builders (website-facing)
    # --------------------------------------------------------
    "scripts/json/01_build_financials_gl.py",

    # --------------------------------------------------------
    # Future steps (add when CSVs exist)
    # --------------------------------------------------------
    # "scripts/07_job_actuals.py",
    # "scripts/08_job_billed_revenue.py",
    # "scripts/json/02_build_financials_jobs.py",
    # "scripts/json/03_build_ap_invoices.py",
    # "scripts/json/04_build_ar_invoices.py",
]

def run_step(path):
    print(f"\n=== Running {path} ===")
    subprocess.run(
        [sys.executable, path],
        check=True
    )
    print(f"=== Finished {path} ===")

def main():
    for step in STEPS:
        run_step(step)

    print("\nALL PIPELINE STEPS COMPLETED SUCCESSFULLY")

if __name__ == "__main__":
    main()
