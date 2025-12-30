import subprocess
import sys

# Ordered list of pipeline steps
STEPS = [
    # Canonical CSVs
    "scripts/01_test_connection.py",
    "scripts/02_accounts.py",
    "scripts/03_gl_history_raw.py",
    "scripts/04_gl_history_derived.py",
    "scripts/05_gl_history_all.py",

    # JSON builders
    "scripts/json/01_build_financials_gl.py",
    # Future:
    # "scripts/json/02_build_financials_jobs.py",
    # "scripts/json/03_build_ap_invoices.py",
    # ...
]

def run_step(path):
    print(f"\n=== Running {path} ===")
    result = subprocess.run(
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
