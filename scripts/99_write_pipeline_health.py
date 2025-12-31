import json
import os
from datetime import datetime, timezone
import pandas as pd

DATA_FILES = [
    "data/accounts.csv",
    "data/gl_history_raw.csv",
    "data/gl_history.csv",
    "data/job_budgets.csv",
    "data/job_actuals.csv",
    "data/ar_receipt_job_allocation.csv",
    "data/labor_job_allocation.csv",
]

JSON_FILES = [
    "public/data/financials_gl.json",
    "public/data/financials_jobs.json",
    "public/data/ap_invoices.json",
    "public/data/ar_invoices.json",
    "public/data/ap_payment_allocations.json",
    "public/data/ar_receipt_allocations.json",
]

OUTFILE = "public/data/pipeline_health.json"

def count_csv(path):
    try:
        return int(pd.read_csv(path).shape[0])
    except Exception:
        return None

def count_json(path):
    try:
        with open(path, "r") as f:
            data = json.load(f)
        if isinstance(data, list):
            return len(data)
        if isinstance(data, dict):
            return sum(len(v) for v in data.values() if isinstance(v, list))
        return None
    except Exception:
        return None

def main():
    now_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    csv_counts = {p: count_csv(p) for p in DATA_FILES if os.path.exists(p)}
    json_counts = {p: count_json(p) for p in JSON_FILES if os.path.exists(p)}

    health = {
        "status": "success",
        "last_refresh_utc": now_utc,
        "csv_row_counts": csv_counts,
        "json_record_counts": json_counts,
        "files_present": {
            "csv": list(csv_counts.keys()),
            "json": list(json_counts.keys()),
        }
    }

    os.makedirs(os.path.dirname(OUTFILE), exist_ok=True)
    with open(OUTFILE, "w") as f:
        json.dump(health, f, indent=2)

    print(f"Wrote {OUTFILE}")

if __name__ == "__main__":
    main()
