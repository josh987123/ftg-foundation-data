import json
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# ------------------------------------------------------------
# Inputs (canonical CSVs)
# ------------------------------------------------------------
JOB_BUDGETS = Path("data/job_budgets.csv")
JOB_ACTUALS = Path("data/job_actuals.csv")
JOB_BILLED_REV = Path("data/job_billed_revenue.csv")

# ------------------------------------------------------------
# Output (website-facing JSON)
# ------------------------------------------------------------
OUT_JSON = Path("public/data/financials_jobs.json")

def load_csv(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV: {path}")
    return pd.read_csv(path, low_memory=False)

def main():
    print("Building financials_jobs.json ...")

    job_budgets = load_csv(JOB_BUDGETS)
    job_actuals = load_csv(JOB_ACTUALS)
    job_billed_revenue = load_csv(JOB_BILLED_REV)

    # 🔑 CRITICAL FIX: convert NaN → None so JSON is valid
    job_budgets = job_budgets.where(pd.notnull(job_budgets), None)
    job_actuals = job_actuals.where(pd.notnull(job_actuals), None)
    job_billed_revenue = job_billed_revenue.where(pd.notnull(job_billed_revenue), None)

    payload = {
        "job_budgets": job_budgets.to_dict(orient="records"),
        "job_actuals": job_actuals.to_dict(orient="records"),
        "job_billed_revenue": job_billed_revenue.to_dict(orient="records"),
        "generated_at": datetime.now(timezone.utc).isoformat()
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, allow_nan=False)

    print(f"Wrote {OUT_JSON}")
    print(
        "Rows:",
        f"job_budgets={len(job_budgets)},",
        f"job_actuals={len(job_actuals)},",
        f"job_billed_revenue={len(job_billed_revenue)}"
    )

if __name__ == "__main__":
    main()
