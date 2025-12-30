import json
import math
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

JOB_BUDGETS = Path("data/job_budgets.csv")
JOB_ACTUALS = Path("data/job_actuals.csv")
JOB_BILLED_REV = Path("data/job_billed_revenue.csv")
OUT_JSON = Path("public/data/financials_jobs.json")

def load_csv(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV: {path}")
    return pd.read_csv(path, low_memory=False)

def sanitize_for_json(obj):
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    return obj

def main():
    print("Building financials_jobs.json ...")

    budgets = load_csv(JOB_BUDGETS).where(pd.notnull(load_csv(JOB_BUDGETS)), None)
    actuals = load_csv(JOB_ACTUALS).where(pd.notnull(load_csv(JOB_ACTUALS)), None)
    billed = load_csv(JOB_BILLED_REV).where(pd.notnull(load_csv(JOB_BILLED_REV)), None)

    payload = {
        "job_budgets": budgets.to_dict(orient="records"),
        "job_actuals": actuals.to_dict(orient="records"),
        "job_billed_revenue": billed.to_dict(orient="records"),
        "generated_at": datetime.now(timezone.utc).isoformat()
    }

    payload = sanitize_for_json(payload)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, allow_nan=False)

    print(f"Wrote {OUT_JSON}")

if __name__ == "__main__":
    main()
