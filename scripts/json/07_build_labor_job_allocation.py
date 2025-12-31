import json
import math
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

CSV = Path("data/labor_job_allocation.csv")
OUT_JSON = Path("public/data/labor_job_allocation.json")

def sanitize(obj):
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return round(obj, 2)
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize(v) for v in obj]
    return obj

def main():
    print("Building labor_job_allocation.json ...")

    df = pd.read_csv(CSV, low_memory=False)
    df = df.where(pd.notnull(df), None)

    payload = {
        "meta": {
            "row_count": len(df),
            "generated_at": datetime.now(timezone.utc).isoformat()
        },
        "data": df.to_dict(orient="records")
    }

    payload = sanitize(payload)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    print(f"Wrote {OUT_JSON}")

if __name__ == "__main__":
    main()
