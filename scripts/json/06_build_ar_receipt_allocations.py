import json
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# ------------------------------------------------------------
# Input CSV (already migrated)
# ------------------------------------------------------------
AR_ALLOC_CSV = Path("data/ar_receipt_job_allocation.csv")

# ------------------------------------------------------------
# Output JSON (website-facing)
# ------------------------------------------------------------
OUT_JSON = Path("public/data/ar_receipt_job_allocation.json")

def main():
    print("Building ar_receipt_job_allocation.json ...")

    if not AR_ALLOC_CSV.exists():
        raise FileNotFoundError(f"Missing required CSV: {AR_ALLOC_CSV}")

    df = pd.read_csv(AR_ALLOC_CSV, low_memory=False)

    # 🔑 CRITICAL FIX: convert NaN → None so JSON is valid
    df = df.where(pd.notnull(df), None)

    payload = {
        "allocations": df.to_dict(orient="records"),
        "row_count": len(df),
        "generated_at": datetime.now(timezone.utc).isoformat()
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    print(f"Wrote {OUT_JSON}")
    print(f"Rows: {len(df)}")

if __name__ == "__main__":
    main()
