import json
import math
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

CSV = Path("data/ap_invoice_summary.csv")
OUT_JSON = Path("public/data/ap_invoices.json")

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
    print("Building ap_invoices.json ...")

    df = pd.read_csv(CSV, low_memory=False)
    df = df.where(pd.notnull(df), None)

    payload = {
        "invoices": df.to_dict(orient="records"),
        "row_count": len(df),
        "generated_at": datetime.now(timezone.utc).isoformat()
    }

    payload = sanitize_for_json(payload)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, allow_nan=False)

    print(f"Wrote {OUT_JSON}")

if __name__ == "__main__":
    main()
