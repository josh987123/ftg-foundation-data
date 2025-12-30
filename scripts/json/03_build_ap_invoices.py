import json
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# ------------------------------------------------------------
# Input CSV (already migrated)
# ------------------------------------------------------------
AP_INVOICE_CSV = Path("data/ap_invoice_summary.csv")

# ------------------------------------------------------------
# Output JSON (website-facing)
# ------------------------------------------------------------
OUT_JSON = Path("public/data/ap_invoices.json")

def main():
    print("Building ap_invoices.json ...")

    if not AP_INVOICE_CSV.exists():
        raise FileNotFoundError(f"Missing required CSV: {AP_INVOICE_CSV}")

    df = pd.read_csv(AP_INVOICE_CSV, low_memory=False)

    payload = {
        "invoices": df.to_dict(orient="records"),
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
