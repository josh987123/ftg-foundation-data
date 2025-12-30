import json
import math
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# -----------------------------
# Paths
# -----------------------------
GL_HISTORY_CSV = Path("data/gl_history.csv")
GL_HISTORY_ALL_CSV = Path("data/gl_history_all.csv")
OUT_JSON = Path("public/data/financials_gl.json")

def load_csv(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV: {path}")
    return pd.read_csv(path, low_memory=False)

def sanitize_for_json(obj):
    """
    Recursively replace NaN / Infinity with None so JSON is strictly valid.
    """
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
    print("Building financials_gl.json ...")

    gl_history = load_csv(GL_HISTORY_CSV)
    gl_history_all = load_csv(GL_HISTORY_ALL_CSV)

    # First-pass cleanup: pandas NaN -> Python None
    gl_history = gl_history.where(pd.notnull(gl_history), None)
    gl_history_all = gl_history_all.where(pd.notnull(gl_history_all), None)

    payload = {
        "gl_history": gl_history.to_dict(orient="records"),
        "gl_history_all": gl_history_all.to_dict(orient="records"),
        "generated_at": datetime.now(timezone.utc).isoformat()
    }

    # Final safety pass: recursively eliminate any remaining NaN/Inf
    payload = sanitize_for_json(payload)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, allow_nan=False)

    print(f"Wrote {OUT_JSON}")
    print(f"Rows: gl_history={len(gl_history)}, gl_history_all={len(gl_history_all)}")

if __name__ == "__main__":
    main()
