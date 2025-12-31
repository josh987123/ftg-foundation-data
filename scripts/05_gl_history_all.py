import pandas as pd
import numpy as np

RAW_GL = "data/gl_history_raw.csv"
ACCTS  = "data/accounts.csv"
OUTFILE = "data/gl_history_all.csv"


# ------------------------------------------------------------
# Utilities
# ------------------------------------------------------------
def normalize_text(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.replace(r"\.0$", "", regex=True)
         .str.strip()
         .replace({"nan": "", "None": ""})
    )


def require_columns(df: pd.DataFrame, cols: list[str], context: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"[FATAL] Missing required columns in {context}: {missing}"
        )


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    print("Building gl_history_all.csv (PARITY LOCKED) ...")

    # ------------------------------------------------------------
    # 1. Load RAW CSV (already SQL-materialized)
    # ------------------------------------------------------------
    df = pd.read_csv(RAW_GL, low_memory=False)

    # These MUST exist if upstream SQL was correct
    require_columns(
        df,
        [
            "Account",
            "Debit",
            "Credit",
            "Jrnl",
            "ActivityDate",
            "MonthStart",
        ],
        "gl_history_raw.csv"
    )

    # ------------------------------------------------------------
    # 2. HARD FILTER — CLS journals
    # ------------------------------------------------------------
    df["Jrnl"] = normalize_text(df["Jrnl"])

    pre_rows = len(df)
    df = df[df["Jrnl"] != "CLS"].copy()
    post_rows = len(df)

    print(f"Filtered CLS journals: {pre_rows - post_rows} rows removed")

    # ------------------------------------------------------------
    # 3. Normalize numeric + date fields
    # ------------------------------------------------------------
    df["Account"] = normalize_text(df["Account"])
    df["Debit"]   = pd.to_numeric(df["Debit"], errors="coerce").fillna(0.0)
    df["Credit"]  = pd.to_numeric(df["Credit"], errors="coerce").fillna(0.0)

    df["ActivityDate"] = pd.to_datetime(df["ActivityDate"], errors="coerce").dt.date
    df["MonthStart"]   = pd.to_datetime(df["MonthStart"], errors="coerce").dt.date

    if df["MonthStart"].isna().any():
        raise ValueError("[FATAL] Null MonthStart values detected")

    # ------------------------------------------------------------
    # 4. Account_Num + NetAmount
    # ------------------------------------------------------------
    df["Account_Num"] = pd.to_numeric(df["Account"], errors="coerce").astype("Int64")
    df["NetAmount"]   = df["Debit"] - df["Credit"]

    # ------------------------------------------------------------
    # 5. Join Accounts table (Power Query parity)
    # ------------------------------------------------------------
    ac = pd.read_csv(ACCTS, low_memory=False)

    require_columns(ac, ["account_no", "description"], "accounts.csv")

    if "Account_Key" in ac.columns:
        ac = ac.drop(columns=["Account_Key"])

    ac["account_no"] = normalize_text(ac["account_no"])
    ac = ac.rename(
        columns={
            "account_no": "Account",
            "description": "Account_Description",
        }
    )[["Account", "Account_Description"]]

    df = df.merge(ac, how="left", on="Account")

    # ------------------------------------------------------------
    # 6. MonthText (yyyy-MM)
    # ------------------------------------------------------------
    df["MonthText"] = pd.to_datetime(df["MonthStart"]).dt.strftime("%Y-%m")

    # ------------------------------------------------------------
    # 7. Group by Account + Month
    # ------------------------------------------------------------
    grouped = (
        df.groupby(
            ["Account", "Account_Num", "Account_Description", "MonthText"],
            as_index=False,
            dropna=False
        )
        .agg(MonthlyAmount=("NetAmount", "sum"))
    )

    # ------------------------------------------------------------
    # 8. Pivot months to columns
    # ------------------------------------------------------------
    pivot = grouped.pivot_table(
        index=["Account", "Account_Num", "Account_Description"],
        columns="MonthText",
        values="MonthlyAmount",
        aggfunc="sum",
        fill_value=0.0
    ).reset_index()

    pivot.columns.name = None

    # ------------------------------------------------------------
    # 9. Sort final result
    # ------------------------------------------------------------
    pivot = pivot.sort_values("Account_Num").reset_index(drop=True)

    # ------------------------------------------------------------
    # 10. Final rounding
    # ------------------------------------------------------------
    month_cols = [
        c for c in pivot.columns
        if c not in ["Account", "Account_Num", "Account_Description"]
    ]

    pivot[month_cols] = pivot[month_cols].apply(
        lambda s: pd.to_numeric(s, errors="coerce").round(2)
    )

    # ------------------------------------------------------------
    # 11. Write output
    # ------------------------------------------------------------
    pivot.to_csv(OUTFILE, index=False)

    print(
        f"Wrote {OUTFILE} "
        f"({len(pivot)} rows × {len(pivot.columns)} columns)"
    )


if __name__ == "__main__":
    main()
