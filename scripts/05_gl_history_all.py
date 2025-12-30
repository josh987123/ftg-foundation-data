import os
import pandas as pd
import numpy as np

RAW_GL = "data/gl_history_raw.csv"
ACCTS = "data/accounts.csv"
OUTFILE = "data/gl_history_all.csv"

def normalize_text(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.replace(r"\.0$", "", regex=True)
         .str.strip()
         .replace({"nan": "", "None": ""})
    )

def main():
    print("Building gl_history_all.csv ...")

    # ------------------------------------------------------------
    # 1. Load raw GL (already excludes CLS in raw, but we re-check)
    # ------------------------------------------------------------
    df = pd.read_csv(RAW_GL, low_memory=False)

    df["Account"] = normalize_text(df["Account"])
    df["Debit"] = pd.to_numeric(df["Debit"], errors="coerce").fillna(0.0)
    df["Credit"] = pd.to_numeric(df["Credit"], errors="coerce").fillna(0.0)

    # Ensure dates
    df["ActivityDate"] = pd.to_datetime(df["ActivityDate"], errors="coerce").dt.date
    df["MonthStart"] = pd.to_datetime(df["MonthStart"], errors="coerce").dt.date

    # Safety: remove CLS if present
    if "Jrnl" in df.columns:
        df = df[df["Jrnl"] != "CLS"].copy()

    # ------------------------------------------------------------
    # 2. Account_Num + NetAmount
    # ------------------------------------------------------------
    df["Account_Num"] = pd.to_numeric(df["Account"], errors="coerce").astype("Int64")
    df["NetAmount"] = df["Debit"] - df["Credit"]

    # ------------------------------------------------------------
    # 3. Join to Accounts table
    # ------------------------------------------------------------
    ac = pd.read_csv(ACCTS, low_memory=False)

    # Remove Account_Key if present (matches Power Query)
    if "Account_Key" in ac.columns:
        ac = ac.drop(columns=["Account_Key"])

    ac["account_no"] = normalize_text(ac["account_no"])
    ac = ac[["account_no", "description"]].rename(
        columns={"account_no": "Account", "description": "Account_Description"}
    )

    df = df.merge(ac, how="left", on="Account")

    # ------------------------------------------------------------
    # 4. MonthText (yyyy-MM)
    # ------------------------------------------------------------
    df["MonthText"] = pd.to_datetime(df["MonthStart"]).dt.strftime("%Y-%m")

    # ------------------------------------------------------------
    # 5. Group by Account + Month
    # ------------------------------------------------------------
    grouped = (
        df.groupby(
            ["Account", "Account_Num", "Account_Description", "MonthText"],
            as_index=False
        )
        .agg(MonthlyAmount=("NetAmount", "sum"))
    )

    # ------------------------------------------------------------
    # 6. Pivot months to columns
    # ------------------------------------------------------------
    pivot = grouped.pivot_table(
        index=["Account", "Account_Num", "Account_Description"],
        columns="MonthText",
        values="MonthlyAmount",
        aggfunc="sum"
    ).reset_index()

    # Flatten column index
    pivot.columns.name = None

    # ------------------------------------------------------------
    # 7. Sort final result
    # ------------------------------------------------------------
    pivot = pivot.sort_values("Account_Num").reset_index(drop=True)

    # ------------------------------------------------------------
    # 8. Final rounding (parity with Power Query CSV)
    # ------------------------------------------------------------
    month_cols = [c for c in pivot.columns if c not in ["Account", "Account_Num", "Account_Description"]]
    for c in month_cols:
        pivot[c] = pd.to_numeric(pivot[c], errors="coerce").round(2)

    pivot.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(pivot)} rows, {len(pivot.columns)} columns)")

if __name__ == "__main__":
    main()
