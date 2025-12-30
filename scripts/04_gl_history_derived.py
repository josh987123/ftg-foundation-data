import os
import pandas as pd
import numpy as np

RAW_FILE = "data/gl_history_raw.csv"
ACCTS_FILE = "data/accounts.csv"
OUTFILE = "data/gl_history_derived.csv"

def _today_pacific_date():
    # Use Pacific time for month-boundary logic
    return pd.Timestamp.now(tz="America/Los_Angeles").date()

def _normalize_id_text(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.replace(r"\.0$", "", regex=True)
         .str.strip()
         .replace({"nan": "", "None": ""})
    )

def main():
    print("Building gl_history_derived.csv from raw + accounts...")

    # ------------------------------------------------------------
    # Load raw GL
    # ------------------------------------------------------------
    df = pd.read_csv(RAW_FILE, low_memory=False)

    for col in ["Account", "Job", "FullAccountNo", "Jrnl"]:
        if col in df.columns:
            df[col] = _normalize_id_text(df[col])

    df["Debit"] = pd.to_numeric(df["Debit"], errors="coerce").fillna(0.0)
    df["Credit"] = pd.to_numeric(df["Credit"], errors="coerce").fillna(0.0)

    df["ActivityDate"] = pd.to_datetime(df["ActivityDate"], errors="coerce").dt.date
    df["MonthStart"] = pd.to_datetime(df["MonthStart"], errors="coerce").dt.date

    # Remove CLS journals
    if "Jrnl" in df.columns:
        df = df[df["Jrnl"] != "CLS"].copy()

    df["Account_Num"] = pd.to_numeric(df["Account"], errors="coerce").astype("Int64")
    df["NetAmount"] = df["Debit"] - df["Credit"]

    # ------------------------------------------------------------
    # Join Accounts descriptions
    # ------------------------------------------------------------
    ac = pd.read_csv(ACCTS_FILE, low_memory=False)

    if "Account_Key" not in ac.columns:
        raise RuntimeError("accounts.csv missing Account_Key")

    desc_col = None
    for c in ["description", "Account_Description", "account_description"]:
        if c in ac.columns:
            desc_col = c
            break
    if desc_col is None:
        raise RuntimeError("accounts.csv missing account description column")

    ac["Account_Key"] = _normalize_id_text(ac["Account_Key"])
    ac = ac[["Account_Key", desc_col]].rename(columns={desc_col: "Account_Description"})

    df["Account_Key"] = _normalize_id_text(df["Account"]).str.zfill(4)
    df = df.merge(ac, how="left", on="Account_Key")

    # ------------------------------------------------------------
    # Monthly aggregation
    # ------------------------------------------------------------
    df = df[["Account", "Account_Num", "Account_Description", "MonthStart", "NetAmount"]].copy()
    df = df[df["MonthStart"].notna() & df["Account_Num"].notna()]

    monthly = (
        df.groupby(
            ["Account", "Account_Num", "Account_Description", "MonthStart"],
            as_index=False
        )
        .agg(MonthlyAmount=("NetAmount", "sum"))
        .sort_values(["Account_Num", "MonthStart"])
        .reset_index(drop=True)
    )

    monthly["CumToDate"] = (
        monthly.groupby(
            ["Account", "Account_Num", "Account_Description"]
        )["MonthlyAmount"].cumsum()
    )

    # ------------------------------------------------------------
    # Date boundaries
    # ------------------------------------------------------------
    current_date = _today_pacific_date()

    start_last_complete = (
        pd.Timestamp(current_date).replace(day=1) - pd.DateOffset(months=1)
    ).date()

    start_previous = (
        pd.Timestamp(start_last_complete).replace(day=1) - pd.DateOffset(months=1)
    ).date()

    start_second_previous = (
        pd.Timestamp(start_previous).replace(day=1) - pd.DateOffset(months=1)
    ).date()

    start_current_year = pd.Timestamp(start_last_complete.year, 1, 1).date()

    # ------------------------------------------------------------
    # Final per-account rollup
    # ------------------------------------------------------------
    def last_value(series, mask):
        s = series[mask].dropna()
        return s.iloc[-1] if len(s) else np.nan

    rows = []

    for (acct, acct_num, acct_desc), g in monthly.groupby(
        ["Account", "Account_Num", "Account_Description"], sort=False
    ):
        g = g.sort_values("MonthStart")

        ms = g["MonthStart"]
        cum = g["CumToDate"]
        amt = g["MonthlyAmount"]

        is_income = 4000 <= int(acct_num) <= 8020

        rows.append({
            "Account": acct,
            "Account_Num": int(acct_num),
            "Account_Description": acct_desc,
            "CumToLastComplete": last_value(cum, ms <= start_last_complete),
            "CumToPrior": last_value(cum, ms <= start_previous),
            "CumToSecondPrior": last_value(cum, ms <= start_second_previous),
            "NetIncomeYTD_Prior": last_value(
                cum,
                is_income & (ms >= start_current_year) & (ms <= start_previous)
            ) if is_income else np.nan,
            "NetIncomeYTD_LastComplete": last_value(
                cum,
                is_income & (ms >= start_current_year) & (ms <= start_last_complete)
            ) if is_income else np.nan,
            "LastCompleteMonthActivity": last_value(amt, ms == start_last_complete),
            "PriorMonthActivity": last_value(amt, ms == start_previous),
        })

    final = pd.DataFrame(rows)

    # ------------------------------------------------------------
    # FINAL ROUNDING (critical for reporting parity)
    # ------------------------------------------------------------
    NUMERIC_COLS = [
        "CumToLastComplete",
        "CumToPrior",
        "CumToSecondPrior",
        "NetIncomeYTD_Prior",
        "NetIncomeYTD_LastComplete",
        "LastCompleteMonthActivity",
        "PriorMonthActivity",
    ]

    for col in NUMERIC_COLS:
        final[col] = pd.to_numeric(final[col], errors="coerce").round(2)

    final = final.sort_values("Account_Num").reset_index(drop=True)

    final.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(final)} rows, {len(final.columns)} columns)")

if __name__ == "__main__":
    main()
