import os
import pandas as pd
import numpy as np

RAW_FILE = "data/gl_history_raw.csv"
ACCTS_FILE = "data/accounts.csv"
OUTFILE = "data/gl_history_derived.csv"

def _today_pacific_date():
    # Use Pacific time for month-boundary logic to match your intent.
    # (Runner default is UTC; this avoids month-boundary weirdness.)
    return pd.Timestamp.now(tz="America/Los_Angeles").date()

def _normalize_id_text(s: pd.Series) -> pd.Series:
    # Match Power Query "type text" feel:
    # - convert to string
    # - strip trailing ".0"
    # - trim whitespace
    return (
        s.astype(str)
         .str.replace(r"\.0$", "", regex=True)
         .str.strip()
         .replace({"nan": "", "None": ""})
    )

def main():
    print("Building gl_history_derived.csv from raw + accounts...")

    # ---------------------------
    # Load raw GL
    # ---------------------------
    df = pd.read_csv(RAW_FILE, low_memory=False)

    # Normalize key text fields
    for col in ["Account", "Job", "FullAccountNo", "Jrnl"]:
        if col in df.columns:
            df[col] = _normalize_id_text(df[col])

    # Debit/Credit numeric
    df["Debit"] = pd.to_numeric(df["Debit"], errors="coerce").fillna(0.0)
    df["Credit"] = pd.to_numeric(df["Credit"], errors="coerce").fillna(0.0)

    # Dates
    df["ActivityDate"] = pd.to_datetime(df["ActivityDate"], errors="coerce").dt.date
    df["MonthStart"] = pd.to_datetime(df["MonthStart"], errors="coerce").dt.date

    # Remove CLS journals
    if "Jrnl" in df.columns:
        df = df[df["Jrnl"] != "CLS"].copy()

    # Account_Num (Power Query: Number.FromText([Account]) as Int64)
    df["Account_Num"] = pd.to_numeric(df["Account"], errors="coerce").astype("Int64")

    # NetAmount = Debit - Credit
    df["NetAmount"] = df["Debit"] - df["Credit"]

    # ---------------------------
    # Join Accounts descriptions
    # ---------------------------
    ac = pd.read_csv(ACCTS_FILE, low_memory=False)

    # Expecting columns from your Accounts export:
    # - Account_Key
    # - description (account description)
    if "Account_Key" not in ac.columns:
        raise RuntimeError("accounts.csv is missing Account_Key. Ensure scripts/02_accounts.py includes it.")

    # Pick the best description column name present
    desc_col = None
    for candidate in ["description", "acct_description", "Account_Description", "account_description"]:
        if candidate in ac.columns:
            desc_col = candidate
            break
    if desc_col is None:
        raise RuntimeError("accounts.csv has no description column. Ensure the accounts export includes the description field.")

    ac["Account_Key"] = _normalize_id_text(ac["Account_Key"])
    ac = ac[["Account_Key", desc_col]].copy()
    ac = ac.rename(columns={desc_col: "Account_Description"})

    # Add Account_Key to GL rows (PadStart to 4)
    df["Account_Key"] = _normalize_id_text(df["Account"]).str.zfill(4)

    df = df.merge(ac, how="left", on="Account_Key")

    # ---------------------------
    # Keep only columns needed for monthly model
    # ---------------------------
    keep = ["Account", "Account_Num", "Account_Description", "MonthStart", "NetAmount"]
    for k in keep:
        if k not in df.columns:
            raise RuntimeError(f"Missing expected column in GL raw pipeline: {k}")
    df = df[keep].copy()

    # Drop rows with null MonthStart or Account_Num (rare, but prevents weird grouping)
    df = df[df["MonthStart"].notna()].copy()
    df = df[df["Account_Num"].notna()].copy()

    # ---------------------------
    # Monthly Summary
    # Group by Account / Account_Num / Account_Description / MonthStart
    # Sum NetAmount -> MonthlyAmount
    # ---------------------------
    monthly = (
        df.groupby(["Account", "Account_Num", "Account_Description", "MonthStart"], dropna=False, as_index=False)
          .agg(MonthlyAmount=("NetAmount", "sum"))
    )

    # Sort like your Power Query
    monthly = monthly.sort_values(["Account_Num", "MonthStart"], ascending=[True, True]).reset_index(drop=True)

    # ---------------------------
    # Cumulative (CumToDate per account)
    # ---------------------------
    monthly["CumToDate"] = (
        monthly.groupby(["Account", "Account_Num", "Account_Description"], dropna=False)["MonthlyAmount"]
               .cumsum()
    )

    # ---------------------------
    # Date boundaries (match your PQ logic)
    # ---------------------------
    current_date = _today_pacific_date()

    start_last_complete = pd.Timestamp(current_date).replace(day=1) - pd.DateOffset(months=1)
    start_last_complete = start_last_complete.date()  # start of last complete month

    start_previous = (pd.Timestamp(start_last_complete).replace(day=1) - pd.DateOffset(months=1)).date()
    start_second_previous = (pd.Timestamp(start_previous).replace(day=1) - pd.DateOffset(months=1)).date()

    start_current_year = pd.Timestamp(start_last_complete.year, 1, 1).date()

    # ---------------------------
    # Final aggregation: one row per account
    # Emulates List.Last(List.RemoveNulls(...)) behavior by picking the last eligible value.
    # ---------------------------
    def last_value_where(series_monthstart, series_value, predicate_mask):
        # Return last value among rows where predicate_mask is True and value not null
        sub = series_value[predicate_mask]
        sub = sub.dropna()
        if len(sub) == 0:
            return np.nan
        return sub.iloc[-1]

    rows = []
    grouped = monthly.groupby(["Account", "Account_Num", "Account_Description"], dropna=False, sort=False)

    for (acct, acct_num, acct_desc), g in grouped:
        g = g.sort_values("MonthStart").reset_index(drop=True)

        ms = g["MonthStart"]
        cum = g["CumToDate"]
        mamt = g["MonthlyAmount"]

        # Cum cutoffs
        cum_last = last_value_where(ms, cum, ms <= start_last_complete)
        cum_prior = last_value_where(ms, cum, ms <= start_previous)
        cum_second = last_value_where(ms, cum, ms <= start_second_previous)

        # Net Income YTD masks (accounts 4000-8020) and date range
        is_income = (pd.notna(acct_num) and int(acct_num) >= 4000 and int(acct_num) <= 8020)
        if is_income:
            mask_prior = (ms >= start_current_year) & (ms <= start_previous)
            mask_last = (ms >= start_current_year) & (ms <= start_last_complete)
            netinc_prior = last_value_where(ms, cum, mask_prior)
            netinc_last = last_value_where(ms, cum, mask_last)
        else:
            netinc_prior = np.nan
            netinc_last = np.nan

        # Monthly activities
        last_act = last_value_where(ms, mamt, ms == start_last_complete)
        prior_act = last_value_where(ms, mamt, ms == start_previous)

        rows.append({
            "Account": acct,
            "Account_Num": int(acct_num) if pd.notna(acct_num) else np.nan,
            "Account_Description": acct_desc,
            "CumToLastComplete": cum_last,
            "CumToPrior": cum_prior,
            "CumToSecondPrior": cum_second,
            "NetIncomeYTD_Prior": netinc_prior,
            "NetIncomeYTD_LastComplete": netinc_last,
            "LastCompleteMonthActivity": last_act,
            "PriorMonthActivity": prior_act,
        })

    final = pd.DataFrame(rows)

    # Sort final output similarly (by Account_Num)
    final = final.sort_values(["Account_Num"], ascending=True).reset_index(drop=True)

    # ------------------------------------------------------------
# Final rounding to match Power Query CSV output
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
    if col in final.columns:
        final[col] = pd.to_numeric(final[col], errors="coerce").round(2)


    final.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(final)} rows, {len(final.columns)} columns)")

if __name__ == "__main__":
    main()
