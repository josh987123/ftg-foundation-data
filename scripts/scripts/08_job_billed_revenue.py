import os
import pyodbc
import pandas as pd

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
SERVER = "sql.foundationsoft.com,9000"
DATABASE = "Cas_5587"
OUTFILE = "data/job_billed_revenue.csv"

def connect():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={os.environ['FOUNDATION_SQL_USER']};"
        f"PWD={os.environ['FOUNDATION_SQL_PASSWORD']};",
        timeout=30
    )

def normalize_text(series):
    return (
        series.astype(str)
              .str.replace(r"\.0$", "", regex=True)
              .str.strip()
              .replace({"nan": "", "None": ""})
    )

def main():
    print("Exporting job_billed_revenue.csv ...")
    conn = connect()

    # ------------------------------------------------------------
    # 1. GL HISTORY – REVENUE ONLY
    # ------------------------------------------------------------
    gl_sql = """
    SELECT
        job_no,
        basic_account_no,
        amount_db,
        amount_cr
    FROM dbo.gl_history
    WHERE journal_no <> 'CLS'
    """
    gl = pd.read_sql(gl_sql, conn)

    gl["job_no"] = normalize_text(gl["job_no"])
    gl["basic_account_no"] = normalize_text(gl["basic_account_no"])
    gl["amount_db"] = pd.to_numeric(gl["amount_db"], errors="coerce").fillna(0.0)
    gl["amount_cr"] = pd.to_numeric(gl["amount_cr"], errors="coerce").fillna(0.0)

    # ------------------------------------------------------------
    # 2. ACCOUNT NUMBER AS INTEGER
    # ------------------------------------------------------------
    gl["Account_Num"] = pd.to_numeric(gl["basic_account_no"], errors="coerce")

    # ------------------------------------------------------------
    # 3. FILTER TO 4000–4999 REVENUE ACCOUNTS
    # ------------------------------------------------------------
    gl = gl[(gl["Account_Num"] >= 4000) & (gl["Account_Num"] < 5000)]

    # ------------------------------------------------------------
    # 4. NET AMOUNT + FLIP SIGN
    # ------------------------------------------------------------
    gl["Billed_Revenue"] = -1 * (gl["amount_db"] - gl["amount_cr"])

    # ------------------------------------------------------------
    # 5. GROUP BY JOB
    # ------------------------------------------------------------
    grouped = (
        gl.groupby("job_no", as_index=False)
          .agg(Billed_Revenue=("Billed_Revenue", "sum"))
    )

    # ------------------------------------------------------------
    # 6. JOBS TABLE (DESCRIPTION)
    # ------------------------------------------------------------
    jobs = pd.read_sql(
        "SELECT job_no, description FROM dbo.jobs",
        conn
    )
    jobs["job_no"] = normalize_text(jobs["job_no"])
    jobs["Job_Description"] = normalize_text(jobs["description"])
    jobs = jobs[["job_no", "Job_Description"]]

    # ------------------------------------------------------------
    # 7. MERGE JOB DESCRIPTIONS
    # ------------------------------------------------------------
    final = grouped.merge(jobs, how="left", on="job_no")

    # ------------------------------------------------------------
    # 8. FINAL SHAPE
    # ------------------------------------------------------------
    final = final.rename(columns={"job_no": "Job_No"})
    final["Billed_Revenue"] = final["Billed_Revenue"].round(2)

    final = final[["Job_No", "Job_Description", "Billed_Revenue"]]
    final = final.sort_values("Job_No").reset_index(drop=True)

    final.to_csv(OUTFILE, index=False)
    print(f"Wrote {OUTFILE} ({len(final)} rows, {len(final.columns)} columns)")

if __name__ == "__main__":
    main()
